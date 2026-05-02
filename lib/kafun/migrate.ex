defmodule Kafun.Migrate do
  @moduledoc """
  Pull data from any S3-compatible source (typically SeaweedFS) into a kafun
  destination. Same code works against any S3 endpoint — both src and dst
  are described by `Kafun.Migrate.client/3` with their own credentials.

  Idempotent: every per-object copy first issues `HEAD` against the
  destination and skips if `(size, etag)` already match. So a partial run
  can be re-invoked and only missing/changed objects move.

  Object-size limit: this migrator does single-shot PUTs (object body
  buffered in the BEAM). Above ~4 GiB you'd want multipart-aware copy;
  for the homelab seaweed → kafun case, that ceiling never matters. Objects
  larger than `:max_size` (default 4 GiB) are skipped with a warning.
  """

  require Logger
  alias Kafun.Migrate.SigV4

  defstruct [:endpoint, :access_key, :secret_key, :region]

  @type t :: %__MODULE__{
          endpoint: String.t(),
          access_key: String.t(),
          secret_key: String.t(),
          region: String.t()
        }

  @default_max_size 4 * 1024 * 1024 * 1024
  @list_page 1000

  @spec client(String.t(), String.t(), String.t() | nil, keyword()) :: t()
  def client(endpoint, access_key, secret_key, opts \\ []) do
    %__MODULE__{
      endpoint: String.trim_trailing(endpoint, "/"),
      access_key: access_key,
      secret_key: secret_key || "",
      region: Keyword.get(opts, :region, "us-east-1")
    }
  end

  ## Public S3 surface

  @spec list_buckets(t()) :: [String.t()]
  def list_buckets(c) do
    case signed(c, :get, "/", [], "", {:hash, ""}) do
      {:ok, %{status: 200, body: body}} -> parse_bucket_names(body)
      other -> raise "list_buckets failed: #{inspect(other)}"
    end
  end

  @doc """
  ListObjectsV2 (paginated). Returns a `Stream` of keys; the page-fetch is
  lazy so we don't pull the whole bucket into memory upfront.
  """
  @spec stream_keys(t(), String.t()) :: Enumerable.t()
  def stream_keys(c, bucket) do
    Stream.resource(
      fn -> {:start, nil} end,
      fn
        :done ->
          {:halt, nil}

        {:start, token} ->
          case list_page(c, bucket, token) do
            {:ok, %{keys: keys, next: nil}} -> {keys, :done}
            {:ok, %{keys: keys, next: t}} -> {keys, {:start, t}}
            {:error, e} -> raise "list_objects: #{inspect(e)}"
          end
      end,
      fn _ -> :ok end
    )
  end

  defp list_page(c, bucket, token) do
    qs =
      [{"list-type", "2"}, {"max-keys", Integer.to_string(@list_page)}]
      |> then(fn q -> if token, do: [{"continuation-token", token} | q], else: q end)

    qstr = qs |> Enum.map_join("&", fn {k, v} -> "#{URI.encode_www_form(k)}=#{URI.encode_www_form(v)}" end)

    case signed(c, :get, "/#{bucket}", [], "?" <> qstr, {:hash, ""}) do
      {:ok, %{status: 200, body: body}} -> {:ok, parse_objects_page(body)}
      {:ok, %{status: status, body: body}} -> {:error, {:http, status, body}}
      err -> err
    end
  end

  @spec head_object(t(), String.t(), String.t()) ::
          {:ok, %{size: non_neg_integer(), etag: String.t(), content_type: String.t() | nil}}
          | :not_found
          | {:error, term()}
  def head_object(c, bucket, key) do
    case signed(c, :head, "/#{bucket}/#{encode_key(key)}", [], "", {:hash, ""}) do
      {:ok, %{status: 200, headers: hdrs}} ->
        {:ok,
         %{
           size: hdr(hdrs, "content-length") |> String.to_integer(),
           etag: hdr(hdrs, "etag") |> strip_quotes(),
           content_type: hdr(hdrs, "content-type")
         }}

      {:ok, %{status: 404}} ->
        :not_found

      {:ok, %{status: status, body: body}} ->
        {:error, {:http, status, body}}

      err ->
        err
    end
  end

  @spec get_object(t(), String.t(), String.t()) ::
          {:ok, binary(), [{String.t(), String.t()}]} | {:error, term()}
  def get_object(c, bucket, key) do
    case signed(c, :get, "/#{bucket}/#{encode_key(key)}", [], "", {:hash, ""}) do
      {:ok, %{status: 200, body: body, headers: hdrs}} -> {:ok, body, hdrs}
      {:ok, %{status: status, body: body}} -> {:error, {:http, status, body}}
      err -> err
    end
  end

  @spec put_object(t(), String.t(), String.t(), binary(), keyword()) ::
          :ok | {:error, term()}
  def put_object(c, bucket, key, body, opts \\ []) do
    headers =
      []
      |> maybe_add("content-type", Keyword.get(opts, :content_type))
      |> Enum.concat(Keyword.get(opts, :user_meta, []) |> Enum.map(fn {k, v} ->
        {"x-amz-meta-" <> to_string(k), to_string(v)}
      end))

    case signed(c, :put, "/#{bucket}/#{encode_key(key)}", headers, "", {:hash, body}, body) do
      {:ok, %{status: status}} when status in 200..204 -> :ok
      {:ok, %{status: status, body: rb}} -> {:error, {:http, status, rb}}
      err -> err
    end
  end

  @spec ensure_bucket(t(), String.t()) :: :ok
  def ensure_bucket(c, bucket) do
    case signed(c, :put, "/#{bucket}", [], "", {:hash, ""}) do
      {:ok, %{status: status}} when status in 200..204 -> :ok
      {:ok, %{status: status, body: body}} -> raise "ensure_bucket #{bucket}: HTTP #{status} #{body}"
      {:error, e} -> raise "ensure_bucket #{bucket}: #{inspect(e)}"
    end
  end

  ## Migration loop

  @doc """
  Copy every object in `bucket` from `src` to `dst`. Returns a summary map.

    * `:dst_bucket` — destination bucket name (default: same as src `bucket`).
       Set to copy across to a renamed bucket on the destination.
    * `:concurrency` (default 8) — `Task.async_stream` parallelism
    * `:dry_run` (default false) — don't write to dst, just count
    * `:verify` (default false) — after each PUT, HEAD the dst to confirm size
    * `:max_size` (default 4 GiB) — skip + warn for objects bigger than this
    * `:on_progress` — `(summary -> any)` called every 50 objects
  """
  @spec run(t(), t(), String.t(), keyword()) :: map()
  def run(src, dst, bucket, opts \\ []) do
    dst_bucket = Keyword.get(opts, :dst_bucket, bucket)
    concurrency = Keyword.get(opts, :concurrency, 8)
    dry_run? = Keyword.get(opts, :dry_run, false)
    verify? = Keyword.get(opts, :verify, false)
    max_size = Keyword.get(opts, :max_size, @default_max_size)
    on_progress = Keyword.get(opts, :on_progress, fn _ -> :ok end)

    if not dry_run?, do: ensure_bucket(dst, dst_bucket)

    started = System.monotonic_time(:second)
    initial = %{copied: 0, skipped: 0, oversize: 0, failed: 0, bytes: 0, errors: []}

    src
    |> stream_keys(bucket)
    |> Stream.with_index(1)
    |> Task.async_stream(
      fn {key, _i} -> migrate_one(src, dst, bucket, dst_bucket, key, dry_run?, verify?, max_size) end,
      max_concurrency: concurrency,
      timeout: :infinity,
      ordered: false
    )
    |> Enum.reduce(initial, fn task_result, acc ->
      acc =
        case task_result do
          {:ok, {:copied, size}} -> %{acc | copied: acc.copied + 1, bytes: acc.bytes + size}
          {:ok, :skipped} -> %{acc | skipped: acc.skipped + 1}
          {:ok, {:oversize, _key, _size}} -> %{acc | oversize: acc.oversize + 1}
          {:ok, {:error, key, reason}} -> %{acc | failed: acc.failed + 1, errors: [{key, reason} | acc.errors]}
          {:exit, reason} -> %{acc | failed: acc.failed + 1, errors: [{:task_exit, reason} | acc.errors]}
        end

      if rem(acc.copied + acc.skipped + acc.failed + acc.oversize, 50) == 0 do
        on_progress.(acc)
      end

      acc
    end)
    |> Map.put(:elapsed_sec, System.monotonic_time(:second) - started)
  end

  defp migrate_one(src, dst, src_bucket, dst_bucket, key, dry_run?, verify?, max_size) do
    with {:ok, src_meta} <- head_object(src, src_bucket, key) do
      cond do
        src_meta.size > max_size ->
          Logger.warning("skipping #{src_bucket}/#{key}: #{src_meta.size} bytes exceeds max_size")
          {:oversize, key, src_meta.size}

        already_copied?(dst, dst_bucket, key, src_meta) ->
          :skipped

        dry_run? ->
          {:copied, src_meta.size}

        true ->
          case copy_object(src, dst, src_bucket, dst_bucket, key, src_meta) do
            :ok ->
              if verify? do
                case head_object(dst, dst_bucket, key) do
                  {:ok, %{size: size}} when size == src_meta.size -> {:copied, src_meta.size}
                  other -> {:error, key, {:verify_mismatch, other}}
                end
              else
                {:copied, src_meta.size}
              end

            {:error, reason} ->
              {:error, key, reason}
          end
      end
    else
      :not_found -> {:error, key, :source_disappeared}
      {:error, reason} -> {:error, key, reason}
    end
  end

  defp already_copied?(dst, bucket, key, src_meta) do
    case head_object(dst, bucket, key) do
      {:ok, %{size: size, etag: etag}} -> size == src_meta.size and etag == src_meta.etag
      :not_found -> false
      _ -> false
    end
  end

  defp copy_object(src, dst, src_bucket, dst_bucket, key, src_meta) do
    with {:ok, body, headers} <- get_object(src, src_bucket, key) do
      ct = src_meta.content_type || hdr(headers, "content-type")
      user_meta = collect_x_amz_meta(headers)
      put_object(dst, dst_bucket, key, body, content_type: ct, user_meta: user_meta)
    end
  end

  defp collect_x_amz_meta(headers) do
    Enum.flat_map(headers, fn {k, v} ->
      lower = String.downcase(k)

      case lower do
        "x-amz-meta-" <> name when name != "" -> [{name, v}]
        _ -> []
      end
    end)
  end

  ## HTTP plumbing

  defp signed(c, method, path, headers, query_suffix, payload, body \\ "") do
    url = c.endpoint <> path <> query_suffix

    signed_headers =
      SigV4.sign(method, url, headers,
        access_key: c.access_key,
        secret_key: c.secret_key,
        region: c.region,
        service: "s3",
        payload: payload
      )

    req_opts = [
      method: method,
      url: url,
      headers: signed_headers,
      body: body,
      decode_body: false,
      retry: false,
      receive_timeout: 60_000
    ]

    case Req.request(req_opts) do
      {:ok, resp} -> {:ok, %{status: resp.status, body: resp.body, headers: flatten_headers(resp.headers)}}
      err -> err
    end
  end

  defp flatten_headers(%{} = h) do
    Enum.flat_map(h, fn {k, vs} ->
      Enum.map(List.wrap(vs), fn v -> {String.downcase(to_string(k)), to_string(v)} end)
    end)
  end

  defp flatten_headers(list) when is_list(list) do
    Enum.map(list, fn {k, v} -> {String.downcase(to_string(k)), to_string(v)} end)
  end

  defp hdr(headers, name) do
    name = String.downcase(name)

    Enum.find_value(headers, fn {k, v} -> if String.downcase(k) == name, do: v end)
  end

  defp strip_quotes(nil), do: nil
  defp strip_quotes(s), do: s |> String.trim() |> String.trim(~s|"|)

  defp maybe_add(headers, _name, nil), do: headers
  defp maybe_add(headers, name, value), do: [{name, value} | headers]

  defp encode_key(key) do
    key
    |> String.split("/")
    |> Enum.map_join("/", &URI.encode_www_form/1)
    |> String.replace("+", "%20")
  end

  ## XML parsing — Saxy SimpleForm tree walks

  defp parse_bucket_names(xml) do
    case Saxy.SimpleForm.parse_string(xml) do
      {:ok, {"ListAllMyBucketsResult", _, children}} ->
        children
        |> Enum.flat_map(fn
          {"Buckets", _, buckets} ->
            Enum.flat_map(buckets, fn
              {"Bucket", _, parts} ->
                case find_text(parts, "Name") do
                  nil -> []
                  name -> [name]
                end

              _ ->
                []
            end)

          _ ->
            []
        end)

      _ ->
        []
    end
  end

  defp parse_objects_page(xml) do
    case Saxy.SimpleForm.parse_string(xml) do
      {:ok, {"ListBucketResult", _, children}} ->
        keys =
          Enum.flat_map(children, fn
            {"Contents", _, parts} ->
              case find_text(parts, "Key") do
                nil -> []
                k -> [k]
              end

            _ ->
              []
          end)

        truncated = find_text(children, "IsTruncated") == "true"
        next = find_text(children, "NextContinuationToken")
        %{keys: keys, truncated: truncated, next: if(truncated, do: next, else: nil)}

      _ ->
        %{keys: [], truncated: false, next: nil}
    end
  end

  defp find_text(children, name) do
    Enum.find_value(children, fn
      {^name, _, parts} ->
        parts
        |> Enum.filter(&is_binary/1)
        |> IO.iodata_to_binary()
        |> String.trim()

      _ ->
        nil
    end)
  end
end
