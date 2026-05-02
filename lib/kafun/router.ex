defmodule Kafun.Router do
  @moduledoc """
  S3-compatible HTTP surface. Endpoints:

      GET    /healthz
      GET    /                          → ListAllMyBuckets
      PUT    /:bucket                   → CreateBucket
      GET    /:bucket?list-type=2&...   → ListObjectsV2
      PUT    /:bucket/<key>             → PutObject       (streamed)
      GET    /:bucket/<key>             → GetObject       (sendfile + Range)
      HEAD   /:bucket/<key>             → HeadObject
      DELETE /:bucket/<key>             → DeleteObject
  """

  use Plug.Router
  require Logger

  alias Kafun.{Auth, Index, Multipart, S3XML, Storage}

  plug Plug.Logger, log: :debug
  plug :fetch_qs
  plug :match
  plug :authenticate
  plug :dispatch

  defp fetch_qs(conn, _), do: Plug.Conn.fetch_query_params(conn)

  ## Health — never auth-gated.

  get "/healthz" do
    send_resp(conn, 200, "ok\n")
  end

  ## Service-level.

  get "/" do
    send_xml(conn, 200, S3XML.list_all_buckets(Index.list_buckets()))
  end

  ## Bucket-level.

  put "/:bucket" do
    if Storage.valid_bucket?(bucket) do
      :ok = Index.ensure_bucket(bucket)
      File.mkdir_p!(Path.join(root(), bucket))
      send_resp(conn, 200, "")
    else
      error(conn, 400, "InvalidBucketName", "bucket name is not valid")
    end
  end

  get "/:bucket" do
    if Storage.valid_bucket?(bucket) do
      list_objects(conn, bucket)
    else
      error(conn, 400, "InvalidBucketName", "bucket name is not valid")
    end
  end

  ## Object-level.

  post "/:bucket/*key_parts" do
    with_object(conn, bucket, key_parts, &dispatch_post/3)
  end

  put "/:bucket/*key_parts" do
    with_object(conn, bucket, key_parts, &dispatch_put/3)
  end

  get "/:bucket/*key_parts" do
    with_object(conn, bucket, key_parts, &do_get/3)
  end

  match "/:bucket/*key_parts", via: :head do
    with_object(conn, bucket, key_parts, &do_head/3)
  end

  delete "/:bucket/*key_parts" do
    with_object(conn, bucket, key_parts, &dispatch_delete/3)
  end

  match _ do
    error(conn, 404, "NoSuchKey", "no route")
  end

  ## Pipeline plug.

  defp authenticate(%Plug.Conn{request_path: "/healthz"} = conn, _), do: conn

  defp authenticate(conn, _) do
    if Auth.disabled?() do
      conn
    else
      case Auth.access_key(conn) do
        {:ok, key} ->
          if Auth.allowed?(key) do
            conn
          else
            error(conn, 403, "InvalidAccessKeyId", "access key not authorized") |> halt()
          end

        :error ->
          error(conn, 403, "MissingAuthenticationToken", "no SigV4 credential found")
          |> halt()
      end
    end
  end

  ## Method dispatchers — branch on multipart query params.

  defp dispatch_post(conn, bucket, key) do
    qs = conn.query_params

    cond do
      Map.has_key?(qs, "uploads") -> do_initiate(conn, bucket, key)
      uid = qs["uploadId"] -> do_complete(conn, bucket, key, uid)
      true -> error(conn, 400, "InvalidRequest", "POST requires ?uploads or ?uploadId=…")
    end
  end

  defp dispatch_put(conn, bucket, key) do
    qs = conn.query_params

    case {qs["uploadId"], qs["partNumber"]} do
      {uid, n_str} when is_binary(uid) and is_binary(n_str) ->
        do_upload_part(conn, uid, n_str)

      {nil, nil} ->
        do_put(conn, bucket, key)

      _ ->
        error(conn, 400, "InvalidArgument", "uploadId and partNumber must be provided together")
    end
  end

  defp dispatch_delete(conn, bucket, key) do
    case conn.query_params["uploadId"] do
      nil -> do_delete(conn, bucket, key)
      uid -> do_abort(conn, uid)
    end
  end

  ## Multipart handlers.

  defp do_initiate(conn, bucket, key) do
    {:ok, upload_id} = Multipart.initiate(bucket, key, first_header(conn, "content-type"))
    send_xml(conn, 200, S3XML.initiate_multipart(bucket, key, upload_id))
  end

  defp do_upload_part(conn, upload_id, part_number_str) do
    case Integer.parse(part_number_str) do
      {n, ""} when n in 1..10_000 ->
        case Multipart.upload_part(conn, root(), upload_id, n) do
          {:ok, conn, etag} ->
            conn
            |> put_resp_header("etag", ~s|"#{etag}"|)
            |> send_resp(200, "")

          {:error, :no_such_upload} ->
            error(conn, 404, "NoSuchUpload", upload_id)
        end

      _ ->
        error(conn, 400, "InvalidArgument", "partNumber must be 1..10000")
    end
  end

  defp do_complete(conn, bucket, key, upload_id) do
    {:ok, body, conn} = Plug.Conn.read_body(conn, length: 5_000_000)

    with {:ok, parts} <- S3XML.parse_complete_body(body),
         {:ok, %{etag: etag}} <- Multipart.complete(root(), upload_id, parts) do
      location = build_location(conn, bucket, key)
      send_xml(conn, 200, S3XML.complete_multipart(location, bucket, key, etag))
    else
      {:error, :no_such_upload} ->
        error(conn, 404, "NoSuchUpload", upload_id)

      {:error, :no_parts} ->
        error(conn, 400, "MalformedXML", "no parts in CompleteMultipartUpload body")

      {:error, {:missing_part, n}} ->
        error(conn, 400, "InvalidPart", "part #{n} not uploaded")

      {:error, {:part_mismatch, n}} ->
        error(conn, 400, "InvalidPart", "etag for part #{n} does not match")

      {:error, :invalid_xml} ->
        error(conn, 400, "MalformedXML", "could not parse CompleteMultipartUpload body")

      {:error, :bad_root} ->
        error(conn, 400, "MalformedXML", "expected <CompleteMultipartUpload>")
    end
  end

  defp do_abort(conn, upload_id) do
    case Multipart.abort(root(), upload_id) do
      :ok -> send_resp(conn, 204, "")
      {:error, :no_such_upload} -> error(conn, 404, "NoSuchUpload", upload_id)
    end
  end

  defp build_location(conn, bucket, key) do
    scheme = if conn.scheme == :https, do: "https", else: "http"
    host = conn.host

    port =
      case {scheme, conn.port} do
        {"http", 80} -> ""
        {"https", 443} -> ""
        {_, p} -> ":#{p}"
      end

    "#{scheme}://#{host}#{port}/#{bucket}/#{key}"
  end

  ## Object handlers.

  defp do_put(conn, bucket, key) do
    {:ok, conn, size, etag} = Storage.stream_put(conn, root(), bucket, key)
    content_type = first_header(conn, "content-type")
    :ok = Index.put(bucket, key, size, etag, content_type, System.system_time(:second))

    conn
    |> put_resp_header("etag", ~s|"#{etag}"|)
    |> send_resp(200, "")
  end

  defp do_get(conn, bucket, key) do
    case Index.get(bucket, key) do
      :not_found ->
        error(conn, 404, "NoSuchKey", key)

      {:ok, meta} ->
        path = Storage.blob_path(root(), bucket, key)
        range_header = first_header(conn, "range")

        case Storage.parse_range(range_header, meta.size) do
          :none ->
            conn
            |> put_meta_headers(meta)
            |> Plug.Conn.send_file(200, path)

          {:ok, start, stop} ->
            length = stop - start + 1

            conn
            |> put_meta_headers(meta)
            |> put_resp_header(
              "content-range",
              "bytes #{start}-#{stop}/#{meta.size}"
            )
            |> Plug.Conn.send_file(206, path, start, length)

          :invalid ->
            error(conn, 416, "InvalidRange", "bad Range header")
        end
    end
  end

  defp do_head(conn, bucket, key) do
    case Index.get(bucket, key) do
      :not_found ->
        # S3 returns 404 with empty body for HEAD; some clients want no XML.
        send_resp(conn, 404, "")

      {:ok, meta} ->
        conn
        |> put_meta_headers(meta)
        |> send_resp(200, "")
    end
  end

  defp do_delete(conn, bucket, key) do
    Storage.delete(root(), bucket, key)
    Index.delete(bucket, key)
    send_resp(conn, 204, "")
  end

  ## ListObjectsV2.

  defp list_objects(conn, bucket) do
    qs = conn.query_params

    prefix = Map.get(qs, "prefix", "")
    max_keys = qs |> Map.get("max-keys", "1000") |> safe_int(1000)

    start_after =
      cond do
        token = Map.get(qs, "continuation-token") -> S3XML.token_decode(token)
        s = Map.get(qs, "start-after") -> s
        true -> ""
      end

    {entries, truncated?, next} =
      Index.list(bucket, prefix: prefix, start_after: start_after, max_keys: max_keys)

    body = S3XML.list_objects(bucket, prefix, max_keys, entries, truncated?, next)
    send_xml(conn, 200, body)
  end

  ## Helpers.

  defp with_object(conn, bucket, key_parts, fun) do
    key = Enum.join(key_parts, "/")

    cond do
      not Storage.valid_bucket?(bucket) ->
        error(conn, 400, "InvalidBucketName", "bucket name is not valid")

      not Storage.valid_key?(key) ->
        error(conn, 400, "InvalidArgument", "object key is not valid")

      true ->
        fun.(conn, bucket, key)
    end
  end

  defp put_meta_headers(conn, meta) do
    conn =
      conn
      |> put_resp_header("etag", ~s|"#{meta.etag}"|)
      |> put_resp_header("content-length", Integer.to_string(meta.size))
      |> put_resp_header("last-modified", http_date(meta.mtime))
      |> put_resp_header("accept-ranges", "bytes")

    case meta.content_type do
      nil -> conn
      ct -> put_resp_header(conn, "content-type", ct)
    end
  end

  defp http_date(unix_seconds) do
    unix_seconds
    |> DateTime.from_unix!()
    |> Calendar.strftime("%a, %d %b %Y %H:%M:%S GMT")
  end

  defp send_xml(conn, status, iolist) do
    conn
    |> put_resp_content_type("application/xml")
    |> send_resp(status, iolist)
  end

  defp error(conn, status, code, message) do
    conn
    |> put_resp_content_type("application/xml")
    |> send_resp(status, S3XML.error(code, message, conn.request_path))
  end

  defp first_header(conn, name) do
    case get_req_header(conn, name) do
      [v | _] -> v
      [] -> nil
    end
  end

  defp safe_int(s, default) do
    case Integer.parse(s) do
      {n, ""} when n > 0 -> n
      _ -> default
    end
  end

  defp root, do: Application.fetch_env!(:kafun, :root)
end
