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

  alias Kafun.{Auth, Index, S3XML, Storage}

  plug Plug.Logger, log: :debug
  plug :match
  plug :authenticate
  plug :dispatch

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

  put "/:bucket/*key_parts" do
    with_object(conn, bucket, key_parts, &do_put/3)
  end

  get "/:bucket/*key_parts" do
    with_object(conn, bucket, key_parts, &do_get/3)
  end

  match "/:bucket/*key_parts", via: :head do
    with_object(conn, bucket, key_parts, &do_head/3)
  end

  delete "/:bucket/*key_parts" do
    with_object(conn, bucket, key_parts, &do_delete/3)
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
    conn = Plug.Conn.fetch_query_params(conn)
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
