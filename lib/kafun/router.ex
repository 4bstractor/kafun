defmodule Kafun.Router do
  @moduledoc """
  S3-compatible HTTP surface. Endpoints:

      GET    /healthz                              → liveness, never auth-gated
      GET    /                                     → ListAllMyBuckets
      PUT    /:bucket                              → CreateBucket
      HEAD   /:bucket                              → HeadBucket
      POST   /:bucket?delete                       → DeleteObjects (multi-delete)
      GET    /:bucket?list-type=2&prefix=&...      → ListObjectsV2 (delimiter-aware)
      GET    /:bucket?uploads&...                  → ListMultipartUploads
      POST   /:bucket/<key>?uploads                → InitiateMultipartUpload
      POST   /:bucket/<key>?uploadId=…             → CompleteMultipartUpload
      PUT    /:bucket/<key>                        → PutObject (streamed)
      PUT    /:bucket/<key>?partNumber=N&uploadId= → UploadPart (streamed)
      GET    /:bucket/<key>                        → GetObject (sendfile + Range)
      GET    /:bucket/<key>?uploadId=…             → ListParts
      HEAD   /:bucket/<key>                        → HeadObject
      DELETE /:bucket/<key>                        → DeleteObject
      DELETE /:bucket/<key>?uploadId=…             → AbortMultipartUpload
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
    cond do
      not Storage.valid_bucket?(bucket) ->
        error(conn, 400, "InvalidBucketName", "bucket name is not valid")

      Map.has_key?(conn.query_params, "uploads") ->
        list_multipart_uploads(conn, bucket)

      true ->
        list_objects(conn, bucket)
    end
  end

  match "/:bucket", via: :head do
    cond do
      not Storage.valid_bucket?(bucket) ->
        send_resp(conn, 400, "")

      Enum.any?(Index.list_buckets(), &(&1.name == bucket)) ->
        send_resp(conn, 200, "")

      true ->
        send_resp(conn, 404, "")
    end
  end

  post "/:bucket" do
    cond do
      not Storage.valid_bucket?(bucket) ->
        error(conn, 400, "InvalidBucketName", "bucket name is not valid")

      Map.has_key?(conn.query_params, "delete") ->
        do_delete_objects(conn, bucket)

      true ->
        error(conn, 400, "InvalidRequest", "POST on a bucket requires ?delete")
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
    with_object(conn, bucket, key_parts, &dispatch_get/3)
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

  defp dispatch_get(conn, bucket, key) do
    case conn.query_params["uploadId"] do
      nil -> do_get(conn, bucket, key)
      uid -> do_list_parts(conn, bucket, key, uid)
    end
  end

  ## Multipart handlers.

  defp do_initiate(conn, bucket, key) do
    {:ok, upload_id} = Multipart.initiate(bucket, key, first_header(conn, "content-type"))

    emit([:multipart, :initiate], %{}, %{
      bucket: bucket,
      key: key,
      upload_id: upload_id
    })

    send_xml(conn, 200, S3XML.initiate_multipart(bucket, key, upload_id))
  end

  defp do_upload_part(conn, upload_id, part_number_str) do
    started = System.monotonic_time(:microsecond)

    case Integer.parse(part_number_str) do
      {n, ""} when n in 1..10_000 ->
        case Multipart.upload_part(conn, root(), upload_id, n) do
          {:ok, conn, etag} ->
            emit(
              [:multipart, :upload_part],
              %{duration: System.monotonic_time(:microsecond) - started},
              %{upload_id: upload_id, part_number: n}
            )

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
    started = System.monotonic_time(:microsecond)
    {:ok, body, conn} = Plug.Conn.read_body(conn, length: 5_000_000)

    with {:ok, parts} <- S3XML.parse_complete_body(body),
         {:ok, %{etag: etag, size: size}} <- Multipart.complete(root(), upload_id, parts) do
      emit(
        [:multipart, :complete],
        %{
          size: size,
          parts: length(parts),
          duration: System.monotonic_time(:microsecond) - started
        },
        %{bucket: bucket, key: key, upload_id: upload_id}
      )

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

      {:error, {:read_error, reason}} ->
        error(conn, 500, "InternalError", "failed to read part: #{inspect(reason)}")
    end
  end

  defp do_abort(conn, upload_id) do
    case Multipart.abort(root(), upload_id) do
      :ok ->
        emit([:multipart, :abort], %{}, %{upload_id: upload_id})
        send_resp(conn, 204, "")

      {:error, :no_such_upload} ->
        error(conn, 404, "NoSuchUpload", upload_id)
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
    started = System.monotonic_time(:microsecond)
    {:ok, conn, size, etag} = Storage.stream_put(conn, root(), bucket, key)
    content_type = first_header(conn, "content-type")
    :ok = Index.put(bucket, key, size, etag, content_type, System.system_time(:second))

    emit(
      [:put, :stop],
      %{size: size, duration: System.monotonic_time(:microsecond) - started},
      %{bucket: bucket, key: key}
    )

    conn
    |> put_resp_header("etag", ~s|"#{etag}"|)
    |> send_resp(200, "")
  end

  defp do_get(conn, bucket, key) do
    started = System.monotonic_time(:microsecond)

    case Index.get(bucket, key) do
      :not_found ->
        error(conn, 404, "NoSuchKey", key)

      {:ok, meta} ->
        path = Storage.blob_path(root(), bucket, key)
        range_header = first_header(conn, "range")

        case Storage.parse_range(range_header, meta.size) do
          :none ->
            emit(
              [:get, :stop],
              %{size: meta.size, duration: System.monotonic_time(:microsecond) - started},
              %{bucket: bucket, key: key, range: false}
            )

            conn
            |> put_meta_headers(meta)
            |> Plug.Conn.send_file(200, path)

          {:ok, start, stop} ->
            length = stop - start + 1

            emit(
              [:get, :stop],
              %{size: length, duration: System.monotonic_time(:microsecond) - started},
              %{bucket: bucket, key: key, range: true}
            )

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
    emit([:delete, :stop], %{}, %{bucket: bucket, key: key})
    send_resp(conn, 204, "")
  end

  ## ListObjectsV2.

  defp list_objects(conn, bucket) do
    started = System.monotonic_time(:microsecond)
    qs = conn.query_params

    prefix = Map.get(qs, "prefix", "")
    delimiter = Map.get(qs, "delimiter")
    max_keys = qs |> Map.get("max-keys", "1000") |> safe_pos_int(1000)

    base_opts = [
      prefix: prefix,
      delimiter: delimiter,
      max_keys: max_keys
    ]

    cursor_opts =
      cond do
        token = Map.get(qs, "continuation-token") ->
          [continuation: S3XML.token_decode(token)]

        s = Map.get(qs, "start-after") ->
          [start_after: s]

        true ->
          []
      end

    {entries, common_prefixes, truncated?, next} =
      Index.list(bucket, base_opts ++ cursor_opts)

    emit(
      [:list, :stop],
      %{
        count: length(entries) + length(common_prefixes),
        duration: System.monotonic_time(:microsecond) - started
      },
      %{bucket: bucket, prefix: prefix, truncated: truncated?}
    )

    body =
      S3XML.list_objects(
        bucket,
        prefix,
        delimiter,
        max_keys,
        entries,
        common_prefixes,
        truncated?,
        next
      )

    send_xml(conn, 200, body)
  end

  defp do_delete_objects(conn, bucket) do
    started = System.monotonic_time(:microsecond)

    case Plug.Conn.read_body(conn, length: 1_048_576) do
      {:more, _, _} ->
        error(conn, 400, "MalformedXML", "Delete body exceeds 1 MiB")

      {:ok, body, conn} ->
        case S3XML.parse_delete_body(body) do
          {:ok, %{keys: []}} ->
            error(conn, 400, "MalformedXML", "no <Object> entries in Delete body")

          {:ok, %{keys: keys}} when length(keys) > 1000 ->
            error(conn, 400, "MalformedXML", "Delete request exceeds 1000 keys")

          {:ok, %{keys: keys, quiet: quiet?}} ->
            {deleted, errors} = run_delete_objects(bucket, keys)

            emit(
              [:delete_objects, :stop],
              %{
                count: length(keys),
                deleted: length(deleted),
                errors: length(errors),
                duration: System.monotonic_time(:microsecond) - started
              },
              %{bucket: bucket}
            )

            send_xml(
              conn,
              200,
              S3XML.delete_result(Enum.reverse(deleted), Enum.reverse(errors), quiet?)
            )

          {:error, _} ->
            error(conn, 400, "MalformedXML", "could not parse Delete body")
        end
    end
  end

  defp run_delete_objects(bucket, keys) do
    Enum.reduce(keys, {[], []}, fn key, {ok, errs} ->
      cond do
        not Storage.valid_key?(key) ->
          {ok, [{key, "InvalidKey", "key is not valid"} | errs]}

        true ->
          :ok = Index.delete(bucket, key)
          :ok = Storage.delete(root(), bucket, key)
          {[key | ok], errs}
      end
    end)
  end

  defp list_multipart_uploads(conn, bucket) do
    qs = conn.query_params

    prefix = Map.get(qs, "prefix", "")
    key_marker = Map.get(qs, "key-marker", "")
    upload_id_marker = Map.get(qs, "upload-id-marker", "")
    max_uploads = qs |> Map.get("max-uploads", "1000") |> safe_pos_int(1000)

    {uploads, truncated?, next_key, next_uid} =
      Index.list_uploads(bucket,
        prefix: prefix,
        key_marker: key_marker,
        upload_id_marker: upload_id_marker,
        max_uploads: max_uploads
      )

    body =
      S3XML.list_multipart_uploads(
        bucket,
        prefix,
        key_marker,
        upload_id_marker,
        max_uploads,
        uploads,
        truncated?,
        next_key,
        next_uid
      )

    send_xml(conn, 200, body)
  end

  defp do_list_parts(conn, bucket, key, upload_id) do
    case Index.get_upload(upload_id) do
      :not_found ->
        error(conn, 404, "NoSuchUpload", upload_id)

      {:ok, %{bucket: ^bucket, key: ^key}} ->
        qs = conn.query_params
        marker = qs |> Map.get("part-number-marker", "0") |> safe_nonneg_int(0)
        max_parts = qs |> Map.get("max-parts", "1000") |> safe_pos_int(1000)

        {parts, truncated?, next} =
          Index.list_parts_paged(upload_id,
            part_number_marker: marker,
            max_parts: max_parts
          )

        body =
          S3XML.list_parts(bucket, key, upload_id, marker, max_parts, parts, truncated?, next)

        send_xml(conn, 200, body)

      {:ok, _other} ->
        # uploadId is real but doesn't belong to this (bucket, key)
        error(conn, 404, "NoSuchUpload", upload_id)
    end
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

  defp safe_pos_int(s, default) do
    case Integer.parse(s) do
      {n, ""} when n > 0 -> n
      _ -> default
    end
  end

  defp safe_nonneg_int(s, default) do
    case Integer.parse(s) do
      {n, ""} when n >= 0 -> n
      _ -> default
    end
  end

  defp root, do: Application.fetch_env!(:kafun, :root)

  defp emit(suffix, measurements, metadata) do
    :telemetry.execute([:kafun | suffix], measurements, metadata)
  end
end
