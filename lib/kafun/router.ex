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
      PUT    /:bucket/<key> + x-amz-copy-source    → CopyObject
      PUT    /:bucket/<key>?partNumber=N&uploadId= → UploadPart (streamed)
      PUT    /:bucket/<key>?…&x-amz-copy-source    → UploadPartCopy
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
  plug :stamp_request
  plug :fetch_qs
  plug :match
  plug :authenticate
  plug :dispatch

  defp stamp_request(conn, _) do
    id = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :upper)

    conn
    |> assign(:request_id, id)
    |> put_resp_header("x-amz-request-id", id)
  end

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

      not Index.bucket_exists?(bucket) ->
        error(conn, 404, "NoSuchBucket", "bucket does not exist")

      Map.has_key?(conn.query_params, "uploads") ->
        list_multipart_uploads(conn, bucket)

      true ->
        list_objects(conn, bucket)
    end
  end

  match "/:bucket", via: :head do
    cond do
      not Storage.valid_bucket?(bucket) ->
        error(conn, 400, "InvalidBucketName", "bucket name is not valid")

      Index.bucket_exists?(bucket) ->
        send_resp(conn, 200, "")

      true ->
        error(conn, 404, "NoSuchBucket", "bucket does not exist")
    end
  end

  post "/:bucket" do
    cond do
      not Storage.valid_bucket?(bucket) ->
        error(conn, 400, "InvalidBucketName", "bucket name is not valid")

      not Index.bucket_exists?(bucket) ->
        error(conn, 404, "NoSuchBucket", "bucket does not exist")

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
    copy_src = first_header(conn, "x-amz-copy-source")

    case {qs["uploadId"], qs["partNumber"], copy_src} do
      {uid, n_str, src} when is_binary(uid) and is_binary(n_str) and is_binary(src) ->
        do_upload_part_copy(conn, uid, n_str, src)

      {uid, n_str, nil} when is_binary(uid) and is_binary(n_str) ->
        do_upload_part(conn, uid, n_str)

      {nil, nil, src} when is_binary(src) ->
        do_copy_object(conn, bucket, key, src)

      {nil, nil, nil} ->
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

  defp do_copy_object(conn, dst_bucket, dst_key, src_header) do
    started = System.monotonic_time(:microsecond)

    with {:ok, src_bucket, src_key} <- parse_copy_source(src_header),
         true <- Storage.valid_bucket?(src_bucket) and Storage.valid_key?(src_key),
         {:ok, src_meta} <- fetch_src_meta(src_bucket, src_key),
         {:ok, _size} <- Storage.copy_blob(root(), src_bucket, src_key, dst_bucket, dst_key) do
      now = System.system_time(:second)
      :ok = Index.put(dst_bucket, dst_key, src_meta.size, src_meta.etag, src_meta.content_type, now)

      emit(
        [:copy, :stop],
        %{size: src_meta.size, duration: System.monotonic_time(:microsecond) - started},
        %{src_bucket: src_bucket, src_key: src_key, bucket: dst_bucket, key: dst_key}
      )

      send_xml(conn, 200, S3XML.copy_object_result(src_meta.etag, now))
    else
      false -> error(conn, 400, "InvalidArgument", "source bucket or key invalid")
      {:error, :invalid_copy_source} -> error(conn, 400, "InvalidArgument", "x-amz-copy-source malformed")
      {:error, :no_such_key} -> error(conn, 404, "NoSuchKey", "source object does not exist")
      {:error, :not_found} -> error(conn, 404, "NoSuchKey", "source object missing on disk")
    end
  end

  defp do_upload_part_copy(conn, upload_id, part_number_str, src_header) do
    started = System.monotonic_time(:microsecond)

    with {n, ""} <- Integer.parse(part_number_str),
         true <- n in 1..10_000,
         {:ok, src_bucket, src_key} <- parse_copy_source(src_header),
         true <- Storage.valid_bucket?(src_bucket) and Storage.valid_key?(src_key),
         {:ok, _upload} <- fetch_upload(upload_id),
         {:ok, src_meta} <- fetch_src_meta(src_bucket, src_key),
         {:range, range} when range != :invalid <-
           {:range,
            parse_copy_range(first_header(conn, "x-amz-copy-source-range"), src_meta.size)},
         {:ok, size, etag} <-
           Storage.copy_part(root(), src_bucket, src_key, upload_id, n, range) do
      now = System.system_time(:second)
      :ok = Index.record_part(upload_id, n, size, etag, now)

      emit(
        [:multipart, :upload_part_copy],
        %{size: size, duration: System.monotonic_time(:microsecond) - started},
        %{upload_id: upload_id, part_number: n, src_bucket: src_bucket, src_key: src_key}
      )

      send_xml(conn, 200, S3XML.copy_part_result(etag, now))
    else
      :error -> error(conn, 400, "InvalidArgument", "partNumber not parseable")
      false -> error(conn, 400, "InvalidArgument", "partNumber must be 1..10000 or source invalid")
      {:error, :invalid_copy_source} -> error(conn, 400, "InvalidArgument", "x-amz-copy-source malformed")
      {:error, :no_such_upload} -> error(conn, 404, "NoSuchUpload", upload_id)
      {:error, :no_such_key} -> error(conn, 404, "NoSuchKey", "source object does not exist")
      {:error, :not_found} -> error(conn, 404, "NoSuchKey", "source object missing on disk")
      {:range, :invalid} -> error(conn, 400, "InvalidArgument", "x-amz-copy-source-range invalid")
    end
  end

  defp fetch_src_meta(bucket, key) do
    case Index.get(bucket, key) do
      {:ok, meta} -> {:ok, meta}
      :not_found -> {:error, :no_such_key}
    end
  end

  defp fetch_upload(upload_id) do
    case Index.get_upload(upload_id) do
      {:ok, u} -> {:ok, u}
      :not_found -> {:error, :no_such_upload}
    end
  end

  defp parse_copy_source(value) when is_binary(value) do
    trimmed =
      value
      |> String.trim_leading("/")
      |> String.split("?", parts: 2)
      |> hd()

    case String.split(trimmed, "/", parts: 2) do
      [bucket, key_enc] when bucket != "" and key_enc != "" ->
        {:ok, bucket, URI.decode(key_enc)}

      _ ->
        {:error, :invalid_copy_source}
    end
  end

  defp parse_copy_range(nil, _size), do: nil
  defp parse_copy_range("", _size), do: nil

  defp parse_copy_range(header, size) do
    case Storage.parse_range(header, size) do
      {:ok, start, stop} -> {start, stop}
      :none -> nil
      :invalid -> :invalid
    end
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
        error(conn, 404, "NoSuchKey", key)

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
    encoding_type = Map.get(qs, "encoding-type")
    fetch_owner? = Map.get(qs, "fetch-owner") == "true"

    base_opts = [
      prefix: prefix,
      delimiter: delimiter,
      max_keys: max_keys
    ]

    # Per S3 spec: when both continuation-token and start-after are sent,
    # continuation-token wins and start-after is ignored.
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
        next,
        encoding_type: encoding_type,
        fetch_owner: fetch_owner?
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

      not Index.bucket_exists?(bucket) ->
        error(conn, 404, "NoSuchBucket", "bucket does not exist")

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
    rid = conn.assigns[:request_id] || ""
    conn = put_resp_header(conn, "x-amz-error-code", code)

    if conn.method == "HEAD" do
      # HEAD has no body; the error code rides on the header instead.
      send_resp(conn, status, "")
    else
      conn
      |> put_resp_content_type("application/xml")
      |> send_resp(status, S3XML.error(code, message, conn.request_path, rid))
    end
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
