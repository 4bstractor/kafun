defmodule Kafun.S3XML do
  @moduledoc """
  S3-compatible XML response bodies. Kept tiny and allocation-light: builds
  iolists, escapes only what `<>&"'` requires.
  """

  @ns "http://s3.amazonaws.com/doc/2006-03-01/"

  @doc "ListAllMyBucketsResult — the body of `GET /`."
  def list_all_buckets(buckets) do
    [
      ~s|<?xml version="1.0" encoding="UTF-8"?>|,
      ~s|<ListAllMyBucketsResult xmlns="#{@ns}">|,
      ~s|<Owner><ID>kafun</ID><DisplayName>kafun</DisplayName></Owner>|,
      "<Buckets>",
      Enum.map(buckets, fn %{name: n, created_at: ts} ->
        [
          "<Bucket><Name>",
          esc(n),
          "</Name><CreationDate>",
          iso8601(ts),
          "</CreationDate></Bucket>"
        ]
      end),
      "</Buckets></ListAllMyBucketsResult>"
    ]
  end

  @doc """
  ListBucketResult (S3 ListObjectsV2) — supports prefix + delimiter + pagination.
  `entries` are object maps; `common_prefixes` is a list of strings; `next` is
  the inclusive lower-bound for the next page (encoded into a continuation token)
  or `nil` when not truncated.
  """
  def list_objects(bucket, prefix, delimiter, max_keys, entries, common_prefixes, truncated?, next, opts \\ []) do
    encoding_type = Keyword.get(opts, :encoding_type)
    fetch_owner? = Keyword.get(opts, :fetch_owner, false)
    enc = if encoding_type == "url", do: &URI.encode/1, else: &Function.identity/1

    [
      ~s|<?xml version="1.0" encoding="UTF-8"?>|,
      ~s|<ListBucketResult xmlns="#{@ns}">|,
      "<Name>",
      esc(bucket),
      "</Name>",
      "<Prefix>",
      esc(enc.(prefix)),
      "</Prefix>",
      if(delimiter, do: ["<Delimiter>", esc(enc.(delimiter)), "</Delimiter>"], else: []),
      "<KeyCount>",
      Integer.to_string(length(entries) + length(common_prefixes)),
      "</KeyCount>",
      "<MaxKeys>",
      Integer.to_string(max_keys),
      "</MaxKeys>",
      "<IsTruncated>",
      if(truncated?, do: "true", else: "false"),
      "</IsTruncated>",
      if(encoding_type, do: ["<EncodingType>", esc(encoding_type), "</EncodingType>"], else: []),
      if(truncated? and next,
        do: ["<NextContinuationToken>", token_encode(next), "</NextContinuationToken>"],
        else: []
      ),
      Enum.map(entries, fn %{key: k, size: sz, etag: etag, mtime: mt} ->
        [
          "<Contents>",
          "<Key>",
          esc(enc.(k)),
          "</Key>",
          "<LastModified>",
          iso8601(mt),
          "</LastModified>",
          ~s|<ETag>"|,
          esc(etag),
          ~s|"</ETag>|,
          "<Size>",
          Integer.to_string(sz),
          "</Size>",
          "<StorageClass>STANDARD</StorageClass>",
          if(fetch_owner?,
            do: "<Owner><ID>kafun</ID><DisplayName>kafun</DisplayName></Owner>",
            else: []
          ),
          "</Contents>"
        ]
      end),
      Enum.map(common_prefixes, fn cp ->
        ["<CommonPrefixes><Prefix>", esc(enc.(cp)), "</Prefix></CommonPrefixes>"]
      end),
      "</ListBucketResult>"
    ]
  end

  @doc "ListMultipartUploadsResult — body of `GET /:bucket?uploads`."
  def list_multipart_uploads(bucket, prefix, key_marker, upload_id_marker,
                             max_uploads, uploads, truncated?, next_key, next_uid) do
    [
      ~s|<?xml version="1.0" encoding="UTF-8"?>|,
      ~s|<ListMultipartUploadsResult xmlns="#{@ns}">|,
      "<Bucket>",
      esc(bucket),
      "</Bucket>",
      "<KeyMarker>",
      esc(key_marker || ""),
      "</KeyMarker>",
      "<UploadIdMarker>",
      esc(upload_id_marker || ""),
      "</UploadIdMarker>",
      if(truncated? and next_key,
        do: [
          "<NextKeyMarker>",
          esc(next_key),
          "</NextKeyMarker>",
          "<NextUploadIdMarker>",
          esc(next_uid || ""),
          "</NextUploadIdMarker>"
        ],
        else: []
      ),
      "<Prefix>",
      esc(prefix || ""),
      "</Prefix>",
      "<MaxUploads>",
      Integer.to_string(max_uploads),
      "</MaxUploads>",
      "<IsTruncated>",
      if(truncated?, do: "true", else: "false"),
      "</IsTruncated>",
      Enum.map(uploads, fn %{key: k, upload_id: uid, initiated_at: ts} ->
        [
          "<Upload>",
          "<Key>",
          esc(k),
          "</Key>",
          "<UploadId>",
          esc(uid),
          "</UploadId>",
          "<Initiator><ID>kafun</ID><DisplayName>kafun</DisplayName></Initiator>",
          "<Owner><ID>kafun</ID><DisplayName>kafun</DisplayName></Owner>",
          "<StorageClass>STANDARD</StorageClass>",
          "<Initiated>",
          iso8601(ts),
          "</Initiated>",
          "</Upload>"
        ]
      end),
      "</ListMultipartUploadsResult>"
    ]
  end

  @doc "ListPartsResult — body of `GET /:bucket/:key?uploadId=…`."
  def list_parts(bucket, key, upload_id, marker, max_parts, parts, truncated?, next_marker) do
    [
      ~s|<?xml version="1.0" encoding="UTF-8"?>|,
      ~s|<ListPartsResult xmlns="#{@ns}">|,
      "<Bucket>",
      esc(bucket),
      "</Bucket>",
      "<Key>",
      esc(key),
      "</Key>",
      "<UploadId>",
      esc(upload_id),
      "</UploadId>",
      "<PartNumberMarker>",
      Integer.to_string(marker),
      "</PartNumberMarker>",
      if(truncated? and next_marker,
        do: ["<NextPartNumberMarker>", Integer.to_string(next_marker), "</NextPartNumberMarker>"],
        else: []
      ),
      "<MaxParts>",
      Integer.to_string(max_parts),
      "</MaxParts>",
      "<IsTruncated>",
      if(truncated?, do: "true", else: "false"),
      "</IsTruncated>",
      "<Initiator><ID>kafun</ID><DisplayName>kafun</DisplayName></Initiator>",
      "<Owner><ID>kafun</ID><DisplayName>kafun</DisplayName></Owner>",
      "<StorageClass>STANDARD</StorageClass>",
      Enum.map(parts, fn %{part_number: n, size: sz, etag: etag, mtime: mt} ->
        [
          "<Part>",
          "<PartNumber>",
          Integer.to_string(n),
          "</PartNumber>",
          "<LastModified>",
          iso8601(mt),
          "</LastModified>",
          ~s|<ETag>"|,
          esc(etag),
          ~s|"</ETag>|,
          "<Size>",
          Integer.to_string(sz),
          "</Size>",
          "</Part>"
        ]
      end),
      "</ListPartsResult>"
    ]
  end

  @doc "InitiateMultipartUploadResult — body of `POST /:bucket/:key?uploads`."
  def initiate_multipart(bucket, key, upload_id) do
    [
      ~s|<?xml version="1.0" encoding="UTF-8"?>|,
      ~s|<InitiateMultipartUploadResult xmlns="#{@ns}">|,
      "<Bucket>",
      esc(bucket),
      "</Bucket>",
      "<Key>",
      esc(key),
      "</Key>",
      "<UploadId>",
      esc(upload_id),
      "</UploadId>",
      "</InitiateMultipartUploadResult>"
    ]
  end

  @doc "CompleteMultipartUploadResult — body of `POST /:bucket/:key?uploadId=…`."
  def complete_multipart(location, bucket, key, etag) do
    [
      ~s|<?xml version="1.0" encoding="UTF-8"?>|,
      ~s|<CompleteMultipartUploadResult xmlns="#{@ns}">|,
      "<Location>",
      esc(location),
      "</Location>",
      "<Bucket>",
      esc(bucket),
      "</Bucket>",
      "<Key>",
      esc(key),
      "</Key>",
      ~s|<ETag>"|,
      esc(etag),
      ~s|"</ETag>|,
      "</CompleteMultipartUploadResult>"
    ]
  end

  @doc """
  Parse a CompleteMultipartUpload request body. Returns a list of
  `{part_number, etag}` in the order the client provided.
  """
  @spec parse_complete_body(String.t()) ::
          {:ok, [{pos_integer(), String.t()}]} | {:error, atom()}
  def parse_complete_body(xml) when is_binary(xml) do
    case Saxy.SimpleForm.parse_string(xml) do
      {:ok, {"CompleteMultipartUpload", _, children}} ->
        {:ok, Enum.flat_map(children, &extract_part/1)}

      {:ok, _} ->
        {:error, :bad_root}

      {:error, _} ->
        {:error, :invalid_xml}
    end
  end

  defp extract_part({"Part", _, kids}) do
    with {:ok, n_str} <- find_child(kids, "PartNumber"),
         {:ok, etag} <- find_child(kids, "ETag"),
         {n, ""} <- Integer.parse(n_str),
         true <- n in 1..10_000 do
      [{n, etag}]
    else
      _ -> []
    end
  end

  defp extract_part(_), do: []

  defp find_child(children, name) do
    Enum.find(children, &match?({^name, _, _}, &1))
    |> case do
      nil -> :not_found
      {_, _, kids} -> {:ok, kids |> Enum.filter(&is_binary/1) |> IO.iodata_to_binary() |> String.trim()}
    end
  end

  @doc "GetBucketLocation response. Always reports our single fixed region."
  @spec bucket_location() :: iodata()
  def bucket_location do
    [
      ~s|<?xml version="1.0" encoding="UTF-8"?>|,
      ~s|<LocationConstraint xmlns="#{@ns}">us-east-1</LocationConstraint>|
    ]
  end

  @doc "GetBucketAcl stub. Reports a single FULL_CONTROL grant to the kafun owner."
  @spec bucket_acl() :: iodata()
  def bucket_acl do
    [
      ~s|<?xml version="1.0" encoding="UTF-8"?>|,
      ~s|<AccessControlPolicy xmlns="#{@ns}">|,
      "<Owner><ID>kafun</ID><DisplayName>kafun</DisplayName></Owner>",
      "<AccessControlList><Grant>",
      ~s|<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">|,
      "<ID>kafun</ID><DisplayName>kafun</DisplayName></Grantee>",
      "<Permission>FULL_CONTROL</Permission>",
      "</Grant></AccessControlList>",
      "</AccessControlPolicy>"
    ]
  end

  @doc "GetBucketVersioning stub. Empty body = unversioned."
  @spec bucket_versioning() :: iodata()
  def bucket_versioning do
    [
      ~s|<?xml version="1.0" encoding="UTF-8"?>|,
      ~s|<VersioningConfiguration xmlns="#{@ns}"/>|
    ]
  end

  @doc "CopyObject response body."
  @spec copy_object_result(String.t(), integer()) :: iodata()
  def copy_object_result(etag, last_modified_unix) do
    [
      ~s|<?xml version="1.0" encoding="UTF-8"?>|,
      ~s|<CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">|,
      "<LastModified>",
      iso8601(last_modified_unix),
      "</LastModified>",
      "<ETag>&quot;",
      esc(etag),
      "&quot;</ETag>",
      "</CopyObjectResult>"
    ]
  end

  @doc "UploadPartCopy response body."
  @spec copy_part_result(String.t(), integer()) :: iodata()
  def copy_part_result(etag, last_modified_unix) do
    [
      ~s|<?xml version="1.0" encoding="UTF-8"?>|,
      ~s|<CopyPartResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">|,
      "<LastModified>",
      iso8601(last_modified_unix),
      "</LastModified>",
      "<ETag>&quot;",
      esc(etag),
      "&quot;</ETag>",
      "</CopyPartResult>"
    ]
  end

  @doc """
  Parse a Multi-Object Delete request body.

      <Delete>
        <Object><Key>k1</Key></Object>
        ...
        <Quiet>true</Quiet>
      </Delete>

  Returns `{:ok, %{keys: [...], quiet: bool}}`. Keys are surfaced in document
  order. Malformed `<Object>` blocks (no `<Key>`) are dropped silently to
  match S3's lenient behaviour.
  """
  @spec parse_delete_body(String.t()) ::
          {:ok, %{keys: [String.t()], quiet: boolean()}} | {:error, atom()}
  def parse_delete_body(xml) when is_binary(xml) do
    case Saxy.SimpleForm.parse_string(xml) do
      {:ok, {"Delete", _, children}} ->
        keys = Enum.flat_map(children, &extract_object_key/1)
        quiet = Enum.any?(children, &match_quiet/1)
        {:ok, %{keys: keys, quiet: quiet}}

      {:ok, _} ->
        {:error, :bad_root}

      {:error, _} ->
        {:error, :invalid_xml}
    end
  end

  defp extract_object_key({"Object", _, kids}) do
    case find_child(kids, "Key") do
      {:ok, k} when k != "" -> [k]
      _ -> []
    end
  end

  defp extract_object_key(_), do: []

  defp match_quiet({"Quiet", _, kids}) do
    kids
    |> Enum.filter(&is_binary/1)
    |> IO.iodata_to_binary()
    |> String.trim()
    |> String.downcase() == "true"
  end

  defp match_quiet(_), do: false

  @doc """
  Build the DeleteResult body. `deleted` is a list of keys; `errors` is a
  list of `{key, code, message}` tuples. In `quiet` mode the `<Deleted>`
  blocks are suppressed (`<Error>` blocks always emit).
  """
  @spec delete_result([String.t()], [{String.t(), String.t(), String.t()}], boolean()) :: iodata()
  def delete_result(deleted, errors, quiet?) do
    [
      ~s|<?xml version="1.0" encoding="UTF-8"?>|,
      ~s|<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">|,
      if(quiet?,
        do: [],
        else: Enum.map(deleted, &["<Deleted><Key>", esc(&1), "</Key></Deleted>"])
      ),
      Enum.map(errors, fn {k, code, msg} ->
        [
          "<Error><Key>",
          esc(k),
          "</Key><Code>",
          esc(code),
          "</Code><Message>",
          esc(msg),
          "</Message></Error>"
        ]
      end),
      "</DeleteResult>"
    ]
  end

  @doc "Standard S3 error body. `request_id` echoes the `x-amz-request-id` header."
  def error(code, message, resource \\ "", request_id \\ "") do
    [
      ~s|<?xml version="1.0" encoding="UTF-8"?>|,
      "<Error>",
      "<Code>",
      esc(code),
      "</Code>",
      "<Message>",
      esc(message),
      "</Message>",
      "<Resource>",
      esc(resource),
      "</Resource>",
      "<RequestId>",
      esc(request_id),
      "</RequestId>",
      "<HostId>",
      esc(request_id),
      "</HostId>",
      "</Error>"
    ]
  end

  @doc "Continuation tokens — opaque to clients, decodable here."
  def token_encode(key), do: Base.url_encode64(key, padding: false)
  def token_decode(nil), do: ""
  def token_decode(""), do: ""

  def token_decode(token) do
    case Base.url_decode64(token, padding: false) do
      {:ok, key} -> key
      _ -> ""
    end
  end

  defp iso8601(unix_seconds) do
    unix_seconds |> DateTime.from_unix!() |> DateTime.to_iso8601()
  end

  defp esc(s) when is_binary(s) do
    s
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace(~s|"|, "&quot;")
    |> String.replace("'", "&apos;")
  end
end
