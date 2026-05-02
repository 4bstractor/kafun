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
  ListBucketResult (S3 ListObjectsV2). `entries` are maps with
  `:key, :size, :etag, :mtime`; `next` is the continuation key or nil.
  """
  def list_objects(bucket, prefix, max_keys, entries, truncated?, next) do
    [
      ~s|<?xml version="1.0" encoding="UTF-8"?>|,
      ~s|<ListBucketResult xmlns="#{@ns}">|,
      "<Name>",
      esc(bucket),
      "</Name>",
      "<Prefix>",
      esc(prefix),
      "</Prefix>",
      "<KeyCount>",
      Integer.to_string(length(entries)),
      "</KeyCount>",
      "<MaxKeys>",
      Integer.to_string(max_keys),
      "</MaxKeys>",
      "<IsTruncated>",
      if(truncated?, do: "true", else: "false"),
      "</IsTruncated>",
      if(truncated? and next,
        do: ["<NextContinuationToken>", token_encode(next), "</NextContinuationToken>"],
        else: []
      ),
      Enum.map(entries, fn %{key: k, size: sz, etag: etag, mtime: mt} ->
        [
          "<Contents>",
          "<Key>",
          esc(k),
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
          "</Contents>"
        ]
      end),
      "</ListBucketResult>"
    ]
  end

  @doc "Standard S3 error body."
  def error(code, message, resource \\ "") do
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
