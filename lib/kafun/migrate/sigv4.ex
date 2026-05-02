defmodule Kafun.Migrate.SigV4 do
  @moduledoc """
  Minimal AWS SigV4 signer for outbound S3 requests. Hand-rolled to keep
  the dep footprint small — `aws_signature` and Req's AWS plugins both
  pull in transitive baggage we don't need for this single use site.

  Signs in the `s3` service namespace. Two payload modes:

  * `:hash` — full hex SHA-256 of the body (used for GETs and small PUTs
    where the caller already has the body in hand).
  * `:unsigned` — emits `x-amz-content-sha256: UNSIGNED-PAYLOAD`. Both
    SeaweedFS and kafun accept this; it lets us streaming-PUT without
    pre-hashing.

  Verified against the canonical AWS test vector (see test).
  """

  @signed_headers_default ~w(host x-amz-content-sha256 x-amz-date)

  @doc """
  Returns request headers (an enriched copy of `headers`) with `Authorization`,
  `x-amz-date`, and `x-amz-content-sha256` populated.

  `opts`:
    * `:access_key` — required
    * `:secret_key` — required (may be `""` for unsigned-network endpoints
       that just need the access key parsed; the signature will be wrong
       but kafun-style services don't verify it)
    * `:region` — default `"us-east-1"`
    * `:service` — default `"s3"`
    * `:payload` — `{:hash, body :: binary}` or `:unsigned`
    * `:now` — `DateTime` override for tests; default `DateTime.utc_now/0`
  """
  @spec sign(atom(), URI.t() | String.t(), [{String.t(), String.t()}], keyword()) ::
          [{String.t(), String.t()}]
  def sign(method, url, headers, opts) do
    uri = URI.parse(to_string(url))
    region = Keyword.get(opts, :region, "us-east-1")
    service = Keyword.get(opts, :service, "s3")
    access = Keyword.fetch!(opts, :access_key)
    secret = Keyword.fetch!(opts, :secret_key)
    now = Keyword.get(opts, :now, DateTime.utc_now())

    payload_hash =
      case Keyword.fetch!(opts, :payload) do
        :unsigned -> "UNSIGNED-PAYLOAD"
        {:hash, body} -> hex_sha256(body)
      end

    iso_time = Calendar.strftime(now, "%Y%m%dT%H%M%SZ")
    iso_date = String.slice(iso_time, 0..7//1)

    base_headers =
      headers
      |> List.keystore("host", 0, {"host", host_header(uri)})
      |> List.keystore("x-amz-content-sha256", 0, {"x-amz-content-sha256", payload_hash})
      |> List.keystore("x-amz-date", 0, {"x-amz-date", iso_time})

    signed_names =
      base_headers
      |> Enum.map(fn {k, _} -> String.downcase(k) end)
      |> Enum.uniq()
      |> Enum.sort()

    canonical_headers =
      signed_names
      |> Enum.map(fn name ->
        value =
          base_headers
          |> Enum.filter(fn {k, _} -> String.downcase(k) == name end)
          |> Enum.map(fn {_, v} -> String.trim(to_string(v)) end)
          |> Enum.join(",")

        "#{name}:#{value}\n"
      end)
      |> IO.iodata_to_binary()

    signed_headers_str = Enum.join(signed_names, ";")

    canonical_request =
      [
        method_string(method),
        "\n",
        canonical_uri(uri),
        "\n",
        canonical_query_string(uri),
        "\n",
        canonical_headers,
        "\n",
        signed_headers_str,
        "\n",
        payload_hash
      ]
      |> IO.iodata_to_binary()

    scope = "#{iso_date}/#{region}/#{service}/aws4_request"

    string_to_sign =
      "AWS4-HMAC-SHA256\n#{iso_time}\n#{scope}\n#{hex_sha256(canonical_request)}"

    signing_key = derive_signing_key(secret, iso_date, region, service)

    signature =
      :crypto.mac(:hmac, :sha256, signing_key, string_to_sign)
      |> Base.encode16(case: :lower)

    auth =
      "AWS4-HMAC-SHA256 Credential=#{access}/#{scope}," <>
        " SignedHeaders=#{signed_headers_str}," <>
        " Signature=#{signature}"

    base_headers ++ [{"authorization", auth}]
  end

  @doc "List of header names this signer always controls."
  def reserved_header_names, do: @signed_headers_default

  ## Internals

  defp method_string(:get), do: "GET"
  defp method_string(:put), do: "PUT"
  defp method_string(:post), do: "POST"
  defp method_string(:delete), do: "DELETE"
  defp method_string(:head), do: "HEAD"
  defp method_string(m) when is_atom(m), do: m |> Atom.to_string() |> String.upcase()
  defp method_string(m) when is_binary(m), do: String.upcase(m)

  defp host_header(%URI{host: h, port: nil}), do: h

  defp host_header(%URI{host: h, port: p, scheme: scheme}) do
    if (scheme == "https" and p == 443) or (scheme == "http" and p == 80) do
      h
    else
      "#{h}:#{p}"
    end
  end

  # AWS canonical URI: the path, with each segment URL-encoded *once* (and
  # *encoded again* for non-S3 services, but for `s3` the spec says encode
  # once). We always encode-once here because that's what S3 wants.
  defp canonical_uri(%URI{path: nil}), do: "/"
  defp canonical_uri(%URI{path: ""}), do: "/"

  defp canonical_uri(%URI{path: path}) do
    path
    |> String.split("/", trim: false)
    |> Enum.map_join("/", &uri_encode_segment/1)
  end

  defp canonical_query_string(%URI{query: nil}), do: ""
  defp canonical_query_string(%URI{query: ""}), do: ""

  defp canonical_query_string(%URI{query: q}) do
    q
    |> URI.decode_query()
    |> Enum.sort_by(fn {k, _} -> k end)
    |> Enum.map_join("&", fn {k, v} -> "#{uri_encode_segment(k)}=#{uri_encode_segment(v)}" end)
  end

  # Per AWS: do NOT encode A-Z a-z 0-9 - _ . ~ ; everything else gets %XX.
  defp uri_encode_segment(s) when is_binary(s) do
    for <<b <- s>>, into: "" do
      cond do
        b in ?A..?Z -> <<b>>
        b in ?a..?z -> <<b>>
        b in ?0..?9 -> <<b>>
        b in [?-, ?_, ?., ?~] -> <<b>>
        true -> "%" <> (b |> Integer.to_string(16) |> String.pad_leading(2, "0") |> String.upcase())
      end
    end
  end

  defp derive_signing_key(secret, date, region, service) do
    :crypto.mac(:hmac, :sha256, "AWS4" <> secret, date)
    |> then(&:crypto.mac(:hmac, :sha256, &1, region))
    |> then(&:crypto.mac(:hmac, :sha256, &1, service))
    |> then(&:crypto.mac(:hmac, :sha256, &1, "aws4_request"))
  end

  defp hex_sha256(data), do: :crypto.hash(:sha256, data) |> Base.encode16(case: :lower)
end
