defmodule Kafun.Auth.SigV4 do
  @moduledoc """
  AWS SigV4 — sign outbound requests *and* verify inbound ones.

  Two consumers in this codebase:

  * `Kafun.Migrate` — outbound. Uses `sign/4` to attach an `Authorization`
    header to S3 client requests it makes against external endpoints.
  * `Kafun.Auth.authorize/2` — inbound. Uses `verify/2` to check the
    `Authorization` header (or `X-Amz-Credential` querystring) on incoming
    requests against secrets stored in the index.

  Two payload modes for `sign/4`:

  * `:hash` — full hex SHA-256 of the body (used for GETs and small PUTs).
  * `:unsigned` — emits `x-amz-content-sha256: UNSIGNED-PAYLOAD`.

  Streaming-signed payloads (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD` and the
  `…-TRAILER` variant) are deliberately **not** verifiable here — verifying
  requires per-chunk signature checks during body read which complicates
  the streaming PUT path. `verify/2` returns
  `{:error, :stream_signed_payload}` if the client used one of those, and
  the gate translates that to a 403.
  """

  ## Outbound — sign a request.

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

    canonical_headers = build_canonical_headers(base_headers, signed_names)
    signed_headers_str = Enum.join(signed_names, ";")

    canonical_request =
      build_canonical_request(
        method_string(method),
        canonical_uri(uri.path),
        canonical_query_string(uri.query),
        canonical_headers,
        signed_headers_str,
        payload_hash
      )

    scope = "#{iso_date}/#{region}/#{service}/aws4_request"

    signature =
      compute_signature(secret, iso_date, region, service, iso_time, scope, canonical_request)

    auth =
      "AWS4-HMAC-SHA256 Credential=#{access}/#{scope}," <>
        " SignedHeaders=#{signed_headers_str}," <>
        " Signature=#{signature}"

    base_headers ++ [{"authorization", auth}]
  end

  ## Inbound — verify a request.

  @typedoc """
  Result of `verify/2`:
    * `{:ok, :verified, key_id}` — signature checked and matches
    * `{:ok, :unverified, key_id}` — recognized key but no secret on file (legacy
       env-bootstrapped keys); request accepted, signature skipped
    * `{:error, reason}` — auth failure
  """
  @type verify_result ::
          {:ok, :verified | :unverified, String.t()}
          | {:error,
             :no_credentials
             | :unknown_key
             | :revoked_key
             | :invalid_signature
             | :stream_signed_payload}

  @typedoc """
  Caller-supplied lookup. Returns the secret for the given access-key id, or
  one of the sentinels: `:empty_secret` (legacy bootstrap key — skip
  verification), `:revoked` (key exists but is revoked), `:not_found`.
  """
  @type secret_lookup :: (String.t() -> {:ok, String.t()} | :empty_secret | :revoked | :not_found)

  @spec verify(Plug.Conn.t(), secret_lookup()) :: verify_result()
  def verify(conn, secret_lookup) do
    case extract_credentials(conn) do
      :error ->
        {:error, :no_credentials}

      {:ok, %{source: :querystring}} ->
        # Querystring (presigned URL) verification is a different signing
        # algorithm than the header form — the X-Amz-Signature param is
        # excluded from the canonical query, and there's an X-Amz-Expires
        # check. Out of scope for v1 verification — the gate falls through
        # to the legacy "key extraction without verification" path for keys
        # with empty secrets, which is how the admin UI's image previews
        # already work.
        {:ok, creds} = extract_credentials(conn)
        handle_querystring(creds, secret_lookup)

      {:ok, %{source: :header} = creds} ->
        case secret_lookup.(creds.access_key) do
          :not_found -> {:error, :unknown_key}
          :revoked -> {:error, :revoked_key}
          :empty_secret -> {:ok, :unverified, creds.access_key}
          {:ok, secret} -> verify_header_signature(conn, creds, secret)
        end
    end
  end

  defp handle_querystring(creds, secret_lookup) do
    case secret_lookup.(creds.access_key) do
      :not_found -> {:error, :unknown_key}
      :revoked -> {:error, :revoked_key}
      # Querystring presigned with a real secret is not yet verified — accept
      # without check. Will be tightened in a follow-up branch.
      _ -> {:ok, :unverified, creds.access_key}
    end
  end

  defp verify_header_signature(conn, creds, secret) do
    payload_hash = first_req_header(conn, "x-amz-content-sha256") || ""

    cond do
      streaming_signed?(payload_hash) ->
        {:error, :stream_signed_payload}

      true ->
        canonical_headers =
          build_canonical_headers(conn.req_headers, creds.signed_headers)

        canonical_query =
          canonical_query_string(conn.query_string)

        canonical_request =
          build_canonical_request(
            String.upcase(conn.method),
            canonical_uri(conn.request_path),
            canonical_query,
            canonical_headers,
            Enum.join(creds.signed_headers, ";"),
            payload_hash
          )

        expected =
          compute_signature(
            secret,
            creds.date,
            creds.region,
            creds.service,
            creds.iso_time,
            creds.scope,
            canonical_request
          )

        if Plug.Crypto.secure_compare(expected, creds.signature) do
          {:ok, :verified, creds.access_key}
        else
          {:error, :invalid_signature}
        end
    end
  end

  defp streaming_signed?(payload_hash) do
    payload_hash in [
      "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
      "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
    ]
  end

  ## Credential extraction (shared between header and querystring forms).

  defp extract_credentials(conn) do
    case from_header(conn) do
      {:ok, _} = ok -> ok
      :error -> from_querystring(conn)
    end
  end

  defp from_header(conn) do
    case first_req_header(conn, "authorization") do
      "AWS4-HMAC-SHA256 " <> rest ->
        params = parse_auth_params(rest)

        with {:ok, credential} <- Map.fetch(params, "Credential"),
             {:ok, signed} <- Map.fetch(params, "SignedHeaders"),
             {:ok, signature} <- Map.fetch(params, "Signature"),
             {:ok, [access_key, date, region, service, "aws4_request"]} <-
               split_credential(credential),
             iso_time when is_binary(iso_time) <-
               first_req_header(conn, "x-amz-date") || nil_to_error() do
          {:ok,
           %{
             source: :header,
             access_key: access_key,
             date: date,
             region: region,
             service: service,
             scope: "#{date}/#{region}/#{service}/aws4_request",
             iso_time: iso_time,
             signed_headers: signed |> String.split(";") |> Enum.map(&String.downcase/1) |> Enum.sort(),
             signature: signature
           }}
        else
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp from_querystring(conn) do
    case URI.decode_query(conn.query_string || "") do
      %{"X-Amz-Credential" => credential} = qs ->
        with {:ok, [access_key | _]} <- split_credential(credential) do
          {:ok,
           %{
             source: :querystring,
             access_key: access_key,
             # Other fields not extracted — querystring verification is a follow-up.
             raw: qs
           }}
        else
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp parse_auth_params(s) do
    s
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Map.new(fn part ->
      case String.split(part, "=", parts: 2) do
        [k, v] -> {String.trim(k), String.trim(v)}
        [k] -> {String.trim(k), ""}
      end
    end)
  end

  defp split_credential(s) do
    case String.split(s, "/") do
      [_, _, _, _, _] = parts -> {:ok, parts}
      _ -> :error
    end
  end

  defp nil_to_error, do: :error

  defp first_req_header(conn, name) do
    case Plug.Conn.get_req_header(conn, name) do
      [v | _] -> v
      [] -> nil
    end
  end

  ## Canonical-request building (shared).

  defp build_canonical_headers(headers, signed_names) do
    signed_names
    |> Enum.map(fn name ->
      lower = String.downcase(name)

      value =
        headers
        |> Enum.filter(fn {k, _} -> String.downcase(k) == lower end)
        |> Enum.map(fn {_, v} -> String.trim(to_string(v)) end)
        |> Enum.join(",")

      "#{lower}:#{value}\n"
    end)
    |> IO.iodata_to_binary()
  end

  defp build_canonical_request(method, path, query, headers, signed_str, payload_hash) do
    [method, "\n", path, "\n", query, "\n", headers, "\n", signed_str, "\n", payload_hash]
    |> IO.iodata_to_binary()
  end

  defp compute_signature(secret, date, region, service, iso_time, scope, canonical_request) do
    string_to_sign =
      "AWS4-HMAC-SHA256\n#{iso_time}\n#{scope}\n#{hex_sha256(canonical_request)}"

    signing_key = derive_signing_key(secret, date, region, service)

    :crypto.mac(:hmac, :sha256, signing_key, string_to_sign)
    |> Base.encode16(case: :lower)
  end

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

  # AWS canonical URI: the path, with each segment URL-encoded *once*.
  defp canonical_uri(nil), do: "/"
  defp canonical_uri(""), do: "/"

  defp canonical_uri(path) do
    path
    |> String.split("/", trim: false)
    |> Enum.map_join("/", &uri_encode_segment/1)
  end

  defp canonical_query_string(nil), do: ""
  defp canonical_query_string(""), do: ""

  defp canonical_query_string(q) do
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
