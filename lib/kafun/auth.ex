defmodule Kafun.Auth do
  @moduledoc """
  Inbound authorization for the S3 surface. Single entry point: `authorize/2`.

  ## Flow

  1. Extract the SigV4 credentials from `Authorization` header (or `X-Amz-Credential`
     querystring for presigned URLs).
  2. Look up the access key in `Kafun.Index.access_keys`. If missing → `:unknown_key`;
     if revoked → `:revoked_key`; if active with empty secret (legacy
     env-bootstrap) → skip signature verification.
  3. Verify the SigV4 signature using `Kafun.Auth.SigV4.verify/2`. Mismatch
     → `:invalid_signature`.
  4. Look up `Kafun.Index.effective_grant/2` for `(key_id, bucket)`. Returns
     the highest tier across the specific-bucket grant and the `*` global
     grant.
  5. Compare the grant tier to the action's required tier
     (`:read | :write | :admin`). Insufficient → `:forbidden`.

  ## Anonymous fallthrough

  If step 1 fails (no credentials at all) AND the operation is `:read` AND
  the bucket has `public_read = true`, the request is allowed. Same code
  path is used for the `*`-keyed `bucket_grants` row, but the
  `buckets.public_read` boolean is the canonical UI knob.

  ## Backwards compatibility (env-bootstrapped keys)

  Keys created from `KAFUN_KEYS` on first boot land in the index with an
  empty secret. SigV4 signatures aren't verified for these — they keep the
  pre-ACL behavior so existing deployments survive the upgrade. Operators
  can rotate to a real secret via the admin UI to opt into signature
  verification.
  """

  alias Kafun.Auth.SigV4
  alias Kafun.Index

  @type action :: :read | :write | :admin

  @type reason ::
          :unauthenticated
          | :unknown_key
          | :revoked_key
          | :invalid_signature
          | :stream_signed_payload
          | :forbidden

  @doc """
  Authorize a request against a specific bucket.

  ## Options

    * `:action` — required. One of `:read | :write | :admin`.
    * `:bucket` — required. The bucket name being acted on.

  Returns `:ok` or `{:error, reason}`. The router maps each reason to an
  HTTP status + S3 error code.
  """
  @spec authorize(Plug.Conn.t(), keyword()) :: :ok | {:error, reason()}
  def authorize(conn, opts) do
    if auth_disabled?() do
      :ok
    else
      action = Keyword.fetch!(opts, :action)
      bucket = Keyword.fetch!(opts, :bucket)

      case SigV4.verify(conn, &lookup_secret/1) do
        {:ok, _verified_or_unverified, key_id} ->
          check_grant(key_id, bucket, action)

        {:error, :no_credentials} ->
          check_anonymous(bucket, action)

        {:error, _} = err ->
          err
      end
    end
  end

  @doc """
  Test/operator escape hatch — returns true when `KAFUN_AUTH_DISABLED=true`.
  Default is false. Set in `config/test.exs` so existing unsigned-conn tests
  keep passing; in production, the operator can flip it temporarily for
  recovery scenarios (locked out of admin, key rotation gone wrong).
  """
  def auth_disabled?, do: Application.get_env(:kafun, :auth_disabled?, false)

  @doc """
  Same as `authorize/2` but at the *service* level (no bucket — e.g. ListAllMyBuckets,
  or CreateBucket where the bucket doesn't yet exist). Returns `:ok` if the
  caller has any active grant; for `CreateBucket` the caller must hold a
  global `*` admin grant.
  """
  @spec authorize_service(Plug.Conn.t(), keyword()) :: {:ok, key_id :: String.t() | :anonymous} | {:error, reason()}
  def authorize_service(conn, opts) do
    action = Keyword.fetch!(opts, :action)

    if auth_disabled?() do
      {:ok, :anonymous}
    else
      authorize_service_real(conn, action)
    end
  end

  defp authorize_service_real(conn, action) do
    case SigV4.verify(conn, &lookup_secret/1) do
      {:ok, _, key_id} ->
        case action do
          :create_bucket ->
            if Index.effective_grant(key_id, "*") == :admin do
              {:ok, key_id}
            else
              {:error, :forbidden}
            end

          :list_buckets ->
            # Always allowed for any active key; `Kafun.Router` filters the
            # response down to the buckets this key has any grant on.
            {:ok, key_id}
        end

      {:error, :no_credentials} ->
        case action do
          :list_buckets -> {:ok, :anonymous}
          :create_bucket -> {:error, :unauthenticated}
        end

      {:error, _} = err ->
        err
    end
  end

  @doc """
  The list of buckets this caller is allowed to see in `ListAllMyBuckets`.
  Used by the router to filter the response. Anonymous callers see only
  buckets with `public_read = true`.
  """
  @spec accessible_buckets(String.t() | :anonymous) :: [String.t()]
  def accessible_buckets(:anonymous) do
    Index.list_buckets()
    |> Enum.filter(fn b -> Index.bucket_public_read?(b.name) end)
    |> Enum.map(& &1.name)
  end

  def accessible_buckets(key_id) when is_binary(key_id) do
    has_global = Index.effective_grant(key_id, "*") != :none

    if has_global do
      Index.list_buckets() |> Enum.map(& &1.name)
    else
      Index.list_grants_for_key(key_id) |> Enum.map(& &1.bucket) |> Enum.reject(&(&1 == "*"))
    end
  end

  ## Internals

  defp lookup_secret(key_id) do
    case Index.get_access_key(key_id) do
      :not_found ->
        :not_found

      {:ok, %{status: :revoked}} ->
        :revoked

      {:ok, %{secret: ""}} ->
        # Legacy env-bootstrapped key — skip signature verification.
        :empty_secret

      {:ok, %{secret: secret}} ->
        {:ok, secret}
    end
  end

  defp check_grant(key_id, bucket, action) do
    grant = Index.effective_grant(key_id, bucket)

    if satisfies?(grant, action) do
      Index.touch_access_key_last_used(key_id)
      :ok
    else
      {:error, :forbidden}
    end
  end

  defp check_anonymous(bucket, :read) do
    if Index.bucket_public_read?(bucket) do
      :ok
    else
      {:error, :unauthenticated}
    end
  end

  defp check_anonymous(_bucket, _action), do: {:error, :unauthenticated}

  # tier ordering: :admin > :write > :read > :none
  defp satisfies?(:admin, _required), do: true
  defp satisfies?(:write, :read), do: true
  defp satisfies?(:write, :write), do: true
  defp satisfies?(:read, :read), do: true
  defp satisfies?(_, _), do: false
end
