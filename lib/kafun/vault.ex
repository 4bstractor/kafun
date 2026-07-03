defmodule Kafun.Vault do
  @moduledoc """
  Encryption at rest for `access_keys.secret`.

  Enabled by setting `KAFUN_MASTER_KEY` (any string ≥ 16 bytes; treat it
  like a password). The AES-256-GCM data key is derived from it with a
  versioned info string, so the master key itself never touches the DB.

  Stored format is `enc:v1:<base64(iv || tag || ciphertext)>`; anything
  without the prefix is legacy plaintext and passes through unchanged, so
  existing databases keep working the moment the env var appears. Boot
  auto-encrypts remaining plaintext rows (`Kafun.Index.encrypt_plaintext_secrets/0`).

  Empty secrets are never encrypted — the empty string is the
  "env-bootstrapped, signature verification skipped" sentinel and must stay
  recognizable as such.

  Rotation (release `rpc`):

      Kafun.Vault.rekey("old-master-key")   # re-encrypt under current KAFUN_MASTER_KEY
                                            # (or back to plaintext if it is now unset)

  Decrypt failures (wrong/missing master key, tampered row) fail closed:
  the ciphertext string is returned as the "secret", which can never match
  a signature or a Basic-auth credential.
  """

  require Logger

  @prefix "enc:v1:"
  @aad "kafun.access_keys"
  @info "kafun.vault.v1:"

  @spec enabled?() :: boolean()
  def enabled?, do: master_key() not in [nil, ""]

  @spec encrypted?(String.t()) :: boolean()
  def encrypted?(stored), do: String.starts_with?(stored, @prefix)

  @doc "Encrypt for storage. Passthrough when the vault is disabled or the secret is empty."
  @spec encrypt(String.t()) :: String.t()
  def encrypt(""), do: ""

  def encrypt(plaintext) do
    case master_key() do
      nil -> plaintext
      "" -> plaintext
      master -> encrypt_with(plaintext, master)
    end
  end

  @doc "Decrypt a stored value. Plaintext rows pass through. Fails closed (see moduledoc)."
  @spec decrypt(String.t()) :: String.t()
  def decrypt(@prefix <> _ = stored) do
    case master_key() do
      nil ->
        Logger.warning("kafun vault: encrypted secret but KAFUN_MASTER_KEY is not set")
        stored

      "" ->
        Logger.warning("kafun vault: encrypted secret but KAFUN_MASTER_KEY is not set")
        stored

      master ->
        case decrypt_with(stored, master) do
          {:ok, plaintext} ->
            plaintext

          :error ->
            Logger.warning("kafun vault: secret failed to decrypt (wrong master key or tampered row)")
            stored
        end
    end
  end

  def decrypt(stored), do: stored

  @doc """
  Re-encrypt every stored secret under the *current* `KAFUN_MASTER_KEY`,
  decrypting with `old_master`. With the vault currently disabled this
  rewrites rows back to plaintext. Returns `{:ok, rewritten_count}` or
  `{:error, {:undecryptable, [key_id]}}` (nothing is written on failure).
  """
  @spec rekey(String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def rekey(old_master), do: Kafun.Index.rekey_secrets(old_master)

  ## Internals — also used by Index.rekey_secrets/1 with an explicit master.

  @doc false
  def encrypt_with(plaintext, master) do
    iv = :crypto.strong_rand_bytes(12)

    {ct, tag} =
      :crypto.crypto_one_time_aead(:aes_256_gcm, derive(master), iv, plaintext, @aad, true)

    @prefix <> Base.encode64(iv <> tag <> ct)
  end

  @doc false
  def decrypt_with(@prefix <> b64, master) do
    with {:ok, <<iv::binary-12, tag::binary-16, ct::binary>>} <- Base.decode64(b64),
         plaintext when is_binary(plaintext) <-
           :crypto.crypto_one_time_aead(:aes_256_gcm, derive(master), iv, ct, @aad, tag, false) do
      {:ok, plaintext}
    else
      _ -> :error
    end
  end

  def decrypt_with(_, _), do: :error

  defp derive(master), do: :crypto.hash(:sha256, @info <> master)

  defp master_key, do: Application.get_env(:kafun, :master_key)
end
