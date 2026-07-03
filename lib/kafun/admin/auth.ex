defmodule Kafun.Admin.Auth do
  @moduledoc """
  HTTP Basic Auth gate for the admin UI. Two credential sources, checked in
  order:

  1. **Access keys** — any active key with the `admin_ui` flag and a
     non-empty secret authenticates as `<key id>:<secret>`. Toggled per key
     on the `/keys` page. This is the preferred model: per-client
     credentials, revocable and rotatable without touching env.
  2. **Legacy shared credential** — `KAFUN_ADMIN_USER` / `KAFUN_ADMIN_PASSWORD`
     env, kept for back-compat and as the bootstrap path (you need *some*
     way into the UI before the first admin_ui key exists).

  The UI is open (no auth) only when *neither* is configured: empty env
  password **and** no admin_ui-flagged keys — the original trusted-network
  model. Flagging a key therefore locks the UI to authenticated access.

  Comparisons are constant-time (`Plug.Crypto.secure_compare/2`). Key auth
  is skipped when the Index isn't running (bare plug tests).
  """

  import Plug.Conn

  def init(opts), do: opts

  def call(conn, _opts) do
    env_password = Application.get_env(:kafun, :admin_password)
    env_configured? = env_password not in [nil, ""]

    if env_configured? or admin_keys_available?() do
      case get_req_header(conn, "authorization") do
        ["Basic " <> b64] -> check_basic(conn, b64, env_password, env_configured?)
        _ -> challenge(conn)
      end
    else
      conn
    end
  end

  defp check_basic(conn, b64, env_password, env_configured?) do
    with {:ok, decoded} <- Base.decode64(b64),
         [user, pass] <- String.split(decoded, ":", parts: 2),
         true <- env_credential?(user, pass, env_password, env_configured?) or access_key?(user, pass) do
      conn
    else
      _ -> challenge(conn)
    end
  end

  defp env_credential?(user, pass, env_password, true) do
    env_user = Application.get_env(:kafun, :admin_user, "admin")
    secure_eq?(user, env_user) and secure_eq?(pass, env_password)
  end

  defp env_credential?(_user, _pass, _env_password, false), do: false

  defp access_key?(_id, ""), do: false

  defp access_key?(id, pass) do
    index_up?() and
      case Kafun.Index.get_access_key(id) do
        {:ok, %{status: :active, admin_ui: true, secret: secret}} when secret != "" ->
          if secure_eq?(pass, secret) do
            Kafun.Index.touch_access_key_last_used(id)
            true
          else
            false
          end

        _ ->
          false
      end
  end

  defp admin_keys_available?, do: index_up?() and Kafun.Index.admin_ui_keys?()

  defp index_up?, do: Process.whereis(Kafun.Index) != nil

  defp secure_eq?(a, b) when is_binary(a) and is_binary(b), do: Plug.Crypto.secure_compare(a, b)
  defp secure_eq?(_, _), do: false

  defp challenge(conn) do
    conn
    |> put_resp_header("www-authenticate", ~s|Basic realm="kafun-admin", charset="UTF-8"|)
    |> send_resp(401, "authentication required\n")
    |> halt()
  end
end
