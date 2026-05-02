defmodule Kafun.Admin.Auth do
  @moduledoc """
  HTTP Basic Auth gate for the admin UI. Configured via `KAFUN_ADMIN_PASSWORD`
  (and optionally `KAFUN_ADMIN_USER`, default `admin`). Empty password leaves
  the UI open — same trusted-network model as the S3 surface.

  This is a deliberate single-shared-credential model. Multi-user support
  would need an actual identity table, which is out of scope for v1.
  """

  import Plug.Conn

  def init(opts), do: opts

  def call(conn, _opts) do
    case Application.get_env(:kafun, :admin_password) do
      nil ->
        conn

      "" ->
        conn

      password ->
        user = Application.get_env(:kafun, :admin_user, "admin")

        case get_req_header(conn, "authorization") do
          ["Basic " <> b64] ->
            check_basic(conn, b64, user, password)

          _ ->
            challenge(conn)
        end
    end
  end

  defp check_basic(conn, b64, user, password) do
    expected = user <> ":" <> password

    case Base.decode64(b64) do
      {:ok, ^expected} -> conn
      _ -> challenge(conn)
    end
  end

  defp challenge(conn) do
    conn
    |> put_resp_header("www-authenticate", ~s|Basic realm="kafun-admin", charset="UTF-8"|)
    |> send_resp(401, "authentication required\n")
    |> halt()
  end
end
