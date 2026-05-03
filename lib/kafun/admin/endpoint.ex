defmodule Kafun.Admin.Endpoint do
  @moduledoc """
  Phoenix Endpoint for the Kafun admin UI. Bound to `KAFUN_ADMIN_PORT`
  (default 8334) on a separate port from the S3 surface so an operator
  can put it behind a different NPM upstream / firewall rule.
  """

  use Phoenix.Endpoint, otp_app: :kafun

  @session_options [
    store: :cookie,
    key: "_kafun_admin_key",
    signing_salt: "kafun-admin-1",
    same_site: "Lax"
  ]

  socket "/live", Phoenix.LiveView.Socket,
    websocket: [connect_info: [session: @session_options]]

  plug Plug.Static,
    at: "/",
    from: :kafun,
    gzip: false,
    only: ~w(assets favicon.ico favicon.png)

  plug Plug.RequestId
  plug Plug.Telemetry, event_prefix: [:kafun, :admin]

  plug Plug.Parsers,
    parsers: [:urlencoded, :multipart, :json],
    pass: ["*/*"],
    json_decoder: Phoenix.json_library()

  plug Plug.MethodOverride
  plug Plug.Head
  plug Plug.Session, @session_options
  plug Kafun.Admin.Router
end
