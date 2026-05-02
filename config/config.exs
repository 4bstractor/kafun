import Config

config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Compile-time Phoenix Endpoint config. Runtime overrides (port, secret) live
# in `runtime.exs`.
config :kafun, Kafun.Admin.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [html: Kafun.Admin.ErrorHTML],
    layout: false
  ],
  pubsub_server: Kafun.PubSub,
  live_view: [signing_salt: "kafun-admin-1"]

config :phoenix, :json_library, Jason

import_config "#{config_env()}.exs"
