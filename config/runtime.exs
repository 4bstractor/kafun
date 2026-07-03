import Config

default_root =
  case config_env() do
    :prod -> nil
    _ -> System.tmp_dir!() |> Path.join("kafun")
  end

root =
  System.get_env("KAFUN_ROOT") || default_root ||
    raise "KAFUN_ROOT must be set in prod"

allowed_keys =
  System.get_env("KAFUN_KEYS", "")
  |> String.split(",", trim: true)
  |> MapSet.new()

bootstrap_buckets =
  System.get_env("KAFUN_BOOTSTRAP_BUCKETS", "")
  |> String.split(",", trim: true)
  |> Enum.map(&String.trim/1)
  |> Enum.reject(&(&1 == ""))

config :kafun,
  root: root,
  db_path: System.get_env("KAFUN_DB") || Path.join(root, "index.db"),
  host: System.get_env("KAFUN_HOST", "0.0.0.0"),
  port: System.get_env("KAFUN_PORT", "8333") |> String.to_integer(),
  allowed_keys: allowed_keys,
  bootstrap_buckets: bootstrap_buckets,
  gc_interval_ms: System.get_env("KAFUN_GC_INTERVAL_SEC", "3600") |> String.to_integer() |> Kernel.*(1000),
  gc_abandon_after_seconds: System.get_env("KAFUN_GC_ABANDON_AFTER_SEC", "86400") |> String.to_integer(),
  gc_blob_grace_seconds: System.get_env("KAFUN_GC_BLOB_GRACE_SEC", "3600") |> String.to_integer()

if level = System.get_env("KAFUN_LOG_LEVEL") do
  config :logger, level: String.to_atom(level)
end

# Encryption at rest for access_keys.secret (see Kafun.Vault). Only set
# when explicitly present so tests can drive it via Application.put_env.
if master = System.get_env("KAFUN_MASTER_KEY") do
  config :kafun, master_key: master
end

# Set `auth_disabled?` only when KAFUN_AUTH_DISABLED is explicitly present.
# Otherwise leave it to compile-time config (config/test.exs sets true so
# unsigned-conn tests keep working). Default for prod (env unset) is the
# enforced state.
if val = System.get_env("KAFUN_AUTH_DISABLED") do
  config :kafun, auth_disabled?: String.downcase(val) in ["1", "true", "yes"]
end

## Admin UI runtime config.

admin_secret =
  case {config_env(), System.get_env("KAFUN_ADMIN_SECRET")} do
    {:prod, nil} ->
      raise """
      KAFUN_ADMIN_SECRET is required in production.
      Generate one with: mix phx.gen.secret  (or `openssl rand -base64 64`).
      Setting a stable secret means admin sessions survive a restart.
      """

    {_, nil} ->
      # Dev/test: fresh per boot. Sessions don't survive restarts.
      :crypto.strong_rand_bytes(64) |> Base.url_encode64() |> binary_part(0, 64)

    {_, s} when byte_size(s) >= 64 ->
      s

    {_, _} ->
      raise "KAFUN_ADMIN_SECRET must be at least 64 bytes when set"
  end

if config_env() == :prod and System.get_env("KAFUN_KEYS", "") == "" do
  IO.warn(
    "KAFUN_KEYS is empty — running with auth disabled in production. " <>
      "OK for trusted-LAN setups (the design point), but make sure that's intentional."
  )
end

{:ok, admin_ip} =
  System.get_env("KAFUN_ADMIN_HOST", "0.0.0.0")
  |> String.to_charlist()
  |> :inet.parse_address()

# Origins allowed to open the LiveView websocket. Comma-separated.
# Empty disables origin checking entirely (LAN-trusted; recommended for
# homelab + NPM front). Specify e.g.
#   KAFUN_ADMIN_ALLOWED_ORIGINS=https://kafun.harvelab.com,http://yomi:8334
# to lock it down.
admin_check_origin =
  case System.get_env("KAFUN_ADMIN_ALLOWED_ORIGINS", "") do
    "" ->
      false

    raw ->
      raw |> String.split(",", trim: true) |> Enum.map(&String.trim/1)
  end

config :kafun, Kafun.Admin.Endpoint,
  http: [
    ip: admin_ip,
    port: System.get_env("KAFUN_ADMIN_PORT", "8334") |> String.to_integer()
  ],
  secret_key_base: admin_secret,
  check_origin: admin_check_origin,
  server: Application.get_env(:kafun, :start_children?, true)

config :kafun,
  admin_password: System.get_env("KAFUN_ADMIN_PASSWORD"),
  admin_user: System.get_env("KAFUN_ADMIN_USER", "admin"),
  # Used by the admin UI's image-preview <img src=…>. Must be reachable
  # from the operator's browser. Typically the public S3 hostname.
  # Empty/unset falls back to `KAFUN_HOST:KAFUN_PORT` for local dev.
  public_s3_url: System.get_env("KAFUN_PUBLIC_S3_URL", ""),
  # Per-file cap for the admin UI drag-and-drop upload, in MiB. Browser-side
  # rejects files larger than this before they hit the server.
  admin_max_upload_mb: System.get_env("KAFUN_ADMIN_MAX_UPLOAD_MB", "256") |> String.to_integer()
