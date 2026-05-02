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

config :kafun,
  root: root,
  db_path: System.get_env("KAFUN_DB") || Path.join(root, "index.db"),
  host: System.get_env("KAFUN_HOST", "0.0.0.0"),
  port: System.get_env("KAFUN_PORT", "8333") |> String.to_integer(),
  allowed_keys: allowed_keys,
  gc_interval_ms: System.get_env("KAFUN_GC_INTERVAL_SEC", "3600") |> String.to_integer() |> Kernel.*(1000),
  gc_abandon_after_seconds: System.get_env("KAFUN_GC_ABANDON_AFTER_SEC", "86400") |> String.to_integer(),
  gc_blob_grace_seconds: System.get_env("KAFUN_GC_BLOB_GRACE_SEC", "3600") |> String.to_integer()

if level = System.get_env("KAFUN_LOG_LEVEL") do
  config :logger, level: String.to_atom(level)
end

## Admin UI runtime config.

admin_secret =
  case System.get_env("KAFUN_ADMIN_SECRET") do
    nil ->
      # Fresh per boot — sessions don't survive restarts. Set the env var
      # to keep a stable signing key.
      :crypto.strong_rand_bytes(64) |> Base.url_encode64() |> binary_part(0, 64)

    s when byte_size(s) >= 64 ->
      s

    _ ->
      raise "KAFUN_ADMIN_SECRET must be at least 64 bytes when set"
  end

{:ok, admin_ip} =
  System.get_env("KAFUN_ADMIN_HOST", "0.0.0.0")
  |> String.to_charlist()
  |> :inet.parse_address()

config :kafun, Kafun.Admin.Endpoint,
  http: [
    ip: admin_ip,
    port: System.get_env("KAFUN_ADMIN_PORT", "8334") |> String.to_integer()
  ],
  secret_key_base: admin_secret,
  server: Application.get_env(:kafun, :start_children?, true)

config :kafun,
  admin_password: System.get_env("KAFUN_ADMIN_PASSWORD"),
  admin_user: System.get_env("KAFUN_ADMIN_USER", "admin")
