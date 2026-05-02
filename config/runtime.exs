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
  allowed_keys: allowed_keys

if level = System.get_env("KAFUN_LOG_LEVEL") do
  config :logger, level: String.to_atom(level)
end
