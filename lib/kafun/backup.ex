defmodule Kafun.Backup do
  @moduledoc """
  Index backup helper. Wraps `Kafun.Index.backup_to/1` with a sensible
  default destination (`/var/backups/kafun/kafun-<UTC-timestamp>.db`) and
  is safe to call from cron via the release's `rpc` command:

      /opt/kafun/bin/kafun rpc 'Kafun.Backup.run()'

  Returns the full path of the snapshot. Does not prune old snapshots —
  pair with a separate `find … -mtime +N -delete` cron entry.
  """

  require Logger

  @default_dir "/var/backups/kafun"

  @spec run(Path.t()) :: {:ok, Path.t()} | {:error, term()}
  def run(dir \\ @default_dir) do
    File.mkdir_p!(dir)
    ts = DateTime.utc_now() |> Calendar.strftime("%Y%m%d-%H%M%S")
    path = Path.join(dir, "kafun-#{ts}.db")

    case Kafun.Index.backup_to(path) do
      :ok ->
        size = File.stat!(path).size
        Logger.info("kafun backup: wrote #{path} (#{size} bytes)")
        {:ok, path}

      {:error, reason} ->
        Logger.error("kafun backup: failed: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
