defmodule Kafun.Application do
  @moduledoc false
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    children =
      if Application.get_env(:kafun, :start_children?, true) do
        root = Application.fetch_env!(:kafun, :root)
        db_path = Application.fetch_env!(:kafun, :db_path)
        host = Application.fetch_env!(:kafun, :host)
        port = Application.fetch_env!(:kafun, :port)

        gc_interval_ms = Application.fetch_env!(:kafun, :gc_interval_ms)
        gc_abandon_after = Application.fetch_env!(:kafun, :gc_abandon_after_seconds)
        gc_blob_grace = Application.fetch_env!(:kafun, :gc_blob_grace_seconds)

        File.mkdir_p!(root)
        Logger.info("kafun starting: root=#{root} db=#{db_path} bind=#{host}:#{port}")

        [
          {Phoenix.PubSub, name: Kafun.PubSub},
          {Kafun.Index, db_path: db_path},
          {Kafun.GC,
           root: root,
           interval_ms: gc_interval_ms,
           abandon_after_seconds: gc_abandon_after,
           blob_grace_seconds: gc_blob_grace},
          {Bandit, plug: Kafun.Router, port: port, ip: parse_ip(host)},
          Kafun.Admin.Endpoint
        ]
      else
        []
      end

    result = Supervisor.start_link(children, strategy: :one_for_one, name: Kafun.Supervisor)

    if Application.get_env(:kafun, :start_children?, true) do
      bootstrap_buckets()
      bootstrap_env_keys()
    end

    result
  end

  # Migrate KAFUN_KEYS env entries into the access_keys table on first boot
  # (idempotent on subsequent boots). Each env key gets:
  #
  #   * a row in access_keys with empty secret (legacy unverified mode)
  #   * a global admin grant (`bucket_grants(<key>, "*", "admin")`)
  #
  # That preserves pre-ACL behavior — env keys keep skipping signature
  # verification and have full access. Operators can rotate to a real
  # secret via the admin UI to opt into SigV4 verification, or revoke
  # the env-bootstrapped key entirely once new keys are in place.
  defp bootstrap_env_keys do
    keys = Application.fetch_env!(:kafun, :allowed_keys)

    case MapSet.size(keys) do
      0 ->
        :ok

      n ->
        Logger.info("kafun bootstrap: ensuring #{n} env-key access record(s)")

        Enum.each(keys, fn key_id ->
          case Kafun.Index.get_access_key(key_id) do
            :not_found ->
              :ok = Kafun.Index.create_access_key(key_id, "", "env-bootstrap (KAFUN_KEYS)")
              :ok = Kafun.Index.upsert_grant(key_id, "*", :admin)

            {:ok, _} ->
              # Already in DB — leave alone. Operator may have rotated the
              # secret; we don't want to clobber their grants either.
              :ok
          end
        end)
    end
  end

  # Pre-create the buckets listed in `KAFUN_BOOTSTRAP_BUCKETS` so a fresh
  # deployment doesn't have to PUT each one with curl before clients can
  # push. Idempotent — `ensure_bucket/1` is `INSERT OR IGNORE`, and
  # `mkdir_p` is happy with an existing dir. Invalid bucket names are
  # logged and skipped rather than crashing the boot.
  defp bootstrap_buckets do
    root = Application.fetch_env!(:kafun, :root)

    case Application.get_env(:kafun, :bootstrap_buckets, []) do
      [] ->
        :ok

      names ->
        Logger.info("kafun bootstrap: ensuring #{length(names)} bucket(s)")

        Enum.each(names, fn name ->
          if Kafun.Storage.valid_bucket?(name) do
            :ok = Kafun.Index.ensure_bucket(name)
            File.mkdir_p!(Path.join(root, name))
          else
            Logger.warning("kafun bootstrap: skipping invalid bucket name #{inspect(name)}")
          end
        end)
    end
  end

  defp parse_ip("0.0.0.0"), do: {0, 0, 0, 0}
  defp parse_ip("127.0.0.1"), do: {127, 0, 0, 1}
  defp parse_ip("::"), do: {0, 0, 0, 0, 0, 0, 0, 0}

  defp parse_ip(host) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, ip} -> ip
      {:error, _} -> raise "invalid KAFUN_HOST: #{host}"
    end
  end
end
