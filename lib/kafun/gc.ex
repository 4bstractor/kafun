defmodule Kafun.GC do
  @moduledoc """
  Periodic janitor. Three passes per tick:

  1. **Abandoned uploads.** Any `uploads` row older than `:abandon_after`
     seconds is aborted (parts removed, rows deleted). Catches multipart
     uploads that the client never finished and never explicitly aborted.

  2. **Orphan part dirs.** Walks `<root>/.uploads/` for subdirs that have
     **no** matching `uploads` row — these are crash-window orphans, where
     the index commit didn't land. Deletes the dirs.

  3. **Orphan blobs and leftover tmps.** Walks `<root>/<bucket>/<aa>/<bb>/`
     and deletes files older than `:blob_grace_seconds` that are either
     `.tmp.<rand>` leftovers from a crashed PUT, or regular blobs with no
     matching `objects` row (PUT crashed between `rename(2)` and the index
     commit). The grace window keeps us from racing legitimate in-flight PUTs.

  Set `KAFUN_GC_INTERVAL_SEC=0` to disable periodic sweeps; `Kafun.GC.run_now/0`
  triggers a sweep on demand regardless.
  """

  use GenServer
  require Logger
  alias Kafun.{Index, Multipart, Storage}

  @name __MODULE__

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: @name)

  @doc "Force a sweep right now. Returns the per-pass counters."
  @spec run_now() :: %{
          abandoned: non_neg_integer(),
          orphans: non_neg_integer(),
          orphan_blobs: non_neg_integer()
        }
  def run_now, do: GenServer.call(@name, :run_now, 60_000)

  @impl true
  def init(opts) do
    interval_ms = Keyword.fetch!(opts, :interval_ms)
    abandon_after = Keyword.fetch!(opts, :abandon_after_seconds)
    blob_grace = Keyword.fetch!(opts, :blob_grace_seconds)
    root = Keyword.fetch!(opts, :root)

    state = %{
      interval_ms: interval_ms,
      abandon_after: abandon_after,
      blob_grace: blob_grace,
      root: root
    }

    schedule(interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    _ = sweep(state)
    schedule(state.interval_ms)
    {:noreply, state}
  end

  @impl true
  def handle_call(:run_now, _from, state) do
    {:reply, sweep(state), state}
  end

  ## Internals.

  defp schedule(0), do: :ok
  defp schedule(ms) when ms > 0, do: Process.send_after(self(), :tick, ms)

  defp sweep(state) do
    started = System.monotonic_time(:microsecond)
    upload_cutoff = System.system_time(:second) - state.abandon_after
    blob_cutoff = System.system_time(:second) - state.blob_grace

    abandoned = sweep_abandoned_uploads(state.root, upload_cutoff)
    orphan_dirs = sweep_orphan_part_dirs(state.root)
    orphan_blobs = sweep_orphan_blobs(state.root, blob_cutoff)

    duration = System.monotonic_time(:microsecond) - started

    :telemetry.execute(
      [:kafun, :gc, :run],
      %{
        abandoned_uploads: abandoned,
        orphan_dirs: orphan_dirs,
        orphan_blobs: orphan_blobs,
        duration: duration
      },
      %{}
    )

    Logger.info(
      "kafun gc: abandoned=#{abandoned} orphan_dirs=#{orphan_dirs} " <>
        "orphan_blobs=#{orphan_blobs} duration_us=#{duration}"
    )

    %{abandoned: abandoned, orphans: orphan_dirs, orphan_blobs: orphan_blobs}
  end

  defp sweep_abandoned_uploads(root, cutoff) do
    cutoff
    |> Index.list_abandoned_uploads()
    |> Enum.reduce(0, fn id, n ->
      case Multipart.abort(root, id) do
        :ok -> n + 1
        {:error, _} -> n
      end
    end)
  end

  defp sweep_orphan_blobs(root, cutoff) do
    root
    |> Storage.list_blob_files()
    |> Enum.reduce(0, fn {bucket, name, mtime, path}, n ->
      cond do
        mtime > cutoff ->
          n

        String.starts_with?(name, ".tmp.") ->
          case File.rm(path) do
            :ok -> n + 1
            _ -> n
          end

        Index.get(bucket, name) == :not_found ->
          case File.rm(path) do
            :ok -> n + 1
            _ -> n
          end

        true ->
          n
      end
    end)
  end

  defp sweep_orphan_part_dirs(root) do
    dir = Path.join(root, ".uploads")

    case File.ls(dir) do
      {:ok, ids} ->
        Enum.reduce(ids, 0, fn id, n ->
          path = Path.join(dir, id)

          if File.dir?(path) and Index.get_upload(id) == :not_found do
            _ = File.rm_rf(path)
            n + 1
          else
            n
          end
        end)

      {:error, _} ->
        0
    end
  end
end
