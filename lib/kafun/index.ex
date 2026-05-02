defmodule Kafun.Index do
  @moduledoc """
  Metadata index backed by SQLite (WAL). Owns one connection through a
  GenServer, with prepared statements for the hot paths. Listing is an
  indexed range query on `(bucket, key)` — never a filesystem walk.
  """

  use GenServer
  alias Exqlite.Sqlite3

  @name __MODULE__

  defmodule State do
    @moduledoc false
    defstruct [:conn, :stmts]
  end

  ## Public API

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: @name)

  @spec put(String.t(), String.t(), non_neg_integer(), String.t(), String.t() | nil, integer()) ::
          :ok
  def put(bucket, key, size, etag, content_type, mtime) do
    GenServer.call(@name, {:put, bucket, key, size, etag, content_type, mtime})
  end

  @spec get(String.t(), String.t()) ::
          {:ok, %{size: non_neg_integer(), etag: String.t(), content_type: String.t() | nil, mtime: integer()}}
          | :not_found
  def get(bucket, key), do: GenServer.call(@name, {:get, bucket, key})

  @spec delete(String.t(), String.t()) :: :ok
  def delete(bucket, key), do: GenServer.call(@name, {:delete, bucket, key})

  @spec ensure_bucket(String.t()) :: :ok
  def ensure_bucket(name), do: GenServer.call(@name, {:ensure_bucket, name})

  @spec list_buckets() :: [%{name: String.t(), created_at: integer()}]
  def list_buckets, do: GenServer.call(@name, :list_buckets)

  @doc """
  Paginated list within a bucket. Options:
    * `:prefix` — keys must start with this string
    * `:start_after` — strict lower bound (used for continuation)
    * `:max_keys` — page size (default/cap 1000)

  Returns `{entries, truncated?, next_token}` where `next_token` is the last
  key of the page when truncated, otherwise `nil`.
  """
  @spec list(String.t(), keyword()) ::
          {[%{key: String.t(), size: non_neg_integer(), etag: String.t(), mtime: integer()}],
           boolean(), String.t() | nil}
  def list(bucket, opts \\ []), do: GenServer.call(@name, {:list, bucket, opts})

  @spec init_upload(String.t(), String.t(), String.t(), String.t() | nil) :: :ok
  def init_upload(upload_id, bucket, key, content_type) do
    GenServer.call(@name, {:init_upload, upload_id, bucket, key, content_type})
  end

  @spec get_upload(String.t()) ::
          {:ok, %{bucket: String.t(), key: String.t(), content_type: String.t() | nil}}
          | :not_found
  def get_upload(upload_id), do: GenServer.call(@name, {:get_upload, upload_id})

  @spec record_part(String.t(), pos_integer(), non_neg_integer(), String.t()) :: :ok
  def record_part(upload_id, part_number, size, etag) do
    GenServer.call(@name, {:record_part, upload_id, part_number, size, etag})
  end

  @spec list_parts(String.t()) ::
          [%{part_number: pos_integer(), size: non_neg_integer(), etag: String.t()}]
  def list_parts(upload_id), do: GenServer.call(@name, {:list_parts, upload_id})

  @spec clear_upload(String.t()) :: :ok
  def clear_upload(upload_id), do: GenServer.call(@name, {:clear_upload, upload_id})

  ## Server

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    db_path = Keyword.fetch!(opts, :db_path)
    File.mkdir_p!(Path.dirname(db_path))
    {:ok, conn} = Sqlite3.open(db_path)

    Enum.each(
      [
        "PRAGMA journal_mode = WAL",
        "PRAGMA synchronous = NORMAL",
        "PRAGMA temp_store = MEMORY",
        "PRAGMA mmap_size = 268435456",
        "PRAGMA busy_timeout = 5000",
        "PRAGMA foreign_keys = ON"
      ],
      &(:ok = Sqlite3.execute(conn, &1))
    )

    :ok =
      Sqlite3.execute(conn, """
      CREATE TABLE IF NOT EXISTS objects (
        bucket       TEXT NOT NULL,
        key          TEXT NOT NULL,
        size         INTEGER NOT NULL,
        etag         TEXT NOT NULL,
        content_type TEXT,
        mtime        INTEGER NOT NULL,
        PRIMARY KEY (bucket, key)
      ) WITHOUT ROWID
      """)

    :ok =
      Sqlite3.execute(conn, """
      CREATE TABLE IF NOT EXISTS buckets (
        name       TEXT PRIMARY KEY,
        created_at INTEGER NOT NULL
      ) WITHOUT ROWID
      """)

    :ok =
      Sqlite3.execute(conn, """
      CREATE TABLE IF NOT EXISTS uploads (
        upload_id    TEXT PRIMARY KEY,
        bucket       TEXT NOT NULL,
        key          TEXT NOT NULL,
        content_type TEXT,
        initiated_at INTEGER NOT NULL
      ) WITHOUT ROWID
      """)

    :ok =
      Sqlite3.execute(conn, """
      CREATE TABLE IF NOT EXISTS parts (
        upload_id   TEXT NOT NULL,
        part_number INTEGER NOT NULL,
        size        INTEGER NOT NULL,
        etag        TEXT NOT NULL,
        PRIMARY KEY (upload_id, part_number)
      ) WITHOUT ROWID
      """)

    {:ok, %State{conn: conn, stmts: prepare_all(conn)}}
  end

  defp prepare_all(conn) do
    %{
      put:
        prep(conn, """
        INSERT INTO objects (bucket, key, size, etag, content_type, mtime)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (bucket, key) DO UPDATE SET
          size = excluded.size,
          etag = excluded.etag,
          content_type = excluded.content_type,
          mtime = excluded.mtime
        """),
      get:
        prep(conn, """
        SELECT size, etag, content_type, mtime FROM objects
        WHERE bucket = ? AND key = ?
        """),
      delete: prep(conn, "DELETE FROM objects WHERE bucket = ? AND key = ?"),
      ensure_bucket:
        prep(conn, "INSERT OR IGNORE INTO buckets (name, created_at) VALUES (?, ?)"),
      list_buckets: prep(conn, "SELECT name, created_at FROM buckets ORDER BY name"),
      list_all:
        prep(conn, """
        SELECT key, size, etag, mtime FROM objects
        WHERE bucket = ? AND key > ?
        ORDER BY key LIMIT ?
        """),
      list_prefix:
        prep(conn, """
        SELECT key, size, etag, mtime FROM objects
        WHERE bucket = ? AND key > ? AND key >= ? AND key < ?
        ORDER BY key LIMIT ?
        """),
      init_upload:
        prep(conn, """
        INSERT INTO uploads (upload_id, bucket, key, content_type, initiated_at)
        VALUES (?, ?, ?, ?, ?)
        """),
      get_upload:
        prep(conn, """
        SELECT bucket, key, content_type FROM uploads WHERE upload_id = ?
        """),
      record_part:
        prep(conn, """
        INSERT INTO parts (upload_id, part_number, size, etag)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (upload_id, part_number) DO UPDATE SET
          size = excluded.size, etag = excluded.etag
        """),
      list_parts:
        prep(conn, """
        SELECT part_number, size, etag FROM parts
        WHERE upload_id = ? ORDER BY part_number
        """),
      drop_parts: prep(conn, "DELETE FROM parts WHERE upload_id = ?"),
      drop_upload: prep(conn, "DELETE FROM uploads WHERE upload_id = ?")
    }
  end

  defp prep(conn, sql) do
    {:ok, stmt} = Sqlite3.prepare(conn, sql)
    stmt
  end

  @impl true
  def handle_call({:put, b, k, sz, etag, ct, mt}, _from, state) do
    run(state, :put, [b, k, sz, etag, ct, mt])
    ensure_bucket_inline(state, b)
    {:reply, :ok, state}
  end

  def handle_call({:get, b, k}, _from, state) do
    case fetch_one(state, :get, [b, k]) do
      [size, etag, ct, mtime] ->
        {:reply, {:ok, %{size: size, etag: etag, content_type: ct, mtime: mtime}}, state}

      nil ->
        {:reply, :not_found, state}
    end
  end

  def handle_call({:delete, b, k}, _from, state) do
    run(state, :delete, [b, k])
    {:reply, :ok, state}
  end

  def handle_call({:ensure_bucket, name}, _from, state) do
    ensure_bucket_inline(state, name)
    {:reply, :ok, state}
  end

  def handle_call(:list_buckets, _from, state) do
    rows = fetch_all(state, :list_buckets, [])
    out = Enum.map(rows, fn [n, ts] -> %{name: n, created_at: ts} end)
    {:reply, out, state}
  end

  def handle_call({:list, bucket, opts}, _from, state) do
    prefix = Keyword.get(opts, :prefix, "")
    start_after = Keyword.get(opts, :start_after, "")
    max_keys = opts |> Keyword.get(:max_keys, 1000) |> max(1) |> min(1000)

    rows =
      case prefix do
        "" ->
          fetch_all(state, :list_all, [bucket, start_after, max_keys + 1])

        _ ->
          case upper_bound(prefix) do
            nil ->
              # Prefix is all 0xFF — degenerate; fall back to >= prefix only.
              # Cheap path: use list_all with start_after = max(start_after, prefix - epsilon).
              fetch_all(state, :list_all, [bucket, max(start_after, prefix), max_keys + 1])

            upper ->
              fetch_all(state, :list_prefix, [bucket, start_after, prefix, upper, max_keys + 1])
          end
      end

    {entries, truncated} =
      if length(rows) > max_keys do
        {Enum.take(rows, max_keys), true}
      else
        {rows, false}
      end

    out =
      Enum.map(entries, fn [k, sz, etag, mt] ->
        %{key: k, size: sz, etag: etag, mtime: mt}
      end)

    next = if truncated, do: List.last(out).key, else: nil
    {:reply, {out, truncated, next}, state}
  end

  def handle_call({:init_upload, id, b, k, ct}, _from, state) do
    run(state, :init_upload, [id, b, k, ct, System.system_time(:second)])
    {:reply, :ok, state}
  end

  def handle_call({:get_upload, id}, _from, state) do
    case fetch_one(state, :get_upload, [id]) do
      [b, k, ct] -> {:reply, {:ok, %{bucket: b, key: k, content_type: ct}}, state}
      nil -> {:reply, :not_found, state}
    end
  end

  def handle_call({:record_part, id, n, sz, etag}, _from, state) do
    run(state, :record_part, [id, n, sz, etag])
    {:reply, :ok, state}
  end

  def handle_call({:list_parts, id}, _from, state) do
    rows = fetch_all(state, :list_parts, [id])
    out = Enum.map(rows, fn [n, sz, etag] -> %{part_number: n, size: sz, etag: etag} end)
    {:reply, out, state}
  end

  def handle_call({:clear_upload, id}, _from, state) do
    run(state, :drop_parts, [id])
    run(state, :drop_upload, [id])
    {:reply, :ok, state}
  end

  @impl true
  def terminate(_reason, %State{conn: conn, stmts: stmts}) do
    Enum.each(stmts, fn {_, s} -> Sqlite3.release(conn, s) end)
    Sqlite3.close(conn)
    :ok
  end

  ## Internals

  defp run(%State{conn: conn, stmts: stmts}, key, args) do
    s = Map.fetch!(stmts, key)
    :ok = Sqlite3.bind(s, args)
    :done = Sqlite3.step(conn, s)
    :ok = Sqlite3.reset(s)
  end

  defp fetch_one(%State{conn: conn, stmts: stmts}, key, args) do
    s = Map.fetch!(stmts, key)
    :ok = Sqlite3.bind(s, args)

    result =
      case Sqlite3.step(conn, s) do
        {:row, row} -> row
        :done -> nil
      end

    :ok = Sqlite3.reset(s)
    result
  end

  defp fetch_all(%State{conn: conn, stmts: stmts}, key, args) do
    s = Map.fetch!(stmts, key)
    :ok = Sqlite3.bind(s, args)
    rows = drain(conn, s, [])
    :ok = Sqlite3.reset(s)
    rows
  end

  defp drain(conn, stmt, acc) do
    case Sqlite3.step(conn, stmt) do
      {:row, row} -> drain(conn, stmt, [row | acc])
      :done -> Enum.reverse(acc)
    end
  end

  defp ensure_bucket_inline(state, name) do
    run(state, :ensure_bucket, [name, System.system_time(:second)])
  end

  @doc false
  # Smallest binary strictly greater than every binary starting with `prefix`.
  # Returns nil if `prefix` is all 0xFF (no upper bound expressible).
  def upper_bound(prefix) when is_binary(prefix) do
    case do_upper(:binary.bin_to_list(prefix) |> Enum.reverse()) do
      :overflow -> nil
      rev -> rev |> Enum.reverse() |> :binary.list_to_bin()
    end
  end

  defp do_upper([0xFF | rest]), do: do_upper(rest)
  defp do_upper([b | rest]), do: [b + 1 | rest]
  defp do_upper([]), do: :overflow
end
