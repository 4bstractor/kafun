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

  @spec put(String.t(), String.t(), non_neg_integer(), String.t(), String.t() | nil, integer(),
            %{optional(String.t()) => String.t()}) :: :ok
  def put(bucket, key, size, etag, content_type, mtime, meta \\ %{}) do
    GenServer.call(@name, {:put, bucket, key, size, etag, content_type, mtime, meta})
  end

  @spec get(String.t(), String.t()) ::
          {:ok,
           %{
             size: non_neg_integer(),
             etag: String.t(),
             content_type: String.t() | nil,
             mtime: integer(),
             meta: %{optional(String.t()) => String.t()}
           }}
          | :not_found
  def get(bucket, key), do: GenServer.call(@name, {:get, bucket, key})

  @spec delete(String.t(), String.t()) :: :ok
  def delete(bucket, key), do: GenServer.call(@name, {:delete, bucket, key})

  @spec ensure_bucket(String.t()) :: :ok
  def ensure_bucket(name), do: GenServer.call(@name, {:ensure_bucket, name})

  @spec list_buckets() :: [%{name: String.t(), created_at: integer()}]
  def list_buckets, do: GenServer.call(@name, :list_buckets)

  @spec bucket_exists?(String.t()) :: boolean()
  def bucket_exists?(name), do: GenServer.call(@name, {:bucket_exists?, name})

  @doc """
  Drop a bucket from the index. Errors if it doesn't exist or still holds
  any objects (S3's `BucketNotEmpty` semantics). Removing the on-disk
  shard tree is the caller's job.
  """
  @spec delete_bucket(String.t()) :: :ok | {:error, :not_found | :not_empty}
  def delete_bucket(name), do: GenServer.call(@name, {:delete_bucket, name})

  @doc """
  ListObjectsV2 with prefix + delimiter + pagination.

  Options:
    * `:prefix` — string prefix
    * `:delimiter` — string delimiter (typically "/") to roll up common prefixes
    * `:start_after` — strict (`>`) lower bound from S3 client
    * `:continuation` — inclusive (`>=`) lower bound used for our own continuation tokens
    * `:max_keys` — page size cap (default/cap 1000)

  Returns `{entries, common_prefixes, truncated?, next_lower_bound}`. When truncated,
  `next_lower_bound` is the inclusive lower bound for the next page (caller encodes it).
  """
  @spec list(String.t(), keyword()) ::
          {[%{key: String.t(), size: non_neg_integer(), etag: String.t(), mtime: integer()}],
           [String.t()],
           boolean(),
           String.t() | nil}
  def list(bucket, opts \\ []), do: GenServer.call(@name, {:list, bucket, opts})

  @spec init_upload(String.t(), String.t(), String.t(), String.t() | nil,
                    %{optional(String.t()) => String.t()}) :: :ok
  def init_upload(upload_id, bucket, key, content_type, meta \\ %{}) do
    GenServer.call(@name, {:init_upload, upload_id, bucket, key, content_type, meta})
  end

  @spec get_upload(String.t()) ::
          {:ok,
           %{
             bucket: String.t(),
             key: String.t(),
             content_type: String.t() | nil,
             meta: %{optional(String.t()) => String.t()}
           }}
          | :not_found
  def get_upload(upload_id), do: GenServer.call(@name, {:get_upload, upload_id})

  @spec record_part(String.t(), pos_integer(), non_neg_integer(), String.t(), integer()) :: :ok
  def record_part(upload_id, part_number, size, etag, mtime) do
    GenServer.call(@name, {:record_part, upload_id, part_number, size, etag, mtime})
  end

  @spec list_parts(String.t()) ::
          [%{part_number: pos_integer(), size: non_neg_integer(), etag: String.t(), mtime: integer()}]
  def list_parts(upload_id), do: GenServer.call(@name, {:list_parts, upload_id})

  @doc """
  Paginated `parts` listing. `part_number_marker` is exclusive (S3 semantics).
  Returns `{entries, truncated?, next_marker}`.
  """
  @spec list_parts_paged(String.t(), keyword()) ::
          {[%{part_number: pos_integer(), size: non_neg_integer(), etag: String.t(), mtime: integer()}],
           boolean(), pos_integer() | nil}
  def list_parts_paged(upload_id, opts \\ []) do
    GenServer.call(@name, {:list_parts_paged, upload_id, opts})
  end

  @doc """
  Paginated multipart-upload listing for a bucket.

  Options: `:prefix`, `:key_marker` (default ""), `:upload_id_marker` (default ""),
  `:max_uploads` (default/cap 1000). Cursor is the `(key, upload_id)` pair —
  matches S3 semantics. Returns `{entries, truncated?, next_key, next_upload_id}`.
  """
  @spec list_uploads(String.t(), keyword()) ::
          {[%{key: String.t(), upload_id: String.t(), initiated_at: integer()}],
           boolean(), String.t() | nil, String.t() | nil}
  def list_uploads(bucket, opts \\ []), do: GenServer.call(@name, {:list_uploads, bucket, opts})

  @spec clear_upload(String.t()) :: :ok
  def clear_upload(upload_id), do: GenServer.call(@name, {:clear_upload, upload_id})

  @spec list_abandoned_uploads(integer()) :: [String.t()]
  def list_abandoned_uploads(before_unix_seconds) do
    GenServer.call(@name, {:list_abandoned_uploads, before_unix_seconds})
  end

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
        meta         TEXT NOT NULL DEFAULT '{}',
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
        initiated_at INTEGER NOT NULL,
        meta         TEXT NOT NULL DEFAULT '{}'
      ) WITHOUT ROWID
      """)

    :ok =
      Sqlite3.execute(conn, """
      CREATE TABLE IF NOT EXISTS parts (
        upload_id   TEXT NOT NULL,
        part_number INTEGER NOT NULL,
        size        INTEGER NOT NULL,
        etag        TEXT NOT NULL,
        mtime       INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (upload_id, part_number)
      ) WITHOUT ROWID
      """)

    # Idempotent migrations for legacy DBs.
    Enum.each(
      [
        "ALTER TABLE parts ADD COLUMN mtime INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE objects ADD COLUMN meta TEXT NOT NULL DEFAULT '{}'",
        "ALTER TABLE uploads ADD COLUMN meta TEXT NOT NULL DEFAULT '{}'"
      ],
      fn sql ->
        case Sqlite3.execute(conn, sql) do
          :ok -> :ok
          {:error, _already_exists} -> :ok
        end
      end
    )

    # Indexes for the multipart-listing and GC paths. PK on `uploads` is
    # `upload_id` so listing-by-bucket would otherwise scan the whole table.
    :ok =
      Sqlite3.execute(conn, """
      CREATE INDEX IF NOT EXISTS uploads_bucket_key
      ON uploads(bucket, key, upload_id)
      """)

    :ok =
      Sqlite3.execute(conn, """
      CREATE INDEX IF NOT EXISTS uploads_initiated_at
      ON uploads(initiated_at)
      """)

    {:ok, %State{conn: conn, stmts: prepare_all(conn)}}
  end

  defp prepare_all(conn) do
    %{
      put:
        prep(conn, """
        INSERT INTO objects (bucket, key, size, etag, content_type, mtime, meta)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (bucket, key) DO UPDATE SET
          size = excluded.size,
          etag = excluded.etag,
          content_type = excluded.content_type,
          mtime = excluded.mtime,
          meta = excluded.meta
        """),
      get:
        prep(conn, """
        SELECT size, etag, content_type, mtime, meta FROM objects
        WHERE bucket = ? AND key = ?
        """),
      delete: prep(conn, "DELETE FROM objects WHERE bucket = ? AND key = ?"),
      ensure_bucket:
        prep(conn, "INSERT OR IGNORE INTO buckets (name, created_at) VALUES (?, ?)"),
      list_buckets: prep(conn, "SELECT name, created_at FROM buckets ORDER BY name"),
      bucket_exists: prep(conn, "SELECT 1 FROM buckets WHERE name = ? LIMIT 1"),
      bucket_has_objects: prep(conn, "SELECT 1 FROM objects WHERE bucket = ? LIMIT 1"),
      delete_bucket: prep(conn, "DELETE FROM buckets WHERE name = ?"),
      list_open:
        prep(conn, """
        SELECT key, size, etag, mtime FROM objects
        WHERE bucket = ? AND key >= ?
        ORDER BY key LIMIT ?
        """),
      list_range:
        prep(conn, """
        SELECT key, size, etag, mtime FROM objects
        WHERE bucket = ? AND key >= ? AND key < ?
        ORDER BY key LIMIT ?
        """),
      init_upload:
        prep(conn, """
        INSERT INTO uploads (upload_id, bucket, key, content_type, initiated_at, meta)
        VALUES (?, ?, ?, ?, ?, ?)
        """),
      get_upload:
        prep(conn, """
        SELECT bucket, key, content_type, meta FROM uploads WHERE upload_id = ?
        """),
      record_part:
        prep(conn, """
        INSERT INTO parts (upload_id, part_number, size, etag, mtime)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (upload_id, part_number) DO UPDATE SET
          size = excluded.size, etag = excluded.etag, mtime = excluded.mtime
        """),
      list_parts:
        prep(conn, """
        SELECT part_number, size, etag, mtime FROM parts
        WHERE upload_id = ? ORDER BY part_number
        """),
      list_parts_paged:
        prep(conn, """
        SELECT part_number, size, etag, mtime FROM parts
        WHERE upload_id = ? AND part_number > ?
        ORDER BY part_number LIMIT ?
        """),
      drop_parts: prep(conn, "DELETE FROM parts WHERE upload_id = ?"),
      drop_upload: prep(conn, "DELETE FROM uploads WHERE upload_id = ?"),
      abandoned_uploads:
        prep(conn, "SELECT upload_id FROM uploads WHERE initiated_at < ? ORDER BY initiated_at"),
      list_uploads_all:
        prep(conn, """
        SELECT key, upload_id, initiated_at FROM uploads
        WHERE bucket = ?
          AND (key > ? OR (key = ? AND upload_id > ?))
        ORDER BY key, upload_id LIMIT ?
        """),
      list_uploads_prefix:
        prep(conn, """
        SELECT key, upload_id, initiated_at FROM uploads
        WHERE bucket = ?
          AND (key > ? OR (key = ? AND upload_id > ?))
          AND key >= ? AND key < ?
        ORDER BY key, upload_id LIMIT ?
        """)
    }
  end

  defp prep(conn, sql) do
    {:ok, stmt} = Sqlite3.prepare(conn, sql)
    stmt
  end

  @impl true
  def handle_call({:put, b, k, sz, etag, ct, mt, meta}, _from, state) do
    run(state, :put, [b, k, sz, etag, ct, mt, encode_meta(meta)])
    ensure_bucket_inline(state, b)
    {:reply, :ok, state}
  end

  def handle_call({:get, b, k}, _from, state) do
    case fetch_one(state, :get, [b, k]) do
      [size, etag, ct, mtime, meta_json] ->
        {:reply,
         {:ok,
          %{
            size: size,
            etag: etag,
            content_type: ct,
            mtime: mtime,
            meta: decode_meta(meta_json)
          }}, state}

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

  def handle_call({:bucket_exists?, name}, _from, state) do
    exists =
      case fetch_one(state, :bucket_exists, [name]) do
        [_] -> true
        _ -> false
      end

    {:reply, exists, state}
  end

  def handle_call({:delete_bucket, name}, _from, state) do
    reply =
      cond do
        fetch_one(state, :bucket_exists, [name]) == nil ->
          {:error, :not_found}

        fetch_one(state, :bucket_has_objects, [name]) != nil ->
          {:error, :not_empty}

        true ->
          run(state, :delete_bucket, [name])
          :ok
      end

    {:reply, reply, state}
  end

  def handle_call({:list, bucket, opts}, _from, state) do
    prefix = Keyword.get(opts, :prefix, "")
    delimiter = Keyword.get(opts, :delimiter)
    max_keys = opts |> Keyword.get(:max_keys, 1000) |> max(1) |> min(1000)

    initial_lower =
      cond do
        cont = opts[:continuation] -> max(cont, prefix)
        sa = opts[:start_after] -> max(sa <> <<0>>, prefix)
        true -> prefix
      end

    upper = if prefix == "", do: nil, else: upper_bound(prefix)

    result = scan(state, bucket, prefix, delimiter, initial_lower, upper, max_keys)
    {:reply, result, state}
  end

  def handle_call({:init_upload, id, b, k, ct, meta}, _from, state) do
    run(state, :init_upload, [id, b, k, ct, System.system_time(:second), encode_meta(meta)])
    {:reply, :ok, state}
  end

  def handle_call({:get_upload, id}, _from, state) do
    case fetch_one(state, :get_upload, [id]) do
      [b, k, ct, meta_json] ->
        {:reply,
         {:ok, %{bucket: b, key: k, content_type: ct, meta: decode_meta(meta_json)}}, state}

      nil ->
        {:reply, :not_found, state}
    end
  end

  def handle_call({:record_part, id, n, sz, etag, mtime}, _from, state) do
    run(state, :record_part, [id, n, sz, etag, mtime])
    {:reply, :ok, state}
  end

  def handle_call({:list_parts, id}, _from, state) do
    rows = fetch_all(state, :list_parts, [id])
    out =
      Enum.map(rows, fn [n, sz, etag, mtime] ->
        %{part_number: n, size: sz, etag: etag, mtime: mtime}
      end)
    {:reply, out, state}
  end

  def handle_call({:list_parts_paged, id, opts}, _from, state) do
    marker = Keyword.get(opts, :part_number_marker, 0)
    max_parts = opts |> Keyword.get(:max_parts, 1000) |> max(1) |> min(1000)
    rows = fetch_all(state, :list_parts_paged, [id, marker, max_parts + 1])

    {entries, truncated} =
      if length(rows) > max_parts do
        {Enum.take(rows, max_parts), true}
      else
        {rows, false}
      end

    out =
      Enum.map(entries, fn [n, sz, etag, mtime] ->
        %{part_number: n, size: sz, etag: etag, mtime: mtime}
      end)

    next = if truncated, do: List.last(out).part_number, else: nil
    {:reply, {out, truncated, next}, state}
  end

  def handle_call({:list_uploads, bucket, opts}, _from, state) do
    prefix = Keyword.get(opts, :prefix, "")
    key_marker = Keyword.get(opts, :key_marker, "")
    upload_id_marker = Keyword.get(opts, :upload_id_marker, "")
    max_uploads = opts |> Keyword.get(:max_uploads, 1000) |> max(1) |> min(1000)

    rows =
      case prefix do
        "" ->
          fetch_all(state, :list_uploads_all, [
            bucket,
            key_marker,
            key_marker,
            upload_id_marker,
            max_uploads + 1
          ])

        _ ->
          case upper_bound(prefix) do
            nil ->
              fetch_all(state, :list_uploads_all, [
                bucket,
                max(key_marker, prefix),
                key_marker,
                upload_id_marker,
                max_uploads + 1
              ])

            upper ->
              fetch_all(state, :list_uploads_prefix, [
                bucket,
                key_marker,
                key_marker,
                upload_id_marker,
                prefix,
                upper,
                max_uploads + 1
              ])
          end
      end

    {entries, truncated} =
      if length(rows) > max_uploads do
        {Enum.take(rows, max_uploads), true}
      else
        {rows, false}
      end

    out =
      Enum.map(entries, fn [k, uid, ts] ->
        %{key: k, upload_id: uid, initiated_at: ts}
      end)

    {next_key, next_uid} =
      if truncated do
        last = List.last(out)
        {last.key, last.upload_id}
      else
        {nil, nil}
      end

    {:reply, {out, truncated, next_key, next_uid}, state}
  end

  def handle_call({:clear_upload, id}, _from, state) do
    run(state, :drop_parts, [id])
    run(state, :drop_upload, [id])
    {:reply, :ok, state}
  end

  def handle_call({:list_abandoned_uploads, before}, _from, state) do
    rows = fetch_all(state, :abandoned_uploads, [before])
    {:reply, Enum.map(rows, fn [id] -> id end), state}
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

  # User metadata is stored as a JSON object of {String.t => String.t}. We use
  # OTP 27's built-in `:json` module so there's no extra dep, and the column
  # stays human-inspectable from the sqlite3 CLI.
  defp encode_meta(meta) when is_map(meta) do
    meta
    |> Map.new(fn {k, v} -> {to_string(k), to_string(v)} end)
    |> :json.encode()
    |> IO.iodata_to_binary()
  end

  defp decode_meta(""), do: %{}
  defp decode_meta(nil), do: %{}

  defp decode_meta(json) when is_binary(json) do
    case :json.decode(json) do
      m when is_map(m) -> m
      _ -> %{}
    end
  rescue
    _ -> %{}
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

  ## ListObjectsV2 scanner with prefix + delimiter support.
  ## Emits Contents and CommonPrefixes; returns next-page lower bound when truncated.

  defp scan(state, bucket, prefix, delim, lower, upper, max_keys) do
    scan_loop(state, bucket, prefix, delim, lower, upper, max_keys, [], [], nil, nil)
  end

  defp scan_loop(state, bucket, prefix, delim, lower, upper, max_keys, contents, cps, last_lower, in_cp) do
    emitted = length(contents) + length(cps)

    cond do
      emitted >= max_keys ->
        finalize(state, bucket, upper, contents, cps, last_lower)

      true ->
        fetch_n = (max_keys - emitted) * 2 + 1
        rows = fetch_range(state, bucket, lower, upper, fetch_n)

        case process_batch(rows, prefix, delim, max_keys, contents, cps, last_lower, in_cp) do
          {:hit_max, c2, cp2, ll2} ->
            finalize(state, bucket, upper, c2, cp2, ll2)

          {:exhausted, c2, cp2, ll2, in_cp2, last_key} ->
            cond do
              length(rows) < fetch_n ->
                {Enum.reverse(c2), Enum.reverse(cp2), false, nil}

              true ->
                new_lower = last_key <> <<0>>

                scan_loop(
                  state,
                  bucket,
                  prefix,
                  delim,
                  new_lower,
                  upper,
                  max_keys,
                  c2,
                  cp2,
                  ll2,
                  in_cp2
                )
            end
        end
    end
  end

  # We've filled `max_keys` — peek at the next row to decide if truncation is real.
  defp finalize(state, bucket, upper, contents, cps, next_lower) do
    case fetch_range(state, bucket, next_lower, upper, 1) do
      [] ->
        {Enum.reverse(contents), Enum.reverse(cps), false, nil}

      _ ->
        {Enum.reverse(contents), Enum.reverse(cps), true, next_lower}
    end
  end

  defp process_batch(rows, prefix, delim, max_keys, contents, cps, last_lower, in_cp) do
    do_process(rows, prefix, delim, max_keys, contents, cps, last_lower, in_cp, nil)
  end

  defp do_process([], _, _, _, c, cp, ll, in_cp, last_key) do
    {:exhausted, c, cp, ll, in_cp, last_key}
  end

  defp do_process([row | rest], prefix, delim, max_keys, c, cp, ll, in_cp, _last) do
    [key, sz, etag, mt] = row

    cond do
      in_cp != nil and String.starts_with?(key, in_cp) ->
        do_process(rest, prefix, delim, max_keys, c, cp, ll, in_cp, key)

      true ->
        case classify(key, prefix, delim) do
          :content ->
            new_c = [%{key: key, size: sz, etag: etag, mtime: mt} | c]
            new_ll = key <> <<0>>

            if length(new_c) + length(cp) >= max_keys do
              {:hit_max, new_c, cp, new_ll}
            else
              do_process(rest, prefix, delim, max_keys, new_c, cp, new_ll, nil, key)
            end

          {:cp, cp_str} ->
            new_cp = [cp_str | cp]
            new_ll = upper_bound(cp_str) || cp_str <> <<0xFF>>

            if length(c) + length(new_cp) >= max_keys do
              {:hit_max, c, new_cp, new_ll}
            else
              do_process(rest, prefix, delim, max_keys, c, new_cp, new_ll, cp_str, key)
            end
        end
    end
  end

  defp classify(_key, _prefix, nil), do: :content

  defp classify(key, prefix, delim) do
    rest = binary_part(key, byte_size(prefix), byte_size(key) - byte_size(prefix))

    case :binary.match(rest, delim) do
      :nomatch ->
        :content

      {pos, len} ->
        cp_len = byte_size(prefix) + pos + len
        {:cp, binary_part(key, 0, cp_len)}
    end
  end

  defp fetch_range(state, bucket, lower, nil, limit) do
    fetch_all(state, :list_open, [bucket, lower, limit])
  end

  defp fetch_range(state, bucket, lower, upper, limit) do
    fetch_all(state, :list_range, [bucket, lower, upper, limit])
  end
end
