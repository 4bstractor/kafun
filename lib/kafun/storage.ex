defmodule Kafun.Storage do
  @moduledoc """
  Filesystem blob layer. Path scheme: `<root>/<bucket>/<aa>/<bb>/<key>` where
  `aa/bb` is sha1(key)[:4]. Writes go to a temp file in the destination shard
  and `rename(2)` into place — atomic visibility, no half-written reads.
  """

  @valid_bucket ~r/^[a-z0-9][a-z0-9.\-]{1,62}$/
  @chunk 65_536

  @spec valid_bucket?(String.t()) :: boolean()
  def valid_bucket?(name), do: Regex.match?(@valid_bucket, name)

  @doc """
  Reject empty / oversize keys, control bytes, and any key whose path
  resolution could escape the bucket directory. The on-disk layout uses the
  raw key as the leaf filename, so a key like `"../../../../tmp/pwned"`
  would otherwise traverse out of `<root>/<bucket>/<aa>/<bb>/`.
  """
  @spec valid_key?(String.t()) :: boolean()
  def valid_key?(""), do: false
  def valid_key?(key) when byte_size(key) > 1024, do: false

  def valid_key?(key) do
    cond do
      String.contains?(key, [<<0>>, "\n", "\r"]) -> false
      String.starts_with?(key, "/") -> false
      has_unsafe_segment?(key) -> false
      true -> true
    end
  end

  defp has_unsafe_segment?(key) do
    Enum.any?(Path.split(key), &(&1 in [".", ".."]))
  end

  @spec blob_path(Path.t(), String.t(), String.t()) :: Path.t()
  def blob_path(root, bucket, key) do
    <<a::binary-size(2), b::binary-size(2), _::binary>> =
      :crypto.hash(:sha, key) |> Base.encode16(case: :lower)

    Path.join([root, bucket, a, b, key])
  end

  @doc """
  Stream the request body to disk. Returns `{:ok, conn, size, etag}` where
  `etag` is hex-MD5 of the body (S3-compatible for non-multipart PUTs).
  """
  @spec stream_put(Plug.Conn.t(), Path.t(), String.t(), String.t()) ::
          {:ok, Plug.Conn.t(), non_neg_integer(), String.t()}
  def stream_put(conn, root, bucket, key),
    do: stream_to_disk(conn, blob_path(root, bucket, key))

  @doc """
  Stream a part body to disk. Mirrors `stream_put/4` but writes to the
  `.uploads/<id>/<n>` slot — the part is held there until the matching
  CompleteMultipartUpload concatenates it into the final blob.
  """
  @spec stream_part_put(Plug.Conn.t(), Path.t(), String.t(), pos_integer()) ::
          {:ok, Plug.Conn.t(), non_neg_integer(), String.t()}
  def stream_part_put(conn, root, upload_id, part_number),
    do: stream_to_disk(conn, part_path(root, upload_id, part_number))

  # Atomic temp+rename write of the streamed request body, with inline MD5.
  defp stream_to_disk(conn, final) do
    File.mkdir_p!(Path.dirname(final))
    tmp = final <> ".tmp." <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

    {:ok, fd} = :file.open(tmp, [:write, :raw, :binary, :delayed_write])

    try do
      {conn, size, ctx} = consume(conn, fd, 0, :crypto.hash_init(:md5))
      :ok = :file.close(fd)
      :ok = :file.rename(tmp, final)
      etag = ctx |> :crypto.hash_final() |> Base.encode16(case: :lower)
      {:ok, conn, size, etag}
    rescue
      e ->
        _ = :file.close(fd)
        _ = :file.delete(tmp)
        reraise e, __STACKTRACE__
    end
  end

  defp consume(conn, fd, size, ctx) do
    case Plug.Conn.read_body(conn, length: @chunk, read_length: @chunk) do
      {:more, chunk, conn} ->
        :ok = :file.write(fd, chunk)
        consume(conn, fd, size + byte_size(chunk), :crypto.hash_update(ctx, chunk))

      {:ok, chunk, conn} ->
        :ok = :file.write(fd, chunk)
        {conn, size + byte_size(chunk), :crypto.hash_update(ctx, chunk)}
    end
  end

  @spec delete(Path.t(), String.t(), String.t()) :: :ok
  def delete(root, bucket, key) do
    _ = :file.delete(blob_path(root, bucket, key))
    :ok
  end

  ## Multipart helpers.

  @spec part_path(Path.t(), String.t(), pos_integer()) :: Path.t()
  def part_path(root, upload_id, part_number) do
    Path.join([root, ".uploads", upload_id, Integer.to_string(part_number)])
  end

  @spec uploads_dir(Path.t(), String.t()) :: Path.t()
  def uploads_dir(root, upload_id), do: Path.join([root, ".uploads", upload_id])

  @doc """
  Concatenate the listed parts (in order) into the destination blob, atomically.
  `parts` accepts either `pos_integer()` or `{pos_integer(), _ignored}` — the
  client-supplied etag is *not* used here; caller validates it separately.
  Returns `{:ok, total_bytes}` or `{:error, {:missing_part, n}}` /
  `{:error, {:read_error, reason}}` on failure.
  """
  @spec concat_parts(Path.t(), String.t(), String.t(), String.t(),
                     [pos_integer() | {pos_integer(), term()}]) ::
          {:ok, non_neg_integer()}
          | {:error, {:missing_part, pos_integer()} | {:read_error, term()}}
  def concat_parts(root, upload_id, bucket, key, parts) do
    final = blob_path(root, bucket, key)
    File.mkdir_p!(Path.dirname(final))
    tmp = final <> ".tmp." <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

    {:ok, out} = :file.open(tmp, [:write, :raw, :binary, :delayed_write])

    try do
      total =
        Enum.reduce(parts, 0, fn part, acc ->
          n = part_number(part)
          path = part_path(root, upload_id, n)

          case :file.open(path, [:read, :raw, :binary, :read_ahead]) do
            {:ok, in_fd} ->
              try do
                acc + copy_file(in_fd, out)
              after
                :file.close(in_fd)
              end

            {:error, _reason} ->
              throw({:missing_part, n})
          end
        end)

      :ok = :file.close(out)
      :ok = :file.rename(tmp, final)
      {:ok, total}
    rescue
      e ->
        _ = :file.close(out)
        _ = :file.delete(tmp)
        reraise e, __STACKTRACE__
    catch
      {:missing_part, n} ->
        _ = :file.close(out)
        _ = :file.delete(tmp)
        {:error, {:missing_part, n}}

      {:read_error, reason} ->
        _ = :file.close(out)
        _ = :file.delete(tmp)
        {:error, {:read_error, reason}}
    end
  end

  defp part_number(n) when is_integer(n), do: n
  defp part_number({n, _}) when is_integer(n), do: n

  defp copy_file(in_fd, out_fd, total \\ 0) do
    case :file.read(in_fd, @chunk) do
      {:ok, data} ->
        :ok = :file.write(out_fd, data)
        copy_file(in_fd, out_fd, total + byte_size(data))

      :eof ->
        total

      {:error, reason} ->
        throw({:read_error, reason})
    end
  end

  @spec cleanup_upload(Path.t(), String.t()) :: :ok
  def cleanup_upload(root, upload_id) do
    _ = File.rm_rf(uploads_dir(root, upload_id))
    :ok
  end

  ## Blob-tree walking — used by GC.

  @doc """
  Walks `<root>/<bucket>/<aa>/<bb>/` and returns one tuple per regular file.
  Skips dot-prefixed top-level dirs (e.g. `.uploads`) and any non-directory
  entries at root (the SQLite DB lives there).

  Returns `[{bucket, filename, mtime_unix, full_path}]`. `filename` is the raw
  on-disk name — it may be a `.tmp.<rand>` leftover from a crashed PUT or the
  actual key.
  """
  @spec list_blob_files(Path.t()) :: [
          {String.t(), String.t(), integer(), Path.t()}
        ]
  def list_blob_files(root) do
    case File.ls(root) do
      {:ok, names} ->
        Enum.flat_map(names, fn name ->
          path = Path.join(root, name)

          if String.starts_with?(name, ".") or not File.dir?(path) do
            []
          else
            list_in_bucket(name, path)
          end
        end)

      _ ->
        []
    end
  end

  defp list_in_bucket(bucket, dir) do
    case File.ls(dir) do
      {:ok, aas} -> Enum.flat_map(aas, &list_in_aa(bucket, dir, &1))
      _ -> []
    end
  end

  defp list_in_aa(bucket, bucket_dir, aa) do
    aa_dir = Path.join(bucket_dir, aa)

    if File.dir?(aa_dir) do
      case File.ls(aa_dir) do
        {:ok, bbs} -> Enum.flat_map(bbs, &list_in_bb(bucket, aa_dir, &1))
        _ -> []
      end
    else
      []
    end
  end

  defp list_in_bb(bucket, aa_dir, bb) do
    bb_dir = Path.join(aa_dir, bb)

    if File.dir?(bb_dir) do
      case File.ls(bb_dir) do
        {:ok, files} ->
          Enum.flat_map(files, fn f ->
            path = Path.join(bb_dir, f)

            case File.stat(path, time: :posix) do
              {:ok, %File.Stat{type: :regular, mtime: mt}} -> [{bucket, f, mt, path}]
              _ -> []
            end
          end)

        _ ->
          []
      end
    else
      []
    end
  end

  @doc "Parse a single-range `Range: bytes=A-B` header against `size`."
  @spec parse_range(String.t() | nil, non_neg_integer()) ::
          :none | {:ok, non_neg_integer(), non_neg_integer()} | :invalid
  def parse_range(nil, _size), do: :none
  def parse_range("", _size), do: :none

  def parse_range("bytes=" <> spec, size) do
    case String.split(spec, "-", parts: 2) do
      [a, ""] ->
        case Integer.parse(a) do
          {start, ""} when start < size -> {:ok, start, size - 1}
          _ -> :invalid
        end

      ["", b] ->
        case Integer.parse(b) do
          {n, ""} when n > 0 -> {:ok, max(size - n, 0), size - 1}
          _ -> :invalid
        end

      [a, b] ->
        with {start, ""} <- Integer.parse(a),
             {stop, ""} <- Integer.parse(b),
             true <- start <= stop and start < size do
          {:ok, start, min(stop, size - 1)}
        else
          _ -> :invalid
        end

      _ ->
        :invalid
    end
  end

  def parse_range(_, _), do: :invalid
end
