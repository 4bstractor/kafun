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

  @spec valid_key?(String.t()) :: boolean()
  def valid_key?(""), do: false
  def valid_key?(key) when byte_size(key) > 1024, do: false
  def valid_key?(key), do: not String.contains?(key, [<<0>>, "\n", "\r"])

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
  def stream_put(conn, root, bucket, key) do
    final = blob_path(root, bucket, key)
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
