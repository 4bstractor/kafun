defmodule Kafun.Multipart do
  @moduledoc """
  S3 multipart upload orchestration. Parts are written to
  `<root>/.uploads/<upload_id>/<n>`; on Complete they're concatenated into the
  final blob, the multipart ETag (`md5-of-md5s-N`) is computed, and the index
  is updated atomically (well — the rename is atomic, the index commit is a
  separate step; see CLAUDE.md for the crash-window note).
  """

  alias Kafun.{Index, Storage}

  @doc "Generate a fresh upload id. Opaque to clients."
  def new_upload_id do
    :crypto.strong_rand_bytes(18) |> Base.url_encode64(padding: false)
  end

  @spec initiate(String.t(), String.t(), String.t() | nil) :: {:ok, String.t()}
  def initiate(bucket, key, content_type) do
    upload_id = new_upload_id()
    :ok = Index.init_upload(upload_id, bucket, key, content_type)
    {:ok, upload_id}
  end

  @spec upload_part(Plug.Conn.t(), Path.t(), String.t(), pos_integer()) ::
          {:ok, Plug.Conn.t(), String.t()} | {:error, :no_such_upload}
  def upload_part(conn, root, upload_id, part_number) do
    case Index.get_upload(upload_id) do
      :not_found ->
        {:error, :no_such_upload}

      {:ok, _} ->
        {:ok, conn, size, etag} = Storage.stream_part_put(conn, root, upload_id, part_number)
        :ok = Index.record_part(upload_id, part_number, size, etag)
        {:ok, conn, etag}
    end
  end

  @doc """
  Concatenate the listed parts into the final object and write the index entry.
  `requested` is `[{part_number, etag_from_client_xml}, ...]` in client-supplied
  order. Returns `{:ok, %{etag, size, bucket, key}}` or an error tuple.
  """
  @spec complete(Path.t(), String.t(), [{pos_integer(), String.t()}]) ::
          {:ok, %{etag: String.t(), size: non_neg_integer(), bucket: String.t(), key: String.t()}}
          | {:error, :no_such_upload | :no_parts | {:part_mismatch, pos_integer()} | {:missing_part, pos_integer()}}
  def complete(root, upload_id, requested) do
    with {:ok, upload} <- fetch_upload(upload_id),
         :ok <- ensure_parts(requested),
         {:ok, ordered} <- validate_parts(upload_id, requested),
         {:ok, total} <- Storage.concat_parts(root, upload_id, upload.bucket, upload.key, ordered) do
      etag = multipart_etag(ordered)

      :ok =
        Index.put(
          upload.bucket,
          upload.key,
          total,
          etag,
          upload.content_type,
          System.system_time(:second)
        )

      :ok = Index.clear_upload(upload_id)
      :ok = Storage.cleanup_upload(root, upload_id)

      {:ok, %{etag: etag, size: total, bucket: upload.bucket, key: upload.key}}
    end
  end

  @spec abort(Path.t(), String.t()) :: :ok | {:error, :no_such_upload}
  def abort(root, upload_id) do
    case Index.get_upload(upload_id) do
      :not_found ->
        {:error, :no_such_upload}

      {:ok, _} ->
        :ok = Index.clear_upload(upload_id)
        :ok = Storage.cleanup_upload(root, upload_id)
        :ok
    end
  end

  ## Internals.

  defp fetch_upload(upload_id) do
    case Index.get_upload(upload_id) do
      {:ok, u} -> {:ok, u}
      :not_found -> {:error, :no_such_upload}
    end
  end

  defp ensure_parts([]), do: {:error, :no_parts}
  defp ensure_parts(_), do: :ok

  defp validate_parts(upload_id, requested) do
    stored =
      upload_id
      |> Index.list_parts()
      |> Map.new(fn p -> {p.part_number, p.etag} end)

    Enum.reduce_while(requested, {:ok, []}, fn {n, etag}, {:ok, acc} ->
      case Map.fetch(stored, n) do
        :error ->
          {:halt, {:error, {:missing_part, n}}}

        {:ok, stored_etag} ->
          if normalize_etag(stored_etag) == normalize_etag(etag) do
            {:cont, {:ok, [{n, stored_etag} | acc]}}
          else
            {:halt, {:error, {:part_mismatch, n}}}
          end
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      err -> err
    end
  end

  defp normalize_etag(etag) when is_binary(etag) do
    etag |> String.trim() |> String.trim(~s|"|) |> String.downcase()
  end

  @doc false
  # S3 multipart ETag = hex(md5(decode_hex(part1_etag) || ...)) <> "-" <> count.
  def multipart_etag(parts) do
    bin =
      parts
      |> Enum.map(fn {_, etag} -> Base.decode16!(etag, case: :mixed) end)
      |> IO.iodata_to_binary()

    digest = :crypto.hash(:md5, bin) |> Base.encode16(case: :lower)
    "#{digest}-#{length(parts)}"
  end
end
