defmodule Mix.Tasks.Kafun.Migrate do
  @moduledoc """
  Migrate buckets from any S3-compatible source (typically SeaweedFS) into a
  kafun destination. Idempotent: re-running picks up where it left off.

  ## Usage

      mix kafun.migrate \\
        --src https://seaweed.harvelab.com \\
        --src-key BYQ9GQ79ZW0A9XBBWQRL \\
        --src-secret rmqrkge90UwNfY4jezN9a9RpNhA24l7pYxA8ZTeNNu \\
        --dst http://localhost:8333 \\
        --dst-key <kafun access key> \\
        --bucket imouto

  Common flags:

    --src URL                 Source endpoint
    --src-key KEY             Source access key
    --src-secret SECRET       Source secret key
    --dst URL                 Destination endpoint (default http://localhost:8333)
    --dst-key KEY             Destination access key (default same as --src-key)
    --dst-secret SECRET       Destination secret (kafun ignores this; default empty)
    --bucket NAME             Migrate just this bucket (omit for all source buckets)
    --concurrency N           Parallel object copies (default 8)
    --dry-run                 Report what would copy; don't write
    --verify                  HEAD destination after PUT to confirm size matches
    --max-size BYTES          Skip objects larger than this (default 4 GiB)
    --region NAME             SigV4 region (default us-east-1)

  ## Resuming

  The tool HEADs every destination key before copying; if size + ETag match,
  it's skipped. Re-running after a partial run only moves missing/changed
  objects. Safe to interrupt with Ctrl-C.
  """
  use Mix.Task
  alias Kafun.Migrate

  @shortdoc "Pull S3-compatible buckets into kafun"

  @flags [
    src: :string,
    src_key: :string,
    src_secret: :string,
    dst: :string,
    dst_key: :string,
    dst_secret: :string,
    bucket: :string,
    concurrency: :integer,
    dry_run: :boolean,
    verify: :boolean,
    max_size: :integer,
    region: :string
  ]

  @aliases [
    s: :src,
    d: :dst,
    b: :bucket,
    c: :concurrency
  ]

  @impl Mix.Task
  def run(argv) do
    {opts, _, _} = OptionParser.parse(argv, strict: @flags, aliases: @aliases)

    src_url = require_flag(opts, :src, "--src https://seaweed-host")
    src_key = require_flag(opts, :src_key, "--src-key <ACCESS_KEY>")
    src_secret = Keyword.get(opts, :src_secret, "")
    dst_url = Keyword.get(opts, :dst, "http://localhost:8333")
    dst_key = Keyword.get(opts, :dst_key, src_key)
    dst_secret = Keyword.get(opts, :dst_secret, "")
    region = Keyword.get(opts, :region, "us-east-1")

    Mix.Task.run("app.config")
    {:ok, _} = Application.ensure_all_started(:req)

    src = Migrate.client(src_url, src_key, src_secret, region: region)
    dst = Migrate.client(dst_url, dst_key, dst_secret, region: region)

    buckets =
      case Keyword.get(opts, :bucket) do
        nil -> Migrate.list_buckets(src)
        b -> [b]
      end

    if buckets == [] do
      IO.puts("No buckets to migrate.")
      System.halt(0)
    end

    IO.puts("Migrating buckets: #{Enum.join(buckets, ", ")}")
    IO.puts("Source:      #{src_url}")
    IO.puts("Destination: #{dst_url}")
    if Keyword.get(opts, :dry_run, false), do: IO.puts("(dry run — no writes)")
    IO.puts("")

    grand =
      Enum.reduce(buckets, %{copied: 0, skipped: 0, oversize: 0, failed: 0, bytes: 0}, fn bucket, acc ->
        IO.puts("=== #{bucket} ===")

        result =
          Migrate.run(src, dst, bucket,
            concurrency: Keyword.get(opts, :concurrency, 8),
            dry_run: Keyword.get(opts, :dry_run, false),
            verify: Keyword.get(opts, :verify, false),
            max_size: Keyword.get(opts, :max_size, 4 * 1024 * 1024 * 1024),
            on_progress: &print_progress(bucket, &1)
          )

        IO.puts("\n  done: #{summarize(result)}")
        IO.puts("  elapsed: #{result.elapsed_sec}s\n")

        merge_summary(acc, result)
      end)

    IO.puts("=== overall ===")
    IO.puts(summarize(grand))
  end

  defp require_flag(opts, key, hint) do
    case Keyword.fetch(opts, key) do
      {:ok, v} when is_binary(v) and v != "" -> v
      _ -> Mix.raise("missing required flag #{hint}")
    end
  end

  defp summarize(s) do
    "copied=#{s.copied} skipped=#{s.skipped} oversize=#{s.oversize} failed=#{s.failed} bytes=#{format_bytes(s.bytes)}"
  end

  defp format_bytes(b) when b < 1024, do: "#{b} B"
  defp format_bytes(b) when b < 1024 * 1024, do: "#{Float.round(b / 1024, 1)} KiB"
  defp format_bytes(b) when b < 1024 * 1024 * 1024, do: "#{Float.round(b / (1024 * 1024), 1)} MiB"
  defp format_bytes(b), do: "#{Float.round(b / (1024 * 1024 * 1024), 2)} GiB"

  defp print_progress(bucket, s) do
    IO.write(
      "\r  [#{bucket}] copied=#{s.copied} skipped=#{s.skipped} failed=#{s.failed} bytes=#{format_bytes(s.bytes)}    "
    )
  end

  defp merge_summary(a, b) do
    %{
      copied: a.copied + b.copied,
      skipped: a.skipped + b.skipped,
      oversize: a.oversize + b.oversize,
      failed: a.failed + b.failed,
      bytes: a.bytes + b.bytes
    }
  end
end
