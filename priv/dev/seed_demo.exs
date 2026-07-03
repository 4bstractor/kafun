# Demo-data seed for the admin UI (README screenshots, local dev poking).
# Buckets, sha1-sharded objects with generated PNGs, access keys + grants.
#
#   KAFUN_ROOT=/tmp/kafun-demo KAFUN_KEYS=HOMELAB-ADMIN \
#     KAFUN_PUBLIC_S3_URL=http://localhost:8333 mix run priv/dev/seed_demo.exs
#
# then serve the same root:  ... mix run --no-halt
# In dev, `mix run` boots the supervision tree, so Index/Storage are up.

defmodule SeedPNG do
  @moduledoc "Minimal pure-Elixir PNG encoder — 8-bit RGB, no deps."

  def write(path, w, h, pixel_fun) do
    rows =
      for y <- 0..(h - 1), into: <<>> do
        row = for x <- 0..(w - 1), into: <<>>, do: rgb(pixel_fun.(x, y))
        <<0>> <> row
      end

    z = :zlib.open()
    :ok = :zlib.deflateInit(z)
    idat = IO.iodata_to_binary(:zlib.deflate(z, rows, :finish))
    :zlib.deflateEnd(z)
    :zlib.close(z)

    ihdr = <<w::32, h::32, 8, 2, 0, 0, 0>>
    png = <<0x89, "PNG\r\n", 0x1A, "\n">> <> chunk("IHDR", ihdr) <> chunk("IDAT", idat) <> chunk("IEND", "")
    File.write!(path, png)
  end

  defp rgb({r, g, b}), do: <<trunc(r), trunc(g), trunc(b)>>

  defp chunk(type, data) do
    payload = type <> data
    <<byte_size(data)::32>> <> payload <> <<:erlang.crc32(payload)::32>>
  end
end

defmodule Seed do
  def root, do: Application.get_env(:kafun, :root) || raise("no :kafun :root configured")

  def days_ago(n), do: System.os_time(:second) - n * 86_400 - :rand.uniform(40_000)

  def put_file(bucket, key, src, content_type, mtime, meta \\ %{}) do
    {:ok, size, etag} = Kafun.Storage.import_file(root(), bucket, key, src)
    :ok = Kafun.Index.put(bucket, key, size, etag, content_type, mtime, meta)
    IO.puts("  #{bucket}/#{key} (#{size} bytes)")
  end

  def gradient(seed) do
    :rand.seed(:exsss, {seed, seed * 7, seed * 13})
    h1 = :rand.uniform(360)
    h2 = h1 + 40 + :rand.uniform(80)
    tilt = 0.6 + :rand.uniform() * 1.2

    fn x, y ->
      t = (x / 1280 + y / 800 * tilt) / (1 + tilt)
      band = :math.sin(t * :math.pi() * 3) * 0.06
      hue = h1 + (h2 - h1) * t
      hsl(hue, 0.5, 0.3 + t * 0.35 + band)
    end
  end

  # hsl -> rgb, h in degrees
  def hsl(h, s, l) do
    h = :math.fmod(:math.fmod(h, 360) + 360, 360)
    c = (1 - abs(2 * l - 1)) * s
    x = c * (1 - abs(:math.fmod(h / 60, 2) - 1))
    m = l - c / 2

    {r, g, b} =
      cond do
        h < 60 -> {c, x, 0}
        h < 120 -> {x, c, 0}
        h < 180 -> {0, c, x}
        h < 240 -> {0, x, c}
        h < 300 -> {x, 0, c}
        true -> {c, 0, x}
      end

    {(r + m) * 255, (g + m) * 255, (b + m) * 255}
  end

  def random_blob(path, mb) do
    File.write!(path, :crypto.strong_rand_bytes(trunc(mb * 1024 * 1024)))
  end
end

alias Kafun.Index

tmp = Path.join(System.tmp_dir!(), "kafun-seed")
File.mkdir_p!(tmp)

IO.puts("== buckets ==")
for b <- ~w(photos backups build-artifacts) do
  :ok = Index.ensure_bucket(b)
  File.mkdir_p!(Path.join(Seed.root(), b))
end

:ok = Index.set_bucket_public_read("photos", true)

IO.puts("== photos ==")
photos = [
  {"2026/07/aurora-01.png", 3, %{"camera" => "X-T5", "lens" => "XF 23mm F2"}},
  {"2026/07/dunes-sunset.png", 9, %{"camera" => "X-T5", "lens" => "XF 56mm F1.2"}},
  {"2026/07/reef-drift.png", 17, %{}},
  {"2026/06/still-harbor.png", 40, %{"camera" => "K-3 III"}},
  {"2026/06/pine-ridge.png", 47, %{}},
  {"2026/06/city-rain.png", 52, %{}},
  {"wallpaper-main.png", 80, %{}},
  {"wallpaper-alt.png", 81, %{}}
]

for {{key, age, meta}, i} <- Enum.with_index(photos) do
  src = Path.join(tmp, "img#{i}.png")
  SeedPNG.write(src, 1280, 800, Seed.gradient(i * 31 + 7))
  Seed.put_file("photos", key, src, "image/png", Seed.days_ago(age), meta)
end

IO.puts("== backups ==")
backups = [
  {"yomi/kafun-index-2026-07-01.db.zst", 1.4, 2},
  {"yomi/kafun-index-2026-06-24.db.zst", 1.3, 9},
  {"chikaku/mongo-2026-07-01.archive.zst", 6.2, 2},
  {"chikaku/mongo-2026-06-24.archive.zst", 5.9, 9},
  {"inochi/etc-2026-07-01.tar.zst", 0.7, 2}
]

for {{key, mb, age}, i} <- Enum.with_index(backups) do
  src = Path.join(tmp, "blob#{i}.bin")
  Seed.random_blob(src, mb)
  Seed.put_file("backups", key, src, "application/zstd", Seed.days_ago(age))
end

IO.puts("== build-artifacts ==")
artifacts = [
  {"kafun/kafun-0.2.1.tar.gz", 2.1, 0, "application/gzip"},
  {"kafun/kafun-0.2.tar.gz", 2.0, 46, "application/gzip"},
  {"giyouden/giyouden-0.1.4-linux-amd64", 8.3, 21, "application/octet-stream"},
  {"gohou/gohou_py-1.1.0-py3-none-any.whl", 0.2, 30, "application/zip"}
]

for {{key, mb, age, ct}, i} <- Enum.with_index(artifacts) do
  src = Path.join(tmp, "art#{i}.bin")
  Seed.random_blob(src, mb)
  Seed.put_file("build-artifacts", key, src, ct, Seed.days_ago(age))
end

IO.puts("== access keys ==")
mk_secret = fn -> :crypto.strong_rand_bytes(24) |> Base.url_encode64(padding: false) end

:ok = Index.create_access_key("IMOUTO-PIPELINE", mk_secret.(), "imouto image pipeline")
:ok = Index.upsert_grant("IMOUTO-PIPELINE", "photos", :write)

:ok = Index.create_access_key("RESTIC-NIGHTLY", mk_secret.(), "nightly restic push from the fleet")
:ok = Index.upsert_grant("RESTIC-NIGHTLY", "backups", :write)

:ok = Index.create_access_key("CI-PUSHER", mk_secret.(), "gitea actions artifact upload")
:ok = Index.upsert_grant("CI-PUSHER", "build-artifacts", :write)

:ok = Index.create_access_key("OLD-LAPTOP", mk_secret.(), "decommissioned 2026-05")
:ok = Index.revoke_access_key("OLD-LAPTOP")

# give the read-side keys some believable last-used timestamps
Index.touch_access_key_last_used("IMOUTO-PIPELINE")
Index.touch_access_key_last_used("RESTIC-NIGHTLY")

File.rm_rf!(tmp)
IO.puts("== seed done ==")
