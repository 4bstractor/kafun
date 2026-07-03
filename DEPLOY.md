# Deploy

Production deployment runbook for kafun on yomi (Void Linux, Docker).

## What's where

| Path | Owner | Purpose |
|------|-------|---------|
| `/srv/kafun/docker-compose.yml` | (you choose) | Compose file. Builds + runs the container. |
| `/srv/kafun/kafun.env` | (you choose) | Env vars — secrets live here, mode 640. |
| `/srv/kafun/src/` | (you choose) | Source tree. `compose build` reads from here. Update via `rsync` from your dev box. |
| `/sanzu/objects/` | (you choose) | ZFS dataset, blob storage + SQLite index. Bind-mounted to `/data` in the container. |
| `/backup/kafun/` | (you choose) | Backup destination for the non-ZFS path (§4). Not needed when ZFS snapshots are the backup. |

The "you choose" ownership rows depend on whether you run the container as
root (matches kavita / booksorter on yomi) or as a specific UID. Either
works; pick what fits your operational reflex.

## 1. Initial deploy on yomi

Assumes `/srv/kafun/{docker-compose.yml,kafun.env}` and `/srv/kafun/src/`
already exist (rsync them from a dev checkout — the templates ship in
this repo at the root).

```sh
cd /srv/kafun

# Sanity-check the env file. KAFUN_ADMIN_SECRET, KAFUN_KEYS, and
# RELEASE_COOKIE should all be populated; KAFUN_BOOTSTRAP_BUCKETS should
# list every bucket clients will push into.
$EDITOR kafun.env

# Build the image. Multi-stage Alpine; ~45 MB final.
docker compose build

# Start it.
docker compose up -d

# Confirm both ports answered:
docker compose logs --tail=30
docker compose ps
```

Expected log lines (early):

```
kafun starting: root=/data db=/data/index.db bind=0.0.0.0:8333
Running Kafun.Router with Bandit ...:8333 (http)
Running Kafun.Admin.Endpoint with Bandit ...:8334 (http)
kafun bootstrap: ensuring 10 bucket(s)
```

Smoke-test from the host:

```sh
KEY=$(grep ^KAFUN_KEYS /srv/kafun/kafun.env | cut -d= -f2)
AUTH="AWS4-HMAC-SHA256 Credential=$KEY/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
curl -sS http://localhost:8333/healthz
curl -sS -I -H "Authorization: $AUTH" http://localhost:8333/wallpapers   # 200 if bootstrap worked
curl -sS http://localhost:8334/buckets > /dev/null && echo "admin ok"
```

## 2. NPM upstream

Point `objects.harvelab.com` at `yomi:8333`. (Same upstream the seaweed
container used to live behind — no NPM-side change required if you
already had it set up.) For the admin UI, `kafun-admin.harvelab.com` →
`yomi:8334` is the recommended pattern; you can also leave admin
LAN-only and skip an NPM entry.

## 3. Updates

### Code changes

```sh
# From your dev checkout, push fresh source:
rsync -avz --delete \
  --exclude='_build/' --exclude='deps/' --exclude='.git/' \
  --exclude='.elixir_ls/' --exclude='.lexical/' --exclude='.iex.exs.local' \
  ./ naka@yomi:/srv/kafun/src/

# On yomi:
cd /srv/kafun
docker compose build         # rebuild image
docker compose up -d         # rolling-style replace
```

### Env-only changes

```sh
$EDITOR /srv/kafun/kafun.env
docker compose up -d         # picks up env_file changes on recreate
```

## 4. Backups

Two things to protect, both under `KAFUN_ROOT`: the blob tree (plain
files, immutable once published — the temp+rename write discipline
means a blob is never modified in place) and the SQLite index (WAL
mode). The two stores tolerate small drift — the GC reconciles on the
next sweep — so a good backup doesn't require stopping the service.

### If the data lives on ZFS (or btrfs / LVM-thin): snapshot the filesystem

A filesystem snapshot captures blobs and index at a single instant —
stronger consistency than anything below, and the recommended path.
SQLite in WAL mode is crash-consistent: a snapshot is a crash image,
and SQLite replays the `-wal` on next open.

Schedule and prune with [sanoid], replicate offsite with `syncoid` /
`zfs send`:

```ini
# /etc/sanoid/sanoid.conf
[sanzu/objects]
        use_template = production

[template_production]
        hourly = 24
        daily = 14
        monthly = 3
        autosnap = yes
        autoprune = yes
```

Restore is a ZFS operation (`zfs rollback` or clone-and-copy); nothing
kafun-specific. Do stop the container before rolling the live dataset
back.

[sanoid]: https://github.com/jimsalterjrs/sanoid

### If not (ext4 / xfs): index snapshot, then blob rsync

Without atomic snapshots, take the two stores in this order — index
first, blobs second. Every row in the index copy references a blob
that already existed when the rsync started (the index commit always
happens *after* the blob rename), so the blob copy is a superset of
what the index copy needs. The residue is harmless in both directions:
keys deleted during the window leave dangling index rows (those GETs
404 — the data was deleted anyway), keys written during the window
leave orphan blobs (the GC reaps them after the grace window).

```sh
#!/bin/sh -e
# /usr/local/bin/kafun-backup — run daily from cron.
ROOT=/sanzu/objects        # KAFUN_ROOT on the host
DEST=/backup/kafun         # wherever your backups land
TS=$(date -u +%Y%m%d-%H%M%S)

# 1. Index first. VACUUM INTO is a consistent point-in-time copy and
#    is safe to run against the live WAL database from the host.
mkdir -p "$DEST/index"
sqlite3 "$ROOT/index.db" "VACUUM INTO '$DEST/index/kafun-$TS.db'"

# 2. Blob tree second. Published blobs never change, so a live rsync
#    is safe. Skip the live index files, in-flight multipart parts,
#    and temp files — none of them belong in a backup.
rsync -a --delete \
  --exclude 'index.db*' \
  --exclude '.uploads/' \
  --exclude '*.tmp.*' \
  "$ROOT/" "$DEST/blobs/"

# 3. Prune old index snapshots.
find "$DEST/index" -name 'kafun-*.db' -mtime +14 -delete
```

```cron
0 3 * * * /usr/local/bin/kafun-backup
```

For versioned or offsite backups, swap step 2's rsync for restic or
borg pointed at the same source with the same excludes — the ordering
argument is unchanged. If you'd rather not install `sqlite3` on the
host, `docker exec kafun /app/bin/kafun rpc 'Kafun.Backup.run("/data/.backups")'`
produces the same `VACUUM INTO` snapshot from inside the container.

### Stop-the-world (simplest correct thing)

If a few seconds of nightly downtime is acceptable, skip the ordering
argument entirely:

```sh
docker compose stop kafun
cp / rsync / tar ...            # everything under $ROOT, minus .uploads/ and *.tmp.*
docker compose start kafun
```

### Restore

1. Stop the container.
2. Put the blob tree back under `KAFUN_ROOT`.
3. Put the chosen index snapshot back as `<root>/index.db`. Delete any
   stale `index.db-wal` / `index.db-shm` sitting next to it — they
   belong to the dead database, not the restored one.
4. Start the container, then reconcile the drift window immediately
   instead of waiting for the next sweep:
   `docker exec kafun /app/bin/kafun rpc 'Kafun.GC.run_now()'`

## 5. Rollback

If a release is bad:

```sh
cd /srv/kafun
docker compose down

# Either: re-checkout the prior source on the dev box and rsync it back over,
# or, if you tag images with a build SHA, re-tag the previous one and
# `compose up -d`. The simplest homelab flow is the first.

rsync ... # previous source onto /srv/kafun/src/
docker compose build
docker compose up -d
```

The blob tree and SQLite index live outside the container on
`/sanzu/objects/`, so rolling back the image never loses data. If a
bad migration corrupted the index, restore your most recent index
snapshot (see §4) following the Restore steps there — blobs stay in
place, only `index.db` is swapped.

## 6. Day-2 ops

| Task | Command |
|------|---------|
| Container status | `docker compose ps` |
| Live logs | `docker compose logs -f --tail=100` |
| Restart | `docker compose restart` |
| Stop | `docker compose down` |
| Start | `docker compose up -d` |
| Rebuild + apply | `docker compose build && docker compose up -d` |
| Trigger GC | `docker exec kafun /app/bin/kafun rpc 'Kafun.GC.run_now()'` |
| GC status | `docker exec kafun /app/bin/kafun rpc 'Kafun.GC.status()'` |
| Run backup ad hoc | `docker exec kafun /app/bin/kafun rpc 'Kafun.Backup.run("/data/.backups")'` |
| Remote console | `docker exec -it kafun /app/bin/kafun remote` |
| Bucket counts | `docker exec kafun /app/bin/sqlite3 -readonly /data/index.db "SELECT bucket, COUNT(*) FROM objects GROUP BY bucket"` (`sqlite3` not currently in image; run from host instead) |
| Bucket counts (host) | `sqlite3 -readonly /sanzu/objects/index.db "SELECT bucket, COUNT(*) FROM objects GROUP BY bucket"` |

## 7. Migrating from another S3 source

Not part of the initial cutover — we purged seaweed and reconstructed
from origin pipelines. Tooling stays in the repo for any future
S3-to-S3 ingest:

```sh
# From a checkout with deps installed:
mix kafun.migrate \
  --src https://other-s3-host \
  --src-key <ACCESS> \
  --src-secret <SECRET> \
  --dst https://objects.harvelab.com \
  --dst-key <KAFUN_KEY> \
  --bucket some-bucket \
  --concurrency 8
```

Idempotent (HEAD-then-skip on each key); safe to interrupt and resume.
See `lib/kafun/migrate.ex` and `lib/mix/tasks/kafun.migrate.ex` for
the full flag list.

## 8. Alternative: bare-metal / runit

If kafun ever moves to its own dedicated Void VM (or any host where
Docker would be overhead), the `rel/sv/kafun/` runit service +
`rel/kafun.env.example` cover that path. The release built by
`MIX_ENV=prod mix release` is self-contained (bundled ERTS); install
to `/opt/kafun`, drop `kafun.env` at `/etc/kafun/`, symlink the runit
service into `/var/service/`. The `chpst -u` line in the run script
becomes the user gate. Tighter, no Docker daemon, but redundant on a
multi-service box like yomi.
