# kafun

A small S3-compatible blob service. Filesystem for the bytes, SQLite for
the metadata index, BEAM for everything else. Single node. Built for the
homelab — one supervised box behind a reverse proxy, ZFS doing the
heavy lifting underneath.

[![ci](https://github.com/4bstractor/kafun/actions/workflows/ci.yml/badge.svg)](https://github.com/4bstractor/kafun/actions/workflows/ci.yml)

## When to use kafun

You probably want kafun if:

- You have one box (or want to act like you do) with a big disk pool
  and need an S3 endpoint for the things that already speak S3 — backups,
  image pipelines, build artifacts, static asset stores.
- You want bytes in a directory tree you can `ls`/`rsync`/`zfs snapshot`
  directly, not opaque object chunks.
- You want metadata in a single SQLite file you can copy off the box,
  inspect with `sqlite3`, or back up with one `cp`.
- You're comfortable trusting your network at the edge and gating
  access with per-key ACLs inside.

You probably don't want kafun if:

- You need HA, replication, or multi-node anything. (Use [Garage] or
  [SeaweedFS].)
- You need IAM, SSE-KMS, versioning, object lock, lifecycle rules, or
  any other big-S3 feature. (Use [MinIO] or actual S3.)
- You're storing billions of objects. SQLite + a single GenServer
  serializes the metadata path; it's fine for tens of millions, not
  designed for billions.

[Garage]: https://garagehq.deuxfleurs.fr/
[SeaweedFS]: https://github.com/seaweedfs/seaweedfs
[MinIO]: https://min.io/

## Quickstart

Generate a session-signing secret once and put it in `.env` next to your
compose file:

```sh
echo "KAFUN_ADMIN_SECRET=$(openssl rand -hex 32)" >> .env
echo "KAFUN_ADMIN_PASSWORD=change-me"             >> .env
echo "KAFUN_KEYS=AKIAEXAMPLEKEY"                  >> .env
```

```yaml
# docker-compose.yml
services:
  kafun:
    image: gitea.harvelab.com/harvelab/kafun:latest
    restart: unless-stopped
    ports:
      - "8333:8333"   # S3 API
      - "8334:8334"   # admin UI
    volumes:
      - /your/big/pool:/data
    environment:
      KAFUN_ROOT: /data
      KAFUN_KEYS: ${KAFUN_KEYS}
      KAFUN_ADMIN_PASSWORD: ${KAFUN_ADMIN_PASSWORD}
      KAFUN_ADMIN_SECRET: ${KAFUN_ADMIN_SECRET}
```

```sh
docker compose up -d
```

Sanity check it's alive:

```sh
curl http://localhost:8333/healthz
```

Then point boto3 / aws-cli at `http://localhost:8333` with your access
key from `KAFUN_KEYS` (any secret works for bootstrap keys; rotate in
the admin UI to opt into real SigV4 verification). Real production
deployment notes — NPM upstream, env hardening, day-2 ops — live in
[DEPLOY.md](./DEPLOY.md).

## Admin UI

A second port (`KAFUN_ADMIN_PORT`, default 8334) serves a Phoenix
LiveView dashboard:

- Bucket browser with paginated listing, drag-and-drop upload,
  inline preview, rename, delete.
- Per-bucket permissions panel: public-read toggle + per-key grants.
- Access-key management: generate, revoke, rotate secret, edit
  description.
- In-flight multipart uploads view + abort.
- GC and telemetry counter status.

The S3 surface and the admin UI are separate auth concerns by design.
Admin UI is gated by HTTP Basic (`KAFUN_ADMIN_USER` /
`KAFUN_ADMIN_PASSWORD`); the S3 surface is gated by SigV4 + the access
key model below.

## Auth

Three permission tiers per `(access_key, bucket)` pair:

- `read` — GetObject, HeadObject, ListObjectsV2
- `write` — read + PutObject, DeleteObject, CopyObject, multipart
- `admin` — write + CreateBucket, DeleteBucket, permission management

A grant on the sentinel bucket `*` applies globally. Anonymous requests
are allowed only when the action is read AND the bucket has
`public_read = true`.

SigV4 signatures are verified for any key with a non-empty secret.
Bootstrap keys from `KAFUN_KEYS` land with empty secrets and skip
verification (back-compat for pre-ACL deployments) — rotate to a real
secret in the admin UI to opt in.

`KAFUN_AUTH_DISABLED=true` is a recovery-only escape hatch that
short-circuits all gating to allow.

## Configuration

| Env var                                 | Default                  | Notes |
|-----------------------------------------|--------------------------|-------|
| `KAFUN_ROOT`                            | `${tmp}/kafun` (dev)     | Required in prod. Blob root + default DB location. |
| `KAFUN_DB`                              | `<root>/index.db`        | SQLite metadata file. |
| `KAFUN_HOST` / `KAFUN_PORT`             | `0.0.0.0` / `8333`       | S3 surface. |
| `KAFUN_ADMIN_HOST` / `KAFUN_ADMIN_PORT` | `0.0.0.0` / `8334`       | Admin UI. |
| `KAFUN_ADMIN_USER` / `KAFUN_ADMIN_PASSWORD` | `admin` / *(empty)*  | Empty password = open admin (trusted-network model). |
| `KAFUN_ADMIN_SECRET`                    | *(per-boot in dev)*      | Session signing key, **required in prod**. 64+ bytes. |
| `KAFUN_ADMIN_ALLOWED_ORIGINS`           | *(empty)*                | Comma-separated CORS origins for the LiveView socket. Empty = no origin check. |
| `KAFUN_PUBLIC_S3_URL`                   | *(falls back to host:port)* | Externally reachable URL of the S3 surface, used for admin UI image previews. |
| `KAFUN_KEYS`                            | *(empty)*                | Bootstrap access keys. Each entry → access_keys row with empty secret + global admin grant. |
| `KAFUN_BOOTSTRAP_BUCKETS`               | *(empty)*                | Comma-separated bucket names. Created on boot if absent. |
| `KAFUN_AUTH_DISABLED`                   | `false`                  | Recovery escape hatch. |
| `KAFUN_LOG_LEVEL`                       | `info`                   | |
| `KAFUN_GC_INTERVAL_SEC`                 | `3600`                   | `0` disables periodic sweeps. |
| `KAFUN_GC_ABANDON_AFTER_SEC`            | `86400`                  | Multipart uploads older than this get aborted. |
| `KAFUN_GC_BLOB_GRACE_SEC`               | `3600`                   | Orphan blobs / `.tmp.*` older than this get reaped. |
| `KAFUN_ADMIN_MAX_UPLOAD_MB`             | `256`                    | Per-file cap on admin UI upload. |

## S3 surface

Implemented and verified against real boto3 + aws-cli:

- **Service:** `ListAllMyBuckets`.
- **Bucket:** `Create`, `Head`, `Delete`, `ListObjectsV2` (delimiter,
  pagination, encoding-type, fetch-owner), `DeleteObjects`.
- **Object:** `Put` (with `If-Match`/`If-None-Match`), `Get` (Range +
  all four conditional headers), `Head`, `Delete`, `Copy` (with the
  `x-amz-copy-source-if-*` namespace), user metadata round-trip
  (`x-amz-meta-*`).
- **Multipart:** `Initiate`, `UploadPart`, `Complete`, `Abort`,
  `ListMultipartUploads`, `ListParts`, `UploadPartCopy`.
- **Sub-resources:** stub responses for `?location|acl|versioning`;
  proper 404 codes for `?policy|cors|lifecycle|tagging`.
- **Wire:** aws-chunked unwrap, `x-amz-request-id` on every response,
  `x-amz-error-code` on HEAD 404s.

Out of scope (not coming): versioning, IAM, SSE-KMS, object lock,
replication, lifecycle rules, bucket policies, website hosting. Use a
different tool if you need those.

Tier-2 polish parked: multi-range Range, `x-amz-bucket-region` header,
list-buckets pagination, object-level tagging stubs, POST-form upload.

## Telemetry

Every handler emits one terminal event with `:duration` (μs) and `:size`
where applicable. Nothing pre-attaches — call
`:telemetry.attach_many/4`.

| Event                                | Measurements                                                   |
|--------------------------------------|----------------------------------------------------------------|
| `[:kafun, :put, :stop]`              | `size`, `duration`                                             |
| `[:kafun, :get, :stop]`              | `size`, `duration`                                             |
| `[:kafun, :delete, :stop]`           | —                                                              |
| `[:kafun, :list, :stop]`             | `count`, `duration`                                            |
| `[:kafun, :multipart, :initiate]`    | —                                                              |
| `[:kafun, :multipart, :upload_part]` | `duration`                                                     |
| `[:kafun, :multipart, :complete]`    | `size`, `parts`, `duration`                                    |
| `[:kafun, :multipart, :abort]`       | —                                                              |
| `[:kafun, :delete_objects, :stop]`   | `count`, `deleted`, `errors`, `duration`                       |
| `[:kafun, :gc, :run]`                | `abandoned_uploads`, `orphan_dirs`, `orphan_blobs`, `duration` |

## Architecture

Two-tier storage. Bytes live at `<root>/<bucket>/<aa>/<bb>/<key>`
(sha1-sharded, two-level fanout). Metadata lives in `<root>/index.db`
keyed by `(bucket, key) WITHOUT ROWID`. Listings hit the index, never
the filesystem. The two stores can drift on a crash mid-write; the GC
reconciles by walking the blob tree and reaping unreferenced files
older than the grace window.

Streaming PUT: `<final>.tmp.<rand>` → drain `Plug.Conn.read_body` in
64 KiB chunks → MD5 inline → `:file.rename` to publish. Multi-GB PUTs
never materialize in memory. aws-chunked encoding is unwrapped in the
same path.

Multipart Complete is ordered: index commit happens *after* the rename,
so a crash leaves an orphan blob (GC reaps) but never a half-installed
index entry.

For the gory details — listing pagination, GC pass design, ACL gate
flow, conditional request ordering — see `CLAUDE.md`.

## Development

```sh
mix deps.get
mix test
KAFUN_ROOT=/tmp/kafun mix run --no-halt
```

Single-test run: `mix test test/kafun_test.exs:LINE`.

## License

[Apache 2.0](./LICENSE)
