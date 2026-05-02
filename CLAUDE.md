# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Kafun is an S3-compatible blob service for the homelab. **Elixir / OTP 27 / Bandit** for the HTTP front; **SQLite (WAL)** for the metadata index; the OS filesystem for the bytes themselves. The Python originals (`legacy/bucket.py`, `legacy/migrate.py`) are kept as design reference only — the live implementation is the mix project at the root.

## Layout

| File | Purpose |
| --- | --- |
| `lib/kafun/application.ex`   | Supervision tree: `Kafun.Index` → `Kafun.GC` → `Bandit`. Reads runtime config; gated on `:start_children?` so tests can suppress it. |
| `lib/kafun/router.ex`        | Plug.Router. Whole S3 surface; dispatches POST/PUT/GET/DELETE on object paths into multipart vs object-level handlers based on query params. |
| `lib/kafun/storage.ex`       | Filesystem blob ops. Path scheme `<root>/<bucket>/<aa>/<bb>/<key>` (sha1-sharded); atomic temp+rename writes; Range parsing; multipart concat; blob-tree walking for GC. |
| `lib/kafun/index.ex`         | Single-conn SQLite GenServer with prepared statements. Tables: `objects`, `buckets`, `uploads`, `parts`. Listing scanner with prefix + delimiter + paginated continuation lives here. |
| `lib/kafun/multipart.ex`     | Initiate / upload-part / complete / abort orchestration. Computes the `md5-of-md5s-N` ETag. |
| `lib/kafun/gc.ex`            | Tick-based janitor — three passes (abandoned uploads, orphan part dirs, orphan blobs / leftover tmps). Emits `[:kafun, :gc, :run]`. |
| `lib/kafun/auth.ex`          | SigV4 access-key extraction (header + querystring presigned URLs). Signature is **not** verified. |
| `lib/kafun/s3_xml.ex`        | iolist XML builders for every S3 response shape we emit + Saxy parser for the CompleteMultipartUpload body. |
| `config/runtime.exs`         | Env-var ingest. |

## Run

```
mix deps.get
mix test
KAFUN_ROOT=/sanzu/objects KAFUN_KEYS=key1,key2 mix run --no-halt
```

Single-test run: `mix test test/kafun_test.exs:LINE` or `mix test --only describe:"Index round-trip"`.

## Configuration

| Env var                       | Default                | Notes |
| ----------------------------- | ---------------------- | ----- |
| `KAFUN_ROOT`                  | `${tmp}/kafun` in dev  | Required in prod. Blob root + default DB location. |
| `KAFUN_DB`                    | `<root>/index.db`      | SQLite metadata file. |
| `KAFUN_HOST` / `KAFUN_PORT`   | `0.0.0.0` / `8333`     | |
| `KAFUN_KEYS`                  | *(empty)*              | Comma-separated allowed access keys. Empty = auth off. |
| `KAFUN_LOG_LEVEL`             | `info`                 | |
| `KAFUN_GC_INTERVAL_SEC`       | `3600`                 | `0` disables periodic sweeps. |
| `KAFUN_GC_ABANDON_AFTER_SEC`  | `86400`                | Multipart uploads older than this get aborted. |
| `KAFUN_GC_BLOB_GRACE_SEC`     | `3600`                 | Orphan blobs / `.tmp.*` older than this get GC'd. |

## Architecture notes

**Two-tier storage.** Bytes live at `<root>/<bucket>/<aa>/<bb>/<key>` (sha1-sharded, two-level fanout). Metadata lives in `<root>/index.db` keyed by `(bucket, key) WITHOUT ROWID`. Listings hit the index — never the filesystem. The two stores can drift if a crash lands between blob `rename(2)` and SQL commit; the GC's third pass reconciles by walking the blob tree and deleting unreferenced files older than the grace window. **Don't try to make the two stores transactional** — the value isn't worth the complexity.

**Streaming PUT.** `Storage.stream_put/4` and `stream_part_put/4` share an inner `stream_to_disk/2` helper: open `<final>.tmp.<rand>`, drain `Plug.Conn.read_body` in 64 KiB chunks, hash MD5 inline, then `:file.rename` to publish. ETag is the canonical S3 single-part value (hex MD5). Multi-GB PUTs never materialize in memory.

**Listing.** Prefix queries are converted to a half-open byte range via `Index.upper_bound/1` (rightmost-non-`0xFF` byte +1, carrying through trailing `0xFF`s). Delimiter / common-prefixes is implemented as a multi-page scanner in `Index.scan_loop/11`: classify each row as `:content` or `{:cp, prefix}`; when emitting a CP, set `in_cp` to skip subsequent rows starting with it, and the next-page lower bound becomes `upper_bound(cp)` so a re-query naturally jumps the whole subtree. Cursors are `>=` (inclusive) — the continuation token is `Base.url_encode64(lower_bound)`. `start-after` from S3 callers maps to `key <> <<0>>` so we keep one SQL shape. After hitting `max_keys`, a 1-row peek confirms whether truncation is real (avoids the trailing-empty-page footgun).

**Multipart.** `POST /:bucket/:key?uploads` initiates and returns an opaque `UploadId` (18 random bytes, url-safe base64). `PUT /:bucket/:key?partNumber=N&uploadId=…` streams the part to `<root>/.uploads/<id>/<n>` with the same temp+rename dance. `POST /:bucket/:key?uploadId=…` parses the client-supplied parts list (Saxy `SimpleForm.parse_string`), validates each `(partNumber, etag)` against what we recorded, concatenates in client-supplied order into the final blob, and writes the index entry. Final ETag is `md5(decode_hex(part1) || …)` plus `-N`. The two stores are ordered: index commit happens *after* the rename, so a crash mid-Complete leaves an orphan blob and orphan upload row but never a half-installed index entry. Cleanup is the GC's job, not the request path's. Multipart listing (`?uploads`, `?uploadId=…`) is paginated by `(key, upload_id)` and `partNumber` markers respectively.

**Index concurrency.** One GenServer owns the SQLite handle and serializes both reads and writes. WAL is on, but we don't exploit it — every call queues. For homelab volumes this is fine; if it becomes a bottleneck, the upgrade path is a `NimblePool` of read connections (writes still through the GenServer to keep `INSERT OR REPLACE` race-free). Two indexes were added beyond the implicit PKs: `uploads(bucket, key, upload_id)` for ListMultipartUploads and `uploads(initiated_at)` for the GC abandoned-upload query. The `parts.mtime` column is added via an idempotent `ALTER TABLE` migration in `Index.init/1`.

**GC.** `Kafun.GC` is a tick-based GenServer in the supervision tree. Each tick runs three passes:
1. **Abandoned uploads** — `uploads` rows older than `:abandon_after`, aborted via `Multipart.abort/2`.
2. **Orphan part dirs** — `<root>/.uploads/<id>/` subdirs without a matching `uploads` row (crash-window orphans).
3. **Orphan blobs / leftover tmps** — `Storage.list_blob_files/1` walks shard tree; deletes files older than `:blob_grace_seconds` that are either `.tmp.<rand>` leftovers or regular blobs with no `objects` row. The grace window is what keeps this from racing legitimate in-flight PUTs.

`KAFUN_GC_INTERVAL_SEC=0` disables periodic sweeps; `Kafun.GC.run_now/0` works regardless. Counts surface via `[:kafun, :gc, :run]` measurements.

**Auth.** `Kafun.Auth.access_key/1` extracts the access key from either the `Authorization: AWS4-HMAC-SHA256 Credential=…/…` header or the `X-Amz-Credential=` querystring (presigned URLs). Signature is **not** verified — same trusted-network model as the Python original. Empty `KAFUN_KEYS` disables auth entirely. **Do not add signature verification without explicit ask.**

**Path traversal protection.** `Storage.valid_key?/1` rejects empty / >1024-byte keys, control bytes (`\0\n\r`), keys starting with `/`, and any key whose `Path.split/1` contains a `.` or `..` segment. This matters because the on-disk layout uses the raw key as the leaf filename — without validation, a key like `"../../../../tmp/pwned"` would write to `/tmp/pwned` after traversing out of `<root>/<bucket>/<aa>/<bb>/`. The validator runs in the router's `with_object/4` wrapper, so every object-level handler is gated.

**Telemetry.** Every handler emits one terminal `[:kafun, <op>, :stop]` event with `:duration` (μs) and `:size` where applicable. Multipart family: `[:kafun, :multipart, :initiate | :upload_part | :complete | :abort]`. GC: `[:kafun, :gc, :run]`. Metadata always carries `:bucket` and `:key` for object ops (or `:upload_id` for multipart). Nothing pre-attaches — consumers call `:telemetry.attach_many/4`. Adding a new event is one line via the router's `emit/3` helper.

**Test isolation.** `config/test.exs` sets `start_children?: false` so `Kafun.Application.start/2` doesn't bring up the shared Index/Bandit/GC. Tests `start_supervised!` their own Index pointing at a per-test tmp DB and start GC with `interval_ms: 0` to disable the tick. Multipart tests `Application.put_env(:kafun, :root, tmp)` to redirect the storage root. If you introduce another long-lived process, gate it on the same flag.

## Roadmap

What's left, roughly in order:

### 1. AWS CLI shakedown (next)
Run `aws s3` and `aws s3api` against a real LAN deployment, find wire-format quirks not covered by boto3 (XML element ordering, error-code expectations, edge cases). Likely fallout: header capitalization, `<Owner>` shape on `Contents`, missing `<EncodingType>` echo, possibly `<ResponseMetadata>` differences.

### 2. CopyObject / UploadPartCopy
`PUT` with `x-amz-copy-source` header — server-side copy without re-uploading. Two flavors: a fresh object (CopyObject) and a part within an in-flight multipart upload (UploadPartCopy with optional `x-amz-copy-source-range`). For Kafun this is just `:file.copy/3` (or sendfile-to-file if we want zero-copy) plus an index `INSERT OR REPLACE`.

### 3. Phoenix admin UI (last per user direction)
Light LiveView app. Probably its own OTP app inside an umbrella (or just a sibling Plug router on a separate port). Pages:
- Buckets list with object counts and total size (need new aggregation queries on `objects`).
- Per-bucket browser with prefix navigation (delimiter listing already does the work).
- In-flight multipart uploads (`Index.list_uploads/2`) with abort buttons.
- GC status: last sweep counts, next tick ETA.
- Telemetry counters live-updated from the existing events.

### Deferred / open questions
- **Versioning, ACLs, bucket policies, server-side encryption.** Not on the homelab path.
- **Content-addressed dedupe.** Rename on-disk file to `sha256(body)` and add a refcount column. Worth it on a homelab where the same backup tarball lands in multiple buckets.
- **Read-connection pool.** Defer until profiling actually says the single-GenServer is the bottleneck. NimblePool with N read conns + the existing writer is the path.
- **Strict prefix-cursor on degenerate `0xFF` keys.** The all-`0xFF` prefix branch in `list_uploads` falls back to a non-prefix-bounded scan and is approximate. Affects literally no real key.
- **List-buckets pagination.** S3 caps at 1000; we don't. Adds `<MaxBuckets>`/markers if anyone ever has 1k+ buckets on a homelab.
- **Hash-named on-disk files.** Defense in depth beyond the validator — store as `<root>/<bucket>/<aa>/<bb>/<sha256(key)>`. Trades human-readable filenames for one fewer attack surface. The validator is doing the job today.

## Known wire-format gaps that the AWS CLI test will likely surface

Calling these out so the test session has a checklist:

- `<EncodingType>` echo on ListObjectsV2 responses — we don't emit it; some clients want it when `encoding-type=url` was requested.
- Header case — Bandit lowercases response headers; `aws-cli` is generally fine with either, but some SigV4-signing clients expect specific casing on `ETag` / `Content-Length`.
- `Date` header — Bandit emits `date:` automatically. Some clients require it for SigV4.
- `<Owner>` element on `<Contents>` is not emitted unless `fetch-owner=true` is requested. We never emit it. aws-cli might complain.
- Error response `<RequestId>` and `<HostId>` — S3 always returns these; we don't. Some clients may log warnings but should still parse.
- `HEAD` on non-existent key returns empty 404 — S3 returns specific `x-amz-error-code` headers in this case for HEAD (since there's no XML body). We don't.
- `aws s3 sync` may use `ListObjectV2` with `start-after` *and* `continuation-token`. We accept either-or, not both. Need to check spec behavior.
