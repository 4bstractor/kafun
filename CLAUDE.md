# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Kafun is an S3-compatible blob service for the homelab. **Elixir / OTP 27 / Bandit** for the HTTP front; **SQLite (WAL)** for the metadata index; the OS filesystem for the bytes themselves. The Python originals (`legacy/bucket.py`, `legacy/migrate.py`) are kept as design reference only — the live implementation is the mix project at the root.

## Layout

- `lib/kafun/application.ex` — supervision tree (`Kafun.Index` → `Bandit`)
- `lib/kafun/router.ex` — Plug.Router; the entire S3 surface lives here
- `lib/kafun/storage.ex` — filesystem blob ops (path scheme, streaming PUT, Range parsing, multipart concat)
- `lib/kafun/index.ex` — single-conn SQLite GenServer, prepared statements, `upper_bound/1` for prefix → range, `uploads`/`parts` tables
- `lib/kafun/multipart.ex` — initiate / upload-part / complete / abort orchestration; computes the `md5-of-md5s-N` ETag
- `lib/kafun/auth.ex` — SigV4 access-key extraction (header **and** querystring presigned URLs)
- `lib/kafun/s3_xml.ex` — iolist builders + a Saxy-backed parser for the CompleteMultipartUpload body
- `config/runtime.exs` — env-var ingest

## Run

```
mix deps.get
mix test
KAFUN_ROOT=/sanzu/objects KAFUN_KEYS=key1,key2 mix run --no-halt
```

Env vars: `KAFUN_ROOT` (blob dir, required in prod), `KAFUN_DB` (defaults to `<root>/index.db`), `KAFUN_HOST` (default `0.0.0.0`), `KAFUN_PORT` (default `8333`), `KAFUN_KEYS` (comma-separated; empty = auth off), `KAFUN_LOG_LEVEL`.

Single-test run: `mix test test/kafun_test.exs:LINE` or `mix test --only describe:"Index round-trip"`.

## Architecture notes

**Two-tier storage.** Bytes live at `<root>/<bucket>/<aa>/<bb>/<key>` (sha1-sharded, two-level fanout). Metadata lives in `<root>/index.db` keyed by `(bucket, key) WITHOUT ROWID`. Listings hit the index — never the filesystem. The two stores can drift if a crash lands between blob `rename(2)` and SQL commit; on a partial PUT the index simply doesn't know about the orphan, and a future GC sweep can reconcile by walking the blob dirs. Don't try to make the two stores transactional — the value isn't worth the complexity.

**Streaming PUT.** `Storage.stream_put/4` opens `<final>.tmp.<rand>` in the destination shard, drains `Plug.Conn.read_body` in 64 KiB chunks, hashes MD5 inline (S3-canonical ETag for non-multipart), then `:file.rename` to publish atomically. Errors unlink the temp. The streaming hash means we never hold the body in memory, even for multi-GB PUTs.

**Listing.** Prefix queries are converted to a half-open byte range via `Index.upper_bound/1` (rightmost-non-`0xFF` byte +1, carrying through trailing `0xFF`s). The all-`0xFF` case has no expressible upper bound; the code falls back to a `>= prefix` scan there. We over-fetch by one row to know whether to set `IsTruncated=true` and emit a `NextContinuationToken` (base64url of the last key). **No delimiter / common-prefixes support yet** — adding it means iterating page rows in the GenServer and re-binding the lower bound when we cross a delimiter (see comment block in `Kafun.Index.handle_call({:list, ...})`).

**Index concurrency.** One GenServer owns the SQLite handle and serializes both reads and writes. WAL is on, but we don't exploit it — every call queues. For homelab volumes this is fine; if it becomes a bottleneck, the upgrade path is a `NimblePool` of read connections (writes still through the GenServer to keep `INSERT OR REPLACE` race-free).

**Auth.** `Kafun.Auth.access_key/1` parses the access key from either the `Authorization: AWS4-HMAC-SHA256 Credential=…/…` header or the `X-Amz-Credential=` querystring (presigned URLs). Signature is **not** verified — same trusted-network model as the Python original. Empty `KAFUN_KEYS` disables auth entirely. Do not add signature verification without explicit ask.

**Multipart.** `POST /:bucket/:key?uploads` initiates and returns an opaque `UploadId` (18 random bytes, url-safe base64). `PUT /:bucket/:key?partNumber=N&uploadId=…` streams the part to `<root>/.uploads/<id>/<n>` with the same temp+rename dance as a regular PUT. `POST /:bucket/:key?uploadId=…` parses the client-supplied parts list (Saxy `SimpleForm.parse_string`), validates each `(partNumber, etag)` against what we recorded, concatenates the parts in client-supplied order into the final blob, and writes the index entry. Final ETag is `md5(decode_hex(part1) || decode_hex(part2) || …)` plus `-N` — the canonical S3 formula. Abort (`DELETE /:bucket/:key?uploadId=…`) just blows away `<root>/.uploads/<id>` and the metadata rows. The two stores are ordered: index commit happens *after* the rename, so a crash mid-Complete leaves an orphan blob and an orphan upload row but never a half-installed index entry. Cleanup is the GC job's problem, not the request path's.

**Test isolation.** `config/test.exs` sets `start_children?: false` so `Kafun.Application.start/2` doesn't bring up the shared Index/Bandit; tests `start_supervised!` their own Index pointing at a per-test tmp DB. If you introduce another long-lived process, gate it on the same flag. Multipart tests `Application.put_env(:kafun, :root, tmp)` to redirect the storage root.

## What's deliberately missing

- `ListMultipartUploads` (`GET /:bucket?uploads`) and `ListParts` (`GET /:bucket/:key?uploadId=…`) — boto3 doesn't need these for the upload happy path; aws-cli does for `ls --recursive` of in-progress uploads.
- `UploadPartCopy` (`PUT /:bucket/:key?partNumber=…&uploadId=…` with `x-amz-copy-source`) — server-side range copy for assembling a new object from chunks of an existing one.
- Delimiter / common-prefixes in ListObjectsV2. Most boto3 callers don't need it; `aws s3 ls s3://bucket/path/` does.
- Versioning, ACLs, bucket policies, server-side encryption.
- Content-addressed dedupe. Easy to retrofit: rename the on-disk file to `sha256(body)` and add a refcount in the index. Worth it on a homelab where the same wallpaper / backup tarball ends up in multiple buckets.
- A GC sweep for orphan blobs / abandoned uploads. Walk `<root>/.uploads/` for dirs older than 24h with no matching `uploads` row → delete; walk shard dirs for files with no matching `objects` row → delete.

## Migrator

`legacy/migrate.py` still works — it speaks vanilla S3 and we expose vanilla S3. Point `DST_ENDPOINT` at our Bandit listener. The Python script's `verify_counts` previously had a "destination doesn't support list" branch; that branch is now unreachable since we serve `ListObjectsV2`.
