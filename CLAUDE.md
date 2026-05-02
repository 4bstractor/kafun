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

**aws-chunked unwrap.** Modern boto3 (≥1.36) and aws-cli send PUT bodies as `Content-Encoding: aws-chunked` with `x-amz-content-sha256: STREAMING-…` and an optional CRC32 trailer. The wire body is `<hex-size>[;chunk-signature=…]\r\n<data>\r\n` repeated, terminated by `0\r\n[trailers]\r\n`. `Storage.aws_chunked?/1` detects this from headers, and `Storage.consume_chunked/6` is a streaming state machine (`:size_line | {:data, n} | :data_crlf | :trailer`) that reads the wire body in 64 KiB pulls, writes only the data segments to disk, and discards extensions and trailers. ETag is computed over the unwrapped bytes. The same unwrap covers `stream_put` and `stream_part_put` because both go through `stream_to_disk`. Without this, every byte of chunk overhead lands in the stored object — a silent +N corruption.

**Listing.** Prefix queries are converted to a half-open byte range via `Index.upper_bound/1` (rightmost-non-`0xFF` byte +1, carrying through trailing `0xFF`s). Delimiter / common-prefixes is implemented as a multi-page scanner in `Index.scan_loop/11`: classify each row as `:content` or `{:cp, prefix}`; when emitting a CP, set `in_cp` to skip subsequent rows starting with it, and the next-page lower bound becomes `upper_bound(cp)` so a re-query naturally jumps the whole subtree. Cursors are `>=` (inclusive) — the continuation token is `Base.url_encode64(lower_bound)`. `start-after` from S3 callers maps to `key <> <<0>>` so we keep one SQL shape. After hitting `max_keys`, a 1-row peek confirms whether truncation is real (avoids the trailing-empty-page footgun). When clients pass both `continuation-token` and `start-after`, CT wins (S3 spec). `encoding-type=url` is echoed in the response and URL-encodes `<Key>`, `<Prefix>`, `<Delimiter>`, and `<CommonPrefixes>`. `fetch-owner=true` emits a fixed `<Owner><ID>kafun</ID><DisplayName>kafun</DisplayName></Owner>` block on `<Contents>` (we don't model per-object ownership).

**Multipart.** `POST /:bucket/:key?uploads` initiates and returns an opaque `UploadId` (18 random bytes, url-safe base64). `PUT /:bucket/:key?partNumber=N&uploadId=…` streams the part to `<root>/.uploads/<id>/<n>` with the same temp+rename dance. `POST /:bucket/:key?uploadId=…` parses the client-supplied parts list (Saxy `SimpleForm.parse_string`), validates each `(partNumber, etag)` against what we recorded, concatenates in client-supplied order into the final blob, and writes the index entry. Final ETag is `md5(decode_hex(part1) || …)` plus `-N`. The two stores are ordered: index commit happens *after* the rename, so a crash mid-Complete leaves an orphan blob and orphan upload row but never a half-installed index entry. Cleanup is the GC's job, not the request path's. Multipart listing (`?uploads`, `?uploadId=…`) is paginated by `(key, upload_id)` and `partNumber` markers respectively.

**Server-side copy.** `PUT` with `x-amz-copy-source: /srcBucket/srcKey` triggers `Storage.copy_blob/5` — a `:file.copy/3` between the sharded paths under the same temp+rename discipline. The destination inherits the source's ETag and content-type from the index (bytes are identical, no rehash). `UploadPartCopy` (the same header on a part-PUT) goes through `Storage.copy_part/6`, which honours `x-amz-copy-source-range: bytes=A-B` by `:file.position/2`-seeking the source FD before streaming exactly that window into the part slot, computing MD5 inline. Source paths in the header may be URL-encoded and may carry a `?versionId=…` suffix (we strip it — versioning is out of scope). Both flavours surface `NoSuchKey`/`NoSuchBucket`/`NoSuchUpload` per S3 semantics; mid-route races (index says yes, blob already gone) also fall through to `NoSuchKey`.

**Multi-Object Delete.** `POST /:bucket?delete` with a `<Delete><Object><Key>…</Key></Object>…[<Quiet>true</Quiet>]</Delete>` body (Saxy parsed via `S3XML.parse_delete_body/1`). Up to 1000 keys per request; body capped at 1 MiB. Each key is run through `Storage.valid_key?/1` and reported as either `<Deleted>` (always idempotent — deleting a non-existent key is a no-op success) or `<Error Code="InvalidKey">`. Quiet mode suppresses `<Deleted>` blocks but always emits `<Error>`. Status is always 200 even on per-key errors; clients dispatch on the response body. Telemetry: `[:kafun, :delete_objects, :stop]` with `:count`, `:deleted`, `:errors`, `:duration`.

**Bucket existence gating.** `Index.bucket_exists?/1` is a prepared `SELECT 1 FROM buckets WHERE name = ? LIMIT 1` and runs in `with_object/4`, the bucket-level `GET`, the bucket-level `POST ?delete`, and `HEAD /:bucket`. Operations against a never-created bucket return `404 NoSuchBucket` instead of silently empty results or auto-creating. CreateBucket (`PUT /:bucket`) is the only path that still calls `Index.ensure_bucket/1`. Note: `Index.put/7` *also* still calls `ensure_bucket_inline` — that's intentional so direct API callers in tests work, but the router always passes through the existence guard before reaching it. **DeleteBucket** (`DELETE /:bucket`) is gated by `Index.delete_bucket/1`, which checks `bucket_has_objects` (a separate prepared statement) before issuing the row delete and returns `:not_empty` for the `BucketNotEmpty` 409. The router additionally calls `File.rmdir/1` on the bucket subdir after a successful index delete — we let it fail silently because in some test scenarios the dir was never created.

**Bucket sub-resources.** `GET /:bucket?location|acl|versioning` return minimal stubs (`us-east-1`, single FULL_CONTROL grant to a fixed `kafun` owner, empty versioning config). `?policy|cors|lifecycle|tagging` return their proper `NoSuchBucketPolicy`/`NoSuchCORSConfiguration`/`NoSuchLifecycleConfiguration`/`NoSuchTagSet` 404 error codes — boto3 expects these specific codes and silently treats them as "feature not configured." Without these branches, ListObjectsV2 would fire instead and clients would get a list of objects when they asked for a config document.

**Request id.** `:stamp_request` plug is the second plug in the pipeline (right after `Plug.Logger`). Each request gets a `:crypto.strong_rand_bytes(8)` upper-hex id assigned to `conn.assigns[:request_id]` and emitted as `x-amz-request-id` on every response. The same id is carried into `<RequestId>` and `<HostId>` of error XML bodies (we don't differentiate the two; real S3 has two distinct values for AWS-internal tracing, irrelevant here). The `error/4` helper detects `conn.method == "HEAD"` and emits empty body + `x-amz-error-code: <Code>` header instead of the XML body — HEAD has no body to carry the code, so the header is the only signal.

**Index concurrency.** One GenServer owns the SQLite handle and serializes both reads and writes. WAL is on, but we don't exploit it — every call queues. For homelab volumes this is fine; if it becomes a bottleneck, the upgrade path is a `NimblePool` of read connections (writes still through the GenServer to keep `INSERT OR REPLACE` race-free). Two indexes were added beyond the implicit PKs: `uploads(bucket, key, upload_id)` for ListMultipartUploads and `uploads(initiated_at)` for the GC abandoned-upload query. Three idempotent `ALTER TABLE` migrations run in `Index.init/1` for legacy DBs: `parts.mtime`, `objects.meta`, `uploads.meta`.

**User metadata.** `x-amz-meta-*` headers on PUT, CopyObject, and InitiateMultipartUpload are collected into a `%{name => value}` map by `Router.collect_user_meta/1`, JSON-encoded via OTP 27's built-in `:json` module, and stored in `objects.meta` (or `uploads.meta` until Complete promotes it). On GET/HEAD, `Router.put_user_meta/2` walks the decoded map and emits each entry as `x-amz-meta-<name>: <value>`. CopyObject in default COPY mode carries source metadata to the destination (the only directive we honour today; REPLACE is a Tier-2 follow-up). Multipart uploads stash meta at Initiate, read it from the `uploads` row at Complete, and pass it to `Index.put`.

**Conditional requests.** `eval_get_preconditions/2`, `eval_put_preconditions/2`, and `eval_copy_preconditions/2` implement RFC 7232 ordering: If-Match overrides If-Unmodified-Since; If-None-Match overrides If-Modified-Since. Etag comparison strips quotes and accepts `*` as a wildcard. PUT honours `If-Match: *` (must exist) and `If-None-Match: *` (must not exist) for atomic write-once semantics. CopyObject reads the same four headers in the `x-amz-copy-source-if-*` namespace against the source's metadata. Date parsing handles RFC 1123 only (`"Sat, 02 May 2026 06:01:23 GMT"`); other forms are treated as missing — same lenience as S3. Failures return either 304 NotModified (safe-method conditional miss) or 412 PreconditionFailed.

**GC.** `Kafun.GC` is a tick-based GenServer in the supervision tree. Each tick runs three passes:
1. **Abandoned uploads** — `uploads` rows older than `:abandon_after`, aborted via `Multipart.abort/2`.
2. **Orphan part dirs** — `<root>/.uploads/<id>/` subdirs without a matching `uploads` row (crash-window orphans).
3. **Orphan blobs / leftover tmps** — `Storage.list_blob_files/1` walks shard tree; deletes files older than `:blob_grace_seconds` that are either `.tmp.<rand>` leftovers or regular blobs with no `objects` row. The grace window is what keeps this from racing legitimate in-flight PUTs.

`KAFUN_GC_INTERVAL_SEC=0` disables periodic sweeps; `Kafun.GC.run_now/0` works regardless. Counts surface via `[:kafun, :gc, :run]` measurements.

**Auth.** `Kafun.Auth.access_key/1` extracts the access key from either the `Authorization: AWS4-HMAC-SHA256 Credential=…/…` header or the `X-Amz-Credential=` querystring (presigned URLs). Signature is **not** verified — same trusted-network model as the Python original. Empty `KAFUN_KEYS` disables auth entirely. **Do not add signature verification without explicit ask.**

**Path traversal protection.** `Storage.valid_key?/1` rejects empty / >1024-byte keys, control bytes (`\0\n\r`), keys starting with `/`, and any key whose `Path.split/1` contains a `.` or `..` segment. This matters because the on-disk layout uses the raw key as the leaf filename — without validation, a key like `"../../../../tmp/pwned"` would write to `/tmp/pwned` after traversing out of `<root>/<bucket>/<aa>/<bb>/`. The validator runs in the router's `with_object/4` wrapper alongside the bucket-existence check, so every object-level handler is gated. Same validator also gates each key inside `DeleteObjects` so a batch delete can't smuggle an `../escape`.

**Telemetry.** Every handler emits one terminal `[:kafun, <op>, :stop]` event with `:duration` (μs) and `:size` where applicable. Multipart family: `[:kafun, :multipart, :initiate | :upload_part | :complete | :abort]`. GC: `[:kafun, :gc, :run]`. Metadata always carries `:bucket` and `:key` for object ops (or `:upload_id` for multipart). Nothing pre-attaches — consumers call `:telemetry.attach_many/4`. Adding a new event is one line via the router's `emit/3` helper.

**Test isolation.** `config/test.exs` sets `start_children?: false` so `Kafun.Application.start/2` doesn't bring up the shared Index/Bandit/GC. Tests `start_supervised!` their own Index pointing at a per-test tmp DB and start GC with `interval_ms: 0` to disable the tick. Multipart tests `Application.put_env(:kafun, :root, tmp)` to redirect the storage root. If you introduce another long-lived process, gate it on the same flag.

## What's left for S3 parity

Done: ListAllMyBuckets, CreateBucket, HeadBucket, DeleteBucket, ListObjectsV2 (delimiter / pagination / encoding-type / fetch-owner), `?location|acl|versioning|policy|cors|lifecycle|tagging` stubs, PutObject (with `If-Match`/`If-None-Match`), GetObject (Range, all four conditional headers), HeadObject, DeleteObject, multipart Initiate/UploadPart/Complete/Abort/ListMultipartUploads/ListParts (Initiate carries user metadata through to Complete), CopyObject (with `x-amz-copy-source-if-*`), UploadPartCopy, DeleteObjects (multi-delete), aws-chunked unwrap, user metadata round-trip (`x-amz-meta-*`), NoSuchBucket gating, x-amz-request-id + error RequestId/HostId, x-amz-error-code on HEAD 404. End-to-end verified against real boto3 (990-image migration) and via curl wire tests.

### Tier 2 — nice-to-haves with realistic clients

- **CopyObject `x-amz-metadata-directive=REPLACE`.** Default COPY mode carries source meta forward (already done); REPLACE would take new `x-amz-meta-*` from the request and overwrite. Common pattern is "copy to self with new content-type" for content-type fixups.
- **Multi-range GET** (`Range: bytes=0-100,200-300`) returning `multipart/byteranges`. Single-range only today. aws-cli doesn't issue these; some video tooling does.
- **`x-amz-bucket-region` header on HeadBucket success.** Real S3 emits this so clients can route cross-region. Single fixed value (`us-east-1`) here.
- **List-buckets pagination.** S3 caps at 1000 buckets per page; we don't paginate. Real homelab user is unlikely to hit this.
- **Object-level tagging** (`PUT/GET/DELETE /:bucket/:key?tagging`). Distinct from bucket tagging (already stubbed as 404). boto3 sometimes calls these, mostly tolerable as 404 NoSuchTagSet.
- **POST-form upload** (browser direct upload via signed policy). aws-cli doesn't use; web frontends do.
- **Strict `If-Match`/`If-None-Match` etag-list semantics.** We currently honour wildcards and a single etag; multi-etag lists (`"a", "b"`) parse but the precedence around quoting/whitespace hasn't been stress-tested against odd clients.

### Tier 3 — explicitly out of scope unless asked

- **Versioning, ACLs, bucket policies, IAM, server-side encryption (SSE-S3/KMS/C), object lock, replication, lifecycle rules, inventory, intelligent-tiering, CORS, website hosting.** Deferred — homelab trusted-network model doesn't need them. If implemented later, most are stub responses; only versioning would touch the index schema.
- **SigV4 signature verification.** Same trusted-network policy. **Do not add without explicit ask.**

### Other known unknowns

- **Header case.** Bandit lowercases response headers; aws-cli/boto3 are tolerant, hand-rolled SigV4 clients may not be. No known incident.
- **Strict prefix-cursor on degenerate `0xFF` keys.** The all-`0xFF` prefix branch in `list_uploads` falls back to a non-prefix-bounded scan and is approximate. Affects literally no real key.
- **Read-connection pool.** Defer until profiling actually says the single-GenServer is the bottleneck. NimblePool with N read conns + the existing writer is the path.
- **Content-addressed dedupe.** Rename on-disk file to `sha256(body)` and add a refcount column. Worth it on a homelab where the same backup tarball lands in multiple buckets.
- **Hash-named on-disk files.** Defense in depth beyond the validator — store as `<root>/<bucket>/<aa>/<bb>/<sha256(key)>`. Trades human-readable filenames for one fewer attack surface. The validator is doing the job today.

## Roadmap

Phoenix admin UI (last per user direction). Light LiveView app — probably its own OTP app inside an umbrella, or a sibling Plug router on a separate port. Pages:
- Buckets list with object counts and total size (needs new aggregation queries on `objects`).
- Per-bucket browser with prefix navigation (delimiter listing already does the work).
- In-flight multipart uploads (`Index.list_uploads/2`) with abort buttons.
- GC status: last sweep counts, next tick ETA.
- Telemetry counters live-updated from the existing events.
