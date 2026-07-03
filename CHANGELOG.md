# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `COMPARISON.md` — positioning vs Garage, SeaweedFS, and MinIO
  (community edition archived Feb 2026), linked from the README.

## [0.2.1] — 2026-07-03

First release published to both registries: `ghcr.io/4bstractor/kafun`
(public) and the homelab registry.

### Changed

- Split CI/release workflows by host: `.gitea/workflows/` publishes the
  homelab image to giyouden (artifacts.harvelab.com); `.github/workflows/`
  publishes the public image to ghcr.io. Gitea skips `.github/workflows/`
  when `.gitea/workflows/` exists, so one branch serves both remotes.

## [0.2] — 2026-05-18

### Changed

- Release workflow publishes to the giyouden homelab registry
  (artifacts.harvelab.com) instead of ghcr.io.

## [0.1.1] — 2026-05-10

### Fixed

- Gitea Actions compatibility in CI and release workflows
  (`runs-on: ubuntu-22.04`, local packaging fixes).

## [0.1] — 2026-05-09

First tagged cut. Production-deployed on yomi since 2026-05-03.

### Added

- S3 wire surface: ListAllMyBuckets, CreateBucket, HeadBucket, DeleteBucket,
  ListObjectsV2 (delimiter, pagination, encoding-type, fetch-owner),
  PutObject, GetObject (Range + all four conditional headers), HeadObject,
  DeleteObject, CopyObject, UploadPartCopy, multipart Initiate /
  UploadPart / Complete / Abort / ListMultipartUploads / ListParts,
  DeleteObjects (multi-delete), `?location|acl|versioning` stubs and
  proper `NoSuchBucketPolicy|CORS|Lifecycle|TagSet` 404s.
- aws-chunked unwrap (modern boto3 / aws-cli streaming PUTs).
- User metadata round-trip (`x-amz-meta-*`) on PUT, CopyObject, and
  multipart Initiate → Complete.
- Conditional requests (`If-Match`, `If-None-Match`, `If-Modified-Since`,
  `If-Unmodified-Since`, plus the `x-amz-copy-source-if-*` namespace).
- `x-amz-request-id` on every response; HEAD 404s emit
  `x-amz-error-code` since they have no body.
- Mutable access keys + per-bucket grants + 3-tier permission model
  (read ⊂ write ⊂ admin) + real SigV4 signature verification +
  anonymous public-read buckets.
- Phoenix admin UI on a separate port: buckets, objects, in-flight
  multiparts, GC status, access keys, per-bucket permissions.
- Filesystem GC: tick-based janitor with three reconciliation passes
  (abandoned uploads, orphan part dirs, orphan blobs / leftover tmps).
- Migrator (`mix kafun.migrate`) for pulling from any S3 source via
  Req + a hand-rolled SigV4 signer.
- Telemetry: terminal `[:kafun, <op>, :stop]` events on every handler.
- Production deploy artifacts: multi-stage Alpine Dockerfile,
  `docker-compose.yml`, env template, runbook (`DEPLOY.md`).

### Known limits

- Streaming-signed payload bodies (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`)
  rejected with 400 for verified keys. Modern boto3 defaults to the
  unsigned variant; aws-cli accepts a config flip.
- Single-shot PUT in the migrator caps at 4 GiB.
- List-buckets does not paginate.
- `access_keys.secret` stored in plain text in SQLite (homelab model).
- Backup story still the docker-exec `Kafun.Backup.run/0` path; ZFS
  snapshot rework deferred.

[Unreleased]: https://github.com/4bstractor/kafun/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/4bstractor/kafun/compare/v0.2...v0.2.1
[0.2]: https://github.com/4bstractor/kafun/compare/v0.1.1...v0.2
[0.1.1]: https://github.com/4bstractor/kafun/compare/v0.1...v0.1.1
[0.1]: https://github.com/4bstractor/kafun/releases/tag/v0.1
