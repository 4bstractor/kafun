# Kafun

A small S3-compatible blob service for the homelab. Filesystem for bytes, SQLite for the index, BEAM for everything else.

## Quick start

```sh
mix deps.get
KAFUN_ROOT=/tmp/kafun mix run --no-halt
```

```sh
curl -X PUT --data-binary @big.tar http://localhost:8333/imouto/big.tar
curl http://localhost:8333/imouto/big.tar -o roundtrip.tar
curl 'http://localhost:8333/imouto?list-type=2&prefix=big'
```

## Configuration

| Env var            | Default                | Notes |
| ------------------ | ---------------------- | ----- |
| `KAFUN_ROOT`       | `${tmp}/kafun` in dev  | Required in prod. Blob root + default DB location. |
| `KAFUN_DB`         | `<root>/index.db`      | SQLite metadata file. |
| `KAFUN_HOST`       | `0.0.0.0`              | |
| `KAFUN_PORT`       | `8333`                 | |
| `KAFUN_KEYS`       | *(empty)*              | Comma-separated allowed access keys. Empty = auth off. |
| `KAFUN_LOG_LEVEL`  | `info`                 | `debug` / `info` / `warning` / `error`. |

## Endpoints

| Method | Path                                          | What |
| ------ | --------------------------------------------- | ---- |
| GET    | `/healthz`                                    | Liveness, never auth-gated. |
| GET    | `/`                                           | `ListAllMyBuckets`. |
| PUT    | `/:bucket`                                    | Idempotent create. |
| GET    | `/:bucket?list-type=2&...`                    | `ListObjectsV2` — `prefix`, `max-keys`, `start-after`, `continuation-token`. |
| PUT    | `/:bucket/<key>`                              | Streamed upload. ETag = MD5 hex. |
| POST   | `/:bucket/<key>?uploads`                      | Initiate multipart. Returns `UploadId`. |
| PUT    | `/:bucket/<key>?partNumber=N&uploadId=…`      | Upload one part. ETag = MD5 of part. |
| POST   | `/:bucket/<key>?uploadId=…`                   | Complete multipart (XML parts list in body). ETag = `md5-of-md5s-N`. |
| DELETE | `/:bucket/<key>?uploadId=…`                   | Abort multipart. |
| GET    | `/:bucket/<key>`                              | `Range: bytes=…` honoured (zero-copy `sendfile(2)`). |
| HEAD   | `/:bucket/<key>`                              | Metadata only. |
| DELETE | `/:bucket/<key>`                              | 204. |

## Auth

SigV4 access key is parsed from the `Authorization` header *or* an `X-Amz-Credential=` query param (presigned URLs). The signature is **not** verified — Kafun assumes a trusted network. To turn auth off entirely, leave `KAFUN_KEYS` unset.

## Tests

```
mix test
```
