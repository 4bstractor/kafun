# kafun vs the alternatives

An honest read on where kafun sits next to [Garage], [SeaweedFS], and
[MinIO]. All four speak S3; they are not interchangeable. Last reviewed
July 2026 — these projects move, check their own docs before deciding.

The short version: kafun is the smallest and least capable of the four,
on purpose. If the trade-offs below don't read as features to you, use
one of the others.

[Garage]: https://garagehq.deuxfleurs.fr/
[SeaweedFS]: https://github.com/seaweedfs/seaweedfs
[MinIO]: https://github.com/minio/minio

## At a glance

|                    | kafun | Garage | SeaweedFS | MinIO (community) |
|--------------------|-------|--------|-----------|-------------------|
| Status (2026-07)   | active | active | active | **archived Feb 2026** |
| Language           | Elixir/BEAM | Rust | Go | Go |
| License            | Apache-2.0 | AGPL-3.0 | Apache-2.0 | AGPL-3.0 |
| Topology           | single node only | 1–N nodes, geo-distributed | 1–N nodes, scale-out | 1–N nodes, erasure sets |
| Bytes on disk      | plain files, one per object | chunked, deduplicated, optionally zstd blocks | append-only needle volumes | erasure-coded parts + `xl.meta` |
| Metadata           | one SQLite file | LMDB or SQLite per node | pluggable filer store (SQLite, MySQL, Redis, …) | embedded, per-object `xl.meta` |
| Versioning         | no | no | yes | yes |
| IAM / policies     | no — per-key 3-tier grants | no — per-key bucket flags | yes (policies, identities) | yes |
| SSE / object lock  | no | no | SSE yes | yes |
| Replication / EC   | no | replica count per zone | replication + optional EC | erasure coding |
| Admin UI           | built in (Phoenix LiveView) | admin REST API, UI emerging | filer UI, basic | stripped from community in 2025 |
| Designed scale     | tens of millions of objects | small clusters | billions of small files | petabyte clusters |

## The axis that actually matters: what happens to your bytes

Every system in this table except kafun transforms your data on the way
to disk — chunking, deduplication, erasure coding, append-only volume
files. That buys them replication, dedup, and scale-out. It also means
the *only* way to get your bytes back is through the running service
(or its recovery tooling).

kafun writes each object as one plain file at
`<root>/<bucket>/<aa>/<bb>/<key>` and keeps all metadata in one SQLite
file next to it. The consequences:

- `ls`, `rsync`, `zfs send`, `restic` — every filesystem tool you
  already trust works on your objects directly.
- Disaster recovery is "mount the disk somewhere else." The worst
  possible kafun failure leaves you with a directory tree of your
  files and a SQLite database you can open with `sqlite3`.
- Filesystem snapshots (ZFS, btrfs, LVM) are consistent-enough
  backups of everything at once.

The price: no dedup, no compression beyond what your filesystem does,
no erasure coding, and one file per object means the filesystem's
limits are your limits. kafun's position is that on a homelab, ZFS
already does checksumming, compression, snapshots, and redundancy
better than an object store reimplementing them one layer up.

## Garage

The closest cousin. Also small, also key-based auth with per-bucket
permissions instead of IAM, also explicitly refuses versioning and the
big-S3 feature set. Rust, AGPL-3.0, built by [Deuxfleurs] for
self-hosted geo-distributed clusters.

**Choose Garage over kafun when** you have — or will have — more than
one machine. Multi-node replication with configurable consistency is
Garage's entire reason to exist, and it's good at it: no consensus
protocol, tolerates nodes on dynamic IPs, handles zones and replica
counts properly. Recent releases added `--single-node` conveniences,
so it runs fine on one box too. It also does static website hosting
from buckets, which kafun doesn't.

**Choose kafun over Garage when** you're one node and staying that
way, and you want your bytes as plain files instead of deduplicated
chunk blocks. Garage's on-disk format is opaque; recovery goes through
Garage. kafun also ships a full admin UI (bucket browser, uploads,
key management) out of the box, where Garage's is a REST API with a
web UI still maturing.

[Deuxfleurs]: https://deuxfleurs.fr/

## SeaweedFS

The most capable actively-developed option. Apache-2.0, Go, modeled on
Facebook's Haystack paper: master + volume servers + filer + S3
gateway, collapsible into one process with `weed server -s3`. Designed
for billions of small files with O(1) disk reads. Its S3 surface has
grown well past kafun's: versioning, SSE, bucket policies, identities.

**Choose SeaweedFS over kafun when** you need any of that — versioning
especially — or when your object count has a "billions" in it, or when
you expect to scale beyond one machine. Since MinIO's archival it's
the default recommendation for a serious self-hosted S3.

**Choose kafun over SeaweedFS when** the moving parts aren't worth it.
Even in single-binary mode, SeaweedFS is four services with distinct
failure modes, a pluggable metadata store to choose, and volume files
you can't read without SeaweedFS. kafun is one supervised BEAM process,
one SQLite file, and a directory tree — ~6,600 lines of Elixir you can
read in an afternoon.

## MinIO

For years the reflex answer, and the reason to address it directly:
**the community edition is no longer a going concern.** MinIO stripped
management features from the community console in mid-2025 (object
browser only), declared maintenance mode in late 2025, and archived
the repository in February 2026. Development continues in the
commercial AIStor product. Existing binaries still run, but there are
no feature releases and only case-by-case security fixes.

**Choose AIStor** if you're an enterprise with the budget and need the
deepest S3 compatibility available off-AWS.

**Don't start a new homelab deployment on MinIO community edition** in
2026. If you're migrating off one, any of the three above works;
`kafun` ships a pull migrator (`mix kafun.migrate`) that speaks SigV4
to any S3 endpoint, MinIO included.

## Why kafun exists anyway

Fair question — Garage and SeaweedFS are excellent. kafun was built
for a niche the others deliberately skip: **one box, a big ZFS pool,
and the conviction that the filesystem is the storage layer, not an
implementation detail to abstract away.**

Everything follows from that: plain files because ZFS already does
integrity and redundancy; SQLite because one node needs no distributed
metadata; a real admin UI because a homelab operator is a person with
a browser, not a fleet automation system; per-key grants instead of
IAM because you can count your clients on one hand; and a hard "no" to
versioning, SSE, and replication because they'd triple the codebase to
duplicate what the layer below already provides.

If that's your situation, the small tool is the right tool. If it
isn't, the table above tells you where to go — no hard feelings.
