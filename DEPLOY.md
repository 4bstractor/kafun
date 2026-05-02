# Deploy

Production deployment runbook. Targets a single Void Linux host (yomi)
fronted by NPM (`objects.harvelab.com`). Service supervision is runit
(Void's default); openrc/systemd variants will land if/when ACL +
multi-user work happens.

## 1. Build the release on the staging machine

```sh
git clean -fdx _build/prod                # or `mix deps.clean --all` if deps were tampered with
MIX_ENV=prod mix deps.get
MIX_ENV=prod mix release --overwrite
tar czf kafun-0.1.0.tgz -C _build/prod/rel kafun
```

The tarball is ~70 MB with bundled ERTS — no Erlang/Elixir needed on yomi.

## 2. Stage the host (one-time, on yomi)

```sh
sudo useradd --system --no-create-home --home-dir /opt/kafun --shell /sbin/nologin kafun
sudo mkdir -p /opt/kafun /etc/kafun /var/backups/kafun /var/log/kafun /sanzu/objects
sudo chown -R kafun:kafun /opt/kafun /var/backups/kafun /var/log/kafun /sanzu/objects
sudo chown root:kafun /etc/kafun
sudo chmod 750 /etc/kafun
```

`/sanzu/objects` is the data dir — change to wherever you want
`KAFUN_ROOT` to point. `/var/log/kafun/` is where svlogd writes the
service log.

## 3. Ship the release

```sh
scp kafun-0.1.0.tgz yomi:/tmp/
ssh yomi "sudo -u kafun tar xzf /tmp/kafun-0.1.0.tgz -C /opt && rm /tmp/kafun-0.1.0.tgz"
# Now: /opt/kafun/{bin,erts-*,lib,releases}
```

## 4. Install the runit service + env file

From this repo on yomi (or scp'd):

```sh
sudo cp -R rel/sv/kafun /etc/sv/kafun
sudo chown -R root:root /etc/sv/kafun
sudo chmod 755 /etc/sv/kafun/run /etc/sv/kafun/log/run

sudo cp rel/kafun.env.example /etc/kafun/kafun.env
sudo chown root:kafun /etc/kafun/kafun.env
sudo chmod 640 /etc/kafun/kafun.env
sudo $EDITOR /etc/kafun/kafun.env
```

Things you **must** edit in `/etc/kafun/kafun.env`:

- `KAFUN_KEYS=` — comma-separated allowed S3 access keys. Generate with
  `openssl rand -hex 10 | tr 'a-f' 'A-F'`. Empty = auth off (LAN-trusted).
- `KAFUN_ADMIN_SECRET=` — required in prod. Generate with
  `openssl rand -base64 64 | tr -d '\n' | head -c 64`. **Set once and
  leave alone** — rotating it logs every active admin session out.
- `RELEASE_COOKIE=` — any stable random string. Required for `kafun rpc`
  to work (used by the backup cron).
- `KAFUN_ADMIN_PASSWORD=` — leave blank for an open LAN admin UI; set
  to a real password if NPM ever exposes it more widely.

## 5. Enable and start the service

```sh
# Symlink into /var/service to start runit supervision.
sudo ln -s /etc/sv/kafun /var/service/kafun

# Wait a few seconds for runit to pick it up, then:
sv status kafun
tail -n 50 /var/log/kafun/current
```

Expected log lines (svlogd prefixes each with a TAI64N timestamp):

```
kafun starting: root=/sanzu/objects db=/sanzu/objects/index.db bind=0.0.0.0:8333
Running Kafun.Router with Bandit 1.11.0 at 0.0.0.0:8333 (http)
Running Kafun.Admin.Endpoint with Bandit 1.11.0 at 0.0.0.0:8334 (http)
```

Smoke test from yomi or another LAN box:

```sh
KEY=<one of KAFUN_KEYS>
AUTH="AWS4-HMAC-SHA256 Credential=$KEY/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
curl -sS http://localhost:8333/healthz
curl -sS -I -H "Authorization: $AUTH" http://localhost:8333/imouto    # → 200 if bucket exists
curl -sS http://localhost:8334/buckets > /dev/null && echo "admin ok"
```

## 6. NPM upstream

In nginx-proxy-manager, point `objects.harvelab.com` at `yomi:8333`. If
you want the admin UI on a separate hostname (recommended), point
`kafun-admin.harvelab.com` (or similar) at `yomi:8334`. Same NPM machine,
different upstream entries.

## 7. Backups

Add a cron entry (as root, `sudo crontab -e`):

```cron
# Snapshot the index at 03:00 UTC every day.
0 3 * * * /opt/kafun/bin/kafun rpc 'Kafun.Backup.run()'

# Prune snapshots older than 14 days.
30 3 * * * find /var/backups/kafun -name 'kafun-*.db' -mtime +14 -delete
```

`Kafun.Backup.run/0` writes
`/var/backups/kafun/kafun-<YYYYMMDD-HHMMSS>.db` via SQLite's
`VACUUM INTO`. Safe with WAL and a busy DB. The cron entry runs as root;
the release `rpc` connects to the kafun node by name, so it inherits
whatever cookie is set in `/etc/kafun/kafun.env`.

The blob tree itself isn't backed up by this — for that, point your
existing backup tool (restic / rsync / borg) at `/sanzu/objects`. The
two stores reconcile: as long as you restore both, kafun's GC will
clean up any drift.

## 8. Rollback

If a release is bad:

```sh
sudo sv down kafun
sudo mv /opt/kafun /opt/kafun.bad-$(date +%s)
sudo -u kafun tar xzf /tmp/kafun-<previous>.tgz -C /opt
sudo sv up kafun
sv status kafun && tail /var/log/kafun/current
```

Index DB and blob tree are untouched between releases — only the
`/opt/kafun/` install dir is replaced. If a bad migration corrupted the
index, restore the most recent `/var/backups/kafun/kafun-*.db` over
`KAFUN_ROOT/index.db` while the service is stopped, then bring it back up.

## 9. Day-2 ops

| Task | Command |
| ---- | ------- |
| Service status | `sv status kafun` |
| Restart | `sudo sv restart kafun` |
| Stop (no autostart) | `sudo sv down kafun && sudo touch /etc/sv/kafun/down` |
| Start | `sudo rm -f /etc/sv/kafun/down && sudo sv up kafun` |
| Tail current log | `tail -f /var/log/kafun/current` (TAI64N timestamps — pipe through `tai64nlocal` for human-readable) |
| Last hour | `tail -n 5000 /var/log/kafun/current \| tai64nlocal` |
| Remote shell | `sudo /opt/kafun/bin/kafun remote` (Ctrl-G then `q` to leave without killing the service) |
| Trigger GC | `sudo /opt/kafun/bin/kafun rpc 'Kafun.GC.run_now()'` (or click "Run GC now" in the admin UI) |
| Status | `sudo /opt/kafun/bin/kafun rpc 'Kafun.GC.status()'` |
| Bucket counts | `sqlite3 -readonly /sanzu/objects/index.db "SELECT bucket, COUNT(*) FROM objects GROUP BY bucket"` |

## 10. When the world gets bigger

If/when ACLs and multi-user land and kafun ships beyond the homelab,
sibling service definitions for openrc (Alpine VMs) and systemd (anything
modern) drop into `rel/openrc/` and `rel/systemd/` next to `rel/sv/`. The
release artifact and env file are unchanged across all three.
