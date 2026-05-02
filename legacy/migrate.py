"""
Migrate all objects from SeaweedFS S3 to the new bucket service.

Streams each object: list keys from source, GET from source, PUT to destination.
Concurrent across N workers. Resumable via per-bucket progress files.

Usage:
    pip install boto3
    python3 migrate.py
"""
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
from botocore.config import Config

# === CONFIG ===
SRC_ENDPOINT = os.environ.get("SRC_ENDPOINT", "http://localhost:8333")
SRC_KEY = os.environ.get("SRC_KEY", "")
SRC_SECRET = os.environ.get("SRC_SECRET", "")

DST_ENDPOINT = os.environ.get("DST_ENDPOINT", "http://localhost:8334")
DST_KEY = os.environ.get("DST_KEY", "")
DST_SECRET = os.environ.get("DST_SECRET", "")

BUCKETS = os.environ.get("BUCKETS", "imouto,cards,wallpapers,wezterm").split(",")
WORKERS = int(os.environ.get("WORKERS", "16"))
PROGRESS_DIR = Path(os.environ.get("PROGRESS_DIR", "./migrate-progress"))
PROGRESS_DIR.mkdir(exist_ok=True)


def make_client(endpoint, key, secret):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        config=Config(signature_version="s3v4", retries={"max_attempts": 3}),
    )


def load_done(bucket: str) -> set[str]:
    f = PROGRESS_DIR / f"{bucket}.done"
    if not f.exists():
        return set()
    return set(f.read_text().splitlines())


def append_done(bucket: str, key: str):
    with open(PROGRESS_DIR / f"{bucket}.done", "a") as f:
        f.write(key + "\n")


def list_all_keys(src, bucket: str):
    """Yield every key in the bucket via paginated ListObjectsV2."""
    paginator = src.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            yield obj["Key"], obj["Size"]


def copy_one(src, dst, bucket: str, key: str) -> tuple[str, bool, str]:
    """GET from src, PUT to dst. Returns (key, success, message)."""
    try:
        obj = src.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        dst.put_object(Bucket=bucket, Key=key, Body=body)
        return (key, True, "")
    except Exception as e:
        return (key, False, str(e))


def migrate_bucket(bucket: str):
    print(f"\n=== {bucket} ===")
    src = make_client(SRC_ENDPOINT, SRC_KEY, SRC_SECRET)
    dst = make_client(DST_ENDPOINT, DST_KEY, DST_SECRET)

    done = load_done(bucket)
    print(f"  resuming: {len(done)} already done")

    total_bytes = 0
    total_count = 0
    errors = []

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {}
        for key, size in list_all_keys(src, bucket):
            if key in done:
                continue
            fut = pool.submit(copy_one, src, dst, bucket, key)
            futures[fut] = (key, size)

            # drain periodically so we don't queue 100k futures at once
            if len(futures) >= WORKERS * 4:
                _drain_some(futures, bucket, errors)

        # final drain
        _drain_all(futures, bucket, errors)

    print(f"  done: {total_count} objects, {len(errors)} errors")
    if errors:
        print(f"  first errors:")
        for k, m in errors[:5]:
            print(f"    {k}: {m}")


def _drain_some(futures: dict, bucket: str, errors: list):
    """Process completed futures, leave the rest in the dict."""
    completed = [f for f in futures if f.done()]
    for fut in completed:
        key, size = futures.pop(fut)
        k, ok, msg = fut.result()
        if ok:
            append_done(bucket, k)
        else:
            errors.append((k, msg))


def _drain_all(futures: dict, bucket: str, errors: list):
    for fut in as_completed(list(futures.keys())):
        key, size = futures[fut]
        k, ok, msg = fut.result()
        if ok:
            append_done(bucket, k)
            print(f"  {k} ({size:,} bytes)")
        else:
            errors.append((k, msg))
            print(f"  FAIL {k}: {msg}", file=sys.stderr)


def verify_counts():
    """Compare object counts between src and dst per bucket."""
    print("\n=== verification ===")
    src = make_client(SRC_ENDPOINT, SRC_KEY, SRC_SECRET)
    dst = make_client(DST_ENDPOINT, DST_KEY, DST_SECRET)
    for bucket in BUCKETS:
        src_count = sum(1 for _ in list_all_keys(src, bucket))
        # destination uses our custom server which doesn't list - count via filesystem instead
        # if dst is also our server, you'd need to walk /sanzu/objects/<bucket>/ directly
        try:
            dst_count = sum(1 for _ in list_all_keys(dst, bucket))
            print(f"  {bucket}: src={src_count}, dst={dst_count}, "
                  f"{'OK' if src_count == dst_count else 'MISMATCH'}")
        except Exception:
            print(f"  {bucket}: src={src_count}, dst=(no list support, check via filesystem)")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "verify":
        verify_counts()
    else:
        for bucket in BUCKETS:
            migrate_bucket(bucket)
        print("\nMigration complete. Run 'python3 migrate.py verify' to check counts.")
