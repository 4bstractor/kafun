"""
Minimal S3-compatible bucket service.

Storage: /sanzu/objects/<bucket>/<aa>/<bb>/<key>
- aa/bb sharded by sha1(key)[:4] for even distribution
- Bucket = top-level directory, must be pre-created on disk
- Key = opaque string, treated as a single filename (no nested paths)

Auth: access key match against ALLOWED_KEYS env var (comma-separated).
      Sig V4 signature is NOT verified - we trust the network.

Run: uvicorn bucket:app --host 0.0.0.0 --port 8333 --workers 4
"""
import hashlib
import os
import re
import time
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, Response

ROOT = Path(os.environ.get("BUCKET_ROOT", "/sanzu/objects"))
ALLOWED_KEYS = set(os.environ.get("ALLOWED_KEYS", "").split(","))

app = FastAPI()


def shard(key: str) -> str:
    h = hashlib.sha1(key.encode()).hexdigest()
    return f"{h[:2]}/{h[2:4]}"


def blob_path(bucket: str, key: str) -> Path:
    if not re.match(r"^[a-z0-9-]+$", bucket):
        raise HTTPException(400, "invalid bucket name")
    return ROOT / bucket / shard(key) / key


def check_auth(authorization: str | None):
    """Extract access key from Sig V4 header, verify it's allowed. Skip signature check."""
    if not ALLOWED_KEYS:
        return  # auth disabled
    if not authorization:
        raise HTTPException(403, "missing auth")
    # Format: "AWS4-HMAC-SHA256 Credential=<KEY>/<DATE>/<REGION>/s3/aws4_request, ..."
    m = re.search(r"Credential=([^/]+)/", authorization)
    if not m or m.group(1) not in ALLOWED_KEYS:
        raise HTTPException(403, "invalid key")


@app.put("/{bucket}/{key:path}")
async def put_object(bucket: str, key: str, request: Request,
                     authorization: str | None = Header(None)):
    check_auth(authorization)
    path = blob_path(bucket, key)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Stream to temp file, then atomic rename
    tmp = path.parent / f".tmp.{uuid4().hex}"
    sha = hashlib.sha256()
    size = 0
    try:
        with open(tmp, "wb") as f:
            async for chunk in request.stream():
                sha.update(chunk)
                size += len(chunk)
                f.write(chunk)
        os.rename(tmp, path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise

    etag = sha.hexdigest()
    return Response(status_code=200, headers={"ETag": f'"{etag}"'})


@app.get("/{bucket}/{key:path}")
async def get_object(bucket: str, key: str, request: Request,
                     authorization: str | None = Header(None),
                     range: str | None = Header(None)):
    check_auth(authorization)
    path = blob_path(bucket, key)
    if not path.exists():
        raise HTTPException(404, "not found")

    if range:
        return _range_response(path, range)

    return FileResponse(path)


@app.head("/{bucket}/{key:path}")
async def head_object(bucket: str, key: str,
                      authorization: str | None = Header(None)):
    check_auth(authorization)
    path = blob_path(bucket, key)
    if not path.exists():
        raise HTTPException(404, "not found")
    st = path.stat()
    return Response(status_code=200, headers={
        "Content-Length": str(st.st_size),
        "Last-Modified": time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(st.st_mtime)),
    })


@app.delete("/{bucket}/{key:path}")
async def delete_object(bucket: str, key: str,
                        authorization: str | None = Header(None)):
    check_auth(authorization)
    path = blob_path(bucket, key)
    path.unlink(missing_ok=True)
    return Response(status_code=204)


@app.get("/")
async def list_buckets(authorization: str | None = Header(None)):
    """Return list of bucket directories. boto3 sometimes calls this on init."""
    check_auth(authorization)
    buckets = [d.name for d in ROOT.iterdir() if d.is_dir()]
    body = '<?xml version="1.0" encoding="UTF-8"?>\n'
    body += '<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    body += '<Owner><ID>local</ID><DisplayName>local</DisplayName></Owner>'
    body += '<Buckets>'
    now = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
    for b in buckets:
        body += f'<Bucket><Name>{b}</Name><CreationDate>{now}</CreationDate></Bucket>'
    body += '</Buckets></ListAllMyBucketsResult>'
    return Response(content=body, media_type="application/xml")


def _range_response(path: Path, range_header: str):
    """Handle Range: bytes=X-Y header."""
    size = path.stat().st_size
    m = re.match(r"bytes=(\d+)-(\d*)", range_header)
    if not m:
        raise HTTPException(416, "invalid range")
    start = int(m.group(1))
    end = int(m.group(2)) if m.group(2) else size - 1
    end = min(end, size - 1)
    length = end - start + 1

    def gen():
        with open(path, "rb") as f:
            f.seek(start)
            remaining = length
            while remaining > 0:
                chunk = f.read(min(65536, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk

    from fastapi.responses import StreamingResponse
    return StreamingResponse(gen(), status_code=206, headers={
        "Content-Range": f"bytes {start}-{end}/{size}",
        "Content-Length": str(length),
        "Accept-Ranges": "bytes",
    })
