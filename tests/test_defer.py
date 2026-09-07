"""Deferred deliveries end to end, against an api whose GOTENBERG_URL points at nothing
(and GOTENBERG_HEALTH_WAIT is short). Run inside that api container:
    docker exec -i <container> python - < tests/test_defer.py
"""
import base64
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import filetype
import psycopg

API = os.environ.get("API", "http://localhost:8000")
DB = os.environ["DATABASE_URL"]
STORE = os.environ.get("STORE", "/store")
RUN = str(int(time.time()))
results = []


def check(name, cond, info=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + (f"   [{info}]" if info and not cond else ""), flush=True)


def call(method, path, body=None, params=None):
    url = API + path + ("?" + urllib.parse.urlencode(params) if params else "")
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null")


def q1(sql, *args):
    with psycopg.connect(DB) as c:
        r = c.execute(sql, args).fetchone()
    return r[0] if r else None


def row(s):
    """parsed_at, {attempts, error, started_at}, seconds until due."""
    with psycopg.connect(DB) as c:
        r = c.execute("SELECT parsed_at, attempts, error, started_at, EXTRACT(EPOCH FROM due_at - now()) FROM files WHERE sha256 = %s", (s,)).fetchone()
    return (r[0], {"attempts": r[1], "error": r[2], "started_at": r[3]}, r[4]) if r else None


def wait_attempts(s, n, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = row(s)
        if r and r[1]["attempts"] == n:
            return r
        time.sleep(1)
    return row(s)


feed, uri = f"defer/{RUN}", "doc"
doc = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"\x00" * 504 + b"\xec\xa5\xc1\x00" + bytes(range(256)) * 8
s = hashlib.sha256(doc).hexdigest()
check("0.1 fixture routes to Gotenberg", filetype.guess(doc).extension == "doc")

st, r = call("POST", "/v1/ingest/file", {"feed": feed, "uri": uri, "bytes_b64": base64.b64encode(doc).decode(), "filename": "x.doc"})
check("1.1 delivery queued", st == 202, f"{st} {r}")
parsed_at, meta, due_in = wait_attempts(s, 1)
check("1.2 Gotenberg unreachable: deferred, attempt 1, still queued, started_at set",
      parsed_at is None and meta["attempts"] == 1 and meta["started_at"] is not None, str(meta))
check("1.3 error names the cause", "gotenberg unhealthy" in (meta["error"] or ""), meta["error"])
check("1.4 due in ~60 s", 45 < due_in <= 61, f"{due_in:.0f}s")
check("1.5 spool entry kept", os.path.exists(os.path.join(STORE, "spool", s)))
st, r = call("POST", "/v1/ingest/ping", {"feed": feed, "uri": uri, "sha256": s})
check("1.6 ping answers 202 while deferred", st == 202, f"{st} {r}")
st, h = call("GET", "/health")
check("1.7 /health: files_retrying=1, files_failed=0", h["files_retrying"] == 1 and h["files_failed"] == 0 and h["files_queued"] == 1, str(h))

st, r = call("POST", "/v1/ingest/file", {"feed": feed, "uri": uri, "bytes_b64": base64.b64encode(doc).decode(), "filename": "x.doc"})
check("2.1 re-delivery answers 202 (not re-spooled, not re-enqueued)", st == 202 and r["status"] == "queued", f"{st} {r}")
parsed_at, meta, due_in = wait_attempts(s, 2)
check("2.2 the re-delivery pulled the retry forward: attempt 2 within a minute", meta["attempts"] == 2, str(meta))
check("2.3 backoff doubled: due in ~120 s", 100 < due_in <= 121, f"{due_in:.0f}s")

call("POST", "/v1/ingest/delete", {"feed": feed, "uri": uri})
st, g = call("POST", "/v1/gc", params={"stale_days": 14})
check("3.1 gc removes the unclaimed queued row", g["files"] == 1 and q1("SELECT count(*) FROM files WHERE sha256 = %s", s) == 0, str(g))

failed = [n for n, ok in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} passed" + (f"; FAILED: {failed}" if failed else ""))
sys.exit(1 if failed else 0)
