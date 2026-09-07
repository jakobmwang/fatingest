"""fatingest e2e: claims name one file, archives live in archive_members, N parse workers.

Runs INSIDE an api container against a live API (needs DATABASE_URL, API at :8000):
    docker exec -i <container> python - < tests/test_main.py
Every feed/uri carries a per-run suffix, so it can be re-run without cleanup.
"""
import base64
import gzip
import hashlib
import io
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import warnings

import psycopg
from PIL import Image, ImageDraw

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
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"null")
        except Exception:
            return e.code, None


def post(path, body):
    return call("POST", path, body)


def b64(b):
    return base64.b64encode(b).decode()


def sha(b):
    return hashlib.sha256(b).hexdigest()


def q(sql, *args):
    with psycopg.connect(DB) as c:
        return c.execute(sql, args).fetchall()


def q1(sql, *args):
    rows = q(sql, *args)
    return rows[0][0] if rows else None


def spool(s):
    return os.path.join(STORE, "spool", s)


def deliver(feed, uri, data, filename="", meta=None):
    return post("/v1/ingest/file", {"feed": feed, "uri": uri, "meta": meta or {},
                                    "bytes_b64": b64(data), "filename": filename})


def ping(feed, uri, s, meta=None):
    return post("/v1/ingest/ping", {"feed": feed, "uri": uri, "sha256": s, "meta": meta or {}})


def wait(feed, uri, s, meta=None, timeout=180):
    """Poll the ping until it stops answering 202."""
    t0 = time.time()
    while True:
        st, body = ping(feed, uri, s, meta)
        if st != 202 or time.time() - t0 > timeout:
            return st, body
        time.sleep(0.5)


def zipbytes(entries):
    buf = io.BytesIO()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")          # duplicate names are the point
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            for name, data in entries:
                z.writestr(name, data)
    return buf.getvalue()


import zipfile  # noqa: E402  (after helpers for readability)

F = f"test/{RUN}"
fa, fb, fc = F + "/a", F + "/b", F + "/c"

# Fixture bytes carry the run id: content is content-addressed, so byte-identical fixtures
# from an earlier run would already be complete and answer 200 instead of 202.
md1 = f"# Alpha\n\nThe quick brown fox. Token ALPHAFOX. Run {RUN}.\n".encode()
md2 = f"# Beta\n\nBeta content. Token BETAQUX. Run {RUN}.\n".encode()
deep = f"# Deep\n\nInside the nested archive. Token NESTEDDEEP{RUN}.\n".encode()
notes = f"plain notes, gzipped inside the zip. Token GZNOTES{RUN}\n".encode()
dup1 = f"# Dup\n\nFirst entry. Token DUPFIRST{RUN}.\n".encode()
dup2 = f"# Dup\n\nSecond entry. Token DUPSECOND{RUN}.\n".encode()
inner = zipbytes([("deep.md", deep)])
outer = zipbytes([("docs/a.md", md1), ("docs/b.md", md2), ("dup.md", dup1), ("dup.md", dup2),
                  ("copy/a.md", md1), ("archives/inner.zip", inner),
                  ("notes.txt.gz", gzip.compress(notes)), ("emptydir/", b"")])
bare = gzip.compress(f"hello from a bare gzip. Token BAREGZ{RUN}\n".encode())

st, h = call("GET", "/health")
check("0.1 health ok", st == 200 and h["ok"], str(h))

print("--- 1. single file: queue -> complete; the claim names one sha")
st, r = deliver(fa, "doc1", md1, "a.md")
check("1.1 delivery answers 202 queued with the file's sha",
      st == 202 and r["status"] == "queued" and r["sha256"] == sha(md1), f"{st} {r}")
st, r = wait(fa, "doc1", sha(md1))
check("1.2 ping settles at 200 known", st == 200 and r["status"] == "known", f"{st} {r}")
check("1.3 items.sha256 = delivered sha",
      q1("SELECT sha256 FROM items WHERE feed=%s AND uri=%s", fa, "doc1") == sha(md1))
st, r = ping(fa, "doc1", sha(md2))
check("1.4 ping with another sha -> 404 'carries other content'",
      st == 404 and "other content" in (r or {}).get("detail", ""), f"{st} {r}")
st, r = deliver(fa, "doc1", md1, "a.md")
check("1.5 re-delivery of complete content -> 200 ingested", st == 200 and r["status"] == "ingested")
check("1.6 spool entry gone after the parse", not os.path.exists(spool(sha(md1))))

print("--- 2. archive: members in archive_members, the claim stays on the archive")
zs = sha(outer)
st, r = deliver(fa, "bundle", outer, "bundle.zip")
check("2.1 archive delivery 202 with the archive's sha", st == 202 and r["sha256"] == zs, f"{st} {r}")
st, r = wait(fa, "bundle", zs)
check("2.2 archive ping settles at 200", st == 200, f"{st} {r}")
meta = q1("SELECT meta FROM files WHERE sha256=%s", zs) or {}
check("2.3 archive row: kind=archive, n_members=6, no manifest",
      meta.get("kind") == "archive" and meta.get("n_members") == 6 and "manifest" not in meta, str(meta))
rows = dict(q("SELECT path, member_sha256 FROM archive_members WHERE archive_sha256=%s", zs))
check("2.4 six member rows (duplicate path collapsed, directory entry skipped)", len(rows) == 6, str(sorted(rows)))
check("2.5 nested archive flattened into the path", rows.get("archives/inner.zip/deep.md") == sha(deep))
check("2.6 duplicate path: the last entry wins", rows.get("dup.md") == sha(dup2))
check("2.7 same content at two paths -> two rows, one file",
      rows.get("docs/a.md") == sha(md1) and rows.get("copy/a.md") == sha(md1))
check("2.8 gz member keeps the gz's path, content decompressed", rows.get("notes.txt.gz") == sha(notes))
check("2.9 all five distinct members are parsed files",
      q1("SELECT count(*) FROM files WHERE sha256 = ANY(%s) AND parsed_at IS NOT NULL",
         list(set(rows.values()))) == 5)
check("2.10 the overwritten duplicate was never parsed", q1("SELECT count(*) FROM files WHERE sha256=%s", sha(dup1)) == 0)
check("2.11 the claim names the archive, not a member",
      q1("SELECT sha256 FROM items WHERE feed=%s AND uri=%s", fa, "bundle") == zs)
st, r = ping(fa, "bundle", sha(md2))
check("2.12 ping with a member's sha under the archive uri -> 404", st == 404, f"{st} {r}")
check("2.13 archive spool entry gone", not os.path.exists(spool(zs)))

print("--- 3. second claim on a complete archive: nothing unpacked, nothing queued")
st, r = deliver(fb, "same-bundle", outer, "bundle.zip")
check("3.1 another feed delivers the same archive -> 200 ingested at once",
      st == 200 and r["status"] == "ingested", f"{st} {r}")
check("3.2 no spool entry written", not os.path.exists(spool(zs)))
st, r = ping(fb, "same-bundle", zs)
check("3.3 ping from the second feed -> 200", st == 200, f"{st} {r}")
claims = q("SELECT feed, uri FROM file_claims WHERE sha256=%s ORDER BY feed", sha(deep))
check("3.4 file_claims resolves the nested member to both claims",
      claims == [(fa, "bundle"), (fb, "same-bundle")], str(claims))
check("3.5 file_claims: content at two paths -> one row per claim (UNION)",
      q1("SELECT count(*) FROM file_claims WHERE sha256=%s", sha(md1)) == 3)

print("--- 4. bare gzip: one member named after the delivered filename")
gs = sha(bare)
deliver(fa, "bare", bare, "hello.txt.gz")
st, r = wait(fa, "bare", gs)
check("4.1 bare gz settles at 200", st == 200, f"{st} {r}")
rows = q("SELECT path, member_sha256 FROM archive_members WHERE archive_sha256=%s", gs)
check("4.2 one member, path 'hello.txt', decompressed content",
      rows == [("hello.txt", sha(gzip.decompress(bare)))], str(rows))

print("--- 5. search: attribution and feed filter through file_claims (lexical only)")
LEX = {"weight_semantic": 0}
st, r = call("GET", "/v1/search", params={"q": f"NESTEDDEEP{RUN}", **LEX})
hits = r["hits"]
srcs = {(s["feed"], s["uri"]) for h in hits for s in h["sources"]}
check("5.1 a unique token finds the nested member once, lexically", len(hits) == 1 and hits[0]["found_by"] == {"lexical": 1}, str(r))
check("5.2 sources name both archive claims", srcs == {(fa, "bundle"), (fb, "same-bundle")}, str(srcs))
st, r = call("GET", "/v1/search", params={"q": f"NESTEDDEEP{RUN}", "filter_feeds": fb, **LEX})
check("5.3 feed filter via file_claims: feed b finds it", len(r["hits"]) == 1)
st, r = call("GET", "/v1/search", params={"q": f"NESTEDDEEP{RUN}", "filter_feeds": F + "/nobody", **LEX})
check("5.4 feed filter: an unrelated feed finds nothing", len(r["hits"]) == 0)
st, r = call("GET", "/v1/search", params={"q": f"DUPFIRST{RUN}", **LEX})
check("5.5 the overwritten duplicate is not indexed", len(r["hits"]) == 0, str(r))
st, r = call("GET", "/v1/search", params={"q": f"DUPSECOND{RUN}", **LEX})
check("5.6 the winning duplicate is", len(r["hits"]) == 1)

print("--- 6. parse workers: a burst of deliveries drains cleanly")
docs = [(f"many{i}", f"# Doc {i}\n\nToken MANYDOC{i}X{RUN}.\n".encode()) for i in range(8)]
for uri, data in docs:
    st, r = deliver(fa, uri, data, uri + ".md")
    assert st == 202, (st, r)
ok = all(wait(fa, uri, sha(data))[0] == 200 for uri, data in docs)
check("6.1 eight concurrent deliveries all settle at 200", ok)
check("6.2 queue drained", q1("SELECT count(*) FROM files WHERE parsed_at IS NULL") == 0)
check("6.3 no leftover spool entries", not any(os.path.exists(spool(sha(d))) for _, d in docs))
check("6.4 one chunk collection per doc",
      all(q1("SELECT count(*) FROM files_chunks WHERE file_sha256=%s", sha(d)) == 1 for _, d in docs))

print("--- 7. chunks deliveries: the claim names the pack sha")
st, r = post("/v1/ingest/chunks", {"feed": fc, "uri": "pre",
                                   "chunks": [{"markdown": f"Pre-chunked text. Token PRECHUNK{RUN}."}]})
check("7.1 markdown-only chunks -> 200 ingested", st == 200 and r["status"] == "ingested", f"{st} {r}")
ps = r["sha256"]
check("7.2 claim names the pack sha", q1("SELECT sha256 FROM items WHERE feed=%s AND uri=%s", fc, "pre") == ps)
st, r = ping(fc, "pre", ps)
check("7.3 ping 200", st == 200, f"{st} {r}")
img = Image.new("RGB", (320, 80), "white")
ImageDraw.Draw(img).text((10, 30), f"RENDER {RUN}", fill="black")
buf = io.BytesIO()
img.save(buf, "PNG")
st, r = post("/v1/ingest/chunks", {"feed": fc, "uri": "render", "chunks": [{"render_b64": b64(buf.getvalue())}]})
check("7.4 render-only chunks -> 202 queued (text inference)", st == 202, f"{st} {r}")
rs = r["sha256"]
st, r = wait(fc, "render", rs)
check("7.5 inference settles at 200 (process_chunks on the worker's transaction)", st == 200, f"{st} {r}")
check("7.6 generated markdown present",
      bool(q1("SELECT c.markdown FROM files_chunks fc JOIN chunks c ON c.sha256=fc.chunk_sha256 "
              "WHERE fc.file_sha256=%s", rs)))
check("7.7 chunks spool entry gone", not os.path.exists(spool(rs)))

print("--- 8. GC: the archive first, then its orphaned members, in one call")
st, r = deliver(fc, "beta-alone", md2, "b.md")
check("8.1 a member delivered on its own is already complete -> 200", st == 200 and r["status"] == "ingested")
st, r = post("/v1/ingest/delete", {"feed": fb, "uri": "same-bundle"})
check("8.2 delete answers existed=True", st == 200 and r["existed"] is True)
st, g = call("POST", "/v1/gc", params={"stale_days": 14})
check("8.3 gc removes nothing while feed a still claims the archive",
      g["files"] == 0 and g["chunks"] == 0, str(g))
before = q1("SELECT count(*) FROM files")
post("/v1/ingest/delete", {"feed": fa, "uri": "bundle"})
st, g = call("POST", "/v1/gc", params={"stale_days": 14})
check("8.4 one gc call removes the archive and its three orphaned members", g["files"] == 4, str(g))
check("8.5 archive_members rows cascaded away",
      q1("SELECT count(*) FROM archive_members WHERE archive_sha256=%s", zs) == 0)
check("8.6 members claimed elsewhere survive",
      q1("SELECT count(*) FROM files WHERE sha256 = ANY(%s)", [sha(md1), sha(md2)]) == 2)
check("8.7 their chunks swept with them", g["chunks"] == 3, str(g))
if g["chunks"] != 3:                       # which orphan's chunk survived, and who holds it?
    with psycopg.connect(DB) as cd:
        for name, content in (("dup2", dup2), ("deep", deep), ("notes", notes)):
            token = content.decode().split("Token ")[1].split(".")[0].split("\n")[0]
            rows = cd.execute("""SELECT c.sha256, (SELECT string_agg(fc.file_sha256 || ' idx' || fc.idx, ',') FROM files_chunks fc WHERE fc.chunk_sha256 = c.sha256),
                                        pg_try_advisory_lock(0) FROM chunks c WHERE c.markdown LIKE %s""", ("%" + token + "%",)).fetchall()
            print(f"   8.7 diag {name}: {rows}")
        locks = cd.execute("""SELECT l.mode, l.granted, a.application_name, left(a.query, 90) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
                              JOIN pg_class r ON r.oid = l.relation WHERE r.relname = 'chunks' AND l.pid <> pg_backend_pid()""").fetchall()
        print(f"   8.7 diag locks on chunks: {locks}")
check("8.8 files after = before - 4", q1("SELECT count(*) FROM files") == before - 4)

print("--- 8b. a spreadsheet with a chart and a picture: cells exact, drawings as chunks of their own")
from openpyxl import Workbook as _Wb                                    # noqa: E402
from openpyxl.chart import BarChart as _Bar, Reference as _Ref          # noqa: E402
from openpyxl.drawing.image import Image as _XLImage                    # noqa: E402
from PIL import Image as _PILImage, ImageDraw as _Draw                  # noqa: E402
_wb = _Wb(); _ws = _wb.active; _ws.title = "Budget"
_ws.append(("Year", "Revenue", "Cost"))
for _y, _r, _c in ((2021, 120, 90), (2022, 135, 97), (2023, 150, 110), (2024, 162, 118)):
    _ws.append((_y, _r, _c))
_ws["A7"] = f"Token SHEETCELL{RUN}"
_ch = _Bar(); _ch.title = f"Revenue vs cost {RUN}"; _ch.add_data(_Ref(_ws, min_col=2, min_row=1, max_col=3, max_row=5), titles_from_data=True)
_ch.set_categories(_Ref(_ws, min_col=1, min_row=2, max_row=5)); _ws.add_chart(_ch, "E2")
_im = _PILImage.new("RGB", (240, 120), "white"); _Draw.Draw(_im).rectangle((10, 10, 230, 110), outline="red", width=6); _Draw.Draw(_im).text((30, 50), f"STAMP {RUN}", fill="red")
_ib = io.BytesIO(); _im.save(_ib, "PNG"); _ib.seek(0); _ws.add_image(_XLImage(_ib), "E20")
_w2 = _wb.create_sheet("Plain"); _w2.append(("Item", "Qty")); _w2.append(("Chairs", 12))
_xb = io.BytesIO(); _wb.save(_xb); xlsx = _xb.getvalue()
st, r = deliver(fa, "budget-book", xlsx, "budget.xlsx")
st, r = wait(fa, "budget-book", sha(xlsx), timeout=300)
check("8b.1 the workbook parses", st == 200, f"{st} {r}")
rows = q("""SELECT fc.idx, c.markdown, c.meta, c.render_sha256 FROM files_chunks fc JOIN chunks c ON c.sha256 = fc.chunk_sha256
            WHERE fc.file_sha256 = %s ORDER BY fc.idx""", sha(xlsx))
fmeta = q1("SELECT meta FROM files WHERE sha256 = %s", sha(xlsx))
cells = [x for x in rows if x[3] is None]; drawings = [x for x in rows if x[3] is not None]
check("8b.2 two cell chunks (one per sheet) with sheet and row range in meta, values exact (the empty row 6 is no row)",
      [x[2].get("sheet") for x in cells] == ["Budget", "Plain"] and cells[0][2].get("rows") == [1, 5] and "| 2023 | 150 | 110 |" in cells[0][1]
      and f"SHEETCELL{RUN}" in cells[0][1], str([(x[0], x[2]) for x in cells]))
check("8b.3 two drawings became chunks of their own, both on sheet Budget, transcribed by the VLM, with their position",
      len(drawings) == 2 and all(x[2].get("sheet") == "Budget" and x[2].get("source") == "vlm" and x[2].get("status") == "ok"
                                 and isinstance(x[2].get("region"), list) and len(x[2]["region"]) == 4 for x in drawings), str([(x[0], x[2]) for x in drawings]))
check("8b.4 order: Budget's cells, then its drawings top to bottom, then Plain's cells",
      [x[0] for x in cells] == [0, 3] and [x[0] for x in drawings] == [1, 2] and drawings[0][2]["region"][3] > drawings[1][2]["region"][3], str([x[0] for x in rows]))
check("8b.5 the chart's transcription names the years, the picture's the stamp",
      any(y in drawings[0][1] for y in ("2021", "2024")) and "STAMP" in drawings[1][1].upper(), f"{drawings[0][1][:120]!r} | {drawings[1][1][:120]!r}")
check("8b.6 the file meta counts the drawings", fmeta.get("num_drawings") == 2 and fmeta.get("num_sheets") == 2, str(fmeta))
plain_wb = _Wb(); plain_wb.active.append(("a", "b")); plain_wb.active.append((1, 2)); plain_wb.active["A4"] = f"PLAIN{RUN}"
_pb = io.BytesIO(); plain_wb.save(_pb); plain_xlsx = _pb.getvalue()
st, r = deliver(fa, "plain-book", plain_xlsx, "plain.xlsx"); st, r = wait(fa, "plain-book", sha(plain_xlsx), timeout=120)
pm = q1("SELECT meta FROM files WHERE sha256 = %s", sha(plain_xlsx))
check("8b.7 a workbook without drawings never goes near LibreOffice: no drawings counted", st == 200 and "num_drawings" not in pm, str(pm))

print("--- 9. Gotenberg: fresh LibreOffice per conversion, one at a time, verdicts")


def minimal_docx(text):
    ct = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
          '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
          '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
          '<Default Extension="xml" ContentType="application/xml"/>'
          '<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
          '</Types>')
    rels = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>'
            '</Relationships>')
    doc = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
           '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>'
           f'<w:p><w:r><w:t>{text}</w:t></w:r></w:p></w:body></w:document>')
    return zipbytes([("[Content_Types].xml", ct.encode()), ("_rels/.rels", rels.encode()),
                     ("word/document.xml", doc.encode())])


import filetype  # noqa: E402
d1 = minimal_docx(f"Office happy path. Token OFFICEHAPPY{RUN}.")
d2 = minimal_docx(f"Second office document, back to back. Token OFFICESECOND{RUN}.")
bad = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"\x00" * 504 + b"\xec\xa5\xc1\x00" + bytes(range(256)) * 8 + RUN.encode()
check("9.0 fixtures route to Gotenberg (docx, doc)",
      filetype.guess(d1).extension == "docx" and filetype.guess(bad).extension == "doc",
      f"{filetype.guess(d1)} {filetype.guess(bad)}")
deliver(fa, "office1", d1, "one.docx")
deliver(fa, "office2", d2, "two.docx")
st1, r1 = wait(fa, "office1", sha(d1))
st2, r2 = wait(fa, "office2", sha(d2))
check("9.1 two office documents back to back both settle at 200 (restart between them)",
      st1 == 200 and st2 == 200, f"{st1} {r1} / {st2} {r2}")
kinds = {r[0] for r in q("SELECT meta->>'kind' FROM files WHERE sha256 = ANY(%s)", [sha(d1), sha(d2)])}
check("9.2 both parsed as office", kinds == {"office"}, str(kinds))
st, r = call("GET", "/v1/search", params={"q": f"OFFICEHAPPY{RUN}", "weight_semantic": 0})
check("9.3 the transcription is searchable", len(r["hits"]) == 1, str(r))
deliver(fa, "office-bad", bad, "bad.doc")
st, r = wait(fa, "office-bad", sha(bad))
meta = q1("SELECT meta FROM files WHERE sha256=%s", sha(bad)) or {}
check("9.4 a document a healthy Gotenberg refuses is unparseable: ping 200, verdict recorded",
      st == 200 and meta.get("kind") == "unparseable" and meta.get("error", "").startswith("gotenberg "), f"{st} {meta}")
st, h = call("GET", "/health")
check("9.5 /health counts it under files_unparseable, nothing retrying or failed",
      h["files_unparseable"] >= 1 and h["files_retrying"] == 0 and h["files_failed"] == 0, str(h))

failed = [n for n, ok in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} passed" + (f"; FAILED: {failed}" if failed else ""))
sys.exit(1 if failed else 0)
