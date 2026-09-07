"""Per-page verdicts end to end against a live stack (real VLM): a PDF with a text page, a
blank page and a picture page; chunk meta; blank chunks outside embedding and ranking.
Runs INSIDE an api container: docker exec -i <container> python - < tests/test_pages.py
"""
import base64
import hashlib
import io
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

import psycopg
import pymupdf
from PIL import Image, ImageDraw

API = os.environ.get("API", "http://localhost:8000")
DB = os.environ["DATABASE_URL"]
RUN = str(int(time.time()))
F = f"pages/{RUN}"
results = []


def check(name, cond, info=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + (f"   [{info}]" if info and not cond else ""), flush=True)


def call(method, path, body=None, params=None):
    url = API + path + ("?" + urllib.parse.urlencode(params) if params else "")
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"null")
        except Exception:
            return e.code, None


def sha(b):
    return hashlib.sha256(b).hexdigest()


def deliver(uri, data, filename, meta=None):
    return call("POST", "/v1/ingest/file", {"feed": F, "uri": uri, "meta": meta or {},
                                            "bytes_b64": base64.b64encode(data).decode(), "filename": filename})


def wait(uri, s, timeout=600):
    t0 = time.time()
    while time.time() - t0 < timeout:
        st, r = call("POST", "/v1/ingest/ping", {"feed": F, "uri": uri, "sha256": s})
        if st != 202:
            return st, r
        time.sleep(2)
    return "timeout", None


def chunks_of(s):
    with psycopg.connect(DB) as c:
        return c.execute("""SELECT fc.idx, ch.markdown, ch.meta, ch.render_sha256, ch.embedding IS NOT NULL
                            FROM files_chunks fc JOIN chunks ch ON ch.sha256 = fc.chunk_sha256
                            WHERE fc.file_sha256 = %s ORDER BY fc.idx""", (s,)).fetchall()


def health():
    return call("GET", "/health")[1]


# --- the document: text page, blank page, picture page ----------------------------------
tok = f"PAGES{RUN}"
doc = pymupdf.open()
p1 = doc.new_page()
p1.insert_text((72, 90), f"Unit page one. Token {tok}.", fontsize=14)
p1.insert_text((72, 120), "A second line of plain text about nothing in particular.", fontsize=11)
p1.insert_text((72, 150), "Council page.", fontsize=11)
p1.insert_link({"kind": pymupdf.LINK_URI, "uri": "https://example.org/side?a=1",
                "from": p1.search_for("Council page.")[0]})
doc.new_page()                                                   # page 2: entirely blank
p3 = doc.new_page()
pic = Image.new("RGB", (900, 900), "white")
d = ImageDraw.Draw(pic)
d.ellipse((80, 80, 520, 520), fill=(200, 40, 40))
d.rectangle((400, 400, 860, 860), fill=(40, 90, 200))
d.polygon([(60, 850), (450, 560), (840, 850)], fill=(40, 160, 60))
pb = io.BytesIO()
pic.save(pb, "PNG")
p3.insert_image(pymupdf.Rect(40, 40, 555, 555), stream=pb.getvalue())
pdf = doc.tobytes()
doc.close()
S = sha(pdf)

print("--- P. a PDF: text page, blank page, picture page")
st, r = deliver("three", pdf, "three.pdf")
check("P.1 delivery queued", st == 202, f"{st} {r}")
t0 = time.time()
st, r = wait("three", S)
check("P.2 parsed: ping 200", st == 200, f"{st} {r}")
rows = chunks_of(S)
check("P.3 three chunks at positions 0, 1, 2 (no page dropped)", [x[0] for x in rows] == [0, 1, 2], str([x[0] for x in rows]))
if len(rows) == 3:
    idx, md, meta, r_sha, emb = rows[0]
    check("P.4 text page: nothing drawn on it, so the text layer is the page and no VLM is asked",
          meta.get("status") == "ok" and meta.get("source") == "text_layer" and meta.get("visual") == 0
          and meta.get("text_chars", 0) > 40 and tok in md, str(meta) + md[:80])
    check("P.4b its links survive into the markdown",
          "https://example.org/side?a=1" in md, md[:200])
    idx, md, meta, r_sha, emb = rows[1]
    check("P.5 blank page: nothing drawn and no text, so it is blank without asking anyone",
          meta.get("status") == "blank" and meta.get("source") == "text_layer" and md == "" and r_sha is not None,
          f"{meta} {md[:120]!r}")
    check("P.6 blank page hints: no text layer, nothing drawn", meta.get("text_chars") == 0 and meta.get("visual") == 0, str(meta))
    idx, md, meta, r_sha, emb = rows[2]
    check("P.7 picture page: no text layer, mostly visual, and it went to the VLM",
          meta.get("text_chars") == 0 and meta.get("visual", 0) >= 0.5 and meta.get("source") == "vlm", str(meta))
    check("P.8 picture page is content, not blank: status ok, described in words, no image link",
          meta.get("status") == "ok" and len(md) > 10 and "![" not in md and ".png" not in md, f"{meta} {md[:80]!r}")

print("--- Q. blank chunks stay outside embedding and ranking")
t0 = time.time()
while time.time() - t0 < 300:
    h = health()
    if h["chunks_pending_embed"] == 0:
        break
    time.sleep(2)
check("Q.1 embed queue drains", h["chunks_pending_embed"] == 0, str(h))
rows = chunks_of(S)
check("Q.2 ok pages embedded, blank page not", len(rows) == 3 and rows[0][4] and rows[2][4] and not rows[1][4], str([x[4] for x in rows]))
with psycopg.connect(DB) as c:
    indexed = c.execute("SELECT indexed_at FROM files WHERE sha256 = %s", (S,)).fetchone()[0]
check("Q.3 the file counts as indexed despite the blank chunk", indexed is not None)
st, r = call("GET", "/v1/search", params={"q": tok, "weight_semantic": 0})
hit = (r or {}).get("hits", [{}])[0] if r and r.get("hits") else {}
check("Q.4 the hit carries the chunk meta", hit.get("meta", {}).get("status") == "ok" and "text_chars" in hit.get("meta", {}), str(hit.get("meta")))
blank_id = None
with psycopg.connect(DB) as c:
    blank_id = c.execute("SELECT chunk_sha256 FROM files_chunks WHERE file_sha256 = %s AND idx = 1", (S,)).fetchone()[0]
st, r = call("GET", "/v1/search", params={"q": "an empty white page with nothing on it", "limit": 50})
check("Q.5 the blank chunk never ranks, even for a query about emptiness",
      st == 200 and all(hh["chunk"] != blank_id for hh in r["hits"]), str(len(r["hits"]) if r else st))

print("--- R. a blank image file gets the same verdict through the image prompt")
white = Image.new("RGB", (640, 480), "white")
wb = io.BytesIO()
white.save(wb, "PNG")
st, r = deliver("white", wb.getvalue(), "white.png")
st, r = wait("white", sha(wb.getvalue()))
rows = chunks_of(sha(wb.getvalue()))
check("R.1 parsed, one chunk, status blank", st == 200 and len(rows) == 1 and rows[0][2].get("status") == "blank" and rows[0][1] == "",
      f"{st} {[(x[1][:60], x[2]) for x in rows]}")

print("--- S. re-delivery of the same PDF is a no-op: no page transcribed again")
with psycopg.connect(DB) as c:
    before = c.execute("SELECT count(*) FROM chunks").fetchone()[0]
st, r = deliver("three-again", pdf, "three.pdf")
check("S.1 known content under a new uri: 200 at once", st == 200, f"{st} {r}")
with psycopg.connect(DB) as c:
    after = c.execute("SELECT count(*) FROM chunks").fetchone()[0]
check("S.2 no new chunks", after == before, f"{before} -> {after}")

failed = [n for n, ok in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} passed" + (f"; FAILED: {failed}" if failed else ""))
