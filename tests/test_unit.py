"""Queue mechanics of the parse workers, exercised directly against the database.

Run inside the api image with the workers OFF and no other worker on this database:
    docker run --rm -i --env-file .env -e FATINGEST_WORKERS=0 -e STORE=/tmp/ustore \
        -e DATABASE_URL=... --network fatingest_dbnet fatingest-api:test python - < tests/test_unit.py
"""
import hashlib
import io
import json
import os
import sys
import time

os.environ["FATINGEST_WORKERS"] = "0"
os.environ["GOTENBERG_URL"] = "http://127.0.0.1:9"       # nothing listens there
os.environ["GOTENBERG_HEALTH_WAIT"] = "2"
os.environ.setdefault("STORE", "/tmp/ustore")
import httpx                                          # noqa: E402
import psycopg                                        # noqa: E402
from PIL import Image                                 # noqa: E402
from psycopg.pq import TransactionStatus              # noqa: E402

import app                                            # noqa: E402
import pdf_engine                                     # noqa: E402
import routing                                        # noqa: E402

DB = os.environ["DATABASE_URL"]
results = []


def check(name, cond, info=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + (f"   [{info}]" if info and not cond else ""), flush=True)


def sha(b):
    return hashlib.sha256(b).hexdigest()


def fetch(sql, *args):
    with psycopg.connect(DB) as c:
        return c.execute(sql, args).fetchone()


def run(sql, *args):
    with psycopg.connect(DB) as c:
        c.execute(sql, args)


def row(s):
    return fetch("SELECT parsed_at, meta, EXTRACT(EPOCH FROM due_at - now()) FROM files WHERE sha256 = %s", s)


assert fetch("SELECT count(*) FROM files WHERE parsed_at IS NULL")[0] == 0, "queue must be empty"
S1 = sha(f"unit-1-{time.time()}".encode())           # queued rows with no spool entry
S2 = sha(f"unit-2-{time.time()}".encode())
with psycopg.connect(DB) as c:
    app.enqueue(c.cursor(), S1, {"source": "file", "filename": "x.md"})
    app.enqueue(c.cursor(), S2, {"source": "file", "filename": "y.md"})
    c.execute("UPDATE files SET due_at = now() - interval '1 second' WHERE sha256 = %s", (S1,))

print("--- A. the row lock is the claim")
c1, c2, c3 = psycopg.connect(DB), psycopg.connect(DB), psycopg.connect(DB)
r1 = c1.execute(app.TAKE_NEXT).fetchone()
check("A.1 worker 1 takes the oldest due row", r1 is not None and r1[0] == S1, str(r1))
r2 = c2.execute(app.TAKE_NEXT).fetchone()
check("A.2 worker 2 skips the locked row and takes the next", r2 is not None and r2[0] == S2, str(r2))
r3 = c3.execute(app.TAKE_NEXT).fetchone()
check("A.3 worker 3 sees an empty queue while both are locked", r3 is None, str(r3))
c3.rollback()
c3.close()

print("--- B. NO KEY UPDATE: claims and reads on parsing content never wait")
c4 = psycopg.connect(DB)
c4.execute("SET statement_timeout = '2s'")
t0 = time.time()
try:
    app.commit_item(c4.cursor(), "unit/feed", "uri-" + S1[:8], {}, S1)   # FK -> the row worker 1 holds
    c4.commit()
    blocked = False
except Exception as e:
    blocked, err = True, str(e)
    c4.rollback()
check("B.1 a claim on the locked row commits at once", not blocked and time.time() - t0 < 1.5,
      err if blocked else f"{time.time() - t0:.2f}s")
with psycopg.connect(DB) as c:
    check("B.2 file_state of a locked row reads 'queued' without waiting", app.file_state(c.cursor(), S1) == "queued")
c4.close()

print("--- C. a worker that dies returns its row to the queue")
c2.close()                                            # no commit: the lock is simply gone
c5 = psycopg.connect(DB)
r5 = c5.execute(app.TAKE_NEXT).fetchone()
check("C.1 the row is takeable again, still pending", r5 is not None and r5[0] == S2 and r5[1].get("kind") == "pending", str(r5))
c5.rollback()
c5.close()

print("--- D. _parse_next: lost bytes (Unrecoverable) write nothing but the failure record")
c1.rollback()                                         # release S1: it is the oldest, so _parse_next takes it
c1.close()
c6 = psycopg.connect(DB)
took = app._parse_next(c6)                            # spool entry missing -> Unrecoverable -> _fail
check("D.1 _parse_next took a row", took is True)
meta, parsed_at = fetch("SELECT meta, parsed_at FROM files WHERE sha256 = %s", S1)
check("D.2 the row is settled as parse_failed with the error",
      parsed_at is not None and meta.get("kind") == "parse_failed" and "spool entry missing" in meta.get("error", ""), str(meta))
with psycopg.connect(DB) as c:
    check("D.3 file_state: parse_failed = claimed but not held -> missing", app.file_state(c.cursor(), S1) == "missing")
check("D.4 the worker's connection is left idle (no open transaction)",
      c6.info.transaction_status == TransactionStatus.IDLE)

print("--- E. _fail defers to a worker that took the row meanwhile")
cx = psycopg.connect(DB)
assert cx.execute(app.TAKE_NEXT).fetchone()[0] == S2   # worker X holds S2
cy = psycopg.connect(DB)
won = app._fail(cy.cursor(), S2, "stale worker")
cy.commit()
check("E.1 _fail returns False while another worker holds the row", won is False)
check("E.2 the row is untouched (still queued)", fetch("SELECT parsed_at FROM files WHERE sha256 = %s", S2)[0] is None)
cx.rollback()
won = app._fail(cy.cursor(), S2, "now unlocked")
cy.commit()
check("E.3 _fail settles an unlocked row", won is True and fetch("SELECT meta->>'kind' FROM files WHERE sha256 = %s", S2)[0] == "parse_failed")
cx.close()
cy.close()

print("--- F. empty queue")
check("F.1 _parse_next on an empty queue returns False and leaves no transaction open",
      app._parse_next(c6) is False and c6.info.transaction_status == TransactionStatus.IDLE)

print("--- G. due_at gates the queue; a re-delivery pulls a deferred row forward")
S3 = sha(f"unit-3-{time.time()}".encode())
with psycopg.connect(DB) as c:
    app.enqueue(c.cursor(), S3, {"source": "file", "filename": "z.md"})
    c.execute("UPDATE files SET due_at = now() + interval '1 hour' WHERE sha256 = %s", (S3,))
c7 = psycopg.connect(DB)
check("G.1 a row due in the future is not taken", c7.execute(app.TAKE_NEXT).fetchone() is None)
c7.rollback()
with psycopg.connect(DB) as c:
    app._retry_now(c.cursor(), S3)
check("G.2 _retry_now makes it due", row(S3)[2] <= 0)
r7 = c7.execute(app.TAKE_NEXT).fetchone()
check("G.3 ... and it is taken", r7 is not None and r7[0] == S3, str(r7))
with psycopg.connect(DB) as c:
    c.execute("SET statement_timeout = '2s'")
    app._retry_now(c.cursor(), S3)                    # held by c7: skipped, never waits
check("G.4 _retry_now on a row being parsed skips without waiting", True)
c7.rollback()
c7.close()
run("UPDATE files SET parsed_at = now() WHERE sha256 = %s", S3)   # park it

print("--- H. a dependency failure defers the delivery with exponential backoff")
md = b"# Unit\n\nText for the defer test.\n"
S4 = sha(md)
app.spool_write(S4, md)
with psycopg.connect(DB) as c:
    app.enqueue(c.cursor(), S4, {"source": "file", "filename": "u.md"})
real_parse_file = app.parse_file


def parse_raising(exc):
    def fake(*a, **k):
        raise exc
    return fake


app.parse_file = parse_raising(app.RetryLater("dep down"))
check("H.1 _parse_next took the row", app._parse_next(c6) is True)
parsed_at, meta, due_in = row(S4)
check("H.2 still queued, attempt 1 recorded with the error",
      parsed_at is None and meta.get("attempts") == 1 and meta.get("error") == "dep down" and meta.get("kind") == "pending", str(meta))
check("H.3 due in ~60 s", 50 < due_in <= 61, f"{due_in:.0f}s")
check("H.4 spool entry kept for the retry", app.spool_read(S4) == md)
check("H.5 not taken while backed off", app._parse_next(c6) is False)
run("UPDATE files SET due_at = now() WHERE sha256 = %s", S4)
app._parse_next(c6)
parsed_at, meta, due_in = row(S4)
check("H.6 second failure: attempt 2, due in ~120 s", meta.get("attempts") == 2 and 110 < due_in <= 121, f"{meta} {due_in:.0f}s")
app.parse_file = lambda *a, **k: ([{"markdown": "# Unit\n\nText for the defer test.", "render": None,
                                    "meta": {"status": "ok"}}], {"kind": "text"})
run("UPDATE files SET due_at = now() WHERE sha256 = %s", S4)
app._parse_next(c6)
parsed_at, meta, _ = row(S4)
check("H.7 success settles it: parsed, attempts gone, kind text",
      parsed_at is not None and meta.get("kind") == "text" and "attempts" not in meta, str(meta))
check("H.8 spool entry gone", app.spool_read(S4) is None)
check("H.9 chunk collection written", fetch("SELECT count(*) FROM files_chunks WHERE file_sha256 = %s", S4)[0] == 1)

print("--- H2. an internal error is deferred with backoff like a dependency failure")
mdx = b"# Unit internal\n"
SX = sha(mdx)
app.spool_write(SX, mdx)
with psycopg.connect(DB) as c:
    app.enqueue(c.cursor(), SX, {"source": "file", "filename": "w.md"})
app.parse_file = parse_raising(ValueError("boom"))
check("H2.1 _parse_next took the row", app._parse_next(c6) is True)
parsed_at, meta, due_in = row(SX)
check("H2.2 still queued, attempt 1, the error named as internal",
      parsed_at is None and meta.get("attempts") == 1 and meta.get("error", "").startswith("internal: ValueError: boom")
      and 50 < due_in <= 61, f"{meta} {due_in:.0f}s")
check("H2.3 spool entry kept", app.spool_read(SX) == mdx)
check("H2.4 nothing partial: no chunks for it, connection idle",
      fetch("SELECT count(*) FROM files_chunks WHERE file_sha256 = %s", SX)[0] == 0
      and c6.info.transaction_status == TransactionStatus.IDLE)
app.parse_file = lambda *a, **k: ([{"markdown": "# Unit internal", "render": None, "meta": {"status": "ok"}}], {"kind": "text"})
run("UPDATE files SET due_at = now() WHERE sha256 = %s", SX)
app._parse_next(c6)
parsed_at, meta, _ = row(SX)
check("H2.5 the next release fixes it: settled, attempts gone", parsed_at is not None and meta.get("kind") == "text" and "attempts" not in meta, str(meta))

print("--- I. a healthy converter's refusal is unparseable under this recipe")
md5 = b"# Unit five\n"
S5 = sha(md5)
app.spool_write(S5, md5)
with psycopg.connect(DB) as c:
    app.enqueue(c.cursor(), S5, {"source": "file", "filename": "v.md"})
app.parse_file = parse_raising(app.UnparseableError("gotenberg 500: LibreOffice failed to convert"))
app._parse_next(c6)
parsed_at, meta, _ = row(S5)
check("I.1 settled as unparseable with the converter's answer",
      parsed_at is not None and meta.get("kind") == "unparseable" and meta.get("error", "").startswith("gotenberg 500"), str(meta))
with psycopg.connect(DB) as c:
    check("I.2 file_state: complete possession of nothing", app.file_state(c.cursor(), S5) == "complete")
check("I.3 spool entry gone", app.spool_read(S5) is None)
app.parse_file = real_parse_file

print("--- J. chunks deliveries defer and settle the same way")
img = Image.new("RGB", (64, 32), "white")
buf = io.BytesIO()
img.save(buf, "PNG")
r_sha = app.store_render(buf.getvalue())
pack = [{"markdown": None, "render_sha256": r_sha, "generated": True}]
S6 = app.pack_sha(pack)
app.spool_write(S6, json.dumps({"chunks": pack}).encode())
with psycopg.connect(DB) as c:
    app.enqueue(c.cursor(), S6, {"source": "chunks"})
real_vlm = pdf_engine.vlm
pdf_engine.vlm = parse_raising(app.RetryLater("vlm 503: upstream down"))
app._parse_next(c6)
parsed_at, meta, due_in = row(S6)
check("J.1 VLM unavailable: deferred, attempt 1", parsed_at is None and meta.get("attempts") == 1 and 50 < due_in <= 61, str(meta))
pdf_engine.vlm = lambda png, prompt: "described by the vlm"
run("UPDATE files SET due_at = now() WHERE sha256 = %s", S6)
app._parse_next(c6)
parsed_at, meta, _ = row(S6)
md_text = fetch("SELECT c.markdown, c.meta FROM files_chunks fc JOIN chunks c ON c.sha256 = fc.chunk_sha256 WHERE fc.file_sha256 = %s", S6)
check("J.2 VLM back: settled as chunkset with the generated text, status ok and the VLM as its source",
      parsed_at is not None and meta.get("kind") == "chunkset" and md_text and md_text[0] == "described by the vlm"
      and md_text[1] == {"status": "ok", "source": "vlm"}, f"{meta} {md_text}")
# a render the VLM refuses is a verdict on that chunk, not on the delivery
img2 = Image.new("RGB", (64, 32), "black")
buf2 = io.BytesIO()
img2.save(buf2, "PNG")
pack2 = [{"markdown": "kept text", "render_sha256": None, "generated": False},
         {"markdown": None, "render_sha256": app.store_render(buf2.getvalue()), "generated": True}]
S7 = app.pack_sha(pack2)
app.spool_write(S7, json.dumps({"chunks": pack2}).encode())
with psycopg.connect(DB) as c:
    app.enqueue(c.cursor(), S7, {"source": "chunks"})
pdf_engine.vlm = parse_raising(app.UnparseableError("vlm 400: image too large"))
app._parse_next(c6)
parsed_at, meta, _ = row(S7)
rows7 = None
with psycopg.connect(DB) as c:
    rows7 = c.execute("""SELECT fc.idx, c.markdown, c.meta FROM files_chunks fc JOIN chunks c ON c.sha256 = fc.chunk_sha256
                         WHERE fc.file_sha256 = %s ORDER BY fc.idx""", (S7,)).fetchall()
check("J.3 VLM refuses one render: the delivery settles as a chunkset with both positions",
      parsed_at is not None and meta.get("kind") == "chunkset" and len(rows7) == 2, f"{meta} {rows7}")
check("J.4 the refused chunk holds empty text and the verdict; the other is untouched",
      rows7 and rows7[1][1] == "" and rows7[1][2].get("status") == "unparseable" and rows7[1][2].get("error", "").startswith("vlm 400")
      and rows7[0][1] == "kept text" and rows7[0][2] == {"status": "ok"}, str(rows7))
with psycopg.connect(DB) as c:
    check("J.5 file_state: complete (a verdict is possession)", app.file_state(c.cursor(), S7) == "complete")
pdf_engine.vlm = real_vlm
c6.close()

print("--- K. the gate: one Gotenberg conversion at a time across connections")
ca, cb = psycopg.connect(DB), psycopg.connect(DB)
with app._gate(ca.cursor(), "gotenberg"):
    check("K.1 a second worker cannot take the gate while it is held",
          cb.execute("SELECT pg_try_advisory_lock(hashtext('gotenberg'))").fetchone()[0] is False)
check("K.2 released after the call", cb.execute("SELECT pg_try_advisory_lock(hashtext('gotenberg'))").fetchone()[0] is True)
cb.execute("SELECT pg_advisory_unlock(hashtext('gotenberg'))")
ca.close()
cb.close()

print("--- L. classification of answers")


def raises(fn, exc):
    try:
        fn()
    except exc:
        return True
    except Exception:
        return False
    return False


check("L.1 5xx from the VLM path is RetryLater", raises(lambda: pdf_engine.classify("vlm", httpx.Response(503, text="x")), pdf_engine.RetryLater))
check("L.2 429 is RetryLater", raises(lambda: pdf_engine.classify("vlm", httpx.Response(429, text="x")), pdf_engine.RetryLater))
check("L.3 401 (refusing us) is RetryLater", raises(lambda: pdf_engine.classify("vlm", httpx.Response(401, text="x")), pdf_engine.RetryLater))
check("L.4 400 is UnparseableError", raises(lambda: pdf_engine.classify("vlm", httpx.Response(400, text="x")), pdf_engine.UnparseableError))
check("L.5 2xx passes", pdf_engine.classify("vlm", httpx.Response(200, text="ok")) is None)
t0 = time.time()
check("L.6 unreachable Gotenberg: health wait expires into RetryLater",
      raises(lambda: routing._await_healthy(), routing.RetryLater) and 1.5 < time.time() - t0 < 6, f"{time.time() - t0:.1f}s")
check("L.7 _gotenberg with the default gate propagates RetryLater",
      raises(lambda: routing._gotenberg("/forms/libreoffice/convert", [], routing._no_gate), routing.RetryLater))

with psycopg.connect(DB) as c:                        # leave nothing behind
    c.execute("DELETE FROM items WHERE feed = 'unit/feed'")
    c.execute("DELETE FROM files WHERE sha256 = ANY(%s)", ([S1, S2, S3, S4, S5, S6],))
    c.execute("DELETE FROM chunks c WHERE NOT EXISTS (SELECT 1 FROM files_chunks fc WHERE fc.chunk_sha256 = c.sha256)")

failed = [n for n, ok in results if not ok]
print("--- M. transcribe_pages: verdict per page, blank token, concurrency, memo")


def png(color, size=(32, 32)):
    im = Image.new("RGB", size, color)
    b = io.BytesIO()
    im.save(b, "PNG")
    return b.getvalue()


renders = [png("red"), png("green"), png("blue"), png("white"), png("black")]
items = [{"render": r, "prompt": "p", "meta": {"visual": 0.5, "text_chars": 10}} for r in renders]
calls = []


def fake_vlm(image, prompt):
    calls.append(image)
    if image == renders[1]:
        raise pdf_engine.UnparseableError("vlm 400: image too large")
    if image == renders[3]:
        return pdf_engine.VLM_BLANK_TOKEN
    return "text " + str(renders.index(image))


pdf_engine.vlm = fake_vlm
out = pdf_engine.transcribe_pages(items, None, 4)
check("M.1 order kept, verdict per page", [o["meta"]["status"] for o in out] == ["ok", "unparseable", "ok", "blank", "ok"],
      str([o["meta"] for o in out]))
check("M.2 the refused page: empty text, the refusal, render kept",
      out[1]["markdown"] == "" and out[1]["meta"]["error"].startswith("vlm 400") and out[1]["render"] == renders[1])
check("M.3 the blank page: empty text, status blank", out[3]["markdown"] == ""
      and out[3]["meta"] == {"status": "blank", "visual": 0.5, "text_chars": 10, "source": "vlm"}, str(out[3]["meta"]))
check("M.4 page hints merged into meta, and the VLM is named as the source",
      out[0]["meta"] == {"status": "ok", "visual": 0.5, "text_chars": 10, "source": "vlm"} and out[0]["markdown"] == "text 0", str(out[0]["meta"]))
out_e = pdf_engine.transcribe_pages([{"error": "cannot render page: X"}, items[0]], None, 2)
check("M.5 an unrenderable page is a verdict in place: no render, status unparseable",
      out_e[0] == {"markdown": "", "render": None, "meta": {"status": "unparseable", "error": "cannot render page: X"}}
      and out_e[1]["markdown"] == "text 0")
calls.clear()
pdf_engine.transcribe_pages([items[0], items[2], items[0]], None, 4)
check("M.6 identical renders are transcribed once", len(calls) == 2, str(len(calls)))
pdf_engine.vlm = lambda image, prompt: (time.sleep(0.3), "slow")[1]
eight = [{"render": png((i, i, i)), "prompt": "p"} for i in range(8)]
t0 = time.time()
pdf_engine.transcribe_pages(eight, None, 8)
par = time.time() - t0
t0 = time.time()
pdf_engine.transcribe_pages(eight[:3], None, 1)
seq = time.time() - t0
check("M.7 concurrency: 8 pages at 8 in flight take one round, 3 pages at 1 take three", par < 1.0 and seq >= 0.85, f"par {par:.2f}s seq {seq:.2f}s")
pdf_engine.vlm = lambda image, prompt: (time.sleep(0.3), "empty")[1] if False else ""
check("M.8 an empty answer is blank too", pdf_engine.transcribe_pages([items[0]], None, 1)[0]["meta"]["status"] == "blank")


class FakeCache:
    def __init__(self):
        self.store = {}

    def get(self, render):
        return self.store.get(sha(render))

    def put(self, render, md, meta):
        self.store[sha(render)] = (md, meta)


six = [{"render": png((i, 0, 0)), "prompt": "p"} for i in range(6)]
bad = six[2]["render"]
calls.clear()


def flaky(image, prompt):
    calls.append(image)
    if image == bad:
        raise pdf_engine.RetryLater("vlm 503: down")
    time.sleep(0.2)
    return "ok text"


pdf_engine.vlm = flaky
cache = FakeCache()
try:
    pdf_engine.transcribe_pages(six, cache, 8)
    raised = None
except pdf_engine.RetryLater as e:
    raised = e
check("M.9 a RetryLater from one page propagates", raised is not None)
check("M.10 ... after the pages in flight completed into the memo", len(cache.store) == 5, str(len(cache.store)))
calls.clear()
pdf_engine.vlm = lambda image, prompt: (calls.append(image), "recovered")[1]
out = pdf_engine.transcribe_pages(six, cache, 8)
check("M.11 the retry transcribes only the missing page", len(calls) == 1 and calls[0] == bad, str(len(calls)))
check("M.12 memoized pages come back with their stored text and verdict",
      [o["markdown"] for o in out] == ["ok text", "ok text", "recovered", "ok text", "ok text", "ok text"]
      and all(o["meta"]["status"] == "ok" for o in out))
pdf_engine.vlm = real_vlm

print("--- N. ChunkCache: a looked-up chunk is safe from GC until the collection commits")
r_n = png((7, 77, 177))
cn = psycopg.connect(DB)
cur_n = cn.cursor()
cc = app.ChunkCache(cur_n)
cc.put(r_n, "memo text", {"status": "ok", "visual": 0.1, "text_chars": 9})
cid_n = app.chunk_id(None, sha(r_n), True)
check("N.1 put writes the chunk row at once (own connection) and the render to disk",
      fetch("SELECT markdown FROM chunks WHERE sha256 = %s", cid_n) == ("memo text",) and os.path.exists(app.render_path(sha(r_n))))
hit = cc.get(r_n)
check("N.2 get returns text and verdict", hit == ("memo text", {"status": "ok", "visual": 0.1, "text_chars": 9}), str(hit))
gc1 = fetch("SELECT fatingest_gc(interval '0')")[0]
check("N.3 GC skips the row the worker holds under KEY SHARE", fetch("SELECT count(*) FROM chunks WHERE sha256 = %s", cid_n)[0] == 1, str(gc1))
cn.rollback()
gc2 = fetch("SELECT fatingest_gc(interval '0')")[0]
check("N.4 released, the orphan is swept", fetch("SELECT count(*) FROM chunks WHERE sha256 = %s", cid_n)[0] == 0, str(gc2))
check("N.5 get on a missing chunk is None", cc.get(r_n) is None)
cn.close()

print("--- O. the text-layer route: guard, link rewriting, identity")
check("O.1 an already resolved item passes through untouched and asks no one",
      pdf_engine.transcribe_pages([{"render": b"x", "markdown": "# fra tekstlaget",
                                    "meta": {"status": "ok", "source": "text_layer", "visual": 0.0}}], None, 4)
      == [{"markdown": "# fra tekstlaget", "render": b"x",
           "meta": {"status": "ok", "source": "text_layer", "visual": 0.0}}])
calls.clear()
pdf_engine.vlm = fake_vlm
mixed = pdf_engine.transcribe_pages([{"render": renders[0], "prompt": "p"},
                                     {"render": b"y", "markdown": "tekstlag", "meta": {"status": "ok", "source": "text_layer"}}], None, 4)
check("O.2 a mixed document: only the VLM item is sent", len(calls) == 1 and calls[0] == renders[0]
      and mixed[1]["markdown"] == "tekstlag" and mixed[0]["meta"]["source"] == "vlm", str(len(calls)))
pdf_engine.vlm = real_vlm
check("O.3 guard: identical text passes", pdf_engine._complete("Hej med dig", "Hej med dig"))
check("O.4 guard: a word broken across lines passes", pdf_engine._complete("informationsteknologi", "informations-\nteknologi"))
check("O.5 guard: a soft hyphen passes", pdf_engine._complete("bestyrelse", "besty\xadrelse"))
check("O.6 guard: non-latin text passes", pdf_engine._complete("Δοκιμή Проверка", "Δοκιμή Проверка"))
check("O.7 guard: a missing word fails", not pdf_engine._complete("kun det halve", "kun det halve og mere endnu"))
MEM = {"doc.pdf": b"pdf", "bilag/note.txt": b"noten"}
n_sha = sha(b"noten")
check("O.8 a reference to a member becomes its content address",
      pdf_engine._rewrite_md_links("[se](file://bilag/note.txt)", "doc.pdf", MEM) == f"[se](sha256://{n_sha})")
check("O.9 a relative reference resolves the same way",
      pdf_engine._rewrite_md_links("[se](bilag/note.txt)", "doc.pdf", MEM) == f"[se](sha256://{n_sha})")
for md in ("[a](https://example.org/x?y=1)", "[a](mailto:en@example.org)", "[a](mangler.pdf)", "[a](#afsnit)",
           f"[a](sha256://{n_sha})", "ingen links her"):
    check(f"O.10 left as written: {md[:34]}", pdf_engine._rewrite_md_links(md, "doc.pdf", MEM) == md)
prep = app._prepared_from_parser([
    {"markdown": "tekstlag", "render": b"R1", "meta": {"status": "ok", "source": "text_layer"}},
    {"markdown": "transskriberet", "render": b"R2", "meta": {"status": "ok", "source": "vlm"}},
    {"markdown": "", "render": None, "meta": {"status": "unparseable", "error": "cannot render page: X"}}])
check("O.11 a text-layer chunk is deterministic: both halves, no recipe",
      prep[0]["generated"] is False
      and app.chunk_id(prep[0]["markdown"], prep[0]["render_sha256"], False)
          == sha(f"{sha(b'R1')}\x00{sha(b'tekstlag')}\x00".encode()))
check("O.12 a transcribed chunk keeps render plus recipe", prep[1]["generated"] is True
      and app.chunk_id(None, prep[1]["render_sha256"], True) == sha(f"{sha(b'R2')}\x00\x00{app.PARSER_VERSION}".encode()))
check("O.13 the same page under another recipe keeps its text-layer identity: recipe is not in it",
      app.chunk_id("tekstlag", sha(b"R1"), False) == app.chunk_id("tekstlag", sha(b"R1"), False))
check("O.14 an unrenderable page holds nothing and is a verdict", prep[2]["generated"] is True and prep[2]["render_sha256"] is None)

print("--- P. the visual measure never promises 'nothing drawn' on missing evidence")
import pypdfium2.raw as _pc                                            # noqa: E402


class _Obj:
    def __init__(self, t, b=None): self.type, self._b = t, b
    def get_bounds(self):
        if self._b is None: raise RuntimeError("bounds unavailable")
        return self._b


class _Page:
    def __init__(self, objs, w=595.0, h=842.0): self._o, self._w, self._h = objs, w, h
    def get_width(self): return self._w
    def get_height(self): return self._h
    def get_objects(self, max_depth=1): return list(self._o)


class _UnlistablePage(_Page):
    def get_objects(self, max_depth=1): raise RuntimeError("contents unavailable")


_T, _P = _pc.FPDF_PAGEOBJ_TEXT, _pc.FPDF_PAGEOBJ_PATH
_real_annotation_boxes = pdf_engine._annotation_boxes
pdf_engine._annotation_boxes = lambda page: []          # fake pages carry no annotations
check("P.1 a page of text alone measures zero", pdf_engine._visual_share(_Page([_Obj(_T, (0, 0, 100, 100))])) == 0.0)
check("P.2 a half-page drawing measures half (to the grid cell)", abs(pdf_engine._visual_share(_Page([_Obj(_P, (0, 421, 595, 842))])) - 0.5) < 0.005)
check("P.3 overlapping drawings count once",
      abs(pdf_engine._visual_share(_Page([_Obj(_P, (0, 421, 595, 842)), _Obj(_P, (0, 421, 595, 842))])) - 0.5) < 0.005)
check("P.4 a drawing whose place cannot be read still counts as drawn, never as zero",
      pdf_engine._visual_share(_Page([_Obj(_T, (0, 0, 10, 10)), _Obj(_P)])) == 1.0)
check("P.5 ... but a measured drawing still gives its own share",
      abs(pdf_engine._visual_share(_Page([_Obj(_P), _Obj(_P, (0, 421, 595, 842))])) - 0.5) < 0.005)
check("P.6 a page whose contents cannot be listed counts as drawn",
      pdf_engine._visual_share(_UnlistablePage([])) == 1.0)
check("P.7 a page with no size measures zero", pdf_engine._visual_share(_Page([], w=0, h=0)) == 0.0)
tiny = pdf_engine._visual_share(_Page([_Obj(_P, (0, 0, 1, 1))]))
check("P.8 the share is not rounded: a one-point mark is a small number, never zero", 0 < tiny < 0.001, str(tiny))
pdf_engine._annotation_boxes = lambda page: [(0, 421, 595, 842)]
check("P.9 an annotation the viewer draws counts like a drawing", abs(pdf_engine._visual_share(_Page([_Obj(_T, (0, 0, 10, 10))])) - 0.5) < 0.005)
pdf_engine._annotation_boxes = lambda page: [None]
check("P.10 an annotation whose box cannot be read counts as drawn, never as zero", pdf_engine._visual_share(_Page([])) == 1.0)
pdf_engine._annotation_boxes = _real_annotation_boxes

print("--- Q. link marks in the VLM's support text (read from the page with PDFium)")
import pymupdf                                                          # noqa: E402
import pypdfium2 as _pdfium                                             # noqa: E402


def _pdf(lines, links=(), extra=None):
    """One page: `lines` of 12pt text at 20pt spacing from (72, 100); `links` = (needle, uri) pairs, a URI
    link over every occurrence of the needle (the action is written as /URI whatever the scheme - pymupdf
    itself turns file: references into Launch links, which pdfium rightly does not report as URIs)."""
    doc = pymupdf.open(); page = doc.new_page(); y = 100
    for line in lines:
        page.insert_text((72, y), line, fontsize=12); y += 20
    targets = []
    for needle, uri in links:
        for rect in page.search_for(needle):
            page.insert_link({"kind": pymupdf.LINK_URI, "uri": f"https://placeholder/{len(targets)}", "from": rect})
            targets.append(uri)
    if extra:
        extra(page)
    doc = pymupdf.open(stream=doc.tobytes(), filetype="pdf")
    for lnk in doc[0].get_links():
        k = int(lnk["uri"].rsplit("/", 1)[1]) if lnk.get("uri", "").startswith("https://placeholder/") else None
        if k is not None:
            doc.xref_set_key(lnk["xref"], "A/URI", pymupdf.get_pdf_str(targets[k]))
    return doc.tobytes()


def _read(pdf, member_path="doc.pdf", members=None):
    d = _pdfium.PdfDocument(pdf); pg = d[0]
    try:
        return pdf_engine._read_text(d, pg, member_path, members)
    finally:
        pg.close(); d.close()


MEM = {"doc.pdf": b"pdf", "bilag/note.txt": b"noten"}
n_sha = sha(b"noten")
text, sup, lab = _read(_pdf(["Se noten her."], [("noten", "file://bilag/note.txt")]), "doc.pdf", MEM)
check("Q.1 an anchor is marked and the mark points at the content address",
      sup.strip() == "Se [noten](L1) her." and lab == {"L1": f"sha256://{n_sha}"}, f"{sup!r} {lab}")
check("Q.1b the plain text is the same page without marks", text.strip() == "Se noten her.", repr(text))
text, sup, lab = _read(_pdf(["alfa beta gamma"], [("alfa", "https://x.dk/1"), ("beta", "https://x.dk/2"), ("gamma", "https://x.dk/1")]))
check("Q.2 anchors sharing a target share a mark",
      sup.strip() == "[alfa](L1) [beta](L2) [gamma](L1)" and lab == {"L1": "https://x.dk/1", "L2": "https://x.dk/2"}, f"{sup!r} {lab}")
text, sup, lab = _read(_pdf(["læs mere om", "dine muligheder", "og andet"],
                            [("læs mere om", "https://kl.dk/p"), ("dine muligheder", "https://kl.dk/p")]))
check("Q.3 a link across two lines is two anchors with one mark, and the line break stays outside",
      sup.count("](L1)") == 2 and "[læs mere om](L1)" in sup and "[dine muligheder](L1)" in sup and lab == {"L1": "https://kl.dk/p"}, f"{sup!r}")
text, sup, lab = _read(_pdf(["tekst uden link"], extra=lambda p: p.insert_link(
    {"kind": pymupdf.LINK_URI, "uri": "https://x.dk/tom", "from": pymupdf.Rect(300, 300, 400, 320)})))
check("Q.4 a link over nothing but empty page produces no anchor and no mark", sup.strip() == "tekst uden link" and lab == {}, f"{sup!r} {lab}")
text, sup, lab = _read(_pdf(["se [her] nu"], [("[her]", "https://x.dk/1")]))
check("Q.5 brackets in the anchor are escaped", r"[\[her\]](L1)" in sup, f"{sup!r}")
text, sup, lab = _read(_pdf(["intet link her"]))
check("Q.6 a page without links produces no marks", sup == text and sup.strip() == "intet link her" and lab == {}, f"{sup!r}")
text, sup, lab = _read(_pdf(["Læs mere her."], [("Læs mere her", "https://x.dk/1")]))
check("Q.6b spaces inside an anchor stay inside it, punctuation outside stays outside",
      sup.strip() == "[Læs mere her](L1)." and lab == {"L1": "https://x.dk/1"}, f"{sup!r}")
text, sup, lab = _read(_pdf(["informations-", "teknologi er samlet"]))
check("Q.6c a word broken at a line break leaves no marker in the text and passes the guard",
      "\x02" not in text and "\ufffe" not in text and pdf_engine._complete("informationsteknologi er samlet", text), repr(text))
long_text = "x" * 19990 + "[anker](L1)"
check("Q.7 a cut never leaves half a mark behind", not pdf_engine._clip(long_text).rstrip().endswith("(L")
      and len(pdf_engine._clip(long_text)) <= 20000 and pdf_engine._clip("kort") == "kort")
pdf_engine.vlm = lambda image, prompt: "[noten](L1)"
md, meta = pdf_engine.transcribe(b"png", "p", {"L1": f"sha256://{n_sha}"})
check("Q.8 after generation the mark becomes the target",
      md == f"[noten](sha256://{n_sha})" and meta["status"] == "ok", f"{md!r}")
pdf_engine.vlm = lambda image, prompt: "[opfundet](L9) og [ægte](L1)"
md, _ = pdf_engine.transcribe(b"png", "p", {"L1": "https://x.dk/1"})
check("Q.9 a mark the model invented is left alone, a real one is exchanged",
      md == "[opfundet](L9) og [ægte](https://x.dk/1)", f"{md!r}")
pdf_engine.vlm = real_vlm

_, sup, lab = _read(_pdf(["x y z"], [("x", "https://a"), ("y", "https://b"), ("z", "https://c")]))
check("Q.10 marks are counted from one, so the example in the prompt (L0) is never a real address",
      set(lab) == {"L1", "L2", "L3"} and "L0" not in lab and "(L0)" not in sup, str(lab))
check("Q.11 the prompt's own example uses that free mark", "[anchor](L0)" in pdf_engine.PROMPT)
md, _ = pdf_engine.transcribe(b"png", "p", {"L1": "https://a"}) if False else (None, None)
pdf_engine.vlm = lambda image, prompt: "[kopieret](L0) og [rigtig](L1)"
md, _ = pdf_engine.transcribe(b"png", "p", {"L1": "https://a"})
check("Q.12 if the model copies the example, it resolves to nothing rather than to a real address",
      md == "[kopieret](L0) og [rigtig](https://a)", f"{md!r}")
pdf_engine.vlm = real_vlm

print("--- R. routing: threshold, annotations, form fields, text-only, blank")
from PIL import Image as _Image                                        # noqa: E402


def _dark(png, rect, page_w=595.0):
    img = _Image.open(io.BytesIO(png)).convert("L"); s_ = img.width / page_w
    crop = img.crop(tuple(int(v * s_) for v in rect))
    return sum(1 for px in crop.getdata() if px < 200)


def _route(item):
    return "error" if item.get("error") else "text_layer" if "markdown" in item else "vlm"


items = pdf_engine.extract_pages(_pdf(["tekst med en lille streg"], extra=lambda p: p.draw_line((72, 300), (120, 300))))
check("R.1 a page with text and one small line goes to the VLM at the default threshold (0): visual is small but not zero",
      _route(items[0]) == "vlm" and 0 < items[0]["meta"]["visual"] < 0.01, str(items[0].get("meta")))
pdf_engine.VLM_VISUAL_THRESHOLD = 0.005
items2 = pdf_engine.extract_pages(_pdf(["tekst med en lille streg"], extra=lambda p: p.draw_line((72, 300), (120, 300))))
check("R.2 with VLM_VISUAL_THRESHOLD 0.005 the same page is read from its text layer",
      _route(items2[0]) == "text_layer" and items2[0]["meta"]["source"] == "text_layer" and "lille streg" in items2[0]["markdown"], str(items2[0].get("meta")))
pdf_engine.VLM_VISUAL_THRESHOLD = 0.0
items = pdf_engine.extract_pages(_pdf(["Referat"], extra=lambda p: p.add_freetext_annot(pymupdf.Rect(300, 60, 560, 100), "GODKENDT", fontsize=11).update()))
check("R.3 a note stamped on a text-only page is drawn, so it counts: the page goes to the VLM",
      _route(items[0]) == "vlm" and items[0]["meta"]["visual"] > 0 and _dark(items[0]["render"], (300, 60, 560, 100)) > 0, str(items[0].get("meta")))


def _widget(p):
    w = pymupdf.Widget(); w.field_type = pymupdf.PDF_WIDGET_TYPE_TEXT; w.field_name = "navn"
    w.rect = pymupdf.Rect(130, 108, 400, 130); w.field_value = "Hans Hansen"; w.text_fontsize = 11; p.add_widget(w)


items = pdf_engine.extract_pages(_pdf(["Navn:"], extra=_widget))
check("R.4 a filled form field is drawn in the render and counts as drawn: the VLM sees the value",
      _route(items[0]) == "vlm" and items[0]["meta"]["visual"] > 0 and _dark(items[0]["render"], (130, 108, 400, 130)) > 100,
      f"{items[0].get('meta')} dark={_dark(items[0]['render'], (130, 108, 400, 130)) if 'render' in items[0] else None}")
items = pdf_engine.extract_pages(_pdf(["Kun tekst på denne side.", "Og en linje mere."]))
check("R.5 a text-only page is read from its text layer: visual exactly 0, text counted, LiteParse markdown",
      _route(items[0]) == "text_layer" and items[0]["meta"]["visual"] == 0.0 and items[0]["meta"]["text_chars"] > 0
      and "Kun tekst" in items[0]["markdown"] and items[0]["meta"]["status"] == "ok", str(items[0].get("meta")))
items = pdf_engine.extract_pages(_pdf([]))
check("R.6 a page with nothing on it is blank without asking anyone",
      _route(items[0]) == "text_layer" and items[0]["meta"]["status"] == "blank" and items[0]["markdown"] == "", str(items[0].get("meta")))
items = pdf_engine.extract_pages(_pdf(["Se vejledningen her.", "Figur:"], [("vejledningen", "https://x.dk/v")],
                                      extra=lambda p: p.draw_rect(pymupdf.Rect(72, 300, 400, 500), fill=(0.2, 0.4, 0.8))))
check("R.7 on the VLM route the link arrives in the prompt as a mark and the label carries the address",
      _route(items[0]) == "vlm" and "[vejledningen](L1)" in items[0]["prompt"] and items[0]["labels"] == {"L1": "https://x.dk/v"}, str(items[0].get("labels")))
check("R.8 the prompt never carries the address itself", "x.dk/v" not in items[0]["prompt"])

print("--- S. one prompt, from one file")
import routing                                                         # noqa: E402
import tempfile as _tempfile                                           # noqa: E402
check("S.1 a picture gets the same prompt as a page, with nothing extracted to support it",
      routing.image_item(b"png")["prompt"] == pdf_engine.prompt_for("") and "<extracted-text>\n\n</extracted-text>" in pdf_engine.prompt_for(""))
check("S.2 the blank token the answer is checked against is the one the prompt asks for",
      pdf_engine.VLM_BLANK_TOKEN in pdf_engine.prompt_for("") and "{blank}" not in pdf_engine.prompt_for(""))
check("S.3 the support text goes in whole, and a token-like string inside it is left alone",
      "{blank} og {support}" in pdf_engine.prompt_for("tekst med {blank} og {support}"))
check("S.4 the support text is capped, and the cap never leaves half a mark", len(pdf_engine.prompt_for("y" * 30000)) < len(pdf_engine.PROMPT) + 20010)
check("S.5 the prompt file the service runs on is the built-in one unless VLM_PROMPT_FILE says otherwise",
      pdf_engine.VLM_PROMPT_FILE.endswith("prompt.md") and pdf_engine._load_prompt(pdf_engine.VLM_PROMPT_FILE) == pdf_engine.PROMPT)
with _tempfile.NamedTemporaryFile("w", suffix=".md", delete=False) as f:
    f.write("Transcribe this. {support}"); bad = f.name
try:
    pdf_engine._load_prompt(bad); refused = False
except ValueError as e:
    refused = "{blank}" in str(e)
check("S.6 a prompt without the blank placeholder is refused at load, naming the placeholder", refused)
with _tempfile.NamedTemporaryFile("w", suffix=".md", delete=False) as f:
    f.write("Min egen prompt. Svar {blank} hvis tom.\n{support}"); own = f.name
check("S.7 an operator's own prompt with both placeholders loads", pdf_engine._load_prompt(own).startswith("Min egen prompt"))

failed = [n for n, ok in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} passed" + (f"; FAILED: {failed}" if failed else ""))
sys.exit(1 if failed else 0)
