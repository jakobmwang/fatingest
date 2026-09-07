"""A link on a page that must go through the VLM: does the real address come back?
Runs INSIDE an api container: docker exec -i <container> python - < tests/test_links_e2e.py"""
import base64, hashlib, json, os, time, urllib.error, urllib.request
import pymupdf, psycopg
API = "http://localhost:8000"; DB = os.environ["DATABASE_URL"]; RUN = str(int(time.time()))
F = f"links/{RUN}"; results = []
def check(n, c, i=""):
    results.append((n, bool(c))); print(("PASS " if c else "FAIL ") + n + (f"   [{i}]" if i and not c else ""), flush=True)
def call(m, p, b=None):
    r = urllib.request.Request(API + p, data=json.dumps(b).encode() if b else None, method=m,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=300) as x: return x.status, json.loads(x.read() or b"null")
    except urllib.error.HTTPError as e: return e.code, json.loads(e.read() or b"null")

URL = f"https://example.org/vejledning-{RUN}?afsnit=3"
doc = pymupdf.open(); p = doc.new_page()
p.insert_text((72, 90), f"Vejledning {RUN}", fontsize=16)
p.insert_text((72, 130), "Reglerne staar i den vedlagte vejledning fra styrelsen.", fontsize=11)
p.insert_text((72, 160), "Laes mere her.", fontsize=11)
sh = p.new_shape(); sh.draw_rect(pymupdf.Rect(60, 300, 500, 620)); sh.finish(fill=(0.2, 0.4, 0.8)); sh.commit()
p.insert_text((90, 340), "FIGUR: fordeling af sager 2024", fontsize=13, color=(1, 1, 1))
doc = pymupdf.open(stream=doc.tobytes(), filetype="pdf"); pg = doc[0]
pg.insert_link({"kind": pymupdf.LINK_URI, "uri": "https://placeholder/x", "from": pg.search_for("Laes mere her.")[0]})
doc = pymupdf.open(stream=doc.tobytes(), filetype="pdf")
doc.xref_set_key(doc[0].get_links()[0]["xref"], "A/URI", pymupdf.get_pdf_str(URL))
pdf = doc.tobytes(); SHA = hashlib.sha256(pdf).hexdigest()

st, r = call("POST", "/v1/ingest/file", {"feed": F, "uri": "vejl", "bytes_b64": base64.b64encode(pdf).decode(), "filename": "vejl.pdf"})
check("L.1 delivery accepted", st in (200, 202), f"{st} {r}")
t0 = time.time()
while time.time() - t0 < 300:
    st, r = call("POST", "/v1/ingest/ping", {"feed": F, "uri": "vejl", "sha256": SHA})
    if st != 202: break
    time.sleep(2)
check("L.2 parsed", st == 200, f"{st} {r}")
with psycopg.connect(DB) as c:
    row = c.execute("""SELECT ch.markdown, ch.meta FROM files_chunks fc JOIN chunks ch ON ch.sha256 = fc.chunk_sha256
                       WHERE fc.file_sha256 = %s ORDER BY fc.idx""", (SHA,)).fetchone()
md, meta = (row or ("", {}))
check("L.3 the page went to the VLM, not the text layer", meta.get("source") == "vlm" and meta.get("visual", 0) > 0, str(meta))
check("L.4 the real address is in the chunk", URL in md, md[:300])
check("L.5 no mark was left behind", "](L1)" not in md and "](L0)" not in md, md[:300])
check("L.6 the anchor text survived", "aes mere her" in md or "Læs mere her" in md, md[:300])
print("\nchunk markdown:\n" + md[:400])
failed = [n for n, ok in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} passed" + (f"; FAILED: {failed}" if failed else ""))
