"""Lexical layer end to end: normalization, vocabulary, fuzzy, regex, published_at, recency.
Runs INSIDE an api container: docker exec -i <container> python - < tests/test_search.py
"""
import base64
import hashlib
import io
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

import psycopg

API = os.environ.get("API", "http://localhost:8000")
DB = os.environ["DATABASE_URL"]
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
        with urllib.request.urlopen(req, timeout=180) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"null")
        except Exception:
            return e.code, None


def post(path, body):
    return call("POST", path, body)


def search(**params):
    st, r = call("GET", "/v1/search", params=params)
    assert st == 200, (st, r, params)
    return r


def lex(q, **extra):
    return search(q=q, weight_semantic=0, **extra)


def sha(b):
    return hashlib.sha256(b).hexdigest()


def b64(b):
    return base64.b64encode(b).decode()


def q(sql, *args):
    with psycopg.connect(DB) as c:
        return c.execute(sql, args).fetchall()


def q1(sql, *args):
    rows = q(sql, *args)
    return rows[0][0] if rows else None


def deliver(feed, uri, data, filename="", meta=None):
    return post("/v1/ingest/file", {"feed": feed, "uri": uri, "meta": meta or {}, "bytes_b64": b64(data), "filename": filename})


def wait(feed, uri, s, meta=None, timeout=180):
    t0 = time.time()
    while True:
        st, body = post("/v1/ingest/ping", {"feed": feed, "uri": uri, "sha256": s, "meta": meta or {}})
        if st != 202 or time.time() - t0 > timeout:
            return st, body
        time.sleep(0.5)


def norm(s):
    return q1("SELECT fatingest_norm(%s)", s)


def toks(s):
    return {r[0] for r in q("SELECT lexeme FROM unnest(to_tsvector('simple', fatingest_norm(%s)))", s)}


F = f"lex/{RUN}"
HEX = "ab12cd34ef56" * 5 + "ab12"
assert len(HEX) == 64

print("--- A. normalization rules (fatingest_norm)")
cases = [
    ("Sag KMD-1234", "Sag KMD1234 KMD 1234"),
    ("EMN-2024-001234", "EMN2024001234 EMN 2024 001234"),
    ("den 2026-09-03", "den 20260903 2026 09 03"),
    ("jf. § 12, stk. 2", "jf. paragraph12 12, stk. 2"),
    ("se §12 og § 12 a", "se paragraph12 12 og paragraph12a 12"),
    ("§ 15 a, stk. 2", "paragraph15a 15, stk. 2"),
    ("§ 12 i loven", "paragraph12 12 i loven"),
    ("§ 12 e-mail", "paragraph12 12 e-mail"),
    ("efter §§ 12-14", "efter paragraph12 1214 12 14"),
    ("pris 1.234,56 kr", "pris 1.234.56 123456 kr"),
    ("kl. 14:30", "kl. 14.30 1430"),
    ("version 2.1.3", "version 2.1.3 213"),
    ("saldo -500 kr og -3,5 grader", "saldo -500 500 kr og -3.5 3.5 35 grader"),   # glue, twin, then the sign rule
    ("e-mail til IT-Afdelingen", "e-mail til IT-Afdelingen"),
    ("a - b, - punkt", "a - b, - punkt"),
    (f"se sha256://{HEX} her", f"se  {HEX}  her"),
]
for src, expected in cases:
    got = norm(src)
    check(f"A norm({src!r})", got == expected, f"got {got!r}")
check("A tokens of '§ 12 a' include paragraph12a and 12", {"paragraph12a", "12"} <= toks("§ 12 a"), str(toks("§ 12 a")))
check("A stray hyphens are separators to the parser", toks("a - b, - punkt") == {"a", "b", "punkt"}, str(toks("a - b, - punkt")))
check("A '-500' keeps its sign as a token", {"-500", "500"} <= toks("saldo -500"), str(toks("saldo -500")))

print("--- B. surface/term pairs (fatingest_vocab)")
pairs = set(q("SELECT surface, term FROM fatingest_vocab(%s)", f"Sag KMD-1234 § 12 e-mail 1.234,56 -500 sha256://{HEX}"))
expected = {("kmd-1234", "kmd1234"), ("kmd-1234", "kmd"), ("kmd-1234", "1234"), ("§12", "paragraph12"), ("§12", "12"),
            ("1.234,56", "1.234.56"), ("1.234,56", "123456"), ("-500", "-500"), ("-500", "500"), (HEX, HEX),
            ("sag", "sag"), ("e-mail", "e-mail"), ("e", "e"), ("mail", "mail")}
check("B exactly the expected pairs", pairs == expected, f"missing {expected - pairs} extra {pairs - expected}")

print("--- C. identifiers: ingest, vocabulary, literal and partial search, regex on surfaces")
doc = f"Sagsnummer KMD-9876 jf. § 77 beløb 9.876,54 kr og CPR 010203-1234, saldo -250 kr. Token IDENT{RUN}.".encode()
st, r = deliver(F, "ident", doc, "ident.md")
st, r = wait(F, "ident", sha(doc))
check("C.0 parsed", st == 200, f"{st} {r}")
surfaces = {r[0] for r in q("SELECT surface FROM vocab WHERE surface IN ('kmd-9876','§77','9.876,54','010203-1234','-250','sagsnummer')")}
check("C.1 vocabulary holds the surfaces", surfaces == {"kmd-9876", "§77", "9.876,54", "010203-1234", "-250", "sagsnummer"}, str(surfaces))
def among(r, token):
    """This run's document is among the hits (earlier runs may hold the same identifiers)."""
    return any(token in h["snippet"] for h in r["hits"])


for query in ["KMD-9876", "kmd9876", "9876", "§77", "§ 77", "77", "9.876,54", "9,876.54", "9876.54", "010203-1234", "-250", "250"]:
    r = lex(query)
    check(f"C.2 q={query!r} finds the document", among(r, f"IDENT{RUN}") and all("lexical" in h["found_by"] for h in r["hits"]),
          f"{len(r['hits'])} hits, lexical_query={r['query']['lexical_query']!r}")
r_sign, r_bare = lex("-250"), lex("250")
check("C.3 the signed query scores the signed document higher than the bare query does",
      r_sign["hits"][0]["score"] >= r_bare["hits"][0]["score"], f"{r_sign['hits'][0]['score']} vs {r_bare['hits'][0]['score']}")
for rx in [r"^\d{6}-\d{4}$", r"^§\d+$", r"^kmd-\d{4}$", r"^9\.876,54$"]:
    r = lex(f"IDENT{RUN}", filter_regex=rx)
    check(f"C.4 filter_regex {rx!r} keeps the document", among(r, f"IDENT{RUN}") and r["query"]["regex_terms"] >= 1, str(r["query"]))
r = lex(f"IDENT{RUN}", filter_regex=r"^zzz\d{9}$")
check("C.5 a regex matching no surface filters everything out", len(r["hits"]) == 0 and r["query"]["regex_terms"] == 0, str(r["query"]))
r = search(q=f"IDENT{RUN}", filter_regex=r"^zzz\d{9}$")
check("C.5b ... also when the semantic ranking has candidates", len(r["hits"]) == 0 and r["query"]["regex_terms"] == 0, str(r["query"]))
st, r = call("GET", "/v1/search", params={"q": "x", "filter_regex": "(unclosed"})
check("C.6 invalid regex -> 422", st == 422, f"{st} {r}")
r = search(filter_regex=r"^kmd-9876$", weight_semantic=0)
check("C.7 regex without q is a ranked grep", among(r, f"IDENT{RUN}") and r["query"]["lexical_query"] != "", str(r["query"]))

print("--- D. fuzzy expansion through the vocabulary")
doc2 = f"Hardwarebestilling foregår via portalen. Token FUZZY{RUN}.".encode()
deliver(F, "fuzzy", doc2, "fuzzy.md")
st, r = wait(F, "fuzzy", sha(doc2))
r = lex("hardware")
fz = [h for h in r["hits"] if f"FUZZY{RUN}" in h["snippet"]]
check("D.1 'hardware' reaches 'hardwarebestilling' (compound) through the fuzzy ranking",
      fz and "hardwarebestilling" in r["query"]["fuzzy_terms"], str(r["query"]))
check("D.1b the exact query carries no expansion; the compound match is found by fuzzy alone",
      r["query"]["lexical_query"].strip() == "hardware" and fz and set(fz[0]["found_by"]) == {"fuzzy"}, str(fz and fz[0]["found_by"]))
r = lex("hardwarebestiling")
check("D.2 a typo is corrected", any(f"FUZZY{RUN}" in h["snippet"] for h in r["hits"]), str(r["query"]))
r = lex("9876")
check("D.3 numbers are not fuzzed: no fuzzy ranking at all", r["query"]["lexical_query"].strip() == "9876" and r["query"]["fuzzy_terms"] == []
      and r["query"]["weights"] == {"lexical": 1.0}, str(r["query"]))
doc3 = f"Ny hardware til alle. Token EXACT{RUN}.".encode()
deliver(F, "exact", doc3, "exact.md")
st, r = wait(F, "exact", sha(doc3))
r = lex("hardware")
uris = [h["sources"][0]["uri"] for h in r["hits"] if h["sources"]]
ex = [h for h in r["hits"] if f"EXACT{RUN}" in h["snippet"]]
fz = [h for h in r["hits"] if f"FUZZY{RUN}" in h["snippet"]]
check("D.4 the exact match ranks above the compound-only match", ex and fz and uris.index("exact") < uris.index("fuzzy"), str(uris[:5]))
check("D.5 found_by: the exact match in both lexical rankings, the compound match in fuzzy only",
      ex and set(ex[0]["found_by"]) == {"lexical", "fuzzy"} and fz and set(fz[0]["found_by"]) == {"fuzzy"},
      f"{ex and ex[0]['found_by']} {fz and fz[0]['found_by']}")
check("D.6 weights echo: weight_lexical shared by the two", r["query"]["weights"] == {"lexical": 0.5, "fuzzy": 0.5}, str(r["query"]["weights"]))

print("--- E. zero-score cut and weights")
r = lex(f"FUZZY{RUN}")
check("E.1 a unique token yields exactly one lexical hit (no padding)", len(r["hits"]) == 1 and r["hits"][0]["found_by"] == {"lexical": 1}, str([h["found_by"] for h in r["hits"]]))
check("E.2 weights echo normalized", r["query"]["weights"] == {"lexical": 1.0}, str(r["query"]["weights"]))
st, r = call("GET", "/v1/search", params={"q": "x", "weight_semantic": 0, "weight_lexical": 0})
check("E.3 no ranking weighted -> 422", st == 422)
st, r = call("GET", "/v1/search", params={"q": "x", "weight_recency": -1})
check("E.4 negative weight -> 422", st == 422)
st, r = call("GET", "/v1/search", params={})
check("E.5 neither q nor regex -> 422", st == 422)

print("--- F. who links to this file: sha256 refs are searchable")
b_md = f"# B\n\nTarget. Token LINKDST{RUN}.\n".encode()
a_md = f"# A\n\nSee [b](b.md). Token LINKSRC{RUN}.\n".encode()
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w") as z:
    z.writestr("a.md", a_md)
    z.writestr("b.md", b_md)
zb = buf.getvalue()
deliver(F, "links", zb, "links.zip")
st, r = wait(F, "links", sha(zb))
check("F.0 archive parsed", st == 200, f"{st} {r}")
a_text = q1("SELECT c.markdown FROM files_chunks fc JOIN chunks c ON c.sha256 = fc.chunk_sha256 WHERE fc.file_sha256 = %s", sha(a_md)) or ""
check("F.1 the link was rewritten to a content address", f"sha256://{sha(b_md)}" in a_text, a_text[:120])
for query in [sha(b_md), f"sha256://{sha(b_md)}"]:
    r = lex(query)
    check(f"F.2 q={query[:20]}… finds the linking chunk", len(r["hits"]) == 1 and r["hits"][0]["sources"][0]["file"] == sha(a_md), str(len(r["hits"])))
r = lex(f"LINKSRC{RUN}", filter_regex=r"^[0-9a-f]{64}$")
check("F.3 regex on the vocabulary lists referenced files", among(r, f"LINKSRC{RUN}") and r["query"]["regex_terms"] >= 1, str(r["query"]))

print("--- G. published_at: reserved meta key, ping compares it, recency ranks by it")
st, r = post("/v1/ingest/chunks", {"feed": F, "uri": "bad", "meta": {"published_at": "yesterday"}, "chunks": [{"markdown": "x"}]})
check("G.1 invalid published_at -> 422", st == 422, f"{st} {r}")
tok = f"RECENCY{RUN}"
d = {}
for uri, meta in [("old", {"published_at": "2020-01-01"}), ("mid", {"published_at": "2026-01-01T09:00:00+01:00"}), ("new", {})]:
    st, r = post("/v1/ingest/chunks", {"feed": F, "uri": uri, "meta": meta, "chunks": [{"markdown": f"Token {tok} {uri}."}]})
    assert st == 200, (st, r)
    d[uri] = r["sha256"]
st, r = post("/v1/ingest/ping", {"feed": F, "uri": "old", "sha256": d["old"], "meta": {}})
check("G.2 ping without the date -> 404 meta differs", st == 404 and "meta" in r["detail"], f"{st} {r}")
st, r = post("/v1/ingest/ping", {"feed": F, "uri": "old", "sha256": d["old"], "meta": {"published_at": "2020-01-01"}})
check("G.3 ping with the date -> 200", st == 200, f"{st} {r}")
r = lex(tok)
check("G.4 without weight_recency: three hits, no dates reported", len(r["hits"]) == 3 and all("published_at" not in h for h in r["hits"]), str(r["hits"]))
r = lex(tok, weight_recency=10)
order = [h["sources"][0]["uri"] for h in r["hits"]]
check("G.5 with weight_recency: newest first - created_at fallback, then 2026, then 2020",
      order == ["new", "mid", "old"] and [h["found_by"]["recency"] for h in r["hits"]] == [1, 2, 3], str(order))
check("G.6 effective dates reported", r["hits"][2]["published_at"].startswith("2020-01-01") and r["hits"][1]["published_at"].startswith("2026-01-01T08:00:00"), str([h.get("published_at") for h in r["hits"]]))
check("G.7 weights echo: recency and lexical normalized", r["query"]["weights"] == {"lexical": round(1 / 11, 4), "recency": round(10 / 11, 4)}, str(r["query"]["weights"]))
st, r2 = post("/v1/ingest/chunks", {"feed": F + "/other", "uri": "mid-copy", "meta": {"published_at": "2015-06-01"}, "chunks": [{"markdown": f"Token {tok} mid."}]})
check("G.8 same content claimed again with an older date -> same file", st == 200 and r2["sha256"] == d["mid"], f"{st} {r2}")
r = lex(tok, weight_recency=10)
order = [h["sources"][0]["uri"] for h in r["hits"]]
check("G.9 the oldest date among the claims wins: 2015 drops it below 2020", order[0] == "new" and order[1] == "old" and set(order[2:]) == {"mid", "mid-copy"} or order == ["new", "old", "mid"], str(order))
for uri in ("tie1", "tie2"):
    post("/v1/ingest/chunks", {"feed": F, "uri": uri, "meta": {"published_at": "2019-05-05"}, "chunks": [{"markdown": f"Token {tok} {uri}."}]})
r = lex(tok, weight_recency=10)
ranks = {h["sources"][0]["uri"]: h["found_by"]["recency"] for h in r["hits"]}
check("G.10 equal dates share a recency rank", ranks.get("tie1") == ranks.get("tie2") and ranks["tie1"] is not None, str(ranks))

print("--- H. semantic ranking joins once embeddings exist")
for _ in range(60):
    st, h = call("GET", "/health")
    if h["chunks_pending_embed"] == 0:
        break
    time.sleep(2)
r = search(q="ordering hardware through the portal")
check("H.1 default weights: semantic and lexical both contribute", r["hits"] and any("semantic" in h["found_by"] for h in r["hits"]), str([h["found_by"] for h in r["hits"][:3]]))
r = search(q="hardware ordering portal", weight_lexical=0)
check("H.2 semantic only: hits without lexical ranks", r["hits"] and all(set(h["found_by"]) == {"semantic"} for h in r["hits"]), str([h["found_by"] for h in r["hits"][:3]]))

failed = [n for n, ok in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} passed" + (f"; FAILED: {failed}" if failed else ""))
sys.exit(1 if failed else 0)
