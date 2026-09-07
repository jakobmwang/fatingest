"""fatingest ingest API.

This is an EPHEMERAL THIN INDEX, not a source of truth: content that is not re-delivered
by its feeds within the GC window disappears. Raw file bytes are never retained beyond the
parse spool (held only until parsed); the system keeps exactly what retrieval needs -
markdown in the database, renders in a content-addressed file store (renders/ab/cd/<sha>).

Endpoints (JSON bodies, see models.py):
  POST /v1/ingest/ping    {feed, uri, sha256, meta?}         -> 200 known | 202 queued | 404 deliver
  POST /v1/ingest/file    {feed, uri, meta?, bytes_b64, filename?}
  POST /v1/ingest/chunks  {feed, uri, meta?, chunks: [{markdown?, render_b64?}]}
  POST /v1/ingest/delete  {feed, uri}
  GET  /v1/search  q, weight_semantic, weight_lexical, weight_recency, filter_feeds, filter_regex, limit
  POST /v1/gc      GET /health

Deliveries answer 202 {status: queued} when parsing or text inference is pending and
200 {status: ingested} when the content is complete on arrival. The client protocol is
the ping: 200 done, 202 wait, 404 deliver. A client that pings until it stops seeing 202
knows its content is fully indexed. Anything that goes wrong while parsing - a dependency
outage (Gotenberg, VLM) or an internal error - is retried here with exponential backoff
while the delivery keeps answering 202; only what no retry can fix (the delivered bytes are
gone: spool or render missing) is recorded as parse_failed, answers 404, and is retried
when the feed re-delivers.

Write model: content rows (files, chunks) are immutable and content-addressed. A claim
(items) names exactly one file - the delivered sha. The relationship rows are replaced as
a set - archive_members per archive when it is parsed, files_chunks per file - and GC
sweeps whatever nothing references any more. A ping never writes anything but touched_at.

Identities:
  file (file delivery):   sha256(bytes)
  file (chunks delivery): sha256(canonical JSON of the delivered halves, PACK_VERSION)
  chunk:                  sha256 over the DELIVERED halves - render sha and/or markdown sha.
                          When fatingest generated the text (VLM transcription of a render),
                          the markdown sha is replaced by PARSER_VERSION: generated content
                          never enters an identity, the recipe that generated it does.
                          chunks.meta (status, error, visual, text_chars) is derived and
                          never part of any identity either.

Because a generated chunk's identity is known before the VLM is called, transcriptions are
memoized in the chunks table as they finish (ChunkCache): a delivery deferred halfway
through a long document resumes with the pages it lacks, and a page seen in another
delivery under the same recipe is never transcribed twice.

Background threads: PARSE_WORKERS identical parse workers each take the oldest due, unlocked
row of the queue `files.parsed_at IS NULL AND due_at <= now()` (SKIP LOCKED: the row lock is
the claim, so there is no dispatcher and no in-process bookkeeping; due_at carries the retry
backoff); the embed worker turns chunks without an embedding into (delivered render |
synthetic markdown render) + markdown -> embedding.
"""
import base64
import binascii
import contextlib
import json
import os
import threading
import time

import psycopg
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from models import ChunksDelivery, Delete, FileDelivery, Ping
from routing import PARSER_VERSION, RetryLater, UnparseableError, image_item, parse_file, transcribe_pages
from store import (STORE, embed, is_image, load_render, render_path, sha, spool_delete,
                   spool_read, spool_write, store_render, synth_render)
from unpack import to_members

DB = os.environ["DATABASE_URL"]
PACK_VERSION = 1  # canonical file format for chunks deliveries
PARSE_WORKERS = max(1, int(os.environ.get("PARSE_WORKERS", "2")))
RETRY_BASE = 60     # seconds; a deferred delivery waits RETRY_BASE * 2**(attempts-1) ...
RETRY_CAP = 3600    # ... capped here. Dependency failures are retried indefinitely.

app = FastAPI(title="fatingest", version="0.2")


def _log(evt: str, **fields):
    print(json.dumps({"evt": evt, **fields}), flush=True)


class Unrecoverable(Exception):
    """What no retry of ours can fix: the delivered bytes are gone (spool entry or delivered
    render missing). The only cure is a re-delivery, so the file is settled as parse_failed
    and the feed's ping answers 404. Every other exception is retried with backoff."""


class ChunkCache:
    """The transcription memo of pdf_engine.transcribe_pages, keyed by chunk identity
    (render sha + PARSER_VERSION, see chunk_id). get() runs on the parse worker's own
    cursor with FOR KEY SHARE: GC deletes chunks with FOR UPDATE SKIP LOCKED, so a row the
    worker is about to reference (and its render on disk) is left alone until the
    collection is committed. put() runs on a connection of its own, autocommitted, so a
    finished page survives a deferred delivery; such a row is an orphan until commit_file
    references it and GC may sweep it meanwhile - which only costs that transcription once
    more, because commit_file re-inserts every chunk from memory."""

    def __init__(self, cur):
        self.cur = cur

    def get(self, render: bytes) -> tuple[str, dict] | None:
        self.cur.execute("SELECT markdown, meta FROM chunks WHERE sha256 = %s FOR KEY SHARE",
                         (chunk_id(None, sha(render), True),))
        row = self.cur.fetchone()
        return (row[0], row[1]) if row else None

    def put(self, render: bytes, markdown: str, meta: dict):
        r_sha = store_render(render)
        with psycopg.connect(DB, autocommit=True) as conn:
            conn.execute("""INSERT INTO chunks(sha256, markdown, render_sha256, meta) VALUES (%s, %s, %s, %s)
                            ON CONFLICT DO NOTHING""",
                         (chunk_id(None, r_sha, True), markdown, r_sha, json.dumps(meta)))


# --- identities -------------------------------------------------------------------------

def chunk_id(markdown: str | None, render_sha: str | None, generated: bool) -> str:
    """Delivered halves only. `generated` marks text produced by fatingest (VLM), which is
    replaced by PARSER_VERSION so that VLM variance can never mint a new identity while
    a deliberate parser change does."""
    md_part = "" if generated else sha(markdown.encode())
    recipe = PARSER_VERSION if generated else ""
    return sha(f"{render_sha or ''}\x00{md_part}\x00{recipe}".encode())


def pack_sha(chunks: list[dict]) -> str:
    """Deterministic file identity for a chunks delivery: the delivered halves, in order."""
    canon = json.dumps({"v": PACK_VERSION, "chunks": [
        {"render_sha256": c["render_sha256"],
         "markdown_sha256": None if c["generated"] else sha(c["markdown"].encode())}
        for c in chunks]}, sort_keys=True, separators=(",", ":")).encode()
    return sha(canon)


# --- shared write steps -----------------------------------------------------------------

def commit_file(cur, file_sha: str, meta: dict, chunks: list[dict] | None) -> int:
    """One file and its complete chunk collection, in one transaction. Chunks: {markdown,
    render_sha256, generated, meta?}. Every chunk row is (re)inserted from memory - a row
    that already exists is left untouched (immutable), one that GC swept meanwhile comes
    back - so the collection is always complete or the whole file fails. The collection is
    replaced as a set."""
    cur.execute("""INSERT INTO files(sha256, meta, parsed_at) VALUES (%s, %s, now())
                   ON CONFLICT (sha256) DO UPDATE
                   SET meta = EXCLUDED.meta, parsed_at = now(), indexed_at = NULL""",
                (file_sha, json.dumps(meta)))
    cur.execute("DELETE FROM files_chunks WHERE file_sha256 = %s", (file_sha,))
    for i, ch in enumerate(chunks or []):
        cid = chunk_id(ch["markdown"], ch["render_sha256"], ch["generated"])
        cur.execute("""INSERT INTO chunks(sha256, markdown, render_sha256, meta) VALUES (%s, %s, %s, %s)
                       ON CONFLICT DO NOTHING""",
                    (cid, ch["markdown"], ch["render_sha256"], json.dumps(ch.get("meta") or {"status": "ok"})))
        cur.execute("INSERT INTO files_chunks(file_sha256, idx, chunk_sha256) VALUES (%s, %s, %s)",
                    (file_sha, i, cid))
    return len(chunks or [])


def commit_item(cur, feed: str, uri: str, meta: dict, file_sha: str):
    """The feed's claim: meta as delivered (absent = empty), content = this one file."""
    cur.execute("""INSERT INTO items(feed, uri, sha256, meta) VALUES (%s, %s, %s, %s)
                   ON CONFLICT (feed, uri) DO UPDATE
                   SET sha256 = EXCLUDED.sha256, meta = EXCLUDED.meta, touched_at = now()""",
                (feed, uri, file_sha, json.dumps(meta)))


def enqueue(cur, file_sha: str, meta: dict):
    """A pending file row IS the queue entry (files.parsed_at IS NULL) and due_at is its
    position. Re-queuing a failed or stale file moves it to the back of the line."""
    cur.execute("""INSERT INTO files(sha256, meta) VALUES (%s, %s)
                   ON CONFLICT (sha256) DO UPDATE
                   SET meta = EXCLUDED.meta, parsed_at = NULL, indexed_at = NULL, due_at = now()""",
                (file_sha, json.dumps({"kind": "pending", **meta})))


def _retry_now(cur, file_sha: str):
    """A re-delivery of a deferred (backed-off) delivery pulls it forward to now. Never waits
    on a worker: a row being parsed is skipped and left alone."""
    cur.execute("""SELECT 1 FROM files WHERE sha256 = %s AND parsed_at IS NULL AND due_at > now()
                   FOR NO KEY UPDATE SKIP LOCKED""", (file_sha,))
    if cur.fetchone():
        cur.execute("UPDATE files SET due_at = now() WHERE sha256 = %s", (file_sha,))


def file_state(cur, s: str) -> str:
    """'complete' = full possession (parsed with the CURRENT parser version, all chunks, every
    delivered render on disk; unparseable files possess nothing and hold it); 'queued' =
    claimed and waiting for the parse worker; 'missing' = anything else, including a failed
    parse (claimed but not held) and a file parsed under another parser version."""
    cur.execute("SELECT meta, parsed_at FROM files WHERE sha256 = %s", (s,))
    row = cur.fetchone()
    if not row:
        return "missing"
    meta, parsed_at = row[0] or {}, row[1]
    kind = meta.get("kind")
    if parsed_at is None or kind == "pending":
        return "queued"
    if meta.get("parser_version") not in (None, PARSER_VERSION):
        return "missing"
    if kind == "unparseable":
        return "complete"
    if kind == "parse_failed":
        return "missing"
    if kind == "archive":   # an archive holds exactly what its members hold
        cur.execute("SELECT member_sha256 FROM archive_members WHERE archive_sha256 = %s", (s,))
        states = {file_state(cur, m) for (m,) in cur.fetchall()}
        if not states:
            return "missing"
        return "missing" if "missing" in states else "queued" if "queued" in states else "complete"
    cur.execute("""SELECT c.render_sha256 FROM files_chunks fc
                   JOIN chunks c ON c.sha256 = fc.chunk_sha256
                   WHERE fc.file_sha256 = %s""", (s,))
    rows = cur.fetchall()
    ok = bool(rows) and all(not r or os.path.exists(render_path(r)) for (r,) in rows)
    return "complete" if ok else "missing"


def _b64(value: str, field: str) -> bytes:
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        raise HTTPException(422, f"{field} is not valid base64")


def _queued(file_sha: str):
    return JSONResponse({"status": "queued", "sha256": file_sha}, status_code=202)


# --- authorization: authentication is the reverse proxy's job; fatingest only ---
# --- authorizes the PRINCIPAL the proxy hands it. The proxy MUST set/override  ---
# --- the principal header itself - never pass it through from clients.        ---
PRINCIPAL_HEADER = os.environ.get("PRINCIPAL_HEADER", "X-Key-Alias")
WRITE_PRINCIPALS = {p.strip() for p in os.environ.get("WRITE_PRINCIPALS", "").split(",")
                    if p.strip()}
READ_PRINCIPALS = {p.strip() for p in os.environ.get("READ_PRINCIPALS", "").split(",")
                   if p.strip()}


def _require(request: Request, allowed: set[str], action: str):
    """Empty allowlist = open (trusted network). /health is never gated - liveness
    must stay reachable for container healthchecks."""
    if allowed and request.headers.get(PRINCIPAL_HEADER, "") not in allowed:
        raise HTTPException(403, f"principal is not allowed to {action}")


def require_write(request: Request):
    _require(request, WRITE_PRINCIPALS, "write")


def require_read(request: Request):
    _require(request, READ_PRINCIPALS, "read")


# --- endpoints --------------------------------------------------------------------------

@app.get("/health")
def health():
    """Liveness + readiness: pings the only hard dependency (the database) and reports
    the parse queue, the embed queue and the retry backlog. Deliberately never fans out
    to VLM/embedding services - their health is their own containers' concern."""
    try:
        with psycopg.connect(DB, connect_timeout=3) as conn, conn.cursor() as cur:
            cur.execute("""SELECT count(*) FILTER (WHERE parsed_at IS NULL),
                                  count(*) FILTER (WHERE parsed_at IS NULL AND due_at > now()),
                                  count(*) FILTER (WHERE meta->>'kind' = 'parse_failed'),
                                  count(*) FILTER (WHERE meta->>'kind' = 'unparseable')
                           FROM files""")
            queued, retrying, failed, unparseable = cur.fetchone()
            cur.execute("SELECT count(*) FROM chunks WHERE embedding IS NULL AND meta->>'status' = 'ok'")
            pending = cur.fetchone()[0]
    except Exception:
        return JSONResponse({"ok": False, "db": False}, status_code=503)
    return {"ok": True, "db": True, "files_queued": queued, "files_retrying": retrying,
            "chunks_pending_embed": pending, "files_failed": failed,
            "files_unparseable": unparseable}


@app.post("/v1/ingest/ping")
def ingest_ping(body: Ping, request: Request):
    """Pure verification of this feed's claim. 200 promises COMPLETE possession: the claim
    exists with exactly this meta, carries this sha256, and the content is fully present
    (current parser version, all chunks, every delivered render on disk). 202 means the
    content is queued for parsing - keep pinging. Anything else is 404 and the feed
    delivers; that re-delivery loop is the only healing and retry mechanism (missing
    embeddings are not checked: the embed worker heals those internally)."""
    require_write(request)
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT meta, sha256 FROM items WHERE feed = %s AND uri = %s",
                    (body.feed, body.uri))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "unknown item - deliver it")
        if row[0] != body.meta:
            raise HTTPException(404, "meta differs - deliver it")
        if row[1] != body.sha256:
            raise HTTPException(404, "item carries other content - deliver it")
        state = file_state(cur, body.sha256)
        if state == "missing":
            raise HTTPException(404, "content incomplete - deliver it")
        cur.execute("UPDATE items SET touched_at = now() WHERE feed = %s AND uri = %s",
                    (body.feed, body.uri))
    if state == "queued":
        return _queued(body.sha256)
    return {"status": "known", "sha256": body.sha256}


@app.post("/v1/ingest/file")
def ingest_file(body: FileDelivery, request: Request):
    """Bytes in. Content already complete is claimed on the spot (200). Anything else is
    spooled and queued (202): the parse worker routes it by type (see routing.py) and
    unpacks archives into members, each its own file, recorded in archive_members. The
    claim names the delivered sha either way, so nothing is unpacked on this path. Bytes
    never outlive the parse."""
    require_write(request)
    data = _b64(body.bytes_b64, "bytes_b64")
    file_sha = sha(data)
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        state = file_state(cur, file_sha)
        if state == "missing":
            spool_write(file_sha, data)
            enqueue(cur, file_sha, {"source": "file", "filename": body.filename})
            state = "queued"
        elif state == "queued":
            _retry_now(cur, file_sha)
        commit_item(cur, body.feed, body.uri, body.meta, file_sha)
    if state == "queued":
        return _queued(file_sha)
    return {"status": "ingested", "sha256": file_sha}


def _prepare_chunks(body: ChunksDelivery) -> list[dict]:
    prepared = []
    for c in body.chunks:
        render = _b64(c.render_b64, "render_b64") if c.render_b64 is not None else None
        if render is not None and not is_image(render):
            raise HTTPException(422, "render_b64 must be a decodable image")
        prepared.append({"markdown": c.markdown,
                         "render_sha256": store_render(render) if render is not None else None,
                         "generated": c.markdown is None})
    return prepared


def _needs_inference(cur, prepared: list[dict]) -> bool:
    for p in prepared:
        if p["generated"]:
            cur.execute("SELECT 1 FROM chunks WHERE sha256 = %s",
                        (chunk_id(None, p["render_sha256"], True),))
            if not cur.fetchone():
                return True
    return False


def _chunkset_meta(prepared: list[dict]) -> dict:
    meta = {"kind": "chunkset", "n_chunks": len(prepared)}
    if any(p["generated"] for p in prepared):
        meta["parser_version"] = PARSER_VERSION   # generated text follows the parser recipe
    return meta


@app.post("/v1/ingest/chunks")
def ingest_chunks(body: ChunksDelivery, request: Request):
    """Pre-chunked content: the integration decides chunk boundaries, render and text.
    A missing half is generated once, when the chunk is new: text from a render via the
    VLM (image prompt) - queued, 202 - and a render from text synthetically at embedding
    time (never stored). Fully delivered content commits on the spot (200). Delivered
    halves define identity; a different markdown for the same render is a different chunk,
    and the previous one is garbage-collected once nothing refers to it."""
    require_write(request)
    prepared = _prepare_chunks(body)
    file_sha = pack_sha(prepared)
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        state = file_state(cur, file_sha)
        if state == "queued":
            _retry_now(cur, file_sha)
        elif state == "complete":
            pass                                   # same halves, same rows: nothing to write
        elif _needs_inference(cur, prepared):
            spool_write(file_sha, json.dumps({"chunks": prepared}).encode())
            enqueue(cur, file_sha, {"source": "chunks"})
            state = "queued"
        else:
            commit_file(cur, file_sha, _chunkset_meta(prepared), prepared)
            state = "complete"
        commit_item(cur, body.feed, body.uri, body.meta, file_sha)
    if state == "queued":
        return _queued(file_sha)
    return {"status": "ingested", "sha256": file_sha}


@app.post("/v1/ingest/delete")
def ingest_delete(body: Delete, request: Request):
    """Withdraws this feed's claim immediately. Content that no other claim references is
    swept by the next /v1/gc. Idempotent."""
    require_write(request)
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM items WHERE feed = %s AND uri = %s", (body.feed, body.uri))
        existed = cur.rowcount > 0
    return {"status": "deleted", "feed": body.feed, "uri": body.uri, "existed": existed}


SEARCH_DEPTH = 200       # candidates per ranking; bounds how deep a heavily filtered search can reach
RRF_K = 60
FUZZY_EXPANSIONS = 3     # vocabulary terms added per query word (typos, compounds)
REGEX_TERMS_MAX = 200    # vocabulary terms a filter_regex may select; beyond that it is too broad
BM25_INDEX = "idx_chunks_markdown_bm25"


def _feed_filter(feeds: str | None) -> tuple[list[str], list[str]] | None:
    """SELECTION by feed. Feed names are a hierarchy, and every entry always selects
    the feed itself AND its whole subtree: 'intranet' matches 'intranet' and
    'intranet/it/hardware', while the '/' boundary guarantees 'intranet/it' never
    matches 'intranet/it-support'. Trailing slashes are trimmed both at ingest and
    here, so they never matter. Authorization - deciding WHICH feeds a given user
    may search - is the calling client's job. Returns (exact names, LIKE patterns)."""
    if not feeds:
        return None
    exact, patterns = [], []
    for entry in (f.strip().rstrip("/") for f in feeds.split(",")):
        if not entry:
            continue
        exact.append(entry)
        escaped = entry.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        patterns.append(escaped + "/%")
    return (exact, patterns) if exact else None


def _regex_terms(cur, regex: str) -> list[str]:
    """Vocabulary terms whose SURFACE matches the regex (case-insensitive, trigram-indexed):
    the regex runs against what readers see in documents, never against internal tokens."""
    try:
        cur.execute("SELECT DISTINCT term FROM vocab WHERE surface ~* %s LIMIT %s",
                    (regex, REGEX_TERMS_MAX + 1))
    except psycopg.errors.InvalidRegularExpression as e:
        raise HTTPException(422, f"filter_regex: {str(e).splitlines()[0]}")
    terms = [r[0] for r in cur.fetchall()]
    if len(terms) > REGEX_TERMS_MAX:
        raise HTTPException(422, f"filter_regex selects more than {REGEX_TERMS_MAX} vocabulary terms - narrow it")
    return terms


def _exact_query(cur, q: str, regex_terms: list[str] | None) -> str:
    """The exact BM25 query: q normalized exactly like the index - the same terms a document
    produces, nothing added. When there is no q, the regex terms are the query. Bag of
    words: no syntax exists."""
    if q:
        cur.execute("SELECT fatingest_norm(%s)", (q,))
        return cur.fetchone()[0]
    return " ".join(regex_terms or [])


def _fuzzy_terms(cur, q: str) -> list[str]:
    """The nearest vocabulary surfaces of each WORD of q (typos, compounds), translated to
    index terms, for the fuzzy BM25 query. Only purely alphabetic surfaces of four letters
    or more are expanded: identifiers, numbers and codes are exact by nature (KMD-9876 must
    never reach KMD-9877)."""
    terms: list[str] = []
    cur.execute("""SELECT DISTINCT surface FROM fatingest_vocab(%s)
                   WHERE surface ~ '^[[:alpha:]]+$' AND length(surface) >= 4""", (q,))
    for (s,) in cur.fetchall():
        cur.execute("""SELECT term FROM (SELECT surface, term FROM vocab
                                         WHERE %s <%% surface AND surface <> %s
                                         ORDER BY surface <<-> %s, surface <-> %s, surface
                                         LIMIT %s) v""", (s, s, s, s, FUZZY_EXPANSIONS))
        terms.extend(r[0] for r in cur.fetchall())
    return list(dict.fromkeys(terms))


def _keep(cur, cands: list[str], queries: dict[str, str], regex_terms: list[str] | None,
          feeds: tuple[list[str], list[str]] | None) -> dict[str, dict[str, float]]:
    """Applies the filters to the candidates in one query. Returns, per surviving chunk, its
    standalone BM25 score against each lexical query ({ranking name: score}): 0 means none
    of that query's terms occur, i.e. the ranked scan returned it as padding and it leaves
    that ranking. A regex that selected no vocabulary term selects no chunk either -
    whatever the other rankings found."""
    if not cands or regex_terms == []:
        return {}
    exact, patterns = feeds if feeds else (None, None)
    names = list(queries)
    scores = ", ".join(f"""CASE WHEN %({n})s = '' THEN 0
                            ELSE fatingest_norm(c.markdown) <@> to_bm25query(%({n})s, %(idx)s) END"""
                       for n in names) or "0"
    cur.execute(f"""
        SELECT c.sha256, {scores}
        FROM chunks c
        WHERE c.sha256 = ANY(%(cands)s)
          AND (%(exact)s::text[] IS NULL OR EXISTS (
                 SELECT 1 FROM files_chunks fc JOIN file_claims cl ON cl.sha256 = fc.file_sha256
                 WHERE fc.chunk_sha256 = c.sha256
                   AND (cl.feed = ANY(%(exact)s) OR cl.feed LIKE ANY(%(patterns)s))))
          AND (%(rx)s = '' OR (fatingest_norm(c.markdown) <@> to_bm25query(%(rx)s, %(idx)s)) <> 0)""",
        {**queries, "idx": BM25_INDEX, "cands": cands, "exact": exact,
         "patterns": patterns, "rx": " ".join(regex_terms) if regex_terms else ""})
    return {row[0]: dict(zip(names, row[1:])) for row in cur.fetchall()}


@app.get("/v1/search")
def search(request: Request, q: str = "", weight_semantic: float = 1.0, weight_lexical: float = 1.0,
           weight_recency: float = 0.0, filter_feeds: str | None = None,
           filter_regex: str | None = None, limit: int = 10):
    """One query, up to four rankings, one fused list.

    - semantic: q embedded like the index (synthetic render + text), nearest chunks
    - lexical:  q normalized like the index, ranked by BM25 - exact terms only
    - fuzzy:    the same plus the vocabulary's nearest surfaces of each word (typos,
                compounds), ranked by BM25; runs only when there is something to add.
                weight_lexical is shared equally by the two, so a chunk that matches
                exactly appears in both and is lifted, one reached only through an
                expansion counts half. Bag of words: quotes, minus, AND/OR mean nothing
    - recency:  the candidates by date - oldest published_at among a chunk's claims, else
                its file's created_at - newest first, ties sharing a rank; only if weighted

    The weights are relative (normalized to sum 1) and fuse the rankings with RRF: a hit
    found by one ranking still ranks, agreement lifts it. Filters narrow: filter_feeds
    selects feed subtrees, filter_regex keeps chunks holding a vocabulary term whose
    surface matches, and stands in for q when q is empty. Every hit reports found_by
    (ranking -> rank) so an agent can see what worked, and the response echoes the exact
    lexical query and the fuzzy terms added to it.
    """
    require_read(request)
    q = q.strip()
    if min(weight_semantic, weight_lexical, weight_recency) < 0 or weight_semantic + weight_lexical <= 0:
        raise HTTPException(422, "weights must be >= 0, with weight_semantic or weight_lexical > 0")
    if not q and not filter_regex:
        raise HTTPException(422, "provide q and/or filter_regex")
    limit = max(1, min(limit, SEARCH_DEPTH))
    feeds = _feed_filter(filter_feeds)
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        regex_terms = _regex_terms(cur, filter_regex) if filter_regex else None
        lexical_query = _exact_query(cur, q, regex_terms) if regex_terms is None or regex_terms else ""
        fuzzy_terms = _fuzzy_terms(cur, q) if q and weight_lexical and lexical_query else []
        queries = {"lexical": lexical_query} if weight_lexical and lexical_query else {}
        if fuzzy_terms:
            queries["fuzzy"] = lexical_query + " " + " ".join(fuzzy_terms)
        ranked: dict[str, list[str]] = {}
        for name, query in queries.items():
            # Rank first, filter after: a predicate here could push the planner off the index
            # scan. Naming the index keeps <@> valid even if it does choose a sequential plan.
            cur.execute("""SELECT sha256 FROM chunks
                           ORDER BY fatingest_norm(markdown) <@> to_bm25query(%s, %s) LIMIT %s""",
                        (query, BM25_INDEX, SEARCH_DEPTH))
            ranked[name] = [r[0] for r in cur.fetchall()]
        if weight_semantic and q:
            qvec = embed(q, synth_render(q))  # same input shape as the index
            cur.execute("""SELECT sha256 FROM chunks WHERE embedding IS NOT NULL
                           ORDER BY embedding <=> %s::vector LIMIT %s""", (json.dumps(qvec), SEARCH_DEPTH))
            ranked["semantic"] = [r[0] for r in cur.fetchall()]
        cands = list(dict.fromkeys(cid for lst in ranked.values() for cid in lst))
        kept = _keep(cur, cands, queries, regex_terms, feeds)
        rankings: dict[str, list[tuple[str, int]]] = {
            name: [(cid, rank) for rank, cid in enumerate(
                (c for c in lst if c in kept and kept[c].get(name, 1) != 0), 1)]
            for name, lst in ranked.items()}
        # weight_lexical is shared by the exact and the fuzzy ranking when both run
        lexical_share = 0.5 if "fuzzy" in rankings else 1.0
        weights = {"semantic": weight_semantic, "lexical": weight_lexical * lexical_share,
                   "fuzzy": weight_lexical * 0.5, "recency": weight_recency}
        # Recency ranks only what a real ranking found: a chunk the lexical scan returned as
        # padding (score 0) must not earn points for merely being new.
        found = list({cid for lst in rankings.values() for cid, _ in lst})
        dates: dict[str, object] = {}
        if weight_recency and found:
            cur.execute("""SELECT fc.chunk_sha256, COALESCE(min(cl.published_at), min(f.created_at))
                           FROM files_chunks fc JOIN files f ON f.sha256 = fc.file_sha256
                           LEFT JOIN file_claims cl ON cl.sha256 = fc.file_sha256
                           WHERE fc.chunk_sha256 = ANY(%s) GROUP BY 1""", (found,))
            dates = dict(cur.fetchall())
            recency, rank, prev = [], 0, None
            for cid, d in sorted(dates.items(), key=lambda x: x[1], reverse=True):
                if d != prev:
                    rank, prev = rank + 1, d
                recency.append((cid, rank))
            rankings["recency"] = recency
        total = sum(weights[name] for name in rankings) or 1.0
        score: dict[str, float] = {}
        found_by: dict[str, dict[str, int]] = {}
        for name, lst in rankings.items():
            w = weights[name] / total
            for cid, rank in lst:
                score[cid] = score.get(cid, 0.0) + w / (RRF_K + rank)
                found_by.setdefault(cid, {})[name] = rank
        hits = []
        for cid, s in sorted(score.items(), key=lambda x: -x[1])[:limit]:
            cur.execute("""
                SELECT c.markdown, c.meta,
                       COALESCE(jsonb_agg(DISTINCT jsonb_build_object(
                           'feed', cl.feed, 'uri', cl.uri, 'file', fc.file_sha256, 'idx', fc.idx))
                           FILTER (WHERE cl.uri IS NOT NULL), '[]')
                FROM chunks c
                JOIN files_chunks fc ON fc.chunk_sha256 = c.sha256
                LEFT JOIN file_claims cl ON cl.sha256 = fc.file_sha256
                WHERE c.sha256 = %s GROUP BY c.markdown, c.meta""", (cid,))
            md, cmeta, srcs = cur.fetchone()
            hit = {"chunk": cid, "score": round(s, 5), "found_by": found_by[cid],
                   "snippet": md[:200], "meta": cmeta, "sources": srcs}
            if cid in dates:
                hit["published_at"] = dates[cid].isoformat()
            hits.append(hit)
        return {"query": {"q": q, "weights": {n: round(weights[n] / total, 4) for n in rankings},
                          "filter_feeds": filter_feeds, "filter_regex": filter_regex,
                          "lexical_query": lexical_query, "fuzzy_terms": fuzzy_terms,
                          "regex_terms": None if regex_terms is None else len(regex_terms)},
                "hits": hits}


@app.post("/v1/gc")
def gc(request: Request, stale_days: int = 14):
    """Referential sweep: database (fatingest_gc), then the render store, then the spool.
    Renders that a queued chunks delivery still needs are live. The cadence is policy (the
    deletion SLA for sources that vanish silently); the counters are the audit trail."""
    require_write(request)
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT fatingest_gc(make_interval(days => %s))", (stale_days,))
        result = cur.fetchone()[0]
        cur.execute("SELECT render_sha256 FROM chunks WHERE render_sha256 IS NOT NULL")
        live = {r[0] for r in cur.fetchall()}
        cur.execute("SELECT sha256, meta->>'source' FROM files WHERE parsed_at IS NULL")
        queued = dict(cur.fetchall())
    for s, source in queued.items():
        if source == "chunks" and (raw := spool_read(s)):
            live |= {c["render_sha256"] for c in json.loads(raw)["chunks"] if c["render_sha256"]}
    swept = 0
    for dirpath, _dirs, names in os.walk(os.path.join(STORE, "renders")):
        for name in names:
            if name.endswith(".tmp") or name not in live:
                os.remove(os.path.join(dirpath, name))
                swept += 1
    result["renders"] = swept
    spooled = 0
    spool_root = os.path.join(STORE, "spool")
    for name in os.listdir(spool_root) if os.path.isdir(spool_root) else []:
        path = os.path.join(spool_root, name)
        if name not in queued and time.time() - os.path.getmtime(path) > 3600:
            os.remove(path)
            spooled += 1
    result["spool"] = spooled
    return result


# --- parse worker -----------------------------------------------------------------------

def _prepared_from_parser(chunks: list[dict]) -> list[dict]:
    """Parser output {markdown, render, meta} -> commit shape. Text the VLM produced is
    generated: its identity is the render plus the recipe. Everything deterministic is its
    own identity - text chunks, and pages resolved from the text layer (render plus text,
    no recipe). An unrenderable page holds nothing (empty text, no render) but keeps its
    position: it is a verdict, hence generated."""
    return [{"markdown": ch["markdown"],
             "render_sha256": store_render(ch["render"]) if ch.get("render") else None,
             "generated": ch["meta"].get("source") == "vlm"
                          or (ch["meta"].get("status") == "unparseable" and not ch.get("render")),
             "meta": ch["meta"]} for ch in chunks]


def process_file(cur, file_sha: str, meta: dict) -> bool:
    """One queued file delivery: unpack, parse every member that is not already complete,
    commit each, and record what the archive contains. Claims are not touched: a claim
    names the delivered sha, and file_claims resolves members through archive_members.
    Returns False when a dependency was unavailable: the members parsed so far are
    committed (content, complete in its own right) and the delivery is deferred."""
    data = spool_read(file_sha)
    if data is None:
        raise Unrecoverable("spool entry missing")
    # Duplicate paths inside one archive (legal in zip and tar): the last entry wins, as
    # it does on extraction. Nested archives arrive flattened, their name kept in the path.
    members = dict(to_members(data, meta.get("filename") or ""))
    cache = ChunkCache(cur)
    is_archive = not (len(members) == 1 and next(iter(members.values())) is data)
    member_sha = {path: sha(b) for path, b in members.items()}
    complete = {s: file_state(cur, s) == "complete" for s in set(member_sha.values())}
    parsed: dict[str, tuple] = {}
    deferred = None
    for path, mdata in members.items():
        s = member_sha[path]
        if complete[s] or s in parsed:
            continue
        try:
            parsed[s] = parse_file(path, mdata, members, lambda name: _gate(cur, name), cache)
        except UnparseableError as e:
            parsed[s] = (None, {"kind": "unparseable", "error": str(e)[:200]})
        except RetryLater as e:   # the dependency, not the document: stop, keep what is done
            deferred = str(e)
            break
    for s, (chunks, fmeta) in parsed.items():
        commit_file(cur, s, {**fmeta, "parser_version": PARSER_VERSION},
                    None if chunks is None else _prepared_from_parser(chunks))
    if deferred is not None:
        _defer(cur, file_sha, meta, deferred)
        return False
    if is_archive:
        commit_file(cur, file_sha, {"kind": "archive", "n_members": len(members),
                                    "parser_version": PARSER_VERSION}, None)
        cur.execute("DELETE FROM archive_members WHERE archive_sha256 = %s", (file_sha,))
        for path, s in member_sha.items():
            cur.execute("""INSERT INTO archive_members(archive_sha256, path, member_sha256)
                           VALUES (%s, %s, %s)""", (file_sha, path, s))
    return True


def process_chunks(cur, file_sha: str, meta: dict) -> bool:
    """One queued chunks delivery: infer text for the render-only chunks (through the same
    memoized, concurrent, per-chunk-verdict path as PDF pages), commit. Returns False when
    the VLM was unavailable and the delivery was deferred."""
    raw = spool_read(file_sha)
    if raw is None:
        raise Unrecoverable("spool entry missing")
    prepared = json.loads(raw)["chunks"]
    items, positions = [], []
    for i, p in enumerate(prepared):
        if p["generated"]:
            png = load_render(p["render_sha256"])
            if png is None:
                raise Unrecoverable("render missing from store")
            items.append(image_item(png))
            positions.append(i)
    if items:
        try:
            results = transcribe_pages(items, ChunkCache(cur))
        except RetryLater as e:
            _defer(cur, file_sha, meta, str(e))
            return False
        for i, r in zip(positions, results):
            prepared[i]["markdown"], prepared[i]["meta"] = r["markdown"], r["meta"]
    commit_file(cur, file_sha, _chunkset_meta(prepared), prepared)
    return True


TAKE_NEXT = """SELECT sha256, meta FROM files WHERE parsed_at IS NULL AND due_at <= now()
               ORDER BY due_at LIMIT 1 FOR NO KEY UPDATE SKIP LOCKED"""


@contextlib.contextmanager
def _gate(cur, name: str):
    """Serializes access to an external module across all workers and replicas: a
    session-level advisory lock on the worker's own connection, held for the call only
    (not for the transaction) and released with the session if the worker dies."""
    cur.execute("SELECT pg_advisory_lock(hashtext(%s))", (name,))
    try:
        yield
    finally:
        cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (name,))


def _defer(cur, file_sha: str, meta: dict, err: str):
    """A dependency was unavailable: keep the row queued, push it back by an exponential
    backoff (RETRY_BASE doubling, capped at RETRY_CAP), count the attempt. Never gives up:
    the queue is the truth and 202 stays honest while the bytes are still in the spool."""
    attempts = int(meta.get("attempts") or 0) + 1
    delay = min(RETRY_BASE * 2 ** (attempts - 1), RETRY_CAP)
    _log("parse_deferred", sha256=file_sha, attempt=attempts, retry_in=delay, err=err[:200])
    cur.execute("""UPDATE files SET due_at = now() + make_interval(secs => %s),
                                    meta = meta || jsonb_build_object('attempts', %s::int, 'error', %s::text)
                   WHERE sha256 = %s""", (delay, attempts, err[:200], file_sha))


def _take(cur, file_sha: str) -> bool:
    """Re-takes a queued row after a rollback released it, the way the workers do. False
    when another worker has picked it up meanwhile: its outcome stands."""
    cur.execute("""SELECT 1 FROM files WHERE sha256 = %s AND parsed_at IS NULL
                   FOR NO KEY UPDATE SKIP LOCKED""", (file_sha,))
    return cur.fetchone() is not None


def _fail(cur, file_sha: str, err: str) -> bool:
    """Records a parse that only a re-delivery can fix: claimed but not held, so pings
    answer 404 and the feed delivers again."""
    _log("parse_failed", sha256=file_sha, err=err[:200])
    if not _take(cur, file_sha):
        return False
    commit_file(cur, file_sha, {"kind": "parse_failed", "error": err[:200],
                                "parser_version": PARSER_VERSION}, None)
    return True


def _parse_next(conn) -> bool:
    """Takes one due queued file inside a single transaction and either settles it (parsed,
    unparseable, parse_failed) or defers it (the spool entry stays for the retry). False
    when nothing is due. Anything unexpected rolls the transaction back - nothing partial
    is ever visible - and is retried with backoff like a dependency outage: a bug fixed by
    the next release heals on its own, and the queue stays the truth. Only Unrecoverable
    (the bytes are gone) is settled as parse_failed for the feed to re-deliver."""
    with conn.cursor() as cur:
        cur.execute(TAKE_NEXT)
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            return False
        file_sha, meta = row[0], row[1] or {}
        try:
            settled = (process_chunks if meta.get("source") == "chunks" else process_file)(cur, file_sha, meta)
            conn.commit()
        except Unrecoverable as e:
            conn.rollback()
            settled = _fail(cur, file_sha, str(e))
            conn.commit()
        except Exception as e:
            conn.rollback()
            settled = False
            if _take(cur, file_sha):
                _defer(cur, file_sha, meta, f"internal: {type(e).__name__}: {e}")
            conn.commit()
        if settled:
            spool_delete(file_sha)   # bytes never outlive the parse
        return True


def parse_worker():
    """One of PARSE_WORKERS identical threads; there is no dispatcher. The queue is
    `files.parsed_at IS NULL AND due_at <= now()` and the row lock is the claim: a worker takes
    the oldest due, unlocked row and holds that transaction open while it parses. No two workers can
    take the same row (SKIP LOCKED), a worker that dies simply drops its lock and the row is
    back in the queue, and a restart resumes without any cleanup. NO KEY UPDATE, not UPDATE:
    a claim delivered for the same content while it parses references the row through a
    foreign key (KEY SHARE) and must not wait for the parse."""
    while True:
        try:
            with psycopg.connect(DB) as conn:
                while True:
                    if not _parse_next(conn):
                        time.sleep(2)
        except Exception as e:
            _log("parse_worker_error", err=str(e)[:200])
            time.sleep(10)


# --- embed worker -----------------------------------------------------------------------

def embed_worker():
    while True:
        try:
            with psycopg.connect(DB) as conn, conn.cursor() as cur:
                # NO KEY UPDATE: files_chunks rows referencing these chunks (KEY SHARE)
                # may be inserted by a parse worker meanwhile and must not wait.
                cur.execute("""SELECT sha256, markdown, render_sha256 FROM chunks
                               WHERE embedding IS NULL AND meta->>'status' = 'ok'
                               LIMIT 8 FOR NO KEY UPDATE SKIP LOCKED""")
                rows = cur.fetchall()
                for cid, md, r_sha in rows:
                    image = (load_render(r_sha) if r_sha else None) or synth_render(md)
                    cur.execute("UPDATE chunks SET embedding = %s::vector WHERE sha256 = %s",
                                (json.dumps(embed(md, image)), cid))
                conn.commit()
                # files with all embeddable chunks embedded -> indexed_at (blank and
                # unparseable chunks have nothing to embed and never hold a file back)
                cur.execute("""
                    UPDATE files f SET indexed_at = now()
                    WHERE f.parsed_at IS NOT NULL AND f.indexed_at IS NULL
                      AND EXISTS (SELECT 1 FROM files_chunks fc WHERE fc.file_sha256 = f.sha256)
                      AND NOT EXISTS (SELECT 1 FROM files_chunks fc JOIN chunks c
                                      ON c.sha256 = fc.chunk_sha256
                                      WHERE fc.file_sha256 = f.sha256 AND c.embedding IS NULL
                                        AND c.meta->>'status' = 'ok')""")
                conn.commit()
                if not rows:
                    time.sleep(3)
        except Exception as e:
            _log("embed_worker_error", err=str(e)[:200])
            time.sleep(10)


if os.environ.get("FATINGEST_WORKERS", "1") != "0":   # "0" = API only (tests, extra replicas)
    for _ in range(PARSE_WORKERS):
        threading.Thread(target=parse_worker, daemon=True).start()
    threading.Thread(target=embed_worker, daemon=True).start()
