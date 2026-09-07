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
gone: spool or render missing) is given up (files.failed_at), answers 404, and is retried
when the feed re-delivers.

A file's progress is a set of milestone dates on its row - created, due, started, parsed,
failed, indexed - each set once when reached; what stands in the way is files.error, and
attempts counts the deferrals. Nothing on the row flips between phases: meta holds only
what the file IS.

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

Background threads: FILE_CONCURRENCY identical parse workers each take the oldest due,
unlocked row of the queue - delivered (due_at set), not concluded, not given up, due - (SKIP
LOCKED: the row lock is the claim, so there is no dispatcher and no in-process bookkeeping;
due_at carries the retry backoff). A GC thread sweeps stale claims and what nothing refers
to any more (GC_INTERVAL), and rarely walks the store for strays (STORE_SWEEP_INTERVAL). Two limits shape the load, both process-wide: FILE_CONCURRENCY files
being parsed at once (an archive's members count one by one - this is what bounds memory,
since a file's renders live in RAM until its commit), and VLM_CONCURRENCY pages in flight
against the VLM (pdf_engine's one pool). A worker holding an archive parses its members in
parallel on whatever file slots are free, so a delivery of many small files fills the VLM
pool as well as one large file does. Everything a parse produces is written the moment it
exists - member rows and archive membership when an archive is opened, each chunk and its
position when a page is done - so nothing is ever an orphan and a deferred delivery resumes
where it was. The embed worker turns chunks without an embedding into (delivered render |
synthetic markdown render) + markdown -> embedding.
"""
import base64
import binascii
import contextlib
import json
import os
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

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
FILE_CONCURRENCY = max(1, int(os.environ.get("FILE_CONCURRENCY", "8")))   # files being parsed at once, process-wide
_FILE_SLOTS = threading.Semaphore(FILE_CONCURRENCY)                        # one permit per file in progress
RETRY_BASE = 60     # seconds; a deferred delivery waits RETRY_BASE * 2**attempts ...
RETRY_CAP = 3600    # ... capped here. Dependency failures are retried indefinitely.
GC_INTERVAL = max(0, int(os.environ.get("GC_INTERVAL", "3600")))                    # seconds between sweeps; 0 = never
GC_STALE_DAYS = max(0, int(os.environ.get("GC_STALE_DAYS", "14")))                  # a claim not touched this long is gone
STORE_SWEEP_INTERVAL = max(0, int(os.environ.get("STORE_SWEEP_INTERVAL", "86400")))  # full walk of the store; 0 = never

app = FastAPI(title="fatingest", version="0.2")


def _log(evt: str, **fields):
    print(json.dumps({"evt": evt, **fields}), flush=True)


class Unrecoverable(Exception):
    """What no retry of ours can fix: the delivered bytes are gone (spool entry or delivered
    render missing). The only cure is a re-delivery, so the file is settled as parse_failed
    and the feed's ping answers 404. Every other exception is retried with backoff."""


class ChunkCache:
    """One file's chunk memo for pdf_engine.transcribe_pages. get() looks a generated chunk
    up by identity (render sha + PARSER_VERSION, see chunk_id) on the worker's connection,
    inside its transaction (member threads share that connection; psycopg connections are
    thread-safe, each call takes its own cursor). put() writes a finished chunk AND its
    position in this file's collection at once, on a connection of its own, autocommitted -
    so the chunk is referenced from the first second and never an orphan, a deferred
    delivery finds its finished pages on retry, and the embed worker can start on them while
    the parse is still running. The file's row exists before any put: a delivery is enqueued
    before it is parsed, and an archive's members are written when the archive is opened.
    commit_file replaces the collection as a set when the parse concludes."""

    def __init__(self, conn, file_sha: str):
        self.conn = conn
        self.file_sha = file_sha

    def get(self, render: bytes) -> tuple[str, dict] | None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT markdown, meta FROM chunks WHERE sha256 = %s",
                        (chunk_id(None, sha(render), True),))
            row = cur.fetchone()
        return (row[0], row[1]) if row else None

    def put(self, render: bytes, markdown: str, meta: dict, positions: list[int], generated: bool = True):
        r_sha = store_render(render)
        cid = chunk_id(None if generated else markdown, r_sha, generated)
        with psycopg.connect(DB, autocommit=True) as conn:
            conn.execute("""INSERT INTO chunks(sha256, markdown, render_sha256, meta) VALUES (%s, %s, %s, %s)
                            ON CONFLICT DO NOTHING""", (cid, markdown, r_sha, json.dumps(meta)))
            for p in positions:
                conn.execute("""INSERT INTO files_chunks(file_sha256, idx, chunk_sha256) VALUES (%s, %s, %s)
                                ON CONFLICT (file_sha256, idx) DO UPDATE SET chunk_sha256 = EXCLUDED.chunk_sha256""",
                             (self.file_sha, p, cid))


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

def commit_file(cur, file_sha: str, meta: dict, chunks: list[dict] | None, error: str | None = None) -> int:
    """The parse concluded: the file's facts, its complete chunk collection, and the verdict
    if there is one, in one transaction. parsed_at is set once (clock_timestamp: a delivery
    is one transaction, and now() would be its start); error is the verdict that the content
    cannot be parsed, or NULL. Chunks: {markdown, render_sha256, generated, meta?,
    position_meta?}. Every chunk row is (re)inserted from memory - a row that already exists
    is left untouched (immutable) - so the collection is always complete or the whole file
    fails. The collection is replaced as a set; meta is merged, so the delivery's own facts
    (source, filename) survive the parse."""
    cur.execute("""INSERT INTO files(sha256, meta, parsed_at, error) VALUES (%s, %s, clock_timestamp(), %s)
                   ON CONFLICT (sha256) DO UPDATE
                   SET meta = files.meta || EXCLUDED.meta, parsed_at = clock_timestamp(), indexed_at = NULL,
                       error = EXCLUDED.error""",
                (file_sha, json.dumps(meta), error))
    cur.execute("DELETE FROM files_chunks WHERE file_sha256 = %s", (file_sha,))
    for i, ch in enumerate(chunks or []):
        cid = chunk_id(ch["markdown"], ch["render_sha256"], ch["generated"])
        cur.execute("""INSERT INTO chunks(sha256, markdown, render_sha256, meta) VALUES (%s, %s, %s, %s)
                       ON CONFLICT DO NOTHING""",
                    (cid, ch["markdown"], ch["render_sha256"], json.dumps(ch.get("meta") or {"status": "ok"})))
        cur.execute("INSERT INTO files_chunks(file_sha256, idx, chunk_sha256, meta) VALUES (%s, %s, %s, %s)",
                    (file_sha, i, cid, json.dumps(ch.get("position_meta") or {})))
    return len(chunks or [])


def commit_item(cur, feed: str, uri: str, meta: dict, file_sha: str):
    """The feed's claim: meta as delivered (absent = empty), content = this one file."""
    cur.execute("""INSERT INTO items(feed, uri, sha256, meta) VALUES (%s, %s, %s, %s)
                   ON CONFLICT (feed, uri) DO UPDATE
                   SET sha256 = EXCLUDED.sha256, meta = EXCLUDED.meta, touched_at = now()""",
                (feed, uri, file_sha, json.dumps(meta)))


def enqueue(cur, file_sha: str, meta: dict):
    """A delivery: the row exists and has a queue position (due_at). Re-delivering a file that
    was given up, is stale under another recipe, or was only ever seen as an archive member
    starts its lifecycle over: the milestones are cleared, the delivery's facts are merged
    into what is known about the file."""
    cur.execute("""INSERT INTO files(sha256, meta, due_at) VALUES (%s, %s, now())
                   ON CONFLICT (sha256) DO UPDATE
                   SET meta = files.meta || EXCLUDED.meta, due_at = now(), started_at = NULL, parsed_at = NULL,
                       failed_at = NULL, indexed_at = NULL, attempts = 0, error = NULL""",
                (file_sha, json.dumps(meta)))


QUEUED = "parsed_at IS NULL AND failed_at IS NULL AND due_at IS NOT NULL"   # the queue, as a predicate


def _retry_now(cur, file_sha: str):
    """A re-delivery of a deferred (backed-off) delivery pulls it forward to now. Never waits
    on a worker: a row being parsed is skipped and left alone."""
    cur.execute(f"""SELECT 1 FROM files WHERE sha256 = %s AND {QUEUED} AND due_at > now()
                    FOR NO KEY UPDATE SKIP LOCKED""", (file_sha,))
    if cur.fetchone():
        cur.execute("UPDATE files SET due_at = now() WHERE sha256 = %s", (file_sha,))


def file_state(cur, s: str) -> str:
    """'complete' = full possession (parsed with the CURRENT parser version, all chunks, every
    delivered render on disk; a file with an unparseable verdict possesses nothing and holds
    it); 'queued' = delivered and waiting for the parse worker; 'missing' = anything else: a
    parse that was given up, a file only seen inside an archive that is still being parsed,
    a file parsed under another parser version."""
    cur.execute("SELECT meta, due_at, parsed_at, failed_at, error FROM files WHERE sha256 = %s", (s,))
    row = cur.fetchone()
    if not row:
        return "missing"
    meta, due_at, parsed_at, failed_at, error = row[0] or {}, row[1], row[2], row[3], row[4]
    if failed_at is not None:
        return "missing"
    if parsed_at is None:
        return "queued" if due_at is not None else "missing"
    if meta.get("parser_version") not in (None, PARSER_VERSION):
        return "missing"
    if error is not None:
        return "complete"
    if meta.get("kind") == "archive":   # an archive holds exactly what its members hold
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
            cur.execute(f"""SELECT count(*) FILTER (WHERE {QUEUED}),
                                   count(*) FILTER (WHERE {QUEUED} AND due_at > now()),
                                   count(*) FILTER (WHERE failed_at IS NOT NULL),
                                   count(*) FILTER (WHERE parsed_at IS NOT NULL AND error IS NOT NULL)
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
    whatever the other rankings found. A chunk no file's collection holds - a page memoized
    while its delivery is still being parsed, or one GC has not swept yet - is not content
    anyone can be pointed to, and leaves as well."""
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
          AND EXISTS (SELECT 1 FROM files_chunks fc WHERE fc.chunk_sha256 = c.sha256)
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


def gc_run(stale_days: int) -> dict:
    """One referential sweep: stale claims, then files nothing claims or contains, then chunks
    no collection holds (fatingest_gc). What the database dropped tells the store what to
    drop: the spool entries of the files that went, and the render files of the chunks that
    went, unless another chunk still points to the render (renders are shared across
    recipes). Nothing is walked."""
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT fatingest_gc(make_interval(days => %s))", (stale_days,))
        result = cur.fetchone()[0]
        candidates = result.pop("gone_renders", []) or []
        gone_files = result.pop("gone_files", []) or []
        cur.execute("SELECT DISTINCT render_sha256 FROM chunks WHERE render_sha256 = ANY(%s)", (candidates,))
        still = {r[0] for r in cur.fetchall()}
    renders = 0
    for r_sha in candidates:
        if r_sha not in still:
            with contextlib.suppress(FileNotFoundError):
                os.remove(render_path(r_sha))
                renders += 1
    for s in gone_files:
        spool_delete(s)
    result["renders"] = renders
    return result


def store_sweep() -> dict:
    """The rare full walk: render files no chunk points to, half-written temp files, and spool
    entries that belong to no queued delivery and are older than an hour. Catches what a crash
    between two steps may have left behind; the routine sweep never needs it."""
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT render_sha256 FROM chunks WHERE render_sha256 IS NOT NULL")
        live = {r[0] for r in cur.fetchall()}
        cur.execute(f"SELECT sha256, meta->>'source' FROM files WHERE {QUEUED}")
        queued = dict(cur.fetchall())
    for s, source in queued.items():
        if source == "chunks" and (raw := spool_read(s)):
            live |= {c["render_sha256"] for c in json.loads(raw)["chunks"] if c["render_sha256"]}
    renders = 0
    for dirpath, _dirs, names in os.walk(os.path.join(STORE, "renders")):
        for name in names:
            if name.endswith(".tmp") or name not in live:
                with contextlib.suppress(FileNotFoundError):
                    os.remove(os.path.join(dirpath, name))
                    renders += 1
    spooled = 0
    spool_root = os.path.join(STORE, "spool")
    for name in os.listdir(spool_root) if os.path.isdir(spool_root) else []:
        path = os.path.join(spool_root, name)
        if name not in queued and time.time() - os.path.getmtime(path) > 3600:
            with contextlib.suppress(FileNotFoundError):
                os.remove(path)
                spooled += 1
    return {"renders": renders, "spool": spooled}


@app.post("/v1/gc")
def gc(request: Request, stale_days: int | None = None):
    """The sweep on demand: what the GC thread does on its own schedule, plus the full walk of
    the store. The stale interval is policy (the deletion SLA for sources that vanish
    silently, default GC_STALE_DAYS); the counters are the audit trail."""
    require_write(request)
    result = gc_run(GC_STALE_DAYS if stale_days is None else stale_days)
    swept = store_sweep()
    result["renders"] += swept["renders"]
    result["spool"] = swept["spool"]
    return result


def gc_worker():
    """Sweeps every GC_INTERVAL seconds and walks the store every STORE_SWEEP_INTERVAL; either
    at 0 is never."""
    last_sweep = time.monotonic()
    while True:
        time.sleep(GC_INTERVAL or 3600)
        if GC_INTERVAL:
            try:
                _log("gc", **gc_run(GC_STALE_DAYS))
            except Exception as e:
                _log("gc_error", err=str(e)[:200])
        if STORE_SWEEP_INTERVAL and time.monotonic() - last_sweep >= STORE_SWEEP_INTERVAL:
            last_sweep = time.monotonic()
            try:
                _log("store_sweep", **store_sweep())
            except Exception as e:
                _log("store_sweep_error", err=str(e)[:200])


# --- parse worker -----------------------------------------------------------------------

def _prepared_from_parser(chunks: list[dict]) -> list[dict]:
    """Parser output {markdown, render, meta, position_meta?} -> commit shape. Text the VLM
    produced is generated: its identity is the render plus the recipe. Everything
    deterministic is its own identity - text chunks, and pages resolved from the text layer
    (render plus text, no recipe). An unrenderable page holds nothing (empty text, no render)
    but keeps its position: the chunk is the shared verdict, the reason is this file's and
    goes to the position."""
    return [{"markdown": ch["markdown"],
             "render_sha256": store_render(ch["render"]) if ch.get("render") else None,
             "generated": ch["meta"].get("source") == "vlm"
                          or (ch["meta"].get("status") == "unparseable" and not ch.get("render")),
             "meta": ch["meta"], "position_meta": ch.get("position_meta")} for ch in chunks]


def _mark_started(file_sha: str):
    """started_at for an archive member, on a connection of its own so it is visible at once.
    Never waits: a member that is also somebody's delivery, locked by the worker parsing it,
    gets its start from that worker instead."""
    with psycopg.connect(DB, autocommit=True) as c, c.transaction():
        if c.execute("SELECT 1 FROM files WHERE sha256 = %s AND started_at IS NULL FOR NO KEY UPDATE SKIP LOCKED", (file_sha,)).fetchone():
            c.execute("UPDATE files SET started_at = clock_timestamp() WHERE sha256 = %s", (file_sha,))


def _parse_member(path: str, data: bytes, members: dict[str, bytes], conn, file_sha: str, own_row: bool) -> tuple:
    """One member on one file slot, in a member thread: (chunks, meta, error) - the parse's
    result, or (None, what is known about the file, the verdict) when the content cannot be
    parsed. RetryLater and anything unexpected propagate through the future. `own_row` says
    the member is the delivery itself (a single file): its row is the worker's, locked and
    already marked started; an archive member's row is marked here. Touches no cursor of
    the worker: the gate takes its own connection and the cache shares the worker's
    (thread-safe)."""
    try:
        if not own_row:
            _mark_started(file_sha)
        try:
            chunks, meta = parse_file(path, data, members, _gate, ChunkCache(conn, file_sha))
            return chunks, meta, None
        except UnparseableError as e:
            return None, dict(e.meta), str(e)[:200]
    finally:
        _FILE_SLOTS.release()


def process_file(cur, file_sha: str, meta: dict) -> bool:
    """One queued file delivery: unpack, parse every member that is not already complete,
    commit each, and record what the archive contains. Claims are not touched: a claim
    names the delivered sha, and file_claims resolves members through archive_members.

    An archive's members and its membership are written the moment it is opened - the shas
    are content-derived, so they are known before any parsing - which is what lets every
    finished page be written at once (ChunkCache) without ever being an orphan: the archive
    is claimed, its members are contained, their chunks are held. A member row created this
    way has no queue position (due_at NULL): it is nobody's delivery, and a ping on it
    answers deliver until it is parsed.

    Members are parsed in parallel, one file slot each: a slot is waited for only while
    none of this delivery's members is running (then we hold nothing, and the members
    running elsewhere will release theirs), otherwise the next member starts as soon as
    one of ours finishes or a slot elsewhere frees up. Returns False when a dependency was
    unavailable: the members that finished are committed (content, complete in its own
    right), nothing new is started, and the delivery is deferred."""
    data = spool_read(file_sha)
    if data is None:
        raise Unrecoverable("spool entry missing")
    # Duplicate paths inside one archive (legal in zip and tar): the last entry wins, as
    # it does on extraction. Nested archives arrive flattened, their name kept in the path.
    members = dict(to_members(data, meta.get("filename") or ""))
    is_archive = not (len(members) == 1 and next(iter(members.values())) is data)
    member_sha = {path: sha(b) for path, b in members.items()}
    if is_archive:
        with psycopg.connect(DB, autocommit=True) as c:
            for s in set(member_sha.values()):
                c.execute("INSERT INTO files(sha256) VALUES (%s) ON CONFLICT DO NOTHING", (s,))
            for path, s in member_sha.items():
                c.execute("""INSERT INTO archive_members(archive_sha256, path, member_sha256) VALUES (%s, %s, %s)
                             ON CONFLICT DO NOTHING""", (file_sha, path, s))
    complete = {s: file_state(cur, s) == "complete" for s in set(member_sha.values())}
    todo, seen = [], set()
    for path, mdata in members.items():
        s = member_sha[path]
        if not complete[s] and s not in seen:
            seen.add(s)
            todo.append((s, path, mdata))
    parsed: dict[str, tuple] = {}
    running: dict = {}
    deferred: str | None = None
    failure: BaseException | None = None

    def collect(done):
        nonlocal deferred, failure
        for fut in done:
            s = running.pop(fut)
            try:
                parsed[s] = fut.result()
            except RetryLater as e:      # the dependency, not the document: stop starting, keep what is done
                deferred = deferred or str(e)
            except BaseException as e:   # re-raised below, once the members in flight have finished
                failure = failure or e

    with ThreadPoolExecutor(max_workers=FILE_CONCURRENCY, thread_name_prefix="member") as ex:
        for s, path, mdata in todo:
            if deferred or failure:
                break
            while not _FILE_SLOTS.acquire(blocking=not running):
                done, _ = wait(running, return_when=FIRST_COMPLETED)
                collect(done)
                if deferred or failure:
                    break
            else:
                running[ex.submit(_parse_member, path, mdata, members, cur.connection, s, not is_archive)] = s
                continue
            break
        if running:
            collect(wait(running).done)
    if failure is not None:
        raise failure
    for s, (chunks, fmeta, error) in parsed.items():
        commit_file(cur, s, {**fmeta, "parser_version": PARSER_VERSION},
                    None if chunks is None else _prepared_from_parser(chunks), error)
    if deferred is not None:
        _defer(cur, file_sha, deferred)
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
    memoized, concurrent, per-chunk-verdict path as PDF pages) on one file slot, commit.
    Returns False when the VLM was unavailable and the delivery was deferred."""
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
        with _FILE_SLOTS:
            try:
                results = transcribe_pages(items, ChunkCache(cur.connection, file_sha), positions)
            except RetryLater as e:
                _defer(cur, file_sha, str(e))
                return False
        for i, r in zip(positions, results):
            prepared[i]["markdown"], prepared[i]["meta"] = r["markdown"], r["meta"]
    commit_file(cur, file_sha, _chunkset_meta(prepared), prepared)
    return True


TAKE_NEXT = f"""SELECT sha256, meta FROM files WHERE {QUEUED} AND due_at <= now()
                ORDER BY due_at LIMIT 1 FOR NO KEY UPDATE SKIP LOCKED"""


@contextlib.contextmanager
def _gate(name: str):
    """Serializes access to an external module across all workers, member threads and
    replicas: a session-level advisory lock on a connection of its own, held for the call
    only and released with the session if the holder dies. Its own connection, because a
    lock wait must never block the worker's connection, which member threads share for
    cache lookups."""
    with psycopg.connect(DB, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(hashtext(%s))", (name,))
        try:
            yield
        finally:
            conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (name,))


def _defer(cur, file_sha: str, err: str):
    """A dependency was unavailable: keep the row queued, push it back by an exponential
    backoff (RETRY_BASE doubling per attempt, capped at RETRY_CAP), count the attempt, record
    the cause. Never gives up: the queue is the truth and 202 stays honest while the bytes
    are still in the spool."""
    cur.execute("""UPDATE files
                   SET due_at = now() + make_interval(secs => LEAST(%s * power(2, attempts), %s)),
                       attempts = attempts + 1, error = %s
                   WHERE sha256 = %s
                   RETURNING attempts, round(EXTRACT(epoch FROM due_at - now()))""",
                (RETRY_BASE, RETRY_CAP, err[:200], file_sha))
    attempts, retry_in = cur.fetchone()
    _log("parse_deferred", sha256=file_sha, attempt=attempts, retry_in=int(retry_in), err=err[:200])


def _take(cur, file_sha: str) -> bool:
    """Re-takes a queued row after a rollback released it, the way the workers do. False
    when another worker has picked it up meanwhile: its outcome stands."""
    cur.execute(f"""SELECT 1 FROM files WHERE sha256 = %s AND {QUEUED}
                    FOR NO KEY UPDATE SKIP LOCKED""", (file_sha,))
    return cur.fetchone() is not None


def _fail(cur, file_sha: str, err: str) -> bool:
    """Gives a delivery up: only a re-delivery can fix it (the bytes are gone), so failed_at
    is set, the cause recorded, and the feed's ping answers 404 from here on."""
    _log("parse_failed", sha256=file_sha, err=err[:200])
    if not _take(cur, file_sha):
        return False
    cur.execute("UPDATE files SET failed_at = clock_timestamp(), error = %s WHERE sha256 = %s", (err[:200], file_sha))
    return True


def _parse_next(conn) -> bool:
    """Takes one due queued file inside a single transaction and either concludes it (parsed,
    or the verdict that it cannot be), gives it up (the bytes are gone), or defers it (the
    spool entry stays for the retry). False when nothing is due. Anything unexpected rolls
    the transaction back and is retried with backoff like a dependency outage: a bug fixed by
    the next release heals on its own, and the queue stays the truth. What was written
    meanwhile on other connections - member rows, finished pages - stays and is picked up by
    the retry."""
    with conn.cursor() as cur:
        cur.execute(TAKE_NEXT)
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            return False
        file_sha, meta = row[0], row[1] or {}
        cur.execute("UPDATE files SET started_at = COALESCE(started_at, clock_timestamp()) WHERE sha256 = %s", (file_sha,))
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
                _defer(cur, file_sha, f"internal: {type(e).__name__}: {e}")
            conn.commit()
        if settled:
            spool_delete(file_sha)   # bytes never outlive the parse
        return True


def parse_worker():
    """One of FILE_CONCURRENCY identical threads; there is no dispatcher. The queue is
    the rows that are delivered, not concluded, not given up and due, and the row lock is the claim: a worker takes
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
    for _ in range(FILE_CONCURRENCY):
        threading.Thread(target=parse_worker, daemon=True).start()
    threading.Thread(target=embed_worker, daemon=True).start()
    threading.Thread(target=gc_worker, daemon=True).start()
