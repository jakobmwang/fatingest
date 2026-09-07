# fatingest

A content-addressed ingestion and hybrid-search store built on Postgres
(pgvectorscale/diskann for vectors, pg_textsearch for BM25, pg_trgm for substring grep)
with a small ingest API and a background embedding worker targeting any
OpenAI-compatible multimodal embedding endpoint.

## Principles

- **Ephemeral thin index — not a source of truth.** Content that is not re-delivered by its
  feeds within the GC window disappears. Raw file bytes are never retained beyond the parse
  spool (`store/spool/<sha256>`, held only until parsed): the system keeps exactly what
  retrieval needs — markdown in the database and renders in a content-addressed file store
  (`store/renders/ab/cd/<sha256>`). Agents that need an original fetch it from the source via
  their own tooling.
- **No fetcher, no crawler.** Integrations own fetching, identifier hygiene, unpacking decisions
  and HTML handling. The system only ever processes bytes it has been handed.
- **Feeds and URIs.** A *feed* is a hierarchical name (`intranet/it/hardware`) for something that
  keeps delivering; a *uri* is the feed's identifier for one piece of content — any URI, not
  necessarily fetchable. A feed's claim on a uri is `(meta, one file)` - the sha it
  delivered - and two feeds may legitimately claim the same uri with different content.
- **Content addressing.** Files and chunks are identified by sha256 of what was delivered.
  Deduplication and garbage collection are structural, not bookkeeping.
- **Immutable content, replaceable relationships.** `files` and `chunks` rows are never
  modified. Relationship rows are replaced as a set — an archive's members
  (`archive_members`) when it is parsed, a file's chunk collection (`files_chunks`) when it
  is (re)parsed — and `/v1/gc` sweeps whatever nothing references any more. Changed content
  means new identities, never updates.
- **Every embedding input is (image + text).** If a chunk has no delivered render, a
  deterministic typographic render of the markdown is generated as embedding input only
  (`RENDERER_VERSION` is pinned; it never enters an identity). Mixing modalities in one index
  degrades retrieval badly (modality gap: image-embedded units score systematically higher
  against text queries regardless of relevance); homogeneous (image + text) inputs close that
  gap completely.
- **The cheapest reading that loses nothing.** A page with nothing drawn on it - no image,
  no vector graphics, no annotation a viewer would draw - holds nothing but its text layer,
  so the text layer *is* the page: it is read directly (LiteParse, with headers, footers,
  small text and links all kept) and the chunk's text is deterministic. Every other page is
  transcribed from its render by the VLM, which costs roughly a thousand times as much per
  page. The line between them is `VLM_VISUAL_THRESHOLD`, the largest drawn share of a page
  that may still skip the VLM; its default is 0, exactly nothing drawn, and raising it is
  the operator's explicit decision that marks up to that size may go undescribed. A guard
  decides on evidence, not on trust: if the page's text layer (PDFium) holds a token the
  direct reading does not, that page goes to the VLM after all. `meta.source` records which
  reading produced the text, and the render is kept and embedded either way, so a reader
  can always look at the page itself.
- **One library per job.** The deterministic half of the page engine is two tools with no
  overlap: PDFium (`pypdfium2`) opens the document, decides what a page provably contains,
  renders it and reads its text layer and links; LiteParse turns a page that has nothing
  drawn on it into markdown. Both are permissively licensed, as is everything else in the
  image, so the project can be released under a permissive licence itself.
- **Verdicts are per chunk, never per document.** A page the VLM refuses, or one that cannot
  be rendered, becomes a chunk with empty text and `meta.status: unparseable`; a page with
  nothing on it becomes `meta.status: blank`. The document keeps every position (a chunk's
  `idx` in a PDF is always its page), the other pages are indexed normally, and blank and
  unparseable chunks are never embedded or ranked. Only a document that cannot be opened at
  all (corrupt, encrypted, unknown type, refused by the converter) is unparseable as a whole.
- **Vanilla BM25 over normalized text.** `text_config='simple'`: no stemming, no stopwords,
  exact tokens - identifiers, names and codes survive, and nothing assumes a language. What
  the parser cannot keep together, a small language-neutral normalization glues before
  tokenization (`fatingest_norm`: `KMD-1234`, `1.234,56`, `§ 12`, `-500`, `sha256://…`), the
  same function for index and query. Semantic matching is the vector index's job.
- **Fuzziness lives in the vocabulary, not the corpus.** Every (surface, term) pair the
  corpus holds sits in `vocab` with a trigram index; typo tolerance, compound matching and
  regex run there and translate to exact BM25 terms. The cost follows the vocabulary, which
  grows sublinearly, not the corpus - there is no trigram index on text. The vocabulary is
  kept exact by the events themselves: a chunk's insertion counts its pairs up, its deletion
  counts them down, and a pair no chunk holds is gone - it never offers a word the corpus
  does not contain.

## Schema (six tables)

```
items           (feed, uri PK, sha256, meta, touched_at)   -- the feed's claim: meta = what THIS feed says, sha256 = the one file it delivered
files           (sha256 PK, meta, created_at, due_at, started_at, parsed_at, failed_at, indexed_at, attempts, error)
archive_members (archive_sha256, path PK, member_sha256)    -- what an archive contains; replaced per (re)parse
files_chunks    (file_sha256, idx PK, chunk_sha256, meta)   -- the file's chunk collection; replaced per (re)parse
chunks          (sha256 PK, markdown, render_sha256, meta, embedding)   -- meta: status, error, visual, text_chars (derived, never identity)
vocab           (surface, term PK, n_chunks)                -- every (surface, index term) pair the corpus holds, and how many chunks hold it
```

A file's progress is a set of milestone dates, each set once when reached and NULL until
then: `created_at` (the row exists: delivered, or unpacked from an archive), `due_at` (queued;
NULL for a file never delivered on its own, pushed forward by backoff on a deferral),
`started_at`, `parsed_at` (the parse concluded, with content or with a verdict), `failed_at`
(given up: the delivered bytes are gone), `indexed_at` (every embeddable chunk embedded).
`attempts` counts the deferrals and `error` is what stands in the way: the last deferral's
cause while queued, or the verdict that the content cannot be parsed. Nothing on the row
flips between phases, and `files.meta` holds only what the file *is*: `kind` (pdf, office,
html, image, spreadsheet, tabular, text, archive, chunkset, binary), `content_type`,
`parser_version`, `n_members`, and the delivery's own facts (`source`, `filename`). The
queue is simply the rows that are delivered, not concluded, not given up and due; the
intervals between the dates are the statistics.

Meta sits where the fact belongs. `chunks.meta` is what fatingest observed about a chunk
and cannot differ between the files holding it: `status` (`ok`, `blank`, `unparseable`),
`error` (the VLM's refusal of this render, when unparseable), `source` (`text_layer` or `vlm`
- which reading produced the text), for spreadsheets `sheet` with `rows` (cell chunks) or
`region` (drawings), and, for pages, `visual` (the share of the page area covered by
everything drawn that is not text - images, vector graphics and the annotations a viewer
draws, such as stamps, notes and form fields - overlaps counted once, 0..1, not rounded: a
single small mark is a small number, never zero) and `text_chars` (characters in the page's
own text layer; `visual` 1 with `text_chars` 0 is a scan, so the transcription is pure OCR).
`files_chunks.meta` is what a position in one file says about the chunk when files may
differ: `error`, why this page of this file could not be rendered (the chunk itself is the
shared empty verdict). Search hits carry the chunk meta.

`items.meta` is free-form except one reserved key, `published_at` (ISO 8601), validated at
ingest and used by search when `weight_recency` is set. A claim names exactly one file;
many claims may name the same file. Archives are
modelled where they belong, between files: `archive_members` says which files an archive
contains, keyed by path (identical content may sit at two paths). Nested archives are
flattened at unpack, so members are always one level deep and the nesting survives only
in the path (`a/b.zip/c.txt`). The view `file_claims (sha256, feed, uri)` resolves every
claim a file serves, directly or through an archive; search attributes chunks with it.

GC is purely referential and runs on its own (`GC_INTERVAL`, default hourly): stale
`items` (`GC_STALE_DAYS`) -> `files` that no claim names and no archive contains (repeated
until nothing is orphaned: deleting an archive cascades its `archive_members`, which frees
its members for the next round) -> unreferenced `chunks` (cascade: `files_chunks`). The
database reports what went, and the store drops exactly that: the spool entries of the
files and the render files of the chunks, unless another chunk still points to the render.
Nothing is ever an orphan for long, because everything a parse produces is written the
moment it exists: an archive's members and membership when it is opened, each chunk and
its position when its page is done. A rare full walk of the store (`STORE_SWEEP_INTERVAL`,
default daily) catches what a crash between two steps may have left behind. `POST /v1/gc`
runs both on demand.

## Identities

- **File, delivered as bytes:** `sha256(bytes)`.
- **File, delivered as chunks:** sha256 of the canonical JSON of the delivered halves per
  chunk (render sha and/or markdown sha), `PACK_VERSION` included. The same delivery yields the
  same sha, always; the packing is never persisted.
- **Chunk:** sha256 over the *delivered* halves — `render_sha256 || 0x00 || markdown_sha256`,
  either side empty when absent. When fatingest generated the text itself (VLM transcription of
  a page or an image), the markdown sha is replaced by `PARSER_VERSION`: generated content never
  enters an identity, because a generative model's output varies; the recipe that generated it
  does, because a changed recipe is a deliberate re-index. Text read from a page's own text
  layer is not generated — the same page yields the same text every time — so such a chunk is
  its render and its text, with no recipe in it.

`PARSER_VERSION` names the parsing recipe in effect — the release's parser code (routing,
prompts, render scale, link rules, splitting, tabular layout), the VLM model, and the
Gotenberg image and fonts. The operator sets it in `.env` (any string; a memorable name
beats a number) and changes it deliberately when any of those change. It is recorded on
every parsed file; a file parsed under another name is *incomplete*: pings answer 404, the
feed re-delivers, the file is re-parsed into a fresh chunk collection, and the old chunks
are garbage-collected. Too many variables affect the output for the version to be derived
automatically, so nothing is ever re-parsed or re-transcribed as a side effect — a re-index
is always a named event.

Three outcomes are kept apart. `unparseable` is the content's fault under this recipe: a
PDF that cannot be opened or is encrypted, an unknown binary type, binary data posing as
text - and any document a *healthy* converter refuses. Gotenberg is health-checked before
every call, runs one conversion at a time (a database advisory lock shared by all workers
and replicas) and restarts LibreOffice and Chromium after every conversion, so nothing
accumulates and its answer is the document's own: any non-2xx, timeouts included, is
`unparseable` - complete possession of nothing, recorded with the error, re-parsed when
`PARSER_VERSION` changes. The same verdict on a single page (the VLM refuses that request
with a 4xx) stays with the page: an `unparseable` chunk, the rest of the document intact.
Everything else that goes wrong while parsing - the dependency's own failure (unreachable,
unhealthy, a 5xx / 429 from the VLM behind its gateway) or an internal error - decides
nothing about the document: the delivery is *deferred* and retried by the worker with
exponential backoff (1 min doubling, capped at 1 h, indefinitely), answering 202 meanwhile;
nothing partial is ever committed, members already parsed stay parsed, pages already
transcribed are remembered (below), and the spool entry stays until the delivery settles.
Only what no retry can fix - the delivered bytes are gone (spool entry or delivered render
missing) - is given up (`failed_at`): claimed but not held, so the feed's next ping answers
404 and the content is retried on re-delivery. `/health` shows all three: `files_retrying`,
`files_unparseable`, `files_failed`.

## Ingest API (POST, JSON)

| endpoint | body | response |
|---|---|---|
| `/v1/ingest/ping` | `{feed, uri, sha256, meta?}` | 200 known (touch) / 202 queued / 404 -> deliver |
| `/v1/ingest/file` | `{feed, uri, meta?, bytes_b64, filename?}` | 202 queued `{sha256}` / 200 ingested when already complete |
| `/v1/ingest/chunks` | `{feed, uri, meta?, chunks: [{markdown?, render_b64?}, ...]}` | 200 ingested `{sha256}` / 202 queued when text inference is pending |
| `/v1/ingest/delete` | `{feed, uri}` | 200 (idempotent) |

Bodies are validated strictly: unknown fields, an empty chunk list, or a chunk with neither
half fail with 422. `meta` is the feed's statement about the uri (title, tags, ...);
absent means empty. One key is reserved: `published_at`, an ISO 8601 date or timestamp
(422 if it does not parse) that search can rank by. Since meta is part of the claim, a
changed date is a changed claim: ping answers 404 and the feed re-delivers. Feed names are normalized (trailing slashes trimmed). Every response
names one file: the `sha256` the client pings from then on.

**The client protocol is the ping: 200 done, 202 wait, 404 deliver.** Ping is verification,
never a write. It answers 200 only when this feed's claim exists with exactly this `meta`,
carries this `sha256`, and the content is completely present: current parser version, all
chunks, every delivered render on disk. It answers 202 while the content is queued for
parsing or waiting for a retry. Only `touched_at` is updated. Anything else is 404 and the
feed delivers. A client that pings until it stops seeing 202 knows its content is fully
indexed. A parse that failed internally answers 404 and is retried when the feed
re-delivers; that re-delivery loop is the outer healing mechanism (which is why the render
store needs neither backup nor replication; missing embeddings heal internally and are not
checked). Re-delivering a deferred delivery pulls its retry forward to now. Content already known under another claim still needs one delivery per new
`(feed, uri)`; it is not re-parsed, so the cost is the upload alone.

**File** deliveries are spooled and queued; `FILE_CONCURRENCY` identical worker threads take
them in due order (arrival order for fresh deliveries). The queue is `files.parsed_at IS NULL
AND due_at <= now()` and the row lock is the
claim (`FOR NO KEY UPDATE SKIP LOCKED`, held for the duration of the parse): no dispatcher,
no in-process bookkeeping, a crashed worker's row is simply back in the queue, and a
restart resumes. A delivery whose dependency was unavailable is deferred with backoff
(`due_at`) and keeps its spool entry until it settles. Routing: archives unpack into members (each member its own file, recorded
in `archive_members`; the archive's own sha is what the feed claims and pings, and it is
complete when its members are); PDFs go through
the page engine, one chunk per page: a page with nothing drawn on it is read from its own
text layer, and a page with any image or vector graphics is rendered and transcribed by the
VLM with that text layer as spelling support. Transcription goes through one pool for the
whole process, `VLM_CONCURRENCY` pages in flight whatever file they belong to, and an
archive's members are parsed in parallel on the `FILE_CONCURRENCY` file slots, so a delivery
of many small files keeps the model as busy as one large file does. Office documents convert via LibreOffice and html via Chromium into the same engine; images become (normalized PNG +
VLM description) chunks; spreadsheets and tabular data become GFM pipe-table chunks batched
by row with the header repeated, each carrying its sheet and row range in meta, and a
workbook's charts, pictures and shapes each become a chunk of their own: the workbook is
converted once with its cell formatting stripped and one page per sheet, and every drawing
on a sheet is rendered alone and transcribed by the VLM, with the sheet and its position in
meta (the cells are never rendered, so a chart on a 20,000-row sheet comes out legible); markdown and text split at ~4000 characters on heading,
paragraph, line, sentence, word boundaries. Local references (embedding and navigation, in html, markdown and the links a text layer
carries) are rewritten to `sha256://<sha>` content addresses by one shared rule; a reference
that resolves to nothing is left as written. A target never passes through the VLM: on that
route each anchor is handed over marked as `[anchor](L1)` and the mark is exchanged for the
real target after generation, because an address is exact or it is worthless and one mutated
character in a content address destroys it. Two links to one target with nothing but whitespace
between them - a URL wrapped at the margin, an anchor running onto the next line - are one link
again, the anchors joined by the whitespace that stood between them. Already-complete members are never re-parsed, and neither are pages: a generated
chunk's identity (render + `PARSER_VERSION`) is known before the VLM is called, so every
finished transcription is written to `chunks` at once and looked up first next time. A
delivery deferred halfway through a long document resumes with the pages it lacks, and a
page that recurs in another delivery is transcribed once. Outcomes live in the database,
not in the response: `files.meta` records the kind and, for failures, the error; `chunks.meta`
the verdict per chunk.

**Chunks** deliveries let the integration decide boundaries, render and text. A chunk needs
`markdown`, `render_b64` or both. A missing half is generated once, when the chunk is new:
text from a render via the VLM (image prompt; this is queued, 202, and goes through the same
memoized, concurrent, per-chunk-verdict path as PDF pages), a render from text
synthetically at embedding time (never stored). Fully delivered content commits on the
spot (200). Delivered halves define identity, so a different markdown for the same render
is a different chunk. `idx` is the 0-based position in the list.

**Delete** withdraws the feed's claim immediately; content no other claim references is
swept by the next `/v1/gc`. Explicit deletions and replacements therefore propagate at GC
cadence, while only silent disappearance at the source waits out `stale_days`.

## Other endpoints

- `GET /v1/search?q=&weight_semantic=1&weight_lexical=1&weight_recency=0&filter_feeds=&filter_regex=&limit=10` -
  one query, up to four rankings, one fused list.
  - **semantic**: `q` embedded like the index (synthetic render + text), nearest chunks.
  - **lexical**: `q` normalized exactly like the index (`fatingest_norm`), ranked by BM25 -
    exact terms only. Bag of words: quotes, minus, AND/OR mean nothing, and there is no
    syntax to learn.
  - **fuzzy**: the same query plus the nearest vocabulary surfaces of each purely
    alphabetic word (typos, compounds - `hardware` also reaches `hardwarebestilling`;
    identifiers and numbers stay exact), ranked by BM25. Runs only when the vocabulary adds
    something. `weight_lexical` is shared equally by the two lexical rankings, so a chunk
    that matches exactly appears in both and is lifted, while one reached only through an
    expansion counts half. The response echoes the exact `lexical_query` and the
    `fuzzy_terms` added to it.
  - **recency**: the candidates ordered by date - the oldest `published_at` among a
    chunk's claims, else its file's `created_at` - newest first, ties sharing a rank.
    Computed only when `weight_recency > 0`; relative to the candidates, not absolute.

  The weights are relative (normalized to sum 1) and fuse the rankings with RRF
  (`1/(60+rank)` per ranking): a hit found by one ranking still ranks, agreement lifts it.
  Each ranking is 200 deep, which bounds how far a heavily filtered search can reach.
  Filters narrow: `filter_feeds` selects feed subtrees (`intranet` matches `intranet` and
  `intranet/it/hardware`, never `intranet-old`), `filter_regex` keeps chunks that hold a
  vocabulary term whose *surface* matches - what a reader sees, so `^§\d+$`, `^kmd-\d{4}$`
  or `^\d{6}-\d{4}$` work - and stands in for `q` when `q` is empty (ranked grep). A regex
  selecting more than 200 vocabulary terms is refused as too broad. Authorization,
  deciding which feeds a given user may search, is the calling client's job. Every hit
  reports `found_by` (ranking -> rank), `sources` (feed, uri, file, idx) for attribution,
  `meta` (the chunk's verdict and page hints, see Schema) and, when recency is weighted,
  its effective `published_at`.
- `POST /v1/gc?stale_days=` - the sweep on demand (default `GC_STALE_DAYS`), plus the full walk
  of the store; the counters are the audit trail.
- `GET /health` - unversioned, never gated: database ping plus the parse queue
  (`files_queued`, of which `files_retrying` are backed off), the embed queue
  (`chunks_pending_embed`, embeddable chunks only), the verdicts (`files_unparseable`: parsed,
  nothing to hold) and the re-delivery backlog (`files_failed`: given up, waiting for the
  feed). It deliberately never checks the VLM or embedding services, whose
  health belongs to their own containers.

## Deployment

`docker compose up -d --build`. Configuration via `.env` - see `.env.example` for the
full annotated list. Schema changes come in two kinds: additive ones (a column, an index)
ship as a dated, idempotent file under `migrations/`, to be run once against the live
database (`docker exec -i fatingest-db psql -U fatingest -d fatingest < migrations/<file>`),
and `initdb/` always carries the result for new databases; anything else is a new
database - the index is ephemeral and its feeds re-deliver. Chat calls route happily through an OpenAI-compatible gateway
(per-key telemetry for free); the embedding endpoint must be reached directly, because
multimodal (image+text) embeddings use a messages-format request that gateways reject
on `/v1/embeddings`.

Authentication is your reverse proxy's job; fatingest authorizes the PRINCIPAL the proxy
hands it. Set `WRITE_PRINCIPALS` to gate `/v1/ingest/*` and `/v1/gc`, and `READ_PRINCIPALS`
to gate `/v1/search`, on the proxy-set `PRINCIPAL_HEADER` (which the proxy MUST set/override
itself - never pass through from clients); `/health` is never gated. Leave the lists
empty only on a trusted network. Keep the `127.0.0.1` admin port private, and include the
Postgres volume in your backup routine. The database must run under a UTF-8-aware
`LC_CTYPE` (the image's default): the lexical normalization relies on `[[:alnum:]]` covering
non-ASCII letters, and `initdb/01-schema.sql` refuses to install otherwise. Fonts for LibreOffice/Chromium go in
`gotenberg/fonts/` (not committed; often license-restricted). Gotenberg runs with a 4 GB
limit, a 240 s conversion timeout and a fresh LibreOffice/Chromium per conversion
(`docker-compose.yml`, passed as flags: Gotenberg ignores `GOTENBERG_*` environment variables);
those limits are part of the recipe, so bump `PARSER_VERSION` when you change them. So are
the pinned versions of `pypdfium2` and `liteparse`, `VLM_VISUAL_THRESHOLD`, the VLM
prompt and `VLM_BLANK_TOKEN`. There is one prompt for everything the VLM sees, a document
page with its text layer as support or a picture with none: `api/prompt.md`, replaceable by
mounting your own file and pointing `VLM_PROMPT_FILE` at it. It must carry the placeholders
`{support}` and `{blank}`; the latter is filled from `VLM_BLANK_TOKEN`, the answer the VLM
is instructed to give for a page or image with nothing on it (default `<blank>`; any string
the model reproduces verbatim works), so the instruction and the check can never
disagree. `VLM_CONCURRENCY` (default 8) is the number of pages in flight against the VLM from
the whole process, and `FILE_CONCURRENCY` (default 8) the number of files being parsed at
once, archive members counted one by one - the first is the load on the model (mind its
other users), the second bounds memory (a file's renders live in RAM until its commit). Both
are tuning, not recipe.

The queues live in the database and workers coordinate through row locks, so additional
replicas may run workers too.

## Tests

Six suites under `tests/`, run against a real database and a real api container - see
`tests/README.md`. They build their fixture PDFs with pymupdf, which is why they run in a
separate test image (`tests/Dockerfile`) and never in the product image.

## Not built yet

- Read endpoints for fetching a chunk, a file's chunks, renders and meta by sha, and an
  item by feed and uri; search pagination and context expansion (neighbouring chunks of a
  hit); the shape of the search answer itself (how much text per hit) is still being decided.
- Spreadsheet drawings are read from .xlsx only; other spreadsheet formats keep their cells.
