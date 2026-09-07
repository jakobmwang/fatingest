-- fatingest: content-addressed ingestion and search store.
-- Identity and GC rules are documented in README.md.

CREATE EXTENSION IF NOT EXISTS vectorscale CASCADE;   -- pulls in pgvector
CREATE EXTENSION IF NOT EXISTS pg_textsearch;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- The normalization below relies on [[:alnum:]] covering non-ASCII letters, which
-- PostgreSQL's regex engine only does under a UTF-8-aware LC_CTYPE (e.g. C.UTF-8). Fail
-- loudly at initdb rather than tokenize æ, ö, é as separators for years.
DO $$ BEGIN
    IF NOT ('æöé' ~ '^[[:alnum:]]+$') THEN
        RAISE EXCEPTION 'database LC_CTYPE must be UTF-8 aware: [[:alnum:]] does not match non-ASCII letters';
    END IF;
END $$;

-- ---------------------------------------------------------------------------------------
-- Lexical normalization. The BM25 index and every lexical query see fatingest_norm(text),
-- never the raw markdown, so these rules are the single definition of what a "term" is.
-- They are language-neutral: nothing here interprets decimals, dates or grammar - each
-- rule only decides what stays together. PostgreSQL's parser then tokenizes the result
-- with the built-in 'simple' configuration (lowercase, no stemming, no stopwords). Every
-- non-alphanumeric character it meets is a separator, which is why § and the sha256://
-- scheme have to be rewritten rather than kept. Changing a rule means REINDEX of the BM25
-- index and a reconciliation of vocab (the statement in migrations/2026-09-07-vocab-counts.sql);
-- chunks themselves are untouched (identities never change).
--
--   1. sha256://<hex>      -> <hex>                 our own link syntax: the hex stands alone,
--                                                   so both the bare sha and the full ref match
--   2. § 12 / §12 / § 12 a -> paragraph12 12        whole + number part (§ itself cannot survive)
--   3. 1.234,56 / 14:30    -> 1.234.56 123456       digit clusters glued (the parser keeps x.y.z as one
--                                                   token) plus a digits-only twin, so 1.234,56, 1,234.56
--                                                   and 1234.56 find each other; no digit-group parts
--   4. KMD-1234 / 12-14    -> kmd1234 kmd 1234      hyphen clusters containing a digit: whole + parts,
--                                                   mirroring what the parser itself does for e-mail
--   5. -500                -> -500 500              a real negative number keeps its sign and adds the
--                                                   bare number: "500" finds it, "-500" ranks it higher
--   Stray hyphens (bullets, dashes, "a - b") are separators to the parser already.
-- ---------------------------------------------------------------------------------------

-- Rewrites one matched cluster. 'hyphen': whole + parts when a digit is present, else the
-- cluster is left to the parser; 'number': glued with dots + digits-only twin; 'blank':
-- removes digit-bearing hyphen clusters (fatingest_vocab isolates the plain words with it).
CREATE FUNCTION fatingest_cluster(m TEXT, kind TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN kind = 'hyphen' AND m ~ '\d' THEN replace(m, '-', '') || ' ' || replace(m, '-', ' ')
        WHEN kind = 'blank'  AND m ~ '\d' THEN ' '
        WHEN kind IN ('hyphen', 'blank')  THEN m
        WHEN kind = 'number'              THEN translate(m, ',:', '..') || ' ' || regexp_replace(m, '[.,:]', '', 'g')
    END
$$;

-- Applies fatingest_cluster to every match of `pattern` in `t`, keeping the text between
-- matches (regexp_split_to_array and regexp_matches return the two interleaved halves).
CREATE FUNCTION fatingest_rewrite(t TEXT, pattern TEXT, kind TEXT) RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE parts TEXT[]; matches TEXT[]; result TEXT := ''; i INT;
BEGIN
    parts := regexp_split_to_array(t, pattern);
    matches := ARRAY(SELECT x[1] FROM regexp_matches(t, pattern, 'g') x);
    FOR i IN 1..coalesce(array_length(parts, 1), 0) LOOP
        result := result || parts[i];
        IF i <= coalesce(array_length(matches, 1), 0) THEN
            result := result || public.fatingest_cluster(matches[i], kind);
        END IF;
    END LOOP;
    RETURN result;
END $$;

-- The five cluster patterns, in the order the rules apply. Capture groups are for
-- fatingest_vocab (surface forms); fatingest_norm uses the same patterns. The § letter
-- suffix (§ 15 a) is taken when attached, or spaced only if punctuation or the end
-- follows - so "§ 12 i loven" and "§ 12 e-mail" keep their one-letter words.
-- Literals are cast and calls are schema-qualified: the function is inlined into the BM25
-- index expression, and index builds run with search_path locked to pg_catalog.
CREATE FUNCTION fatingest_norm(t TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
    SELECT regexp_replace(                                                          -- 5
             public.fatingest_rewrite(                                                     -- 4
               public.fatingest_rewrite(                                                   -- 3
                 regexp_replace(                                                    -- 2
                   regexp_replace(t, 'sha256://([0-9a-f]{64})', ' \1 ', 'g'),       -- 1
                   '§+\s*(\d+)(?:([a-zA-Z])\y|\s([a-zA-Z])(?=[.,;:)]|$)|\y)', 'paragraph\1\2\3 \1', 'g'),
                 '\d+(?:[.,:]\d+)+'::text, 'number'::text),
               '[[:alnum:]]+(?:-[[:alnum:]]+)+'::text, 'hyphen'::text),
             '(^|[^[:alnum:]])-(\d+(?:\.\d+)?)', '\1-\2 \2', 'g')
$$;

-- (surface, term) pairs for a text: `surface` is what a reader sees (lowercased, spacing
-- canonical), `term` is the index token it produces. Plain words map to themselves; a
-- cluster maps to every token its rewrite yields (kmd-1234 -> kmd1234, kmd, 1234). The
-- vocabulary is built from this, and the same function turns a query into its surfaces.
CREATE FUNCTION fatingest_vocab(t TEXT) RETURNS TABLE(surface TEXT, term TEXT)
LANGUAGE sql IMMUTABLE AS $$
    WITH clusters(surface) AS (
        SELECT m[1]                                          FROM regexp_matches(t, 'sha256://([0-9a-f]{64})', 'g') m
        UNION ALL
        SELECT '§' || m[1] || lower(coalesce(m[2], m[3], ''))
                                                             FROM regexp_matches(t, '§+\s*(\d+)(?:([a-zA-Z])\y|\s([a-zA-Z])(?=[.,;:)]|$)|\y)', 'g') m
        UNION ALL
        SELECT m[1]                                          FROM regexp_matches(t, '(\d+(?:[.,:]\d+)+)', 'g') m
        UNION ALL
        SELECT lower(m[1])                                   FROM regexp_matches(t, '([[:alnum:]]+(?:-[[:alnum:]]+)+)', 'g') m
         WHERE m[1] ~ '\d'
        UNION ALL
        SELECT m[2]                                          FROM regexp_matches(t, '(^|[^[:alnum:]])(-\d+(?:\.\d+)?)', 'g') m
    ), rest AS (                                             -- the text with every cluster blanked
        SELECT regexp_replace(
                 public.fatingest_rewrite(
                   regexp_replace(
                     regexp_replace(
                       regexp_replace(t, 'sha256://[0-9a-f]{64}', ' ', 'g'),
                       '§+\s*\d+(?:[a-zA-Z]\y|\s[a-zA-Z](?=[.,;:)]|$)|\y)', ' ', 'g'),
                     '\d+(?:[.,:]\d+)+', ' ', 'g'),
                   '[[:alnum:]]+(?:-[[:alnum:]]+)+'::text, 'blank'::text),
                 '(^|[^[:alnum:]])-\d+(?:\.\d+)?', '\1 ', 'g') AS t
    )
    SELECT c.surface, l.lexeme FROM clusters c, unnest(to_tsvector('simple', public.fatingest_norm(c.surface))) l
    UNION
    SELECT l.lexeme, l.lexeme FROM rest, unnest(to_tsvector('simple', public.fatingest_norm(rest.t))) l
$$;

-- Content rows are immutable and content-addressed. A file's progress is a set of milestone
-- dates, each set once when reached and NULL until then - never a phase value that flips
-- from one thing to another. meta holds intrinsic facts only: kind (what the file IS: pdf,
-- office, archive, spreadsheet, text, image, chunkset...), content_type, parser_version,
-- n_members, and the delivery's own facts (source, filename).
CREATE TABLE files (
    sha256      TEXT PRIMARY KEY,
    meta        JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),     -- the row exists: delivered, or unpacked from an archive
    due_at      TIMESTAMPTZ,                            -- queued: NULL for a file never delivered on its own (an
                                                        -- archive member); pushed forward by backoff on a deferral
    started_at  TIMESTAMPTZ,                            -- a parse began
    parsed_at   TIMESTAMPTZ,                            -- the parse concluded: content committed, or the verdict below
    failed_at   TIMESTAMPTZ,                            -- given up: the delivered bytes are gone, only a re-delivery helps
    indexed_at  TIMESTAMPTZ,                            -- every embeddable chunk embedded
    attempts    INTEGER NOT NULL DEFAULT 0,             -- parse attempts that ended in a deferral
    error       TEXT                                    -- what stands in the way: the last deferral's cause while
                                                        -- queued, or the verdict that the content cannot be parsed
);
-- The queue: delivered, not concluded, not given up, due.
CREATE INDEX idx_files_due ON files(due_at) WHERE parsed_at IS NULL AND failed_at IS NULL;
CREATE INDEX idx_files_unindexed ON files(parsed_at) WHERE parsed_at IS NOT NULL AND indexed_at IS NULL;

-- A feed's claim about a uri: meta is what THIS feed says (title, tags, ...), sha256 is
-- the one file the feed delivered for it, touched_at is the liveness signal that keeps
-- the claim out of GC. Many claims may name the same file; a claim names exactly one.
-- meta is free-form except one reserved key: published_at (ISO 8601), which search can
-- rank by; it is validated at ingest, so the cast in file_claims is safe.
CREATE TABLE items (
    feed        TEXT NOT NULL,
    uri         TEXT NOT NULL,
    sha256      TEXT NOT NULL REFERENCES files(sha256),  -- RESTRICT on purpose: GC removes items before files
    meta        JSONB NOT NULL DEFAULT '{}',
    touched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (feed, uri)
);
CREATE INDEX idx_items_touched ON items(touched_at);
CREATE INDEX idx_items_sha ON items(sha256);

-- Relationship rows: what an archive contains. Written when the archive is parsed and
-- replaced as a set on every (re)parse; removed with the archive (cascade). Nested
-- archives are flattened at unpack, so members are always one level deep and a member is
-- never itself an archive; the nesting survives only in the path (a/b.zip/c.txt). Keyed by
-- path, not member: identical content may sit at several paths in one archive.
CREATE TABLE archive_members (
    archive_sha256  TEXT NOT NULL REFERENCES files(sha256) ON DELETE CASCADE,
    path            TEXT NOT NULL,
    member_sha256   TEXT NOT NULL REFERENCES files(sha256),   -- RESTRICT on purpose: GC removes archives before members
    PRIMARY KEY (archive_sha256, path)
);
CREATE INDEX idx_archive_members_member ON archive_members(member_sha256);

CREATE TABLE chunks (
    sha256         TEXT PRIMARY KEY,                    -- over the DELIVERED halves (+ PARSER_VERSION when the
                                                        -- text was generated); see README "Identities"
    markdown       TEXT NOT NULL,                       -- '' for a blank or unparseable chunk
    render_sha256  TEXT,                                -- render as DELIVERED (NULL = text-only; a synthetic render
                                                        -- is generated as embedding input only)
    meta           JSONB NOT NULL DEFAULT '{}',         -- derived, never identity: status ok|blank|unparseable,
                                                        -- error; for pages visual (0..1) and text_chars
    embedding      vector(1024)                         -- must match EMBED_DIM in the API service; stays NULL for
                                                        -- chunks whose status is not ok (nothing to embed)
);
CREATE INDEX idx_chunks_unembedded ON chunks(sha256) WHERE embedding IS NULL AND meta->>'status' = 'ok';
-- The diskann and bm25 indexes live in 02-indexes.sql. There is deliberately no trigram
-- index on the corpus: fuzzy matching and regex run against the vocabulary below.

-- The vocabulary: every (surface, term) pair the corpus holds right now, and how many
-- chunks hold it. Fuzzy matching and regex run here, against what readers see (surface),
-- and translate to index terms - so their cost follows the vocabulary (sublinear in the
-- corpus), not the corpus. The events keep it exact, the way renders are kept: a chunk's
-- insertion counts its pairs up, its deletion (GC) counts them down, and a pair no chunk
-- holds is gone - the vocabulary never offers a word the corpus does not contain.
CREATE TABLE vocab (
    surface  TEXT NOT NULL,
    term     TEXT NOT NULL,
    n_chunks INTEGER NOT NULL,                          -- chunks holding this pair; a row at zero does not exist
    PRIMARY KEY (surface, term)
);
CREATE INDEX idx_vocab_surface_trgm ON vocab USING gist(surface gist_trgm_ops);

-- The pairs one chunk contributes: each once, within the lengths the vocabulary keeps. Both
-- triggers and the reconciliation in migrations/ count through this one function, so up and
-- down are mirror images.
CREATE FUNCTION vocab_pairs(t TEXT) RETURNS TABLE(surface TEXT, term TEXT)
LANGUAGE sql IMMUTABLE AS $$
    SELECT DISTINCT v.surface, v.term FROM public.fatingest_vocab(t) v
     WHERE length(v.surface) <= 120 AND length(v.term) <= 80
$$;

-- Rows are locked in one fixed order, (surface, term), on both the counting-up and the
-- counting-down side, so concurrent parse workers and the GC can never deadlock on the
-- vocabulary.
CREATE FUNCTION vocab_collect() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO public.vocab(surface, term, n_chunks)      -- qualified: restores run with search_path ''
    SELECT p.surface, p.term, 1 FROM public.vocab_pairs(NEW.markdown) p
     ORDER BY 1, 2
    ON CONFLICT (surface, term) DO UPDATE SET n_chunks = vocab.n_chunks + 1;
    RETURN NULL;
END $$;
CREATE TRIGGER chunks_vocab AFTER INSERT ON chunks FOR EACH ROW EXECUTE FUNCTION vocab_collect();

-- Once per DELETE statement, over everything it removed (GC deletes chunks in one batch):
-- each pair is counted down by the number of gone chunks that held it, and a row at zero
-- goes. The rows are locked in order (MATERIALIZED: the ordered locking happens as such, not
-- folded into the update's join) and updated afterwards; the delete only ever meets rows this
-- statement has just zeroed, since no other transaction leaves a row at zero behind.
CREATE FUNCTION vocab_release() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    WITH held AS (
        SELECT p.surface, p.term, count(*) AS n
          FROM gone g, LATERAL public.vocab_pairs(g.markdown) p
         GROUP BY 1, 2
    ), locked AS MATERIALIZED (
        SELECT v.surface, v.term, h.n
          FROM public.vocab v JOIN held h ON h.surface = v.surface AND h.term = v.term
         ORDER BY 1, 2 FOR UPDATE OF v
    )
    UPDATE public.vocab v SET n_chunks = v.n_chunks - l.n
      FROM locked l WHERE v.surface = l.surface AND v.term = l.term;
    DELETE FROM public.vocab WHERE n_chunks <= 0;
    RETURN NULL;
END $$;
CREATE TRIGGER chunks_vocab_release AFTER DELETE ON chunks REFERENCING OLD TABLE AS gone
    FOR EACH STATEMENT EXECUTE FUNCTION vocab_release();

-- Relationship rows: a file's current chunk collection, replaced as a set whenever the
-- file is (re)parsed or (re)delivered.
CREATE TABLE files_chunks (
    file_sha256   TEXT NOT NULL REFERENCES files(sha256) ON DELETE CASCADE,
    idx           INTEGER NOT NULL,                     -- 0-based position, not a page number
    chunk_sha256  TEXT NOT NULL REFERENCES chunks(sha256),
    meta          JSONB NOT NULL DEFAULT '{}',          -- what this position in this file says about the chunk, when it
                                                        -- can differ between files holding the same chunk: e.g. error,
                                                        -- why this page could not be rendered
    PRIMARY KEY (file_sha256, idx)                      -- (file, idx), NOT (file, chunk): identical pages may repeat
);
CREATE INDEX idx_files_chunks_chunk ON files_chunks(chunk_sha256);

-- Every claim a file serves: directly, or through an archive that contains it. This is
-- how search attributes a chunk to (feed, uri) and finds its published_at. Members are
-- one level deep, so no recursion; UNION (not ALL) because one archive may hold the same
-- member at two paths.
CREATE VIEW file_claims AS
    SELECT i.sha256, i.feed, i.uri, (i.meta->>'published_at')::timestamptz AS published_at
      FROM items i
    UNION
    SELECT am.member_sha256, i.feed, i.uri, (i.meta->>'published_at')::timestamptz
      FROM archive_members am JOIN items i ON i.sha256 = am.archive_sha256;

-- Purely referential GC; run periodically (API: POST /v1/gc). The order is everything:
-- stale claims -> unreferenced files -> unreferenced chunks (cascade: files_chunks).
-- A file is referenced by a claim or by an archive that contains it. Deleting an archive
-- cascades its archive_members, which orphans its members - but only as seen by the NEXT
-- statement, so the file sweep repeats until it removes nothing. Members are one level
-- deep, so that is two rounds; the loop states the invariant instead of assuming it.
-- SKIP LOCKED: a row a worker holds (being parsed, being embedded) is left for the next
-- run - GC never waits on a worker, like everything else on the request path.
-- Explicit deletions and replacements leave orphans behind immediately, so they propagate
-- at GC cadence; only silent disappearance at the source waits out the stale interval.
-- Returns what it removed, and what the store may now drop: the shas of the files that went
-- (their spool entries) and the render shas of the chunks that went (the API deletes the
-- render files no remaining chunk points to - a render can be shared across recipes).
CREATE OR REPLACE FUNCTION fatingest_gc(stale INTERVAL) RETURNS JSONB
LANGUAGE plpgsql AS $$
DECLARE n_i BIGINT; n_f BIGINT := 0; n_c BIGINT; n BIGINT;
        gone_files TEXT[] := '{}'; batch TEXT[]; renders TEXT[];
BEGIN
    DELETE FROM items WHERE touched_at < now() - stale;
    GET DIAGNOSTICS n_i = ROW_COUNT;
    LOOP
        WITH gone AS (
            DELETE FROM files WHERE sha256 IN (
                SELECT f.sha256 FROM files f
                 WHERE NOT EXISTS (SELECT 1 FROM items i WHERE i.sha256 = f.sha256)
                   AND NOT EXISTS (SELECT 1 FROM archive_members am WHERE am.member_sha256 = f.sha256)
                 FOR UPDATE SKIP LOCKED)
            RETURNING sha256)
        SELECT count(*), coalesce(array_agg(sha256), '{}') INTO n, batch FROM gone;
        n_f := n_f + n;
        gone_files := gone_files || batch;
        EXIT WHEN n = 0;
    END LOOP;
    WITH gone AS (
        DELETE FROM chunks WHERE sha256 IN (
            SELECT c.sha256 FROM chunks c
             WHERE NOT EXISTS (SELECT 1 FROM files_chunks fc WHERE fc.chunk_sha256 = c.sha256)
             FOR UPDATE SKIP LOCKED)
        RETURNING render_sha256)
    SELECT count(*), coalesce(array_agg(DISTINCT render_sha256) FILTER (WHERE render_sha256 IS NOT NULL), '{}')
      INTO n_c, renders FROM gone;
    RETURN jsonb_build_object('items', n_i, 'files', n_f, 'chunks', n_c,
                              'gone_files', to_jsonb(gone_files), 'gone_renders', to_jsonb(renders));
END $$;
