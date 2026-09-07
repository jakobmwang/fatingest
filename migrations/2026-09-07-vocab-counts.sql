-- The vocabulary counts the chunks holding each (surface, term) pair and forgets a pair at
-- zero, so it is at every moment exactly the words the corpus contains. Additive and
-- idempotent: run once against a live database created before this date, with the queue idle
-- (docker exec -i fatingest-db psql -U fatingest -d fatingest < this file). initdb/01-schema.sql
-- already carries the result for new databases.
--
-- Idle queue: the ALTER holds the table until COMMIT, so a chunk inserted meanwhile waits inside
-- the old trigger body, which counts nothing when it resumes - that one chunk's words would be
-- short by one.
BEGIN;
ALTER TABLE vocab ADD COLUMN IF NOT EXISTS n_chunks INTEGER NOT NULL DEFAULT 0;
ALTER TABLE vocab ALTER COLUMN n_chunks DROP DEFAULT;

CREATE OR REPLACE FUNCTION vocab_pairs(t TEXT) RETURNS TABLE(surface TEXT, term TEXT)
LANGUAGE sql IMMUTABLE AS $$
    SELECT DISTINCT v.surface, v.term FROM public.fatingest_vocab(t) v
     WHERE length(v.surface) <= 120 AND length(v.term) <= 80
$$;

CREATE OR REPLACE FUNCTION vocab_collect() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO public.vocab(surface, term, n_chunks)      -- qualified: restores run with search_path ''
    SELECT p.surface, p.term, 1 FROM public.vocab_pairs(NEW.markdown) p
     ORDER BY 1, 2
    ON CONFLICT (surface, term) DO UPDATE SET n_chunks = vocab.n_chunks + 1;
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION vocab_release() RETURNS TRIGGER LANGUAGE plpgsql AS $$
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
DROP TRIGGER IF EXISTS chunks_vocab_release ON chunks;
CREATE TRIGGER chunks_vocab_release AFTER DELETE ON chunks REFERENCING OLD TABLE AS gone
    FOR EACH STATEMENT EXECUTE FUNCTION vocab_release();

-- Reconciliation: the counts from the chunks that exist, and nothing else. One pass over the
-- corpus - the only one there is; from here the triggers keep the count. This is also the
-- recovery should the vocabulary ever be in doubt (a changed fatingest_norm, for one).
UPDATE vocab SET n_chunks = 0;
INSERT INTO vocab(surface, term, n_chunks)
SELECT p.surface, p.term, count(*) FROM chunks c, LATERAL vocab_pairs(c.markdown) p
 GROUP BY 1, 2 ORDER BY 1, 2
ON CONFLICT (surface, term) DO UPDATE SET n_chunks = EXCLUDED.n_chunks;
DELETE FROM vocab WHERE n_chunks <= 0;
COMMIT;
