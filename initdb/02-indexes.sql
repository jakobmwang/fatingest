-- Syntax verified against timescale/timescaledb-ha:pg17.
CREATE INDEX idx_chunks_embedding ON chunks USING diskann(embedding vector_cosine_ops);
-- BM25 over the NORMALIZED text (see fatingest_norm in 01-schema.sql); every lexical query
-- goes through the same function. 'simple' = vanilla BM25: no stemming, no stopwords, exact
-- tokens - identifiers, names and codes survive, and the config is language-neutral.
-- Changing fatingest_norm means REINDEX here and TRUNCATE + rebuild of vocab.
CREATE INDEX idx_chunks_markdown_bm25 ON chunks USING bm25((fatingest_norm(markdown))) WITH (text_config='simple');
