# Tests

Six suites, all plain Python scripts that print `PASS`/`FAIL` per check and a summary line.
They run against a real Postgres (a separate database on the same server) and, for the
end-to-end suites, a real api container with its VLM, embedding and Gotenberg dependencies -
nothing is mocked except where a suite says so. Fixtures are built with pymupdf, so the
suites run in the test image below, never in the product image.

| suite | what it covers | needs |
|---|---|---|
| `test_unit.py` | queue mechanics (row locks, backoff, verdicts), the page engine's deterministic half (visual share, annotations, text layer, link marks, threshold routing), the prompt | database only, no live api |
| `test_pages.py` | per-page verdicts end to end: text page, blank page, picture page, embedding, search | live api with VLM |
| `test_links_e2e.py` | a link on a page the VLM transcribes comes back with its real address | live api with VLM |
| `test_main.py` | deliveries, claims, archives, pings, GC, office conversion, concurrent workers | live api with VLM and Gotenberg |
| `test_search.py` | normalization, vocabulary, fuzzy, regex, published_at, recency, fusion | live api with embeddings |
| `test_defer.py` | deferred deliveries and backoff against an api whose Gotenberg is unreachable | live api with `GOTENBERG_URL` pointing at nothing |

## Setup

A test database on the running Postgres, with the same schema:

```sh
docker exec fatingest-db psql -U fatingest -d fatingest -c "CREATE DATABASE fatingest_test"
docker exec -i fatingest-db psql -U fatingest -d fatingest_test < initdb/01-schema.sql
docker exec -i fatingest-db psql -U fatingest -d fatingest_test < initdb/02-indexes.sql
```

The test image (the product image plus pymupdf):

```sh
docker compose build api                       # -> fatingest-api:dev
docker build -t fatingest-api:test tests/
```

`DBURL` below is `postgresql://fatingest:<password>@db:5432/fatingest_test`.

## Unit suite

Runs with no api container on the test database: it drives the queue itself, and a live
worker on the same database would take its rows. Stop any test api container first.

```sh
docker run --rm -i --env-file .env -e FATINGEST_WORKERS=0 -e STORE=/tmp/store \
  -e DATABASE_URL=$DBURL --network fatingest_dbnet fatingest-api:test python - < tests/test_unit.py
```

## End-to-end suites

A test api container on the test database, with its own render store and the same networks
as the product container:

```sh
mkdir -p /tmp/fatingest-test-store
docker run -d --name fatingest-test --env-file .env -e DATABASE_URL=$DBURL -e STORE=/store \
  -e GOTENBERG_URL=http://gotenberg:3000 -e GOTENBERG_TIMEOUT=300 \
  -v /tmp/fatingest-test-store:/store --network fatingest_dbnet fatingest-api:test
docker network connect fatingest_convertnet fatingest-test
docker network connect <your proxy network> fatingest-test      # where the VLM gateway lives, if any

for t in test_pages test_links_e2e test_main test_search; do
  docker exec -i fatingest-test python - < tests/$t.py
done
```

`test_defer.py` needs an api whose Gotenberg is unreachable: stop the container above first
(its workers would take the suite's deliveries and, finding no spool entry in their own store,
give them up), then start a second container with `-e GOTENBERG_URL=http://127.0.0.1:9
-e GOTENBERG_HEALTH_WAIT=2` and run the suite in it.

Every fixture carries a per-run suffix, so the suites can be re-run without cleanup; the
unit suite removes its own rows. Afterwards: `docker rm -f fatingest-test` and
`DROP DATABASE fatingest_test`.
