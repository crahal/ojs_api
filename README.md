# ojs_api

End-to-end pipeline for turning a large Open Journal Systems MySQL dump into:

1. table-level parquet files
2. one wide denormalized joined parquet file
3. one conservative deduplicated entity parquet file
4. a read-only FastAPI service over that deduplicated file

The repo is built for large local files. The scripts use DuckDB, PyArrow, and Splink in ways that stay bounded in memory and avoid loading the full dump into pandas.

## Workflow

From repo root, the pipeline is:

```bash
python src/0_convert_db.py
python src/1_join_raw.py
python src/2_deduplicate.py
python src/3_build_api.py
```

If you want all Python dependencies up front:

```bash
python -m pip install -r requirements.txt
```

## Repository Layout

- [src/0_convert_db.py](/home/jinx/Dropbox/ojs_api/src/0_convert_db.py:1): stream the OJS SQL dump into per-table parquet
- [src/1_join_raw.py](/home/jinx/Dropbox/ojs_api/src/1_join_raw.py:1): build one wide denormalized joined parquet file
- [src/2_deduplicate.py](/home/jinx/Dropbox/ojs_api/src/2_deduplicate.py:1): conservatively deduplicate the joined file with Splink
- [src/3_build_api.py](/home/jinx/Dropbox/ojs_api/src/3_build_api.py:1): serve the deduplicated file as a read-only FastAPI service
- [notebooks/3_build_api_demo.ipynb](/home/jinx/Dropbox/ojs_api/notebooks/3_build_api_demo.ipynb:1): notebook showing how to call the API
- [Dockerfile](/home/jinx/Dropbox/ojs_api/Dockerfile:1): API container image
- [docker-compose.yml](/home/jinx/Dropbox/ojs_api/docker-compose.yml:1): API container deployment example
- [requirements.txt](/home/jinx/Dropbox/ojs_api/requirements.txt:1): Python dependencies for the project

## Inputs And Outputs

Main input:

- `data/raw_sql/database.sql`
- `data/raw_sql/database.sql.gz`

Main outputs:

- `data/raw_parquet/*.parquet`
- `data/raw/jonied.parquet`
- `clean/deduplicated.parquet`

Each major stage also writes a JSON summary sidecar.

## Step 0: SQL Dump To Table Parquet

Script:

- [src/0_convert_db.py](/home/jinx/Dropbox/ojs_api/src/0_convert_db.py:1)

Default run:

```bash
python src/0_convert_db.py
```

Compressed dump:

```bash
python src/0_convert_db.py --input data/raw_sql/database.sql.gz
```

Safer first run on one table:

```bash
python src/0_convert_db.py --tables contexts
```

Resume a partial run without rewriting completed parquet files:

```bash
python src/0_convert_db.py --skip-existing
```

Faster bounded-memory run:

```bash
python src/0_convert_db.py \
  --batch-rows 25000 \
  --max-batch-bytes 134217728 \
  --report-every 0 \
  --progress-refresh-seconds 5 \
  --skip-existing
```

What it writes:

- one parquet file per table under `data/raw_parquet/`
- `data/raw_parquet/_table_stats.parquet`
- `data/raw_parquet/_table_stats.json`
- `data/raw_parquet/_column_stats.parquet`

Important safety notes:

- the dump is streamed instead of pre-scanned
- `--max-batch-bytes` is the real protection against oversized Arrow flushes
- if a large flush fails, the writer retries that batch in smaller chunks instead of discarding the run
- rerunning with `--skip-existing` preserves finished tables

## Step 1: Join The Raw Tables

Script:

- [src/1_join_raw.py](/home/jinx/Dropbox/ojs_api/src/1_join_raw.py:1)

It keeps the Jonas join structure, but this stage does not filter on `group_id` or `removed_at`:

```sql
SELECT c.id, d.issn, b.name
FROM records c
INNER JOIN contexts b ON c.context_id = b.id
INNER JOIN issns d ON b.id = d.context_id
INNER JOIN endpoints e ON b.endpoint_id = e.id
```

The important change is the output projection. The script now writes a wide denormalized parquet, not just the three selected Jonas columns. By default it includes:

- record fields with `record_` prefixes
- context fields with `context_` prefixes
- ISSN fields with `issn_` prefixes
- endpoint fields with `endpoint_` prefixes
- compatibility columns `id`, `issn`, and `name` for the downstream dedupe step

By default it does not include `records.metadata`, because that single column is the biggest size and runtime multiplier. If you explicitly want it in the flat file, opt in with `--include-record-metadata`.

When `--include-record-metadata` is enabled, the script automatically switches to a streaming Arrow-batch writer and clamps DuckDB to one worker thread by default. That path is slower, but it is there specifically to avoid the out-of-memory failure you hit with the one-shot parquet `COPY`.

Run:

```bash
python src/1_join_raw.py
```

Rebuild an existing output with the new wide projection:

```bash
python src/1_join_raw.py --overwrite
```

If you explicitly want the `records.metadata` blob carried through too:

```bash
python src/1_join_raw.py --overwrite --include-record-metadata
```

If you want even smaller in-memory batches in metadata mode:

```bash
python src/1_join_raw.py --overwrite --include-record-metadata --stream-batch-rows 5000
```

What it writes:

- `data/raw/jonied.parquet`
- `data/raw/jonied.parquet.summary.json`

Why it is safe:

- uses DuckDB parquet-to-parquet instead of pandas
- uses an explicit projected column list instead of materializing whole source tables in Python
- writes to a temp parquet first, then renames
- defaults to `memory_limit=1GB`
- logs timestamped input and output shape/size stats
- excludes `records.metadata` unless you opt in, which keeps the default flat file materially smaller
- streams metadata-mode output in bounded Arrow batches instead of relying on one monolithic parquet export

Useful options:

- `--input-dir PATH`
- `--output PATH`
- `--overwrite`
- `--include-record-metadata`
- `--stream-batch-rows N`
- `--memory-limit TEXT`
- `--threads N`
- `--temp-dir PATH`
- `--no-progress`

## Step 2: Deduplicate Conservatively

Script:

- [src/2_deduplicate.py](/home/jinx/Dropbox/ojs_api/src/2_deduplicate.py:1)

Run:

```bash
python src/2_deduplicate.py
```

What it writes:

- `clean/deduplicated.parquet`
- `clean/deduplicated.parquet.summary.json`

What the dedupe does:

- reduces the joined raw rows to distinct journal observations first
- uses Splink with DuckDB
- uses only exact matching (no fuzzy scoring)
- prefers metadata identity fields when present (`metadata_title`, `metadata_identifier`, `metadata_article_url`)

Conservative merge rules:

- exact normalized title matches
- exact normalized paper-level identifier matches
- exact normalized article URL matches
- matching uses OR logic across these keys
- title matching has a safety cap via `--max-title-block-size` to avoid huge pair explosions on very common titles

Output columns:

- `dedupe_id`
- `canonical_id`
- `canonical_issn`
- `canonical_name`
- `canonical_issn_key`
- `canonical_name_key`
- `source_row_count`
- `journal_observation_count`
- `is_merged_cluster`
- `matched_rules`
- `matched_observation_ids`
- `source_ids`
- `all_issns`
- `all_names`

Useful options:

- `--input PATH`
- `--output PATH`
- `--overwrite`
- `--memory-limit TEXT`
- `--threads N`
- `--temp-dir PATH`
- `--no-progress`
- `--max-title-block-size N`

## Step 3: Build And Serve The API

Script:

- [src/3_build_api.py](/home/jinx/Dropbox/ojs_api/src/3_build_api.py:1)

This is a read-only FastAPI service over `clean/deduplicated.parquet`.

Run locally:

```bash
python src/3_build_api.py
```

By default it serves:

- host: `127.0.0.1`
- port: `8000`
- input file: `clean/deduplicated.parquet`

Default credentials:

- username: `admin`
- password: `OJSpassword`

You can override the credentials without editing code:

```bash
export OJS_API_USERNAME=admin
export OJS_API_PASSWORD='choose-a-better-password'
python src/3_build_api.py
```

Important note:

- there is no rate limiting layer in this API

Useful options:

- `--input PATH`
- `--host HOST`
- `--port PORT`
- `--memory-limit TEXT`
- `--threads N`
- `--temp-dir PATH`
- `--oai-page-size N`

### API Endpoints

Open endpoints:

- `GET /health`
- `GET /oai`

Authenticated endpoints:

- `GET /`
- `GET /meta`
- `GET /entities`
- `GET /entities/{dedupe_id}`

Built-in docs:

- `GET /docs`
- `GET /openapi.json`

### Authentication

The API uses HTTP Basic Auth.

Example with `curl`:

```bash
curl -u admin:OJSpassword http://127.0.0.1:8000/health
```

### Example API Calls

Health check:

```bash
curl http://127.0.0.1:8000/health
```

API metadata:

```bash
curl -u admin:OJSpassword http://127.0.0.1:8000/meta
```

First page of entities:

```bash
curl -u admin:OJSpassword \
  "http://127.0.0.1:8000/entities?limit=10&sort_by=canonical_name&sort_order=asc"
```

Search by name or ISSN substring:

```bash
curl -u admin:OJSpassword \
  "http://127.0.0.1:8000/entities?q=journal&limit=10"
```

Filter to merged title matches:

```bash
curl -u admin:OJSpassword \
  "http://127.0.0.1:8000/entities?merged_only=true&match_rule=exact_normalized_title&limit=10"
```

Fetch one entity:

```bash
curl -u admin:OJSpassword \
  "http://127.0.0.1:8000/entities/1"
```

### `/entities` Query Parameters

- `q`: substring search on canonical name or canonical ISSN
- `issn`: exact ISSN filter after normalization
- `merged_only`: `true` or `false`
- `match_rule`: for example `exact_normalized_title`, `exact_paper_identifier`, or `exact_article_url`
- `limit`: page size, default `100`, max `1000`
- `offset`: pagination offset
- `sort_by`: `dedupe_id`, `canonical_id`, `canonical_name`, `canonical_issn`, `source_row_count`, `journal_observation_count`
- `sort_order`: `asc` or `desc`

### OAI-PMH Endpoint

The API also exposes an OAI-PMH endpoint at `GET /oai`.

Supported verbs:

- `Identify`
- `ListMetadataFormats`
- `ListSets`
- `ListIdentifiers`
- `ListRecords`
- `GetRecord`

Supported `metadataPrefix` values:

- `ojs_issn`: one OAI record per unique ISSN
- `ojs_entity`: full deduplicated entity payload (`*` fields)

How to list unique ISSNs:

```bash
curl "http://127.0.0.1:8000/oai?verb=ListIdentifiers&metadataPrefix=ojs_issn"
```

How to query one ISSN at a time:

```bash
curl "http://127.0.0.1:8000/oai?verb=ListRecords&metadataPrefix=ojs_entity&set=issn:12345678"
```

How to get `*` for entity records through OAI:

```bash
curl "http://127.0.0.1:8000/oai?verb=ListRecords&metadataPrefix=ojs_entity"
```

Fetch one full entity by OAI identifier:

```bash
curl "http://127.0.0.1:8000/oai?verb=GetRecord&metadataPrefix=ojs_entity&identifier=oai:ojs-api:entity:1"
```

## Jupyter Notebook

Notebook:

- [notebooks/3_build_api_demo.ipynb](/home/jinx/Dropbox/ojs_api/notebooks/3_build_api_demo.ipynb:1)

It shows:

- health check
- metadata retrieval
- list entities
- name search
- merged-cluster filtering
- fetch one entity by `dedupe_id`

Start the API first, then open the notebook in Jupyter:

```bash
jupyter --version >/dev/null 2>&1 || python -m pip install jupyter
jupyter notebook notebooks/3_build_api_demo.ipynb
```

## Docker

Files:

- [Dockerfile](/home/jinx/Dropbox/ojs_api/Dockerfile:1)
- [docker-compose.yml](/home/jinx/Dropbox/ojs_api/docker-compose.yml:1)
- [scripts/compose_start.sh](/home/jinx/Dropbox/ojs_api/scripts/compose_start.sh:1)
- [requirements.txt](/home/jinx/Dropbox/ojs_api/requirements.txt:1)

### Build The Image

```bash
docker build -t ojs-api .
```

### Run Pipeline + API With Compose

`docker compose up` now runs the full `src` pipeline in this order:

1. `src/0_convert_db.py`
2. `src/1_join_raw.py`
3. `src/2_deduplicate.py`
4. `src/3_build_api.py`

Default command:

```bash
docker compose up --build
```

Compose mounts `./data` and `./clean` read-write into the container, so the pipeline outputs are persisted on the host.

Required host input before first run:

- `data/raw_sql/database.sql` or
- `data/raw_sql/database.sql.gz`

Watch logs:

```bash
docker compose logs -f ojs-api
```

Stop:

```bash
docker compose down
```

### Compose Runtime Flags

You can tune behavior with environment variables in `docker-compose.yml`:

- `OJS_CONVERT_ARGS`: args for `0_convert_db.py`
- `OJS_JOIN_ARGS`: args for `1_join_raw.py`
- `OJS_DEDUPE_ARGS`: args for `2_deduplicate.py`
- `OJS_API_ARGS`: args for `3_build_api.py`
- `OJS_FORCE_CONVERT=1`: force rerun step 0 even if required parquet files exist
- `OJS_FORCE_JOIN=1`: force rerun step 1 even if `data/raw/jonied.parquet` exists
- `OJS_FORCE_DEDUPE=1`: force rerun step 2 even if `clean/deduplicated.parquet` exists

Force a full recompute once:

```bash
OJS_FORCE_CONVERT=1 OJS_FORCE_JOIN=1 OJS_FORCE_DEDUPE=1 docker compose up --build
```

### API-Only Container Mode

If you already have `clean/deduplicated.parquet` and only want to serve the API:

```bash
docker run \
  --rm \
  -p 8000:8000 \
  -e OJS_API_USERNAME=admin \
  -e OJS_API_PASSWORD=OJSpassword \
  -v "$(pwd)/clean:/app/clean:ro" \
  ojs-api \
  python src/3_build_api.py \
  --input /app/clean/deduplicated.parquet \
  --host 0.0.0.0 \
  --port 8000
```

## Deployment Notes

If you want to deploy this on a server:

1. copy this repo to the server
2. copy SQL dump to `data/raw_sql/database.sql` (or `.gz`)
3. set secure credentials via `OJS_API_USERNAME` and `OJS_API_PASSWORD`
4. run `docker compose up --build`
5. wait for pipeline completion, then query API at `http://<host>:8000`

Recommended production changes:

- override `OJS_API_PASSWORD`
- bind to `0.0.0.0` only when you actually want remote access
- put a reverse proxy in front of the container if you want TLS or domain routing
- keep the API read-only

## What I Verified In This Workspace

Verified directly:

- `src/0_convert_db.py` compiles
- `src/1_join_raw.py` compiles
- `src/2_deduplicate.py` compiles and runs end-to-end on a bounded synthetic parquet sample
- `src/3_build_api.py` compiles
- the API query layer over the deduplicated parquet works against a synthetic sample

Not verified here:

- full rerun of the real `123G` SQL dump
- full real join output, because `data/raw/jonied.parquet` is not present in this workspace
- live HTTP requests across separate sandboxed processes, because this tool environment blocks local socket connections between sessions
