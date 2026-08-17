# ojs_api

Reproducible SQL pipeline and read-only API for monthly PKP Beacon dumps.

The project retains immutable raw MySQL SQL, builds a temporal deduplicated
article catalogue from every available snapshot, emits portable clean SQL, and
serves current records plus additions, modifications, removals, restorations,
and merges.

See:

- [PROCESSING_GUIDE.md](PROCESSING_GUIDE.md) for a short, exact explanation of
  how the bibliometric records are filtered, normalized, deduplicated, tracked,
  and reported.
- [HOW_TO_DEPLOY.md](HOW_TO_DEPLOY.md) for server installation, secrets,
  initial backfill, Compose, TLS, and monthly scheduling.
- [LIGHTSAIL_DEPLOYMENT.md](LIGHTSAIL_DEPLOYMENT.md) for a copy-paste AWS
  Lightsail deployment using the conservative 16 GB single-host profile.
- [HOW_TO_CALL.md](HOW_TO_CALL.md) for REST bootstrap, event-cursor
  synchronization, provenance, and OAI-PMH harvesting.

## Data layout

```text
data/
  raw/
    pkpbeacon-YYYY-MM-DD.sql
    pkpbeacon-latest.sql -> newest dated raw dump
  clean/
    pkpbeacon-clean-YYYY-MM-DD.sql.gz
    pkpbeacon-clean-YYYY-MM-DD.sql.gz.sha256
    pkpbeacon-clean-latest.sql.gz -> newest clean export
    pkpbeacon-changes-YYYY-MM-DD.json
    pkpbeacon-changes-YYYY-MM-DD.json.sha256
    pkpbeacon-changes-latest.json -> newest row-change report
    pkpbeacon-release-YYYY-MM-DD.json  # atomic completion manifest
    mysql-YYYY-MM-DD/
    mysql-current -> newest validated serving database
```

Raw dumps are never modified. The compressed clean files contain SQL only; no
Parquet or other generated data format is used.

## Pipeline

Create the API client key and the separate SELECT-only database password, then
set the MySQL root password used only by the builder/publisher and MySQL:

```bash
make credentials
export MYSQL_ROOT_PASSWORD=replace-with-a-long-random-value
```

Run a normal monthly update:

```bash
make update
```

For an already-running deployment, use the unattended wrapper. It performs a
cheap no-op when PKP has no newer snapshot and atomically publishes a new
release without restarting MySQL or the API:

```bash
make automatic-update
```

For a dated-file archive or placeholder website, set `OJS_SOURCE_INDEX_URL` to
an HTML page linking `pkpbeacon-YYYY-MM-DD.sql[.gz]`. The scraper downloads all
missing files; a late historical file blocks publication instead of being
silently skipped. The deployment guide includes the once-daily cron entry.

Rebuild every local raw snapshot in chronological order on a fresh install:

```bash
make rebuild-history
```

On an already deployed server, rebuild into a separate `--clean-dir`; the
pipeline deliberately refuses to replace the database currently targeted by
`mysql-current`. The deployment guide gives the controlled replay command.

Verify the current database, clean export, provenance, and counts:

```bash
make verify
```

`make verify` cold-starts the versioned MySQL directory. Run it before Compose
is started or while the serving MySQL container is stopped; unattended monthly
updates perform their own validation without touching the mounted directory.

The downloader reads the Beacon credential from
`~/.config/ojs-api/beacon.ini`, which must be outside the repository and mode
`600`:

```ini
[beacon]
username = beacon-research
password = ...
```

The complete pipeline performs:

1. Authenticated, resumable download and gzip validation.
2. Atomic publication of a dated raw SQL dump.
3. Import into a fresh MySQL 8.4 data directory while hashing the source.
4. Import of the immediately preceding clean SQL state.
5. Parallel, range-sharded XML extraction for new or changed source records.
6. Indexed deterministic identity matching and stable-ID merging.
7. Temporal source, article, event, and snapshot updates.
8. Count, identity, provenance, and row-change validation.
9. Checksummed JSON reporting plus configurable mass-loss/merge release guards.
10. Deterministic gzip export of clean SQL plus SHA-256.
11. Atomic release-manifest commit and advancement of current raw, clean, and
    serving pointers; an interrupted pointer update is repaired on the next run.

If no prior clean state exists, all dated raw snapshots are processed in order.
Monthly matching scans a compact source index but parses XML only for records
whose Beacon/OAI timestamps or source state changed. `--full-rescan` is
available for periodic audits. `OJS_METADATA_WORKERS` controls extraction
parallelism and defaults to 1 in the 16 GB deployment environment;
`--resume-building` can continue an interrupted build from its validated
post-index checkpoint.

Each successful snapshot reports source additions/changes/removals and exact
article `added`, `modified`, `removed`, `restored`, and `merged` event counts.
An anomalous release is quarantined until an operator reviews its report and
uses the explicit override.

## Temporal SQL model

- `ojs_articles`: canonical current records and persistent tombstones
- `ojs_article_sources`: source aliases and source-level lifecycle
- `ojs_article_keys`: reconciled exact-key lookup for current retained aliases
- `ojs_article_events`: append-only downstream change cursor
- `ojs_snapshots`: source hashes, build hashes, and release counts
- `ojs_pipeline_metadata`: current runtime build provenance

Month-to-month identity is anchored by the stable Beacon source record and a
scoped OAI identifier. Cross-source matching uses exact normalized DOI, OJS/OMP
article URL, and a conservative title/first-author/year fingerprint. Matching
is blocked by indexed keys and never performs an all-pairs comparison. A
corrected source row replaces its old keys for future matching. Key frequency
is measured across all currently present aliases; fingerprints shared by more
than 25 sources and stronger keys shared by more than 1,000 are quarantined
from matching and from the persistent lookup.

An article becomes removed only when all known source aliases are inactive. A
duplicate alias disappearing modifies provenance without deleting an article
that still has an active source. When two established entities merge, the
lowest previously published article ID wins, a new lower source ID cannot
displace it, and every loser remains as a permanent redirect tombstone. Merges
are intentionally not split automatically: a suspected false merge requires a
reviewed history rebuild from before the bad bridge.

## API

Generate the private client and database secrets, copy `.env.example` to `.env`,
replace its placeholders, build the first release, and start:

```bash
make credentials
make update
docker compose up -d --build
```

The API connects as the separately provisioned `ojs_api` account, which has
`SELECT` only on the four serving tables. It never receives the MySQL root
password.

Primary endpoints:

- `/health`
- `/meta`
- `/snapshots`
- `/articles`
- `/articles/{article_id}`
- `/articles/{article_id}/sources`
- `/changes`
- `/oai`

Every REST and OAI-PMH data call uses HTTP Basic authentication, with the API
key as the password. OAI-PMH implements all six verbs and returns XML. Only
`/health` is unauthenticated; it contains no catalogue data. No application
rate limiter is configured. The generated OpenAPI routes are disabled on the
deployed service; use `HOW_TO_CALL.md` as the caller reference.

## Tests

```bash
make test
```

The integration fixture runs three real MySQL snapshots and verifies additions,
metadata/provenance modifications, duplicate merges, removals, restorations,
clean SQL export/import, and stable tombstones.
