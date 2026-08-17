# How the Bibliometric Data Is Processed

In one sentence: each dated MySQL dump is kept unchanged, compared with the
previous clean release, converted into stable article records, audited for
additions/changes/removals/merges, and published only after validation.

## The two kinds of row

- A **source row** is one record exactly as observed at one journal endpoint.
- An **article** is the stable API record. Several source rows can belong to the
  same article when different endpoints describe the same work.

The source rows provide provenance. The article rows are what API clients
normally synchronize.

## What happens to a new file

1. **Discover it.** The daily job checks either the PKP Beacon endpoint or the
   configured HTML index. The index scraper looks for links named
   `pkpbeacon-YYYY-MM-DD.sql` or `pkpbeacon-YYYY-MM-DD.sql.gz` and downloads
   every missing dated file.

2. **Validate and retain it.** A gzip is expanded with a size limit. The file
   must identify itself as a MySQL dump, and the completion date inside the
   dump must equal the date in its filename. It is then stored read-only under
   `data/raw/`; raw files are never edited.

3. **Process in date order.** If several files arrived, the oldest unprocessed
   file is processed first. A newly discovered file older than the published
   history is never silently skipped: the job stops and asks for a controlled
   history rebuild.

4. **Build safely beside production.** The dump is imported into a new
   `mysql-YYYY-MM-DD.building` directory. The current API database remains
   untouched. The immediately preceding clean SQL export is loaded to carry
   stable IDs and lifecycle history forward.

5. **Select bibliometric records.** Records are joined to their context,
   endpoint, and ISSN. Only contexts with an ISSN that normalizes to exactly
   eight characters are in scope. The release report counts raw rows and rows
   excluded from this scope.

6. **Extract and normalize metadata.** OAI/DC XML supplies title, creators,
   date, identifiers, DOI, article URL, and the other API fields. Normal monthly
   runs parse only new or changed source rows; `--full-rescan` reparses all rows
   for an audit. Incremental correctness therefore assumes the source updates a
   record timestamp/state field whenever its XML changes. Parsing is split into
   indexed ID ranges and can run in parallel.

7. **Merge exact duplicates.** Matching is deterministic and exact—there is no
   fuzzy score:

   | Key | Normalized value |
   | --- | --- |
   | OAI | endpoint identity + source OAI identifier |
   | DOI | lowercase DOI |
   | URL | normalized OJS/OMP article or book URL |
   | Fingerprint | normalized title + first creator + publication year |

   Key frequency is checked across all present aliases, not just the rows that
   changed in this file. Fingerprints seen more than 25 times and stronger keys
   seen more than 1,000 times are discarded before matching. When a source
   corrects a key, its prior value is retired from future matching. Existing
   published article IDs take priority over IDs from newly arriving aliases.
   If two established articles become connected, the lowest previously
   published article ID wins and every loser becomes a permanent merge
   tombstone pointing to it. A new alias never displaces an already published
   ID merely because its source ID is lower. A later correction prevents new
   false matches but does not automatically split an already published merge;
   that requires a reviewed history rebuild.

8. **Choose the canonical row.** For each article, active and present sources
   are preferred, followed by richer metadata, then the lowest source ID as a
   deterministic tie-breaker. All aliases remain in `ojs_article_sources` so
   the chosen value can be traced back to its origin.

9. **Apply lifecycle rules.** A new article is `added`; changed content or
   provenance is `modified`; a returned article is `restored`. A missing or
   source-removed alias becomes inactive, but the article is only `removed`
   when no active alias remains. Removed and merged article IDs are retained as
   tombstones rather than physically deleted.

10. **Audit and publish.** The build validates row totals, status totals,
    identity assignments, checksums, and provenance. It writes a checksummed
    change report. Large unexpected source loss, article loss, removals, or
    merges block publication for human review. An atomic release manifest is
    written only after the database, export, report, and checksums are complete,
    so an interrupted run is retried safely. A successful clean SQL release is
    imported into a staging schema and all serving tables are swapped in one
    atomic MySQL rename.

## Where changes are reported

For snapshot `YYYY-MM-DD`:

- `data/clean/pkpbeacon-changes-YYYY-MM-DD.json` contains source-row totals,
  ISSN-scope exclusions, missing/title-less/keyless source counts, and article
  counts for `added`, `modified`, `removed`, `restored`, and `merged` events.
- The adjacent `.sha256` file protects the report from accidental corruption.
- `pkpbeacon-release-YYYY-MM-DD.json` is the final completion marker binding the
  clean export and report checksums; files without it are not treated as a
  processed release.
- `data/clean/pkpbeacon-changes-latest.json` points to the current report.
- `ojs_article_events` stores every API-visible event with stable article ID,
  before/after hashes, operation (`upsert` or `delete`), and reason.
- `GET /changes?after_event_id=...` is the supported downstream cursor. It
  returns the actual affected article rows, not only aggregate counts.

The aggregate source categories can overlap—for example, a newly observed row
may already be marked removed by its source. Article event types do not overlap
within one article and snapshot.

## Commands

```bash
# Process every pending local/Beacon snapshot
make update

# Run the complete locked daily check, build, guard, and live publication
make automatic-update

# Reparse the complete history in a new, isolated audit directory
python src/run_pipeline.py --skip-download --clean-dir data/audit-clean \
  --rebuild-history --full-rescan --force-rebuild

# Verify the current database, export, provenance, report, and checksums
make verify
```

`data/audit-clean` must be new and must have enough space for a second build.
Using a separate directory keeps the serving `mysql-current` database and its
release pointers untouched while the audit is running.

The implementation is split between `src/scrape_updates.py` (discovery),
`src/run_pipeline.py` (orchestration and reports),
`sql/01_build_ojs_tables.sql` (bibliometric transformation), and
`src/publish_live.py` (atomic live publication).
