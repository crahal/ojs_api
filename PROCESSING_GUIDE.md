# How the Bibliometric Data Is Processed

In one sentence: each incoming MySQL dump is checked unchanged, compared with the
previous clean release, converted into stable article records, audited for
additions/changes/removals/merges, and published only after validation.

## The two kinds of row

- A **source row** is one record exactly as observed at one journal endpoint.
- An **article** is the stable API record. Several source rows can belong to the
  same article when different endpoints describe the same work.

The source rows provide provenance. The article rows are what API clients
normally synchronize.

## What happens to a new file

1. **Discover it.** The daily cron checks the authenticated PKP Beacon gzip
   endpoint. If its HTTP validators are unchanged, there is no download or
   rebuild. An optional standalone HTML scraper is also available for archives.
   Before source-changing work, the coordinator, direct pipeline and both
   downloaders acquire the same raw-directory `.source-activity.lock`. Downloads
   and processing are mutually exclusive for that directory. A busy invocation
   exits successfully with `source_activity_busy`, without queueing work or
   caching a new dump; the next daily check retries. Beacon's read-only `--check`
   can still inspect HEAD/local status without GET requests or file changes.
   The HTML scraper's `--check` may GET its index page, but does not download
   data archives or write files.

2. **Validate it without expanding it on disk.** Read the gzip in bounded
   chunks, check its CRC, hash both compressed and SQL bytes, and read the SQL
   completion date. Save one immutable `pkpbeacon-YYYY-MM-DD.sql.gz` with that
   footer date. HTTP Last-Modified may differ; the saved metadata records the
   relationship. Same-date changed bytes stop for review.

3. **Process in date order.** If several files arrived, the oldest unprocessed
   file is processed first. A newly discovered file older than the published
   history is never silently skipped: the job stops and asks for a controlled
   history rebuild. A retained gzip already processed in that history is
   recognized after its bulky clean export is pruned only when its current
   validation metadata, committed manifest and checksummed report agree on the
   original snapshot provenance. Missing evidence or changed input still stops
   the job for review.

4. **Build safely beside production.** The gzip streams directly into a new
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
   for an audit. Every source XML is hashed, so an edit is detected even if the
   source forgot to update its timestamp. Parsing uses indexed ID ranges with
   one worker on the small host; large XML and metadata tables use MySQL
   compression, and temporary work can spill to disk.
   In compact mode, the metadata staging table keeps identity, matching keys,
   hashes and the small fields needed to select the canonical source. It does
   not store another copy of the full XML and 13 payload-only text fields for
   every source. Those values are read from the original imported record only
   when its canonical payload is needed. This changes temporary storage, not
   matching rules or the fields ultimately returned by the API.

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
   Compact mode releases matching work tables after their final use. Once all
   required canonical payloads have been materialized, it saves the exact total
   raw-row count with the snapshot date and source hash, then empties the
   imported `records` table before creating the next article-state copy. The
   compressed source file is retained. Reports use the bound, saved count, so
   excluded source rows are still counted accurately. This point is already in
   the non-resumable finalization phase; metadata resume is forbidden if the
   raw-reclamation audit exists, including after an interrupted cleanup.

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
    so an interrupted run is retried safely. Imported raw tables are dropped;
    the database contains only the clean serving state. The coordinator briefly
    stops MySQL/API, switches the serving directory, restarts and checks the
    authenticated API. Failed checks restore the previous release.
    The report also records the storage profile and a SHA-256 of the rendered
    SQL phases, alongside the unchanged source SQL checksum. This distinguishes
    compact storage implementations without rewriting their semantic inputs.

11. **Remove obsolete large artifacts.** Only after the new release works,
    remove older managed raw gzips, exports and unmounted databases. Keep the
    latest input, current database, one clean export for the next update, and
    small audit reports. A temporary old/candidate pair is needed for recovery;
    no third serving copy or expanded raw SQL file is created.

## Reading build progress

The daily check runs at 03:17 UTC on a server configured for UTC: 04:17 London
time during British Summer Time and 03:17 during GMT. It leaves an active build
running and does not start an overlapping copy. One coordinator run can last
up to `OJS_MAX_RUNTIME_HOURS=720` (30 days); the systemd service has a 31-day
outer limit. Neither limit predicts how long the catalogue will take.

The production service records progress in the system journal. Reconnect over
SSH at any time and run:

```bash
sudo journalctl -u ojs-api-update.service -f -o short-iso
sudo journalctl -u ojs-api-update.service -n 50 --no-pager -o short-iso
sudo journalctl -u ojs-api-update.service --since yesterday --no-pager -o short-iso
```

Each `[progress]` line contains one JSON object on stderr, flushed immediately.
`timestamp` is UTC; `event` is `started`, `heartbeat`, `completed`, `failed` or
a named milestone such as `sql-stage` or `dedup-pass`. `stage` identifies the
operation; `step` identifies a SQL substage where known.
`elapsed_seconds` is time spent in that stage. `idle_seconds` is time since
that stage's last observed counter change or milestone, not the database's own
idle time. An outer stage may show increasing idle time while a nested stage
reports useful work.
Where available, records include these measurements:

| Measurement | Interpretation |
| --- | --- |
| `processed_bytes`, `total_bytes` where known | Download, validation, import, export or checksum work observed so far; streamed import bytes do not mean MySQL has committed those rows. |
| Metadata ID range, batches and rows | Ranges actually examined and rows hashed/parsed; ID ranges can contain gaps, so ID position is not a row percentage. |
| Named SQL stage | Wrangling, duplicate-key construction/filtering, graph initialization, canonical selection, lifecycle/event work or cleanup currently executing. |
| `dedup-pass` event: `pass_number`, `changed_rows` | A completed label-propagation pass and rows it changed; these are convergence measurements, not a prediction of remaining passes. |
| `process_pid`, `process_cpu_seconds`, `process_rss_bytes`, `disk_free_bytes` | Optional Linux resource samples to help interpret expensive stages; process samples are not the entire service's resource total. |

For example, this illustrative record says pass 3 finished and changed 128 rows:

```text
[progress] {"timestamp":"2026-10-06T12:00:00+00:00","event":"dedup-pass","stage":"temporal finalization phase","step":"deduplication","elapsed_seconds":7200.0,"idle_seconds":0.0,"pass_number":3,"changed_rows":128}
```

Long SQL statements continue to produce periodic heartbeats. SQL progress comes
only from allowlisted named markers and numeric pass/count fields; it does not
print source rows, XML, SQL statements or passwords. Start/completion and named
stage changes are reported immediately. Periodic output defaults to 30 seconds;
set `OJS_PROGRESS_SECONDS` to a finite value from 1 through 3600 seconds in the
private environment to change that interval.

A heartbeat proves the reporting process is alive, not that useful work is
advancing. A long single query may leave `idle_seconds` growing while CPU or
I/O counters increase. Inspect the service's CPU/I/O, memory and disk alongside
the log before diagnosing a stall; consistently unchanged counters and no
CPU/I/O need investigation. The first full 4 GB build has not been benchmarked,
so there is no measured whole-build ETA or honest global percentage. A stage's
`completed` event also does not mean the release has passed its remaining gates
or become live. A kill/reboot may prevent a final event: consult systemd status.

Validated metadata-processing checkpoints can resume after interruption.
Finalization, including deduplication and lifecycle work, cannot resume midway;
an interrupted `finalizing` candidate must be rebuilt. Increasing the runtime
setting does not change the deadline of a process already running, so applying
it immediately requires a controlled restart with that rebuild cost. See the
[restart procedure](HOW_TO_DEPLOY.md#failed-download-or-build).

The deployment journal policy retains bounded recent diagnostic history and
may prune old logs before 14 days when its size budget is reached. It covers
the host's default journal, including other services. Small checksummed audit
reports below are retained separately from these progress logs. See
[operations and resource checks](HOW_TO_DEPLOY.md#monitoring) and the
[journal installation steps](LIGHTSAIL_DEPLOYMENT.md#6-build-the-api-image-and-install-the-bounded-service).

## Where changes are reported

For snapshot `YYYY-MM-DD`:

- `data/clean/pkpbeacon-changes-YYYY-MM-DD.json` contains source-row totals,
  ISSN-scope exclusions, missing/title-less/keyless source counts, and article
  counts for `added`, `modified`, `removed`, `restored`, and `merged` events.
- The adjacent `.sha256` file protects the report from accidental corruption.
- `pkpbeacon-release-YYYY-MM-DD.json` is the final completion marker binding the
  clean export and report checksums; files without it are not treated as a
  processed release.
- `data/clean/pkpbeacon-changes-latest.json` points to the latest completed
  build's report. A completed build is not necessarily live yet: `GET /meta`
  identifies the release currently served by the API.
- `pkpbeacon-cleanup-YYYY-MM-DD.json` records generated files removed after a
  successful release, including paths and byte counts.
- `pkpbeacon-bootstrap-cleanup-YYYY-MM-DD.json` records deletion of an explicitly
  approved historical server gzip and its metadata sidecar by the optional
  post-publication hook. Here the date is the historical input's date, not the
  newly published release. Local/off-host originals are not accessed.
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
python3 src/run_pipeline.py --skip-download --clean-dir data/audit-clean \
  --compact-storage --rebuild-history --full-rescan --force-rebuild

# Verify the current database, export, provenance, report, and checksums
make verify
```

`data/audit-clean` must be new and must have enough space for a second build.
Restore any pruned historical raw files from a deliberate backup first. Older
event history stays in the current database, but old raw files are not kept
indefinitely. Using a separate directory keeps the serving `mysql-live` database and its
release pointers untouched while the audit is running.

A separate clean-output directory does not bypass the shared source lock:
commands with the same canonical raw-data directory remain mutually exclusive.
The lock uses Linux `flock` on a local filesystem and releases automatically
when its holders exit. Keep the lock file in place; deleting it can let two
workers lock different files and run concurrently. See the
[non-blocking availability check](LIGHTSAIL_DEPLOYMENT.md#8-install-the-daily-cron).
The lock is advisory: manual downloads, `rsync` and direct file writes bypass
these entry points. Do not add or replace raw inputs manually during a build.

The implementation is split between `src/download_beacon.py` (discovery/download),
`src/run_pipeline.py` (orchestration and reports),
`sql/01_build_ojs_tables.sql` (bibliometric transformation), and
`src/compact_update.py` (verified live publication and bounded retention).
