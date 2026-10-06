# ojs_api

PKP Beacon bibliometric processing and a read-only REST/OAI-PMH API, configured
for a small 4 GB AWS Lightsail instance. It checks Beacon daily and can spend
several days processing a new dump with one worker.

Start with [LIGHTSAIL_DEPLOYMENT.md](LIGHTSAIL_DEPLOYMENT.md) for the deployment
steps and current cost assumptions. The starting estimate is about USD
49.60/month for a 4 GB instance and 256 GB data disk; peak full-data storage has
not yet been benchmarked. [PROCESSING_GUIDE.md](PROCESSING_GUIDE.md) explains
the data transformations in simple terms. [HOW_TO_CALL.md](HOW_TO_CALL.md)
documents callers, and [HOW_TO_DEPLOY.md](HOW_TO_DEPLOY.md) covers operations.

## Daily workflow

The source is `https://beacon.publicknowledgeproject.org/mysql/pkpbeacon.gz`.
A private, ignored `.secrets/beacon.ini` supplies Basic authentication; never
commit the password. The checked-in cron calls the bounded systemd service at
03:17 UTC. There is one job at a time, including when a build lasts several days.

1. Check HTTP validators. Unchanged data needs no download or processing.
2. Download one resumable gzip and validate its CRC, SQL footer and checksums.
   No expanded SQL file is written.
3. Stream the input into a fresh MySQL candidate; import the previous clean
   export to preserve stable IDs and cumulative event history.
4. Hash all source XML and parse only new/changed records. Match exact OAI,
   DOI, article URL and conservative bibliometric keys; reconcile corrected
   keys and block globally noisy keys.
5. Report additions, modifications, removals, restorations and merges. Block
   abnormal losses/merges before publication.
6. Drop imported raw tables and finished staging tables. Keep only clean API
   tables plus provenance in the candidate; write one compressed clean export.
7. Briefly stop the API/MySQL, switch to the validated candidate directory,
   restart and check the authenticated API. Failed checks restore the old
   directory.
8. Remove older managed inputs, exports and unmounted database directories
   after success. Keep the latest input, one live database, one clean export,
   and small audit reports/manifests.

The old API stays available during processing. Clients should retry connection
errors/503s during the short handover. This avoids a third database copy used
by the former online table-import publisher.

## Files and commands

```text
.secrets/beacon.ini                    # source credential, ignored, mode 600
data/raw/pkpbeacon-YYYY-MM-DD.sql.gz    # newest immutable compressed input
data/raw/pkpbeacon-latest.sql.gz        # input pointer
data/clean/mysql-YYYY-MM-DD/            # clean database
data/clean/mysql-current               # completed build candidate
data/clean/mysql-live                  # independently committed serving pointer
data/clean/pkpbeacon-clean-YYYY-MM-DD.sql.gz
data/clean/pkpbeacon-changes-YYYY-MM-DD.json
data/clean/pkpbeacon-release-YYYY-MM-DD.json
data/clean/pkpbeacon-cleanup-YYYY-MM-DD.json
```

After following the setup guide:

```bash
make check-source       # HEAD and local status; no download
make update             # full check/build/publish/cleanup
make publish-live       # publish/restart a completed candidate without a download
make test
```

Use the installed service for production builds so memory limits apply:

```bash
sudo systemctl start --no-block ojs-api-update.service
sudo journalctl -u ojs-api-update.service -f -o short-iso
sudo journalctl -u ojs-api-update.service -n 50 --no-pager -o short-iso
sudo journalctl -u ojs-api-update.service --since yesterday --no-pager -o short-iso
```

`[progress]` JSON lines report stage/SQL step, elapsed and idle time, real byte
and metadata counters, and duplicate-merging passes. Heartbeats default to
30 seconds (`OJS_PROGRESS_SECONDS`); they show the reporter is alive, while
counters and CPU/I/O help assess advancement. No whole-build ETA is promised.
Install the bounded persistent journal policy in the
[deployment guide](LIGHTSAIL_DEPLOYMENT.md#6-build-the-api-image-and-install-the-bounded-service)
to retain logs after reboots. Its 128 MiB/14-day policy covers other host
services too and may prune older logs sooner; audit reports remain separate.
See [reading progress](PROCESSING_GUIDE.md#reading-build-progress).

`make verify` cold-starts a MySQL directory: stop the serving containers first.
Ordinary updates validate their candidate separately before switching.

## Data history

Source aliases retain provenance. Published article IDs remain stable;
removed/merged IDs become tombstones. Cross-source merges prefer the lowest
already-published ID, and a new lower source ID cannot replace it. Corrected
identity keys are retired from future matching. Already published merges are
not automatically split; repair requires an explicitly reviewed history replay.

`GET /changes?after_event_id=...` is the downstream event cursor. Counts and
hashes appear in the per-release JSON report and small manifests. Events remain
in the latest clean state even after old disk artifacts are pruned.

Local retention is deliberately finite. Exact old-input replays require a
separate intentional backup; this deployment does not pay to keep every raw
snapshot indefinitely. Existing legacy `.sql` inputs are not automatically
deleted. Do not copy them all onto a new small server.

The optional `src/scrape_updates.py` supports HTML archives independently; the
production daily job uses the real Beacon endpoint directly. Legacy online
table publication remains in `src/publish_live.py` for existing deployments,
but is not the compact default.

## API

`/health` is public; all data calls require HTTP Basic API user/key over HTTPS.
Endpoints include `/meta`, `/snapshots`, `/articles`,
`/articles/{article_id}/sources`, `/changes`, and all six verbs at `/oai`.
The API uses a SELECT-only MySQL account. Defaults are a 200-record REST maximum,
50-record OAI pages and eight concurrent connections. See
[HOW_TO_CALL.md](HOW_TO_CALL.md).
