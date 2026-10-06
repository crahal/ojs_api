# Deployment and operations

Use [LIGHTSAIL_DEPLOYMENT.md](LIGHTSAIL_DEPLOYMENT.md) for the complete AWS
installation, cost assumptions, private credentials, initial build, TLS and
daily cron. The default is now 4 GB RAM, compressed storage and a brief restart
at publication. The previous 16 GB/1 TB layout is no longer the recommendation.

Production uses `https://13.135.237.76` with a publicly trusted IP certificate;
no custom domain is required. Caddy 2.11.7 from its official repository manages
the Let’s Encrypt `shortlived` certificate and forwards to `127.0.0.1:8000`.
Keep public TCP 80 available for HTTP-01 renewals and TCP 443 for clients. See
the [IP HTTPS configuration](LIGHTSAIL_DEPLOYMENT.md#7-smoke-test-the-api-and-configure-https).

## Production entry point

Install `deploy/ojs-api-update.service`, `deploy/journald-ojs-api.conf` and
`deploy/ojs-api.cron` as shown in the Lightsail guide. The cron runs daily at
03:17 UTC and starts the service; keep the server timezone set to `UTC`. This is
04:17 in London during British Summer Time and 03:17 during GMT. Systemd runs
it as `ojs` with a 2 GiB memory limit and 1 GiB swap allowance. The coordinator's
`OJS_MAX_RUNTIME_HOURS=720` permits 30 days, with `TimeoutStartSec=31d` as the
service's outer limit. A multi-day run cannot overlap itself. Do not also enable
the optional timer. `scripts/automatic_update.sh` sources the private environment and calls
`src/compact_update.py`; source passwords are read from a private INI file.

The entire service group is capped separately from the serving MySQL/API
containers. Docker limits are 1 GiB and 256 MiB; the two MySQL buffer pools are
768 MiB (builder) and 512 MiB (serving). Keep one metadata worker on 4 GB.

## What is retained

The current compressed raw dump, current clean MySQL directory and current
compressed clean export serve distinct purposes: retry/reproducibility,
live queries, and the next incremental build. The previous live directory
exists during building and health verification. Older managed large artifacts
are then removed, with filenames and byte counts in a cleanup JSON report.
Reports/manifests and the cumulative database event history are retained.

There is no expanded SQL dump on the compact path. MySQL native compression
trades CPU for smaller raw, metadata and article tables. Raw import tables
are dropped before serving. Temporary metadata tables are dropped at their
last use. Publisher handover uses the built directory directly, so it does
not import a second copy of the clean catalogue into a live staging schema.

Unknown legacy files, symlinked artifacts, mounted directories and incomplete
builds are protected. Migrate old files deliberately; a fresh deployment should
start with an empty data tree. Cleanup is permanent, so choose a bounded off-host
backup if exact historical inputs must remain available.

The optional `deploy/ojs-api-bootstrap-cleanup.conf.example` adds an
`ExecStartPost` for the explicitly approved server copies dated `2026-01-01`
and `2026-07-01`. It checks the authenticated live API, release marker, retained
reports and stat-bound source hashes before removing only those old gzips and
their metadata sidecars. No live release means deferred cleanup; missing or
changed proof prevents deletion, and already removed targets are a no-op.
Per-date audits remain in
`data/clean/pkpbeacon-bootstrap-cleanup-YYYY-MM-DD.json`. Local/off-host originals
are untouched. Follow the [hook installation instructions](LIGHTSAIL_DEPLOYMENT.md#9-retention-recovery-and-operating-cost)
after approving the exact dates; `daemon-reload` is sufficient, without
restarting the active build.

## Monitoring

```bash
sudo systemctl status ojs-api-update.service
# Follow the running build, or inspect recent retained output after reconnecting.
sudo journalctl -u ojs-api-update.service -f -o short-iso
sudo journalctl -u ojs-api-update.service -n 50 --no-pager -o short-iso
sudo journalctl -u ojs-api-update.service --since yesterday --no-pager -o short-iso
sudo -u ojs docker compose -f /srv/ojs_api/docker-compose.yml logs --tail=100 mysql ojs-api
df -h /srv/ojs_api/data
free -h
systemctl show ojs-api-update.service -p MainPID -p CPUUsageNSec -p IOReadBytes -p IOWriteBytes -p MemoryCurrent -p MemoryPeak -p Result -p ExecMainStatus
curl --fail http://127.0.0.1:8000/health
```

Use Ctrl-C to leave the live log view; the service keeps running. Docker logs
and `docker stats` describe the serving containers. The builder runs on the
host: inspect its systemd resource counters and process tree with the commands
above. Compare CPU/I/O counters across readings; some kernels may not expose
all accounting values.

Build progress is emitted immediately to stderr as `[progress]` followed by
one JSON object. Each record has a UTC timestamp, event (`started`, `heartbeat`,
`completed`, `failed` or a named milestone), stage, elapsed time and time since
observable progress.
`OJS_PROGRESS_SECONDS=30` controls periodic heartbeats; accepted values are
finite seconds from 1 through 3600. Stage transitions and completion do not
wait for this interval. Available counters describe actual bytes processed,
metadata ID ranges/batches, deduplication passes and changed rows. Named SQL
steps identify long wrangling statements. See the field interpretation in
[PROCESSING_GUIDE.md](PROCESSING_GUIDE.md#reading-build-progress).

A heartbeat means the reporting process is alive; it does not prove the
database is advancing. Growing `idle_seconds` with an unchanged SQL step can
mean one expensive query. Check CPU/I/O changes, memory and disk before judging
it stalled; sustained idle counters with no CPU/I/O deserve investigation.
There is no reliable whole-build percentage or ETA before measuring this data.
Absence of a `failed` event does not prove success after a kill, reboot or OOM:
also inspect the service result and kernel journal.

The installed journal policy persists logs across SSH disconnects and reboots,
with a 128 MiB persistent budget, 32 MiB runtime budget and 14-day retention
limit. Those limits cover the whole host's default journal, including other
services, and busy hosts may retain less history. Old archived logs are pruned;
the checksummed release/change reports in `data/clean` remain. Follow the
[installation and policy notes](LIGHTSAIL_DEPLOYMENT.md#6-build-the-api-image-and-install-the-bounded-service)
before applying the policy to an existing server.

Alert on nonzero service result, stale snapshot date, low disk, memory-limit
kills and missing/old backups. A disk reserve is a stop condition, not a peak
space prediction. The full catalogue's RAM/storage envelope must be measured
on the chosen instance before claiming production capacity.

## Failed download or build

An HTTP 401 from Beacon means its credential was rejected; update only the
private credential file and re-run `make check-source` after correcting it.

Downloads resume only when a saved remote identity still matches. Changed or
unknown validators invalidate a partial; gzip CRC/footer/hash validation must
pass before publication. An immutable same-date revision stops for review.

An interrupted pipeline can resume a validated checkpoint during metadata
processing. Finalization has no resumable checkpoint: interrupting the
`finalizing` phase requires rebuilding the generated candidate, including work
already done in that phase. The previous serving release remains separate.
Keep the failed report/log while investigating. Increase disk/RAM only after
seeing which resource failed; do not bypass release guards to solve resource
errors.

The 30-day coordinator deadline is captured when its Python process starts.
Editing `.env` and running `systemctl daemon-reload` does not extend that running
process's deadline. To apply the new limit to an existing process, first inspect
its stage and plan for the checkpoint limits above. Then perform a controlled
stop, install the service and edit the private environment, and start again:

```bash
sudo systemctl stop ojs-api-update.service
sudo -u ojs editor /srv/ojs_api/.env  # Set OJS_MAX_RUNTIME_HOURS=720.
sudo install -o root -g root -m 644 /srv/ojs_api/deploy/ojs-api-update.service /etc/systemd/system/ojs-api-update.service
sudo systemctl daemon-reload
sudo systemctl start --no-block ojs-api-update.service
sudo journalctl -u ojs-api-update.service -f -o short-iso
```

Do not restart an advancing finalization merely to refresh its configuration.
If it can finish before the active cutoff, let it finish and apply new limits
to the next run. The 30-day limit is a cutoff, not a promised completion time.

## Release recovery

The build's `mysql-current` and serving `mysql-live` pointers are separate.
A durable journal records a switch before containers are stopped. Failed
authentication/health/provenance checks restore the prior directory. A crash
during switching is recovered before another build begins. Cleanup begins
only after the candidate is verified live.

To retry a completed candidate without contacting Beacon:

```bash
sudo -u ojs -H /bin/bash -lc 'cd /srv/ojs_api && make publish-live'
```

Do not manually change `mysql-live`, erase journal files or delete a mounted
database to force progress. The coordinator pins the exact directory in
`OJS_LIVE_DATA_DIR` when recreating containers. Avoid manually starting Compose
against an absent pointer: the image could initialize an empty database.

For a deliberate cold verification, stop the service and serving containers,
load the private environment as `ojs`, run `make verify`, then restart through
`make publish-live`. Never cold-start a second MySQL process on a mounted
running data directory.

## Row-change review and history replay

Each `pkpbeacon-changes-YYYY-MM-DD.json` records source and article counts,
exclusions, event totals, source hash, build hash and release-guard results.
The adjacent checksum protects accidental corruption. Article events identify
the changed IDs, hashes, operation and reason; `/changes` supplies them to
callers.

Large source loss, article loss, removals or merges quarantine a release.
Review the input and report before an operator deliberately runs the pipeline
with `--allow-anomalous-release`. Do not put that override in cron or `.env`.

Older snapshots discovered after newer published state require chronological
replay. Restore the necessary raw archive to a separate working location;
compact retention does not preserve it automatically. Use a new clean directory:

```bash
python3 src/run_pipeline.py --raw-dir /PATH/TO/RESTORED/RAW \
  --clean-dir /PATH/TO/NEW/CLEAN --skip-download \
  --compact-storage --rebuild-history --full-rescan
```

A same-date correction is not an ordinary deployment: stable IDs/event cursors
already observed by callers may need reconciliation. Review the rebuilt reports
and explicitly plan the corrective release. Do not point the daily job at a
partly rebuilt history or quietly overwrite same-date published artifacts.

## Existing large-host deployments

The previous `src/publish_live.py` online table swap remains available for
existing operators, with its lock/checksum/rollback tests. It requires extra
disk and is not called by the compact job. Take a verified backup before
migrating: keep the old live datadir until a compact candidate is built and
verified, stop the old containers, then publish using the new independent live
pointer. Legacy databases lack the compact ownership marker and are not
automatically pruned. Never change the Compose bind path on a running deployment
without identifying its actual current database first.

Raw SQL is executable input. Use the trusted authenticated Beacon source.
The pipeline disables networking on its builder, client commands, local infile
and server file access. Keep source credentials outside the container build
context; keep backup secrets encrypted and access-limited.
