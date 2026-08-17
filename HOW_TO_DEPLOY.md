# How to Deploy

This deployment has three continuously connected parts:

1. The host pipeline checks PKP, downloads immutable monthly SQL dumps, and
   builds a validated clean SQL release beside the serving database.
2. MySQL and FastAPI run continuously in Docker Compose.
3. The live publisher imports a completed clean release into a staging schema
   and atomically swaps all API tables without restarting either container.

Routine monthly updates do not stop the API. The staging import can make a
small server slower temporarily because it shares disk and MySQL resources.
Each SQL statement sees complete old or new tables; an HTTP response that runs
several statements can cross the swap, so downstream synchronizers must still
use the documented release/event cursor reconciliation.

For an AWS Lightsail instance with 16 GB RAM, use the dedicated
[LIGHTSAIL_DEPLOYMENT.md](LIGHTSAIL_DEPLOYMENT.md) runbook. It includes the
required attached-disk layout, emergency swap, container memory limits,
preflight checks, Lightsail firewall, snapshots, alarms, and retention steps.

## 1. Server requirements

Use a dedicated non-root account. These examples assume:

```text
account: ojs
checkout: /srv/ojs_api
```

Install:

- Python 3.12 or later
- MySQL Community Server and client tools 8.4 on the host, at the exact patch
  version selected by `OJS_MYSQL_IMAGE`
- Docker Engine and Docker Compose v2
- `flock`, `gzip`, `openssl`, `nice`, and preferably `ionice`
- a TLS reverse proxy such as Caddy or nginx

The `ojs` account must be allowed to run Docker; membership in the Docker group
is effectively root access and should be limited accordingly. Do not expose MySQL publicly.
The API itself binds only to `127.0.0.1:8000`; expose HTTPS through the reverse
proxy instead.

The initial history build is large. Allow space for retained raw dumps,
compressed clean exports, one pipeline build database, and a temporary copy of
the five clean API tables during live publication. Check before deployment:

```bash
df -h /srv/ojs_api/data
du -sh /srv/ojs_api/data/raw /srv/ojs_api/data/clean
```

For the current 123-136 GB raw snapshots, use fast local SSD storage with at
least 1 TB free for the initial backfill. A 32 GB host remains preferable for
fast history rebuilds, but the checked-in conservative profile supports a
16 GB single host with one metadata worker, bounded MySQL memory, emergency
swap, and a separately attached data disk. The root disk of a typical 16 GB
Lightsail bundle is not sufficient.
Check `free -h` before starting: sustained swapping makes the large indexed
transactions dramatically slower, so stop or move unrelated memory-heavy
workloads first. Routine monthly updates need less transient space, but retain
the `OJS_MIN_FREE_GB` guard and measure growth before lowering it. Sparse-file
support is strongly preferred; network filesystems and quota-limited home
directories are poor build targets. Dated raw files are intentionally retained,
so 130 GB monthly snapshots can add more than 1.5 TB per year before clean
exports, databases, backups, and staging overhead. Define retention and off-host
archive capacity before enabling the timer; never delete the only raw or clean
copy needed to reproduce a published release.

The input is executable MySQL SQL, not a passive CSV. Use only a trusted source
with an authenticated digest/signature if one is available. The pipeline turns
off client commands, networking, local infile, and server file access, but a
high-assurance deployment should run the builder in an isolated container or VM
with no home-directory/secrets mount or outbound network and promote only its
validated clean export.

## 2. Install

```bash
sudo install -d -o ojs -g ojs /srv/ojs_api
sudo -u ojs git clone YOUR_REPOSITORY_URL /srv/ojs_api
cd /srv/ojs_api
```

The host pipeline uses the Python standard library. The API dependencies are
installed in its Docker image.

## 3. Configure secrets

### PKP download credential

Store the PKP Beacon credential outside the repository:

```bash
sudo -u ojs install -d -m 700 /home/ojs/.config/ojs-api
sudo -u ojs install -m 600 /dev/null /home/ojs/.config/ojs-api/beacon.ini
sudo -u ojs editor /home/ojs/.config/ojs-api/beacon.ini
```

```ini
[beacon]
username = beacon-research
password = REPLACE_WITH_THE_PKP_PASSWORD
```

Do not put this password in Git, `.env`, a systemd unit, or a command-line
argument.

### API client key and database credential

Generate both secrets without printing either one:

```bash
cd /srv/ojs_api
sudo -u ojs ./scripts/generate_api_credentials.sh
sudo -u ojs stat -c '%a %n' \
  .secrets .secrets/api-client.env .secrets/db-api-password
```

Expected permissions are `700` on `.secrets` and `600` on both files. The
client file format is:

```dotenv
OJS_API_USERNAME=...
OJS_API_KEY=...
```

`.secrets/db-api-password` is a separate one-line password. During each build,
the pipeline provisions `ojs_api@'%'` with `SELECT` only on the four tables read
by the API. Compose mounts both files read-only; the API never receives
`MYSQL_ROOT_PASSWORD`.

`.secrets/` is ignored by Git and excluded from Docker builds. Give an
authorized caller only the client file through a secure secret-sharing channel;
never share the database password or paste either secret into tickets, logs,
shell history, or documentation.

Regenerate rather than reusing a development key on an unrelated server. After
suspected exposure, rotate the client file and recreate the API container. For
the database file, either apply the same new value with `ALTER USER` as MySQL
root or let a controlled new build provision it, then recreate the API
container. Securely redistribute only the client file and revoke old copies.

### Deployment environment

```bash
cd /srv/ojs_api
sudo -u ojs cp .env.example .env
sudo -u ojs chmod 600 .env
openssl rand -hex 32
id -u ojs
id -g ojs
```

Edit `.env` and set:

- `MYSQL_ROOT_PASSWORD` to the generated database password
- `OJS_ADMIN_EMAIL` to the repository contact address
- `OJS_PUBLIC_BASE_URL` to the external HTTPS origin, without `/oai`
- `OJS_HOST_UID` and `OJS_HOST_GID` to the `ojs` account values
- `OJS_API_CREDENTIALS_HOST_FILE` to `.secrets/api-client.env`
- `OJS_DB_API_PASSWORD_FILE` to `.secrets/db-api-password`
- `OJS_MYSQL_IMAGE` to the same exact MySQL patch release as the host tools

Resource-constrained defaults are included:

```dotenv
OJS_METADATA_WORKERS=1
OJS_MYSQL_BUFFER_POOL_SIZE=2G
OJS_SERVING_MYSQL_BUFFER_POOL_SIZE=5G
OJS_SERVING_MYSQL_MEMORY_LIMIT=7g
OJS_API_MEMORY_LIMIT=512m
OJS_MIN_DATA_FILESYSTEM_GB=900
OJS_MIN_FREE_GB=300
OJS_MIN_AVAILABLE_MEMORY_MB=2048
OJS_UPDATE_WORKING_SET_PERCENT=125
OJS_UPDATE_HEADROOM_GB=20
OJS_UPDATE_NICE=15
```

Increase workers and the serving buffer pool only after observing spare CPU,
memory, and disk bandwidth. The pipeline and serving MySQL must use the same
`MYSQL_ROOT_PASSWORD`; only those components receive it.
On a larger dedicated build host, increase these values gradually while
watching resident memory, swap, disk latency, and MySQL temporary-table usage.
Do not copy large-host worker counts onto the 16 GB profile.

## 4. Initial data build

Load the environment and build every retained raw snapshot in chronological
order. This can run for many hours on current data, so launch it from a
persistent terminal multiplexer or a service supervisor and capture its
output; do not make its lifetime depend on an SSH connection.

```bash
sudo -u ojs -H tmux new -s ojs-build
```

Inside that session, run:

```bash
cd /srv/ojs_api
set -a
. ./.env
set +a
make rebuild-history
make verify
```

Raw files are named `data/raw/pkpbeacon-YYYY-MM-DD.sql`. The build produces:

```text
data/clean/pkpbeacon-clean-YYYY-MM-DD.sql.gz
data/clean/pkpbeacon-clean-YYYY-MM-DD.sql.gz.sha256
data/clean/pkpbeacon-clean-latest.sql.gz
data/clean/pkpbeacon-changes-YYYY-MM-DD.json
data/clean/pkpbeacon-changes-YYYY-MM-DD.json.sha256
data/clean/pkpbeacon-changes-latest.json
data/clean/pkpbeacon-release-YYYY-MM-DD.json
data/clean/mysql-YYYY-MM-DD/
data/clean/mysql-current
```

For a new server without historical dumps, `make update` downloads and builds
the currently available PKP snapshot. PKP cannot supply older versions that it
no longer hosts.

## 5. Start the service

```bash
sudo -u ojs docker compose config --quiet
sudo -u ojs docker compose up -d --build
sudo -u ojs docker compose ps
```

Check MySQL/API readiness and authenticated XML:

```bash
curl --fail http://127.0.0.1:8000/health

sudo -u ojs -H /bin/bash -lc '
  set -a
  . /srv/ojs_api/.secrets/api-client.env
  set +a
  curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
    http://127.0.0.1:8000/meta
  curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" \
    "http://127.0.0.1:8000/oai?verb=Identify"
'

sudo -u ojs docker compose exec -T mysql sh -eu -c \
  'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -e \
  "SHOW GRANTS FOR '\''ojs_api'\''@'\''%'\'';"'
```

`/health` contains no data and remains unauthenticated for Docker and local
monitoring. Every data route, including all six OAI-PMH verbs, requires the
user and key. Confirm the grant output contains only four table-level `SELECT`
grants and no global/database-wide write privilege.

## 6. TLS and ports

Basic authentication must only be used over HTTPS. A minimal Caddy site is:

```caddyfile
api.example.org {
    encode zstd gzip
    reverse_proxy 127.0.0.1:8000
}
```

Open TCP 80/443 for the reverse proxy. Keep 8000 bound to loopback and do not
open 3306. Confirm that the externally visible OAI endpoint returns `401`
without credentials and XML with credentials. Configure request-rate,
concurrency, header/body-size, and idle-time limits in the reverse proxy or an
upstream WAF; the application deliberately has no built-in rate limiter. Keep
access logs bounded and redact authorization headers and query tokens.

## 7. Unattended updates

Run one manual end-to-end check while the API is serving:

```bash
sudo -u ojs -H /bin/bash -lc \
  'cd /srv/ojs_api && ./scripts/automatic_update.sh'
```

The wrapper:

1. Takes a non-blocking per-checkout lock.
2. Refuses to start below `OJS_MIN_FREE_GB` free space on `data/`, below
   `OJS_MIN_AVAILABLE_MEMORY_MB` currently available RAM, or below a
   conservative working-space estimate based on the last raw dump and current
   MySQL database.
3. Checks the PKP endpoint, or scrapes every missing dated link from the
   configured source index.
4. Validates filename/footer dates, gzip expansion, and raw immutability.
5. Processes all pending files chronologically; late historical files stop the
   job and require a controlled replay instead of being silently omitted.
6. Resumes a safe metadata checkpoint, or rebuilds only disposable generated
   staging state if finalization was interrupted.
7. Runs XML extraction with `OJS_METADATA_WORKERS` under `nice`/`ionice`.
8. Produces a checksummed row-change report and blocks anomalous losses,
   removals, or merges.
9. Produces and validates a dated clean SQL export and checksum, then commits
   an atomic manifest binding the export and report. Incomplete artifacts are
   retried and interrupted pointer publication is repaired on the next run.
10. Verifies the report and export before executing candidate SQL in
    `<database>_next`.
11. Atomically renames all five clean tables into the live database under a
    separate publisher lock.

The final rename is one MySQL statement. Existing requests complete against
the old tables; subsequent requests use the new tables. Neither `mysql` nor
`ojs-api` is restarted.

### Optional HTML-index scraper

To use a dated-file archive instead of the built-in single Beacon URL, edit
`.env` and replace the placeholder:

```dotenv
OJS_SOURCE_INDEX_URL=https://example.com/ojs-data/
OJS_SOURCE_MAX_EXPANDED_GB=500
OJS_SOURCE_ALLOW_CROSS_ORIGIN=0
```

The HTML page must retain links named `pkpbeacon-YYYY-MM-DD.sql` or
`pkpbeacon-YYYY-MM-DD.sql.gz`. Retaining all dates is required if every monthly
transition must be represented. Optional Basic credentials use
`OJS_SOURCE_USERNAME` and `OJS_SOURCE_PASSWORD`; authenticated indexes must use
HTTPS. Credentials are never forwarded across origins. Only set
`OJS_SOURCE_ALLOW_CROSS_ORIGIN=1` for an explicitly trusted unauthenticated CDN.

Load `.env` and perform a read-only check before enabling a scheduler:

```bash
sudo -u ojs -H /bin/bash -lc '
  cd /srv/ojs_api
  set -a
  . ./.env
  set +a
  python3 src/scrape_updates.py --check --json
  ./scripts/automatic_update.sh
'
```

The scraper is idempotent and installs all missing files atomically. If an
archive later reveals an older missing date, the downloaded file remains in
`data/raw/` but publication stops. Follow the controlled history-rebuild runbook
below; do not delete the file to hide the alert.

### systemd timer (recommended)

The checked-in service assumes `/srv/ojs_api` and user/group `ojs`:

```bash
sudo install -m 644 deploy/ojs-api-update.service \
  /etc/systemd/system/ojs-api-update.service
sudo install -m 644 deploy/ojs-api-update.timer \
  /etc/systemd/system/ojs-api-update.timer
sudo systemctl daemon-reload
sudo systemctl enable --now ojs-api-update.timer
sudo systemctl start ojs-api-update.service
sudo journalctl -u ojs-api-update.service -f
```

The timer checks once daily with a randomized delay. Daily checks are small;
the expensive pipeline runs only when a newer dated dump is pending. The
checked-in service bounds the builder at 6 GB memory and 2 GB swap and gives it
low CPU/I/O weight so a failed build is preferable to exhausting the serving
host.

### cron alternative

Use cron only when systemd is unavailable, and do not configure both. This
entry runs the scraper/check/build/publish workflow exactly once each day.
Add the entry to the `ojs` account with `crontab -e` (a matching checked-in
example is at `deploy/ojs-api.crontab.example`):

```cron
17 3 * * * cd /srv/ojs_api && ./scripts/automatic_update.sh >> /srv/ojs_api/data/automatic-update.log 2>&1
```

The log is therefore created by `ojs` rather than relying on `/var/log` write
access. Configure log rotation for it, and monitor nonzero cron results or the
absence of a recent `[update] complete` line.

## 8. Operations and recovery

Useful status commands:

```bash
systemctl list-timers ojs-api-update.timer
journalctl -u ojs-api-update.service --since today
sudo -u ojs docker compose logs --tail=200 mysql ojs-api
readlink -f data/raw/pkpbeacon-latest.sql
readlink -f data/clean/pkpbeacon-clean-latest.sql.gz
readlink -f data/clean/pkpbeacon-changes-latest.json
python3 -m json.tool data/clean/pkpbeacon-changes-latest.json
curl --fail http://127.0.0.1:8000/health
```

A failed build does not publish partial data. The timer retries on its next
run. A failed staging import leaves the live tables untouched. A failed
post-swap validation attempts the reverse atomic rename before returning an
error.

### Release guard and row-change report

Every release report contains raw/in-scope source totals and article event
counts for `added`, `modified`, `removed`, `restored`, and `merged`. Verify its
sidecar from `data/clean` with `sha256sum -c FILE.json.sha256`.

These `.env` values are publish-blocking fractions relative to the preceding
release:

```dotenv
OJS_MAX_ACTIVE_SOURCE_DROP_FRACTION=0.10
OJS_MAX_ACTIVE_ARTICLE_DROP_FRACTION=0.05
OJS_MAX_ARTICLE_REMOVAL_FRACTION=0.05
OJS_MAX_ARTICLE_MERGE_FRACTION=0.01
```

If any limit is exceeded, the live API remains unchanged and the pipeline
writes `pkpbeacon-changes-YYYY-MM-DD.quarantined.json` plus checksum. Review the
affected IDs through the staged database/source data, compare upstream totals,
and obtain a recorded human approval. Only then rerun the pipeline manually
with `--allow-anomalous-release` and run `src/publish_live.py`. Never add that
override to cron, systemd, or `.env`.

Exact-key merges are deliberately irreversible in the incremental catalogue.
A corrected source key is retired so it cannot create another false match, but
it does not split IDs that clients have already observed. If a merge is judged
incorrect, use the isolated history-rebuild procedure below from a snapshot
before the bad bridge, review the rebuilt change history, and only then promote
it as an explicitly approved corrective release.

Set `OJS_KEEP_PREVIOUS_RELEASE=1` to retain the prior live tables for manual
inspection. This approximately doubles clean-table storage and is off by
default.

Full-history/schema rebuilds are different from routine monthly updates. The
pipeline refuses to force-delete the directory targeted by `mysql-current`.
For a late historical snapshot, choose a new directory with enough capacity
for the complete history and run:

```bash
test ! -e data/rebuild-clean
install -d -m 700 data/rebuild-clean
python3 src/run_pipeline.py --skip-download --clean-dir data/rebuild-clean \
  --rebuild-history --full-rescan --force-rebuild --resume-building
python3 src/publish_live.py \
  --export data/rebuild-clean/pkpbeacon-clean-latest.sql.gz
```

Review and validate every report/export before the second command. The live
publisher still applies its checksum and provenance checks. Promote the
complete rebuilt artifact set to the normal `data/clean` paths only after the
authenticated API smoke test; keep the old clean directory until a restart and
restore drill succeeds. Do not try to append an older snapshot to newer
temporal state, and do not reuse a partly populated rebuild directory.

Monitor timer/service failures and free-space trend in the host monitoring
system; a persistent nonzero service exit is an alert, not a harmless retry.
Record latest snapshot date, report guard status, event counts, API health, and
backup age. At least quarterly, run an isolated `--full-rescan` build and compare
its clean state/report with the incremental result; any difference indicates an
upstream XML change that did not advance its timestamp/state contract. Test a
restore on a separate host at least quarterly.

Back up immutable raw SQL, every clean SQL/report, checksum and release
manifest, the repository revision, `.env`, both API secret files, and the
source credential. Keep secret backups encrypted and separate from data
backups. Binary logging is disabled,
so this deployment has release-level restore only—not point-in-time recovery.
Retain at least two independently stored validated releases and regularly prove
that a clean SQL export can be imported and served.

Pin the MySQL image and Python dependency versions for each release. Upgrade
the host MySQL tools and `OJS_MYSQL_IMAGE` together in a tested maintenance
window; a floating container patch can mutate a bind-mounted datadir so an
older host `mysqld` can no longer verify it.
