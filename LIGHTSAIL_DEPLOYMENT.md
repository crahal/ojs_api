# Deploy on a small AWS Lightsail instance

Deployment target: 6 October 2026. The default is a **4 GB RAM** Linux host,
one processing worker, compressed data, and bounded local retention. Processing
may take several days. The API keeps serving the previous release during the
build; a successful release causes a short MySQL/API restart.

## 1. Choose the cost and capacity

Start with Ubuntu 24.04 LTS, x86-64, the general-purpose **4 GB / 2 vCPU /
80 GB SSD / public IPv4** bundle at **USD 24/month**. For the current full
catalogue, budget a **256 GB attached SSD** at **USD 25.60/month**, for a starting
total of **USD 49.60/month** before tax, snapshots and transfer overages.
That is about 73% below the previous USD 184 estimate.

These are AWS list prices checked on 5 October 2026:
[AWS pricing](https://aws.amazon.com/lightsail/pricing/),
[bundle specifications](https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-bundles.html).

The 256 GB disk is an initial capacity estimate, **not a full-dataset benchmark**.
Compression ratios, article count, indexes and temporary SQL tables determine
the peak. The preflight no longer requires a 1 TB disk. It is possible to use
only included storage if a measured complete build fits, but the 80 GB root disk
has not been demonstrated sufficient for this catalogue. Do not buy a smaller
disk based on the compressed download size alone.

The general-purpose plan is burstable: its sustained baseline is 20% per vCPU.
Slow processing after CPU credits fall is expected. An optional USD 42/month
compute-optimized 4 GB bundle includes 160 GB SSD and two dedicated vCPUs; it
may be a better total price if a measured build fits that disk.
[CPU baseline](https://docs.aws.amazon.com/lightsail/latest/userguide/baseline-cpu-performance.html),
[dedicated compute announcement](https://aws.amazon.com/about-aws/whats-new/2026/04/lightsail-compute-optimized-instances/).
Stopping an instance does not stop billing; do not use stop/start as a saving.

Only these large artifacts remain after a successful update:

- One downloaded raw `.sql.gz`, retained unchanged for validation/retry.
- One clean serving MySQL directory.
- One compressed clean export, required to carry temporal state into the next build.

Small checksummed reports, release manifests and cleanup records remain for
audit. During a build, the old live directory and one candidate coexist so a
failure can leave the old release working. There is no expanded raw SQL file,
third live-import database, or indefinite archive of monthly databases.

## 2. Create the instance, network and data disk

Publish the reviewed repository changes to your chosen deployment branch before
cloning it below; uncommitted workstation edits are not transferred by Git.
Never add `.env`, `.secrets`, or the local data archive to that commit.

In Lightsail, create the OS-only instance in your chosen Region. Attach a static
IPv4 address and point your API hostname's DNS A record to it. Set firewall
rules on both IPv4 and IPv6: TCP 22 from your admin IP only, TCP 80/443 publicly.
Keep ports 8000 and 3306 closed. Disable IPv6 if you will not configure it.
[Firewall instructions](https://docs.aws.amazon.com/lightsail/latest/userguide/understanding-firewall-and-port-mappings-in-amazon-lightsail.html).

SSH as `ubuntu`, then install the basic tools and create the service account:

```bash
sudo apt update
sudo apt upgrade -y
sudo apt install -y ca-certificates curl git gzip make openssl python3 tmux util-linux cron caddy
sudo timedatectl set-timezone UTC
sudo useradd --create-home --shell /bin/bash ojs
sudo install -d -o ojs -g ojs /srv/ojs_api
sudo -u ojs git clone YOUR_REPOSITORY_URL /srv/ojs_api
```

Attach the data SSD in the same Availability Zone. Identify the actual blank
device using `lsblk`; the console's device name may appear as NVMe in Linux.

```bash
lsblk -o NAME,SIZE,FSTYPE,LABEL,UUID,MOUNTPOINTS
```

The following format command is **only for the new empty data disk**. Replace the
placeholder with its exact device. Never run it on the OS disk or an existing
filesystem.

```bash
sudo mkfs.ext4 -m 1 -L ojs-data /dev/REPLACE_WITH_BLANK_DATA_DISK
sudo blkid /dev/REPLACE_WITH_BLANK_DATA_DISK
sudo install -d -o ojs -g ojs /srv/ojs_api/data
sudo editor /etc/fstab
```

Add the data filesystem by its UUID:

```fstab
UUID=REPLACE_WITH_UUID /srv/ojs_api/data ext4 defaults,nofail,noatime 0 2
```

```bash
sudo mount -a
sudo chown ojs:ojs /srv/ojs_api/data
findmnt -T /srv/ojs_api/data
df -h /srv/ojs_api/data
```

[AWS disk installation instructions](https://docs.aws.amazon.com/lightsail/latest/userguide/create-and-attach-additional-block-storage-disks-linux-unix.html).
If capacity proves insufficient, snapshot the disk and restore to a larger disk.
Only perform that expansion after measuring the required peak.

## 3. Install Docker and MySQL 8.4

Install Docker Engine and Compose using its official Ubuntu repository:

```bash
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
sudo tee /etc/apt/sources.list.d/docker.sources >/dev/null <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker ojs
sudo systemctl enable --now docker
```

[Official Docker instructions](https://docs.docker.com/engine/install/ubuntu/).
Docker group membership is root-equivalent; keep this account dedicated.

Install host MySQL Community **8.4 LTS** using the
[official MySQL APT instructions](https://dev.mysql.com/doc/refman/8.4/en/linux-installation-apt-repo.html):
download the current `mysql-apt-config` package, install with `sudo dpkg -i`,
select the 8.4 LTS series, then `sudo apt update && sudo apt install mysql-server`.
The builder uses `mysqld`, `mysql`, `mysqladmin` and `mysqldump`. Disable the
distribution's idle server and record the exact patch:

```bash
sudo systemctl disable --now mysql
mysqld --version
sudo apt-mark hold mysql-community-server mysql-community-server-core mysql-community-client mysql-community-client-core mysql-community-client-plugins
```

Set the matching exact patch in `OJS_MYSQL_IMAGE` later. Host and container open
the same physical database, so upgrade both together in a tested maintenance
window. Do not leave packages held forever without planned security updates.
Never upgrade MySQL while a build or release switch is running.

If the package enables AppArmor, merge `deploy/apparmor-mysqld-ojs` into
`/etc/apparmor.d/local/usr.sbin.mysqld` and reload the actual installed profile:

```bash
sudo editor /etc/apparmor.d/local/usr.sbin.mysqld
sudo apparmor_parser -r /etc/apparmor.d/usr.sbin.mysqld
```

If there is no such profile/local include, check the installed package layout;
do not disable AppArmor globally.

Make Docker wait for the data mount. Run `sudo systemctl edit docker.service`
and enter:

```ini
[Unit]
RequiresMountsFor=/srv/ojs_api/data
```

Then run `sudo systemctl daemon-reload && sudo systemctl restart docker`.

## 4. Add emergency swap and set the memory budget

Check `swapon --show` first. If no swap file already exists, create 4 GB on the
root disk:

```bash
sudo dd if=/dev/zero of=/swapfile bs=1M count=4096 status=progress
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
sudo editor /etc/fstab
```

Add `/swapfile none swap sw 0 0` to fstab, once. Set swappiness:

```bash
printf 'vm.swappiness=10\n' | sudo tee /etc/sysctl.d/90-ojs.conf
sudo sysctl --system
free -h
```

The checked-in limits are 2 GiB for the entire builder service, 1 GiB for serving
MySQL, and 256 MiB for the API, leaving roughly 768 MiB for Ubuntu/Docker. Builder
and serving buffer pools are 768 MiB and 512 MiB. One metadata worker, disk-backed
temporary tables and native InnoDB compression trade CPU time for memory/storage.
Swap is a reserve: sustained swapping means the build needs a smaller working
set or a larger host, not more workers.

## 5. Configure the private environment and credentials

```bash
cd /srv/ojs_api
sudo -u ojs ./scripts/generate_api_credentials.sh
sudo -u ojs cp .env.example .env
sudo -u ojs chmod 600 .env
openssl rand -hex 32
id -u ojs
id -g ojs
sudo -u ojs editor .env
```

Set `MYSQL_ROOT_PASSWORD` to the random value, `OJS_HOST_UID/GID` to the account
numbers, `OJS_MYSQL_IMAGE` to the exact host patch, `OJS_ADMIN_EMAIL` to your
address, and `OJS_PUBLIC_BASE_URL` to the public HTTPS origin. Leave the small-host
resource values at their defaults for the first build.

`OJS_PROGRESS_SECONDS=30` emits periodic progress while lengthy steps run.
It accepts finite seconds from 1 through 3600. Start/completion events and SQL
stage changes are also logged, so the daily job remains observable while a
single statement takes hours.

The source URL is already set to:

```text
https://beacon.publicknowledgeproject.org/mysql/pkpbeacon.gz
```

The source credential lives in `.secrets/beacon.ini`, mode 600, excluded from Git
and Docker builds. The provided credential has been installed in that private
file in the development checkout. Transfer it over SSH into the equivalent
private path on the server; it will not arrive through `git clone`. Alternatively
create it with `sudo -u ojs editor /srv/ojs_api/.secrets/beacon.ini`:

```ini
[beacon]
username = beacon-research
password = ENTER_THE_CURRENT_BEACON_PASSWORD_HERE
```

```bash
sudo -u ojs chmod 600 /srv/ojs_api/.secrets/beacon.ini
sudo -u ojs -H /bin/bash -lc 'cd /srv/ojs_api && make lightsail-preflight'
sudo -u ojs -H /bin/bash -lc 'cd /srv/ojs_api && make check-source'
```

**Credential verification on 5 October 2026 returned HTTP 401 for HEAD and
a one-byte GET.** Correct the private password before deployment; the check
must authenticate successfully. No full remote dump was downloaded in that
verification. Passwords never belong in cron entries, command arguments or Git.

## 6. Build the API image and install the bounded service

The service sends both output streams to the journal. Install the accompanying
policy to keep logs across reboots with bounded storage. **This policy applies
to the whole host's default journal, including other services.** Review any
existing local logging policy before installing it; later drop-ins can override
these settings.

```bash
cd /srv/ojs_api
sudo -u ojs docker compose build --pull ojs-api
sudo install -o root -g root -m 644 deploy/ojs-api-update.service /etc/systemd/system/ojs-api-update.service
sudo systemd-analyze cat-config systemd/journald.conf
sudo install -d -o root -g root -m 755 /etc/systemd/journald.conf.d
sudo install -o root -g root -m 644 deploy/journald-ojs-api.conf /etc/systemd/journald.conf.d/60-ojs-api.conf
sudo systemctl restart systemd-journald
sudo journalctl --flush
sudo systemd-analyze cat-config systemd/journald.conf
sudo journalctl --disk-usage
sudo systemctl daemon-reload
sudo systemctl start --no-block ojs-api-update.service
sudo journalctl -u ojs-api-update.service -f -o short-iso
```

The policy sets `Storage=persistent`, `SystemMaxUse=128M`,
`RuntimeMaxUse=32M`, `SystemKeepFree=1G`, `MaxRetentionSec=14day` and daily
file rotation. It prunes archived journal files; active files can make reported
usage exceed the nominal budget. Size pressure can shorten the retained history.
The free-space setting limits journal growth; it cannot reserve disk against
other applications. These settings leave checksummed audit reports untouched.
See the [upstream journal configuration documentation](https://github.com/systemd/systemd/blob/main/man/journald.conf.xml).

`journalctl --flush` moves runtime logs to persistent storage after the
configuration is activated. Use `restart systemd-journald` as shown, rather
than separate stop/start commands, to preserve logging stream connections.
[Upstream flush documentation](https://github.com/systemd/systemd/blob/main/man/journalctl.xml),
[journal service restart documentation](https://github.com/systemd/systemd/blob/main/man/systemd-journald.service.xml).
Check `journalctl --list-boots` after a future reboot to confirm history is
available. The commands above are server installation steps, not actions taken
automatically by the pipeline.

The Python image is pinned to the current 3.12 patch listed by the
[official image maintainers](https://hub.docker.com/_/python). Review image and
dependency security updates regularly and rebuild/test deliberately; the daily
data job does not upgrade application images. Host/container MySQL patch
mismatches are rejected before database verification or service restart.

The service does the first authenticated download, validation, build and
publication. It stays independent of your SSH session. The first build can take
days; the daily scheduler will not start another copy while it is running.
The API is available only after the first complete release.

The build streams gzip directly into MySQL without writing expanded SQL.
Changed records are parsed and deduplicated; all source XML is hashed so an
upstream edit is detected even if its timestamp did not change. Raw imported
tables and obsolete staging tables are dropped before the final database is
served. Reports and anomaly gates still run before publication.

On later updates, the old API runs throughout processing. Publication briefly
stops the containers, switches the live database, and checks the new authenticated
API. Failed checks restore the previous release. Clients should retry transient
connection errors/503s around publication. Logs record which old files were
removed after successful checks.

Monitor a complete first build:

```bash
sudo systemctl status ojs-api-update.service
sudo journalctl -u ojs-api-update.service -n 50 --no-pager -o short-iso
sudo journalctl -u ojs-api-update.service --since yesterday --no-pager -o short-iso
free -h
df -h /srv/ojs_api/data
sudo -u ojs docker stats --no-stream
systemctl show ojs-api-update.service -p MainPID -p CPUUsageNSec -p IOReadBytes -p IOWriteBytes -p MemoryCurrent -p MemoryPeak -p Result -p ExecMainStatus
```

Docker statistics cover serving MySQL/API containers; the host builder is
accounted for by `ojs-api-update.service`. Compare its CPU/I/O counters over time.
Look for `[progress]` JSON lines: they identify the stage/SQL step, elapsed and
idle seconds, and available byte, metadata-range or deduplication-pass counters.
An unchanged step with heartbeats can still be one long database statement.
A heartbeat alone proves only that the reporting process is alive; increasing
counters or CPU/I/O supply additional evidence. No global percentage or exact
ETA is implied. [Progress field guide](PROCESSING_GUIDE.md#reading-build-progress).

The disk reserve monitor stops work if free space falls below 8 GiB. The
20 GiB starting floor is a guard, not a prediction that 20 GiB is enough.
Use the measured peak to decide capacity; do not disable the reserve to force
a build. A memory-limit failure is reported and the old release remains live.

## 7. Smoke-test the API and configure HTTPS

```bash
curl --fail http://127.0.0.1:8000/health
sudo -u ojs -H /bin/bash -lc '
  . /srv/ojs_api/.secrets/api-client.env
  curl --fail --user "$OJS_API_USERNAME:$OJS_API_KEY" http://127.0.0.1:8000/meta
'
```

Edit `/etc/caddy/Caddyfile`, substituting the same hostname as `.env`:

```caddyfile
api.your-domain.example {
    encode zstd gzip
    reverse_proxy 127.0.0.1:8000
}
```

```bash
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl enable --now caddy
sudo systemctl reload caddy
curl --fail https://api.your-domain.example/health
```

Check that an unauthenticated request to `/meta` returns 401. All data endpoints
require the API user/key. The app has a bounded concurrency/page size, not a
per-client rate limiter; use a proxy/WAF if shared clients require one.

## 8. Install the daily cron

The repository provides a root-owned system cron file that starts the memory-
limited, non-root service at **03:17 UTC every day**:

```bash
cd /srv/ojs_api
sudo systemctl disable --now ojs-api-update.timer 2>/dev/null || true
sudo install -o root -g root -m 644 deploy/ojs-api.cron /etc/cron.d/ojs-api
sudo systemctl enable --now cron
sudo cat /etc/cron.d/ojs-api
```

Do not install the optional systemd timer as well. If using `sudo crontab -e`
instead, copy the entry in `deploy/ojs-api.crontab.example`; do not replace an
existing root crontab wholesale.

An unchanged HEAD validator produces a cheap no-op. A changed remote response
downloads one gzip, checks its CRC/footer/hash, and starts processing. The footer
date is authoritative; the HTTP Last-Modified date need not equal it. Same-date
content changes are rejected for review instead of silently overwriting history.
If an update is still building, the daily start leaves that single run alone;
the next check happens after it finishes.

## 9. Retention, recovery and operating cost

After verified publication, automatic cleanup removes older **managed** raw
gzips, clean exports and unmounted completed build directories. It retains the
newest input, current database, current clean export, and small audit files.
Symlinks, mounted databases, incomplete builds and unrecognized legacy inputs
are not blindly deleted. Existing user-supplied `.sql` files are preserved;
on a fresh server, do not copy the old raw archive into this deployment.

Cleanup frees local storage permanently. Older raw inputs are no longer
available for exact historical replays unless you deliberately backed them up.
The cumulative event history and stable-ID state remain in the current database
and clean export. An interrupted build retains only its retry/checkpoint state;
inspect it before starting a different dated build.

For a no-extra-storage local recovery checkpoint, keep the latest compressed
clean export already produced. For host-loss recovery, keep one verified copy
off-host or one rolling manual Lightsail snapshot. Additional snapshot storage
costs USD 0.05/GB-month; do not enable an unbounded backup archive. Instance
snapshots include attached disks. Stop builds/containers for a consistent manual
recovery snapshot, then restart through `make publish-live`.
[AWS snapshot billing](https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-frequently-asked-questions-faq-billing-and-account-management.html).

Alert on service failure, low disk/RAM, backup age and stale snapshot date.
Test a restore before treating any backup as reliable. A single Beacon URL only
exposes the currently hosted dump; data versions that disappear between checks
cannot be recovered by this application.

If the first build exceeds 4 GB RAM despite the bounded profile, review its
error/peak before changing settings. The 8 GB general-purpose bundle is USD
44/month; it can reuse the attached disk. If disk fills, expand only by the
measured requirement. These choices still avoid the prior 16 GB plus 1 TB
default.
