# Deploy on a 16 GB AWS Lightsail Instance

This is the small-host production profile for this repository. It assumes one
Ubuntu Lightsail instance runs the API, serving MySQL, and the monthly builder.
It deliberately trades build speed for predictable memory use.

The important limitation is storage: the current Beacon SQL dump is roughly
130 GB before MySQL indexes and staging copies. A 16 GB Lightsail instance's
root disk alone is not enough. Use an attached SSD data disk and archive old
raw snapshots and releases off-host.

## 1. Choose the Lightsail resources

In the Lightsail console, create:

1. An **OS-only Ubuntu 24 LTS** instance in the Region nearest the API users.
2. The **standard Xlarge Linux/public-IPv4 bundle**: 4 vCPU, 16 GB RAM,
   320 GB root SSD, and 6 TB transfer. As of 17 August 2026 AWS lists it at
   USD 84/month. The cheaper memory-optimized 16 GB bundle has only 2 vCPU and
   a 160 GB root disk, so it is a poor fit for this disk-heavy batch job.
3. A **static IPv4 address**, attached before configuring DNS and TLS.
4. An attached SSD disk in the same Availability Zone:

   - 1 TB is the operational minimum if old files are archived and pruned
     promptly.
   - 2 TB is the safer starting point if several monthly raw files remain
     online.

AWS currently lists attached SSD storage at USD 0.10/GB-month, so the practical
minimum is about USD 184/month before snapshots and transfer overages: USD 84
for the instance plus roughly USD 100 for a 1 TB disk. Treat these as planning
figures and check the current
[Lightsail billing FAQ](https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-frequently-asked-questions-faq-billing-and-account-management.html)
in the deployment Region.

AWS documents the current bundles at
[Lightsail instance bundles](https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-bundles.html).
Attached disks persist independently of the instance, are encrypted by
default, and can be up to 16 TB; see
[Lightsail block storage](https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-faq-block-storage.html).

This plan is burstable. Its published CPU baseline is 40% per vCPU, so a large
initial build can slow after burst capacity is spent. That is expected; do not
solve it by raising worker counts. See
[Lightsail CPU baseline and burst capacity](https://docs.aws.amazon.com/lightsail/latest/userguide/baseline-cpu-performance.html).

## 2. Set the Lightsail firewalls

Configure both the IPv4 and IPv6 firewalls. They are independent.

| Port | Source | Purpose |
| --- | --- | --- |
| TCP 22 | Your fixed admin CIDR only | SSH |
| TCP 80 | Anywhere | ACME redirect/challenge |
| TCP 443 | Anywhere | Public HTTPS API |

Do not open 3306 or 8000. MySQL is internal and Compose binds the API to host
loopback only. If IPv6 is not needed, disable it instead of leaving a more
permissive IPv6 firewall. AWS notes that the most permissive overlapping rule
wins; see
[Lightsail firewalls](https://docs.aws.amazon.com/lightsail/latest/userguide/understanding-firewall-and-port-mappings-in-amazon-lightsail.html).

Point the API hostname's DNS `A` record at the attached static IPv4 address.
The normal instance address changes after stop/start; the attached static
address does not.

## 3. Patch Ubuntu and install the host tools

SSH as the default `ubuntu` user:

```bash
sudo apt update
sudo DEBIAN_FRONTEND=noninteractive apt full-upgrade -y
sudo apt install -y \
  ca-certificates caddy curl git gnupg gzip make openssl python3 tmux util-linux
```

Install Docker Engine and the Compose plugin from Docker's official Ubuntu
repository:

```bash
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  -o /etc/apt/keyrings/docker.asc
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
sudo apt install -y \
  docker-ce docker-ce-cli containerd.io docker-buildx-plugin \
  docker-compose-plugin
sudo systemctl enable --now docker
```

These are the current upstream steps from
[Install Docker Engine on Ubuntu](https://docs.docker.com/engine/install/ubuntu/).

Install MySQL Community Server and client **8.4 LTS** from the official
[MySQL APT repository](https://dev.mysql.com/doc/refman/8.4/en/linux-installation-apt-repo.html).
Download the current `mysql-apt-config` package, install it with `dpkg -i`,
select the MySQL 8.4 LTS series, run `sudo apt update`, and install
`mysql-server`. Do not mix Ubuntu's native MySQL packages with Oracle's APT
packages.
The pipeline uses the host `mysqld`, `mysql`, `mysqladmin`, and `mysqldump`
binaries, but starts its own isolated server. Disable the distribution service
after installing the binaries so it does not consume RAM:

```bash
mysqld --version
mysql --version
sudo systemctl disable --now mysql
```

Record the exact patch version. Later, set `OJS_MYSQL_IMAGE` to the matching
official image tag. Do not let a different MySQL patch release mutate the same
data directory. Hold the installed MySQL packages against unattended upgrades
and upgrade the host binaries plus `OJS_MYSQL_IMAGE` together during a tested
maintenance window.

Record and hold the installed community packages (adjust the list only if the
APT repository reports different installed package names):

```bash
dpkg-query -W -f='${binary:Package} ${Version}\n' \
  mysql-community-server mysql-community-client
sudo apt-mark hold \
  mysql-community-server mysql-community-server-core \
  mysql-community-client mysql-community-client-core \
  mysql-community-client-plugins
```

Create the deployment account and checkout:

```bash
sudo useradd --create-home --shell /bin/bash ojs
sudo usermod -aG docker ojs
sudo install -d -o ojs -g ojs /srv/ojs_api
sudo -u ojs git clone YOUR_REPOSITORY_URL /srv/ojs_api
```

Docker-group membership is root-equivalent. Do not use this account for email,
browsing, or unrelated applications.

## 4. Format and mount the attached data disk

First identify the new, empty device:

```bash
lsblk -o NAME,SIZE,FSTYPE,LABEL,UUID,MOUNTPOINTS
```

The Lightsail console shows the device it attached, but Linux may expose an
NVMe name. In the commands below, replace `/dev/REPLACE_WITH_NEW_DISK` with the
exact blank device from `lsblk`.

**Stop if `FSTYPE`, files, partitions, or a mount point already exist.** The
next command destroys everything on the selected device and is only for a new
blank disk.

```bash
sudo mkfs.ext4 -L ojs-data /dev/REPLACE_WITH_NEW_DISK
sudo blkid /dev/REPLACE_WITH_NEW_DISK
sudo install -d -o ojs -g ojs /srv/ojs_api/data
```

Add one line to `/etc/fstab`, using the UUID printed by `blkid`:

```fstab
UUID=REPLACE_WITH_UUID /srv/ojs_api/data ext4 defaults,nofail,noatime 0 2
```

Mount and verify it before downloading any data:

```bash
sudo mount -a
sudo chown ojs:ojs /srv/ojs_api/data
findmnt -T /srv/ojs_api/data
df -h /srv/ojs_api/data
```

On this dedicated host, make Docker require the data mount before it can honor
container restart policies:

```bash
sudo systemctl edit docker.service
```

Enter:

```ini
[Unit]
RequiresMountsFor=/srv/ojs_api/data
```

Then apply it:

```bash
sudo systemctl daemon-reload
sudo systemctl restart docker
systemctl cat docker.service
```

Without this dependency, a failed `nofail` mount could let Docker start MySQL
against an empty directory on the root disk. The update service has the same
mount dependency checked in.

Mounting the disk directly at the repository's `data/` path means the pipeline,
Docker bind mount, disk-space guard, and systemd sandbox all refer to the same
filesystem. AWS's Linux disk procedure is documented at
[create and attach a disk](https://docs.aws.amazon.com/lightsail/latest/userguide/create-and-attach-additional-block-storage-disks-linux-unix.html).

If the MySQL package installed an AppArmor profile, merge the checked-in rules
from `deploy/apparmor-mysqld-ojs` into
`/etc/apparmor.d/local/usr.sbin.mysqld`, preserving any existing local rules,
then reload the profile:

```bash
sudo editor /etc/apparmor.d/local/usr.sbin.mysqld
sudo apparmor_parser -r /etc/apparmor.d/usr.sbin.mysqld
```

Do not disable AppArmor globally. The narrow rules allow the builder to use the
attached data tree and its private socket/log directory. If the profile or its
`local/` include does not exist, confirm the installed MySQL package's policy
layout before creating files.

## 5. Add emergency swap

Swap is an OOM safety net, not working memory. Create 8 GB on the root disk:

```bash
sudo dd if=/dev/zero of=/swapfile bs=1M count=8192 status=progress
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

Add this line to `/etc/fstab`:

```fstab
/swapfile none swap sw 0 0
```

Keep normal workloads out of swap:

```bash
printf 'vm.swappiness=10\n' | sudo tee /etc/sysctl.d/90-ojs-small-host.conf
sudo sysctl --system
free -h
swapon --show
```

If swap use grows continuously during a build, stop and lower memory settings.
Do not accept sustained SSD swapping as normal operation.

## 6. Configure credentials and the 16 GB profile

Create the API and SELECT-only database credentials:

```bash
cd /srv/ojs_api
sudo -u ojs ./scripts/generate_api_credentials.sh
sudo -u ojs cp .env.example .env
sudo -u ojs chmod 600 .env
openssl rand -hex 32
```

Use that final random value for `MYSQL_ROOT_PASSWORD`; it is separate from the
two secrets created by the script.

Create the PKP credential file outside the checkout:

```bash
sudo -u ojs install -d -m 700 /home/ojs/.config/ojs-api
sudo -u ojs install -m 600 /dev/null \
  /home/ojs/.config/ojs-api/beacon.ini
sudo -u ojs editor /home/ojs/.config/ojs-api/beacon.ini
```

Its contents are:

```ini
[beacon]
username = beacon-research
password = REPLACE_WITH_THE_PKP_PASSWORD
```

Edit `/srv/ojs_api/.env`. Replace every placeholder and confirm these small-host
values remain in place:

```dotenv
MYSQL_ROOT_PASSWORD=REPLACE_WITH_A_LONG_RANDOM_VALUE
OJS_ADMIN_EMAIL=admin@your-domain.example
OJS_PUBLIC_BASE_URL=https://api.your-domain.example
OJS_MYSQL_IMAGE=mysql:REPLACE_WITH_EXACT_HOST_8.4_PATCH
OJS_HOST_UID=REPLACE_WITH_ID_U_OJS
OJS_HOST_GID=REPLACE_WITH_ID_G_OJS

OJS_OAI_PAGE_SIZE=100
OJS_METADATA_WORKERS=1
OJS_MYSQL_BUFFER_POOL_SIZE=2G
OJS_BUILDER_MYSQL_MAX_CONNECTIONS=16
OJS_BUILDER_MYSQL_TEMPTABLE_MAX_RAM=256M
OJS_SERVING_MYSQL_BUFFER_POOL_SIZE=5G
OJS_SERVING_MYSQL_MAX_CONNECTIONS=50
OJS_SERVING_MYSQL_TEMPTABLE_MAX_RAM=256M
OJS_SERVING_MYSQL_MEMORY_LIMIT=7g
OJS_SERVING_MYSQL_MEMORY_RESERVATION=5g
OJS_SERVING_MYSQL_MEMSWAP_LIMIT=9g
OJS_API_MEMORY_LIMIT=512m
OJS_API_MEMSWAP_LIMIT=768m
OJS_MIN_DATA_FILESYSTEM_GB=900
OJS_MIN_FREE_GB=300
OJS_MIN_AVAILABLE_MEMORY_MB=2048
OJS_UPDATE_WORKING_SET_PERCENT=125
OJS_UPDATE_HEADROOM_GB=20
OJS_UPDATE_NICE=15
OJS_KEEP_PREVIOUS_RELEASE=0
```

Obtain the numeric account values with:

```bash
id -u ojs
id -g ojs
```

If using the HTML scraper, replace—not merely uncomment—the placeholder source
URL. Never enable a cron job that still points to `example.com`.

## 7. Run the small-host preflight

Start a new login session so the `ojs` account receives Docker-group
membership, then run:

```bash
sudo -u ojs -H /bin/bash -lc \
  'cd /srv/ojs_api && ./scripts/lightsail_preflight.sh'
```

The preflight fails if required tools or secrets are missing, less than 14 GB
RAM is visible, less than 4 GB swap exists, current available memory is below
the configured floor, the data filesystem is smaller than 900 GiB, `data/` is
still on the root filesystem, the disk lacks the configured free space,
Compose is invalid, or placeholders remain.

Do not lower `OJS_MIN_FREE_GB` merely to force a build onto a nearly full disk.
Increase the attached disk by snapshotting it and creating a larger disk, which
is the Lightsail-supported resize path.

## 8. Build the first release

Do the first build before starting Compose so serving MySQL is not competing
for memory. Use a persistent `tmux` session:

```bash
sudo -u ojs -H tmux new -s ojs-build
```

Inside that `tmux` session, run:

```bash
cd /srv/ojs_api
set -a
. ./.env
set +a
make update
make verify
```

If `OJS_SOURCE_INDEX_URL` is configured for an HTML archive instead, use this
block in place of the preceding build block:

```bash
python3 src/scrape_updates.py --index-url "$OJS_SOURCE_INDEX_URL"
python3 src/run_pipeline.py --skip-download
make verify
```

Detach with `Ctrl-b d`; reattach with:

```bash
sudo -u ojs -H tmux attach -t ojs-build
```

On this burstable four-vCPU host, the build can take many hours. Keep
`OJS_METADATA_WORKERS=1`. Monitor in another SSH session:

```bash
free -h
vmstat 5
df -h /srv/ojs_api/data
ps -o pid,ni,%cpu,%mem,rss,cmd -C mysqld -C python3
```

For a first Lightsail deployment, process the latest available snapshot with
`make update`. Do not replay years of history on this host unless the attached
disk, CPU time, and archive policy were sized for it. A full historical build
can instead be produced on a temporary larger machine and transferred as a
validated clean export plus report, checksums, and manifest.

## 9. Start MySQL and the API

```bash
cd /srv/ojs_api
sudo -u ojs docker compose config --quiet
sudo -u ojs docker compose up -d --build
sudo -u ojs docker compose ps
```

The Compose profile caps serving MySQL at 7 GB plus 2 GB permitted swap and
caps the API at 512 MB. MySQL's buffer pool is 5 GB. The monthly host builder
uses a separate 2 GB buffer pool and one metadata worker.

Smoke-test locally:

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
```

## 10. Configure HTTPS

AWS-managed Lightsail certificates do not attach directly to a plain VM; they
attach to services such as a Lightsail load balancer or CDN. This single-VM
profile uses Caddy and Let's Encrypt.

Create `/etc/caddy/Caddyfile`:

```caddyfile
api.your-domain.example {
    encode zstd gzip
    reverse_proxy 127.0.0.1:8000
}
```

Then validate and reload:

```bash
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl enable --now caddy
sudo systemctl reload caddy
curl --fail https://api.your-domain.example/health
```

Do not proceed until HTTPS works and unauthenticated data endpoints return
`401`. The AWS certificate limitation is described in
[TLS certificates in Lightsail](https://docs.aws.amazon.com/lightsail/latest/userguide/understanding-tls-ssl-certificates-in-lightsail-https.html).

## 11. Enable the daily update timer

Use systemd, not cron, on Ubuntu:

```bash
cd /srv/ojs_api
sudo install -m 644 deploy/ojs-api-update.service \
  /etc/systemd/system/ojs-api-update.service
sudo install -m 644 deploy/ojs-api-update.timer \
  /etc/systemd/system/ojs-api-update.timer
sudo systemctl daemon-reload
sudo systemctl enable --now ojs-api-update.timer
sudo systemctl start ojs-api-update.service
sudo journalctl -u ojs-api-update.service -f
```

The job runs daily, but the expensive build runs only when a new dated file is
pending. It is low-priority, requires at least 300 GB free on the **data**
filesystem and 2 GB currently available RAM, and is constrained by systemd to
6 GB memory plus 2 GB swap. Once a prior release exists, the wrapper raises the
disk requirement dynamically to cover the last raw file plus current build
database, 25% growth margin, and 20 GB scratch headroom. A failed build leaves
the serving release intact.

If systemd cannot be used, install the checked-in once-daily cron entry for the
`ojs` account. Do not enable both schedulers:

```bash
sudo apt install -y cron
sudo systemctl enable --now cron
sudo -u ojs crontab /srv/ojs_api/deploy/ojs-api.crontab.example
sudo -u ojs crontab -l
```

## 12. Configure snapshots and monitoring

Enable Lightsail automatic instance snapshots well away from the 03:20–05:20
randomized update start—for example 18:00 UTC. A new-data build can take longer
than that, so monitor the monthly run and expect occasional overlap rather than
claiming any clock time guarantees separation. Instance snapshots include
attached disks. Automatic snapshots are daily and AWS retains the latest seven;
they are also deleted if the source resource is deleted. Keep periodic manual
snapshots and copy a monthly recovery point to another Region.

See:

- [automatic snapshot configuration](https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-configuring-automatic-snapshots.html)
- [Lightsail snapshot behavior](https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-faq-snapshots.html)
- [copy snapshots between Regions](https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-copying-snapshots-from-one-region-to-another.html)

Create Lightsail alarms for:

- instance status-check failure;
- sustained high CPU;
- low burst-capacity percentage;
- unexpected network traffic.

Lightsail does not publish host RAM, swap, or filesystem-free metrics. Monitor
those on the host or with an external agent. Useful commands are:

```bash
free -h
swapon --show
df -h /srv/ojs_api/data
sudo -u ojs docker stats --no-stream
systemctl list-timers ojs-api-update.timer
journalctl -u ojs-api-update.service --since today
sudo -u ojs docker compose -f /srv/ojs_api/docker-compose.yml \
  logs --tail=200 mysql ojs-api
```

## 13. Keep the attached disk bounded

A 1 TB disk requires active retention management. Every ~130 GB monthly raw
snapshot consumes another large fraction before MySQL build files and clean
exports.

After every successful release:

1. Verify the new JSON report, export sidecar, and release manifest.
2. Copy the immutable raw dump, clean export, report, sidecars, and manifest to
   versioned off-host storage. Preserve the report's source SHA-256.
3. Retain at least two independent recoverable release copies.
4. Restart Compose during a short maintenance window so its bind mount follows
   the newest `mysql-current` target:

   ```bash
   cd /srv/ojs_api
   sudo -u ojs docker compose down
   sudo -u ojs docker compose up -d
   curl --fail http://127.0.0.1:8000/health
   ```

5. Confirm both paths before considering an old build database for removal:

   ```bash
   readlink -f data/clean/mysql-current
   sudo -u ojs docker inspect ojs-mysql --format \
     '{{range .Mounts}}{{if eq .Destination "/var/lib/mysql"}}{{.Source}}{{end}}{{end}}'
   find data/clean -maxdepth 1 -type d -name 'mysql-????-??-??' -print
   ```

   The first two commands must identify the same directory. Never remove that
   directory, a `.building` directory under investigation, or the only copy of
   a release. Review old candidates individually; cleanup is intentionally not
   automated.

6. Keep the newest raw snapshots locally and archive older ones before removing
   them. Restore all dated raw files from the archive before any history replay.

Take a manual Lightsail snapshot before filesystem cleanup. Do not treat a
snapshot in the same account/Region as the only backup.

## 14. When 16 GB Lightsail is no longer enough

Reduce pressure in this order:

1. Keep one metadata worker.
2. Lower the serving buffer pool to 4 GB and its container limit to 6 GB.
3. Lower the builder buffer pool to 1 GB.
4. Move the initial/history build to a temporary larger host.
5. Move the workload to EC2 with EBS if Lightsail burst CPU or attached-disk
   throughput makes updates miss their operational window.

Never fix an OOM by disabling the release guard, increasing concurrency, or
allowing unlimited swap. The pipeline is restartable; preserving the live API
and the integrity of its release history takes priority over build speed.
