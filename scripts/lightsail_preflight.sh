#!/usr/bin/env bash
set -Eeuo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

env_file="${OJS_ENV_FILE:-$project_root/.env}"
if [[ ! -r "$env_file" ]]; then
    printf 'FAIL: deployment environment is not readable: %s\n' "$env_file" >&2
    exit 1
fi

set -a
# shellcheck disable=SC1090
. "$env_file"
set +a

failures=0
fail() {
    printf 'FAIL: %s\n' "$*" >&2
    failures=$((failures + 1))
}

for command_name in \
    docker \
    findmnt \
    flock \
    gzip \
    mysql \
    mysqladmin \
    mysqld \
    mysqldump \
    openssl \
    python3; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        fail "required command is missing: $command_name"
    fi
done
if command -v docker >/dev/null 2>&1 \
    && ! docker compose version >/dev/null 2>&1; then
    fail "Docker Compose v2 is unavailable to the deployment user"
fi
if command -v docker >/dev/null 2>&1 \
    && ! docker info >/dev/null 2>&1; then
    fail "the deployment user cannot reach the Docker daemon"
fi

minimum_free_gb="${OJS_MIN_FREE_GB:-20}"
minimum_memory_mb="${OJS_MIN_AVAILABLE_MEMORY_MB:-512}"
if [[ ! "$minimum_free_gb" =~ ^[0-9]+$ ]]; then
    fail "OJS_MIN_FREE_GB must be a non-negative integer"
    minimum_free_gb=20
fi
if [[ ! "$minimum_memory_mb" =~ ^[0-9]+$ ]]; then
    fail "OJS_MIN_AVAILABLE_MEMORY_MB must be a non-negative integer"
    minimum_memory_mb=512
fi

total_memory_kb="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
available_memory_kb="$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)"
swap_total_kb="$(awk '/^SwapTotal:/ {print $2}' /proc/meminfo)"
if (( total_memory_kb < 3 * 1024 * 1024 )); then
    fail "less than 3 GiB of physical RAM is visible; select at least the 4 GB plan"
fi
if (( available_memory_kb < minimum_memory_mb * 1024 )); then
    fail "less than ${minimum_memory_mb} MiB of memory is currently available"
fi
if (( swap_total_kb < 4 * 1024 * 1024 )); then
    fail "less than 4 GiB of emergency swap is configured"
fi

data_root="$project_root/data"
if [[ ! -d "$data_root" ]]; then
    fail "data directory does not exist: $data_root"
else
    if [[ ! -w "$data_root" ]]; then
        fail "data directory is not writable by the deployment user: $data_root"
    fi
    read -r filesystem_kb available_kb < <(
        df -Pk "$data_root" | awk 'NR == 2 {print $2, $4}'
    )
    if (( available_kb < minimum_free_gb * 1024 * 1024 )); then
        fail "data filesystem has less than ${minimum_free_gb} GiB free"
    fi
    root_source="$(findmnt -n -o SOURCE -T / 2>/dev/null || true)"
    data_source="$(findmnt -n -o SOURCE -T "$data_root" 2>/dev/null || true)"
    if [[ -n "$root_source" && "$root_source" == "$data_source" ]]; then
        printf 'NOTE: data uses the root filesystem; the first full build must establish that its peak fits.\n'
    fi
    printf 'Data filesystem: %s (%s GiB total, %s GiB free)\n' \
        "${data_source:-unknown}" \
        "$((filesystem_kb / 1024 / 1024))" \
        "$((available_kb / 1024 / 1024))"
fi

check_private_file() {
    local path="$1"
    local label="$2"
    if [[ ! -f "$path" ]]; then
        fail "$label does not exist: $path"
        return
    fi
    local mode
    mode="$(stat -c '%a' "$path")"
    if (( 10#$mode % 100 != 0 )); then
        fail "$label has group/other permission bits: $path (mode $mode)"
    fi
}

check_private_file "$env_file" ".env"
check_private_file \
    "${OJS_API_CREDENTIALS_HOST_FILE:-$project_root/.secrets/api-client.env}" \
    "API credential file"
check_private_file \
    "${OJS_DB_API_PASSWORD_FILE:-$project_root/.secrets/db-api-password}" \
    "API database password file"
check_private_file \
    "${OJS_BEACON_CREDENTIALS_FILE:-$project_root/.secrets/beacon.ini}" \
    "Beacon credential file"

if [[ -z "${MYSQL_ROOT_PASSWORD:-}" \
    || "${MYSQL_ROOT_PASSWORD,,}" == *replace* ]]; then
    fail "MYSQL_ROOT_PASSWORD is unset or still a placeholder"
fi
if [[ -z "${OJS_ADMIN_EMAIL:-}" \
    || "$OJS_ADMIN_EMAIL" == *example.org* \
    || "$OJS_ADMIN_EMAIL" == *.example* ]]; then
    fail "OJS_ADMIN_EMAIL is unset or still a placeholder"
fi
if [[ -z "${OJS_PUBLIC_BASE_URL:-}" \
    || "$OJS_PUBLIC_BASE_URL" == *example.org* \
    || "$OJS_PUBLIC_BASE_URL" == *.example* \
    || ! "$OJS_PUBLIC_BASE_URL" =~ ^https://[^/]+$ ]]; then
    fail "OJS_PUBLIC_BASE_URL must be a non-placeholder HTTPS origin without a path"
fi
if [[ "${OJS_SOURCE_INDEX_URL:-}" == "https://example.com/ojs-data/" ]]; then
    fail "OJS_SOURCE_INDEX_URL still points at the placeholder website"
fi
if [[ "${OJS_HOST_UID:-}" != "$(id -u)" ]]; then
    fail "OJS_HOST_UID does not match the deployment user's UID"
fi
if [[ "${OJS_HOST_GID:-}" != "$(id -g)" ]]; then
    fail "OJS_HOST_GID does not match the deployment user's primary GID"
fi

if command -v mysqld >/dev/null 2>&1; then
    host_mysql_description="$(mysqld --version)"
    host_mysql_version="$(
        sed -nE 's/.*Ver ([0-9]+\.[0-9]+\.[0-9]+).*/\1/p' \
            <<<"$host_mysql_description"
    )"
    printf 'Host MySQL: %s\n' "$host_mysql_description"
fi
configured_mysql_image="${OJS_MYSQL_IMAGE:-mysql:8.4.0}"
configured_mysql_tag="${configured_mysql_image##*:}"
printf 'Configured serving image: %s\n' "$configured_mysql_image"
if [[ -z "${host_mysql_version:-}" ]]; then
    fail "could not parse the host MySQL patch version"
elif [[ ! "$configured_mysql_tag" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    fail "OJS_MYSQL_IMAGE must use an exact numeric patch tag"
elif [[ "$configured_mysql_tag" != "$host_mysql_version" ]]; then
    fail "host MySQL $host_mysql_version does not match image tag $configured_mysql_tag"
fi
printf 'Memory: %s MiB total, %s MiB available, %s MiB swap\n' \
    "$((total_memory_kb / 1024))" \
    "$((available_memory_kb / 1024))" \
    "$((swap_total_kb / 1024))"

if command -v docker >/dev/null 2>&1 \
    && ! docker compose config --quiet >/dev/null 2>&1; then
    fail "docker compose config validation failed"
fi

if (( failures > 0 )); then
    printf 'Preflight failed with %s problem(s).\n' "$failures" >&2
    exit 1
fi
printf 'Lightsail 4 GiB configuration preflight passed; full-data peak storage/RAM still require measurement.\n'
