#!/usr/bin/env bash
set -Eeuo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

env_file="${OJS_ENV_FILE:-$project_root/.env}"
if [[ ! -r "$env_file" ]]; then
    printf 'error: deployment environment is not readable: %s\n' "$env_file" >&2
    exit 1
fi

set -a
# shellcheck disable=SC1090
. "$env_file"
set +a

: "${MYSQL_ROOT_PASSWORD:?MYSQL_ROOT_PASSWORD must be set in .env}"
python_bin="${PYTHON:-python3}"
minimum_free_gb="${OJS_MIN_FREE_GB:-300}"
minimum_available_memory_mb="${OJS_MIN_AVAILABLE_MEMORY_MB:-2048}"
working_set_percent="${OJS_UPDATE_WORKING_SET_PERCENT:-125}"
headroom_gb="${OJS_UPDATE_HEADROOM_GB:-20}"
update_nice="${OJS_UPDATE_NICE:-15}"

for numeric_setting in \
    "$minimum_free_gb" \
    "$minimum_available_memory_mb" \
    "$working_set_percent" \
    "$headroom_gb" \
    "$update_nice"; do
    if [[ ! "$numeric_setting" =~ ^[0-9]+$ ]]; then
        printf 'error: capacity and nice settings must be non-negative integers\n' \
            >&2
        exit 2
    fi
done
if (( update_nice > 19 )); then
    printf 'error: OJS_UPDATE_NICE must be between 0 and 19\n' >&2
    exit 2
fi
if (( working_set_percent < 100 )); then
    printf 'error: OJS_UPDATE_WORKING_SET_PERCENT must be at least 100\n' >&2
    exit 2
fi

data_root="$project_root/data"
mkdir -p "$data_root/clean"
exec 9>data/clean/.automatic-update.lock
if ! flock -n 9; then
    printf '[update] another automatic update is already running; exiting\n'
    exit 0
fi

available_kb="$(df -Pk "$data_root" | awk 'NR == 2 {print $4}')"
required_kb="$((minimum_free_gb * 1024 * 1024))"
latest_raw="$(readlink -f "$data_root/raw/pkpbeacon-latest.sql" 2>/dev/null || true)"
current_database="$(readlink -f "$data_root/clean/mysql-current" 2>/dev/null || true)"
raw_kb=0
database_kb=0
if [[ -f "$latest_raw" ]]; then
    raw_bytes="$(stat -c '%s' "$latest_raw")"
    raw_kb="$(((raw_bytes + 1023) / 1024))"
fi
if [[ -d "$current_database" ]]; then
    database_kb="$(
        du -sk -- "$current_database" 2>/dev/null | awk '{print $1}' || true
    )"
    if [[ ! "$database_kb" =~ ^[0-9]+$ ]]; then
        printf '[update] warning: could not size the current database; using the fixed disk floor only\n' >&2
        database_kb=0
    fi
fi
if (( raw_kb > 0 && database_kb > 0 )); then
    estimated_kb="$((
        ((raw_kb + database_kb) * working_set_percent + 99) / 100
        + headroom_gb * 1024 * 1024
    ))"
    if (( estimated_kb > required_kb )); then
        required_kb="$estimated_kb"
    fi
    printf '[update] estimated working space: %s GiB (last raw + current database, %s%% plus %s GiB)\n' \
        "$(((estimated_kb + 1024 * 1024 - 1) / 1024 / 1024))" \
        "$working_set_percent" \
        "$headroom_gb"
fi
if (( available_kb < required_kb )); then
    printf 'error: the update needs an estimated %s GiB free on the data filesystem but only %s GiB is available\n' \
        "$(((required_kb + 1024 * 1024 - 1) / 1024 / 1024))" \
        "$((available_kb / 1024 / 1024))" >&2
    exit 1
fi

available_memory_kb="$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)"
required_memory_kb="$((minimum_available_memory_mb * 1024))"
if [[ -n "$available_memory_kb" ]] \
    && (( available_memory_kb < required_memory_kb )); then
    printf 'error: less than %s MiB of memory is available; refusing to start update\n' \
        "$minimum_available_memory_mb" >&2
    exit 1
fi
printf '[update] capacity gate passed: %s GiB disk free (%s GiB required), %s MiB memory available\n' \
    "$((available_kb / 1024 / 1024))" \
    "$(((required_kb + 1024 * 1024 - 1) / 1024 / 1024))" \
    "$((available_memory_kb / 1024))"

pipeline=(
    "$python_bin" src/run_pipeline.py
    --resume-building
    --force-rebuild
)
runner=(nice -n "$update_nice")
if command -v ionice >/dev/null 2>&1; then
    runner=(ionice -c 2 -n 7 "${runner[@]}")
fi

source_index_url="${OJS_SOURCE_INDEX_URL:-}"
if [[ -n "$source_index_url" ]]; then
    scraper=(
        "$python_bin" src/scrape_updates.py
        --index-url "$source_index_url"
    )
    if [[ "${OJS_SOURCE_ALLOW_CROSS_ORIGIN:-0}" == "1" ]]; then
        scraper+=(--allow-cross-origin)
    fi
    printf '[update] scraping the configured source index for every missing snapshot\n'
    "${runner[@]}" "${scraper[@]}"
    pipeline+=(--skip-download)
else
    printf '[update] checking the PKP Beacon snapshot endpoint\n'
fi

printf '[update] processing every pending snapshot in chronological order\n'
"${runner[@]}" "${pipeline[@]}"

publish=("$python_bin" src/publish_live.py)
if [[ "${OJS_KEEP_PREVIOUS_RELEASE:-0}" == "1" ]]; then
    publish+=(--keep-previous)
fi
printf '[update] ensuring the validated release is live\n'
"${publish[@]}"
printf '[update] complete\n'
