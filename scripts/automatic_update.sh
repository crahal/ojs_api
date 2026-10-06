#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

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
export PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

update_nice="${OJS_UPDATE_NICE:-15}"
if [[ ! "$update_nice" =~ ^[0-9]+$ ]] || (( update_nice > 19 )); then
    printf 'error: OJS_UPDATE_NICE must be an integer between 0 and 19\n' >&2
    exit 2
fi
runner=(nice -n "$update_nice")
if command -v ionice >/dev/null 2>&1; then
    runner=(ionice -c 2 -n 7 "${runner[@]}")
fi
exec "${runner[@]}" "${PYTHON:-python3}" src/compact_update.py "$@"
