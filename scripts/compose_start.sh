#!/usr/bin/env bash
set -euo pipefail

if [[ -d /app ]]; then
  cd /app
else
  cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi
APP_ROOT="$(pwd)"

timestamp() {
  date -u +"%Y-%m-%d %H:%M:%S UTC"
}

log() {
  echo "[$(timestamp)] $*"
}

run_convert_step() {
  local force_convert="${OJS_FORCE_CONVERT:-0}"
  local convert_args="${OJS_CONVERT_ARGS:---skip-existing --report-every 0 --progress-refresh-seconds 5}"

  local required=(
    "data/raw_parquet/records.parquet"
    "data/raw_parquet/contexts.parquet"
    "data/raw_parquet/issns.parquet"
    "data/raw_parquet/endpoints.parquet"
  )
  local missing=0
  for path in "${required[@]}"; do
    if [[ ! -f "${path}" ]]; then
      missing=1
      break
    fi
  done

  if [[ "${force_convert}" == "1" || "${missing}" == "1" ]]; then
    log "Running src/0_convert_db.py"
    # shellcheck disable=SC2086
    python src/0_convert_db.py ${convert_args}
  else
    log "Skipping src/0_convert_db.py (required table parquet files already exist)"
  fi
}

run_join_step() {
  local force_join="${OJS_FORCE_JOIN:-0}"
  local join_args="${OJS_JOIN_ARGS:---overwrite}"

  if [[ "${force_join}" == "1" || ! -f "data/raw/jonied.parquet" ]]; then
    log "Running src/1_join_raw.py"
    # shellcheck disable=SC2086
    python src/1_join_raw.py ${join_args}
  else
    log "Skipping src/1_join_raw.py (data/raw/jonied.parquet already exists)"
  fi
}

run_dedupe_step() {
  local force_dedupe="${OJS_FORCE_DEDUPE:-0}"
  local dedupe_args="${OJS_DEDUPE_ARGS:---overwrite}"

  if [[ "${force_dedupe}" == "1" || ! -f "data/clean/deduplicated.parquet" ]]; then
    log "Running src/2_deduplicate.py"
    # shellcheck disable=SC2086
    python src/2_deduplicate.py ${dedupe_args}
  else
    log "Skipping src/2_deduplicate.py (data/clean/deduplicated.parquet already exists)"
  fi
}

run_api_step() {
  local api_args="${OJS_API_ARGS:-}"
  if [[ -z "${api_args}" ]]; then
    api_args="--input ${APP_ROOT}/data/clean/deduplicated.parquet --host 0.0.0.0 --port 8000"
  fi
  log "Starting src/3_build_api.py"
  # shellcheck disable=SC2086
  exec python src/3_build_api.py ${api_args}
}

mkdir -p "${APP_ROOT}/data/raw_sql" "${APP_ROOT}/data/raw_parquet" "${APP_ROOT}/data/raw" "${APP_ROOT}/data/clean"

run_convert_step
run_join_step
run_dedupe_step
run_api_step
