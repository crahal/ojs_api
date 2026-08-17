#!/usr/bin/env bash
set -Eeuo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
destination="${1:-$project_root/.secrets/api-client.env}"
username="${2:-ojs-api-client}"
database_destination="${3:-$project_root/.secrets/db-api-password}"

if [[ ! "$username" =~ ^[A-Za-z0-9_.@+-]+$ ]]; then
    printf 'error: username contains unsupported characters\n' >&2
    exit 2
fi
if [[ -e "$destination" ]]; then
    printf 'error: refusing to overwrite existing credentials: %s\n' "$destination" >&2
    exit 1
fi
if [[ -e "$database_destination" ]]; then
    printf 'error: refusing to overwrite existing database credentials: %s\n' \
        "$database_destination" >&2
    exit 1
fi
if ! command -v openssl >/dev/null 2>&1; then
    printf 'error: openssl is required\n' >&2
    exit 1
fi

umask 077
mkdir -p "$(dirname "$destination")"
mkdir -p "$(dirname "$database_destination")"
api_key="$(openssl rand -hex 32)"
database_password="$(openssl rand -hex 32)"
printf 'OJS_API_USERNAME=%s\nOJS_API_KEY=%s\n' \
    "$username" "$api_key" > "$destination"
printf '%s\n' "$database_password" > "$database_destination"
chmod 600 "$destination"
chmod 600 "$database_destination"
unset api_key database_password

printf 'Created private API credentials at %s (mode 600).\n' "$destination"
printf 'Created private API database password at %s (mode 600).\n' \
    "$database_destination"
printf 'The secrets were not printed. Transfer them only through a secure channel.\n'
