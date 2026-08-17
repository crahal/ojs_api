#!/usr/bin/env python3
"""Serve the temporal PKP Beacon article catalogue."""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import re
import secrets
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from xml.etree import ElementTree as ET

import pymysql
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pymysql.cursors import DictCursor

DEFAULT_DB_HOST = "127.0.0.1"
DEFAULT_DB_PORT = 3306
DEFAULT_DB_NAME = "pkpbeacon_db"
DEFAULT_DB_USER = "ojs_api"
DEFAULT_ARTICLE_TABLE = "ojs_articles"
DEFAULT_SOURCE_TABLE = "ojs_article_sources"
DEFAULT_EVENT_TABLE = "ojs_article_events"
DEFAULT_SNAPSHOT_TABLE = "ojs_snapshots"

IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")
ISSN_CLEAN_RE = re.compile(r"[^0-9Xx]+")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
OAI_NS = "http://www.openarchives.org/OAI/2.0/"
OAI_DC_NS = "http://www.openarchives.org/OAI/2.0/oai_dc/"
DC_NS = "http://purl.org/dc/elements/1.1/"
OAI_IDENTIFIER_PREFIX = "oai:pkp-beacon:article:"
OAI_SET_PREFIX = "issn:"
OAI_METADATA_PREFIX = "oai_dc"
XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"
CREDENTIAL_NAME_RE = re.compile(r"^[A-Za-z0-9_.@+-]+$")
LOGGER = logging.getLogger(__name__)
ET.register_namespace("", OAI_NS)
ET.register_namespace("oai_dc", OAI_DC_NS)
ET.register_namespace("dc", DC_NS)

ARTICLE_COLUMNS = (
    "article_id",
    "status",
    "merged_into_article_id",
    "canonical_source_record_id",
    "source_count",
    "active_source_count",
    "version_number",
    "date_added",
    "date_modified",
    "date_removed",
    "application",
    "journal_issn",
    "journal_title",
    "endpoint_oai_url",
    "source_oai_identifier",
    "record_update_date",
    "record_publish_date",
    "title",
    "creators",
    "subjects",
    "description",
    "publisher",
    "published",
    "types",
    "formats",
    "identifiers",
    "source_title",
    "languages",
    "relations",
    "coverage",
    "rights",
    "doi",
    "article_url",
)
ARTICLE_INT_COLUMNS = {
    "article_id",
    "merged_into_article_id",
    "canonical_source_record_id",
    "source_count",
    "active_source_count",
    "version_number",
}
SOURCE_INT_COLUMNS = {
    "source_record_id",
    "article_id",
    "context_id",
    "endpoint_id",
    "source_identifier",
}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return default if raw in (None, "") else int(raw)


def quote_identifier(identifier: str) -> str:
    if not IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"unsafe SQL identifier: {identifier!r}")
    return f"`{identifier}`"


def normalize_issn(raw: str) -> str:
    normalized = ISSN_CLEAN_RE.sub("", raw).upper()
    if len(normalized) != 8:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="issn must normalize to exactly eight characters",
        )
    return normalized


def validate_date(raw: str | None, field: str) -> str | None:
    if raw is None:
        return None
    if not DATE_RE.fullmatch(raw):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{field} must be YYYY-MM-DD",
        )
    try:
        date.fromisoformat(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{field} is not a valid date",
        ) from exc
    return raw


def decode_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


def coerce_ints(row: dict[str, Any], columns: set[str]) -> dict[str, Any]:
    for column in columns:
        if row.get(column) is not None:
            row[column] = int(row[column])
    return row


def parse_metadata_xml(raw: str | bytes | None) -> dict[str, list[str]]:
    if raw is None:
        return {}
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return {}
    metadata_node = root.find(".//{*}metadata")
    if metadata_node is None or len(metadata_node) == 0:
        return {}
    output: dict[str, list[str]] = {}
    for node in metadata_node[0].iter():
        if node is metadata_node[0]:
            continue
        value = " ".join("".join(node.itertext()).split())
        if not value:
            continue
        local_name = node.tag.rsplit("}", 1)[-1]
        output.setdefault(local_name, []).append(value)
    return output


def normalize_article(
    row: dict[str, Any],
    *,
    structured_metadata: bool,
    include_metadata_xml: bool,
) -> dict[str, Any]:
    output = coerce_ints(dict(row), ARTICLE_INT_COLUMNS)
    metadata_xml = output.get("metadata_xml")
    if structured_metadata:
        output["metadata"] = parse_metadata_xml(metadata_xml)
    if not include_metadata_xml:
        output.pop("metadata_xml", None)
    return output


def encode_token(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_token(raw: str) -> dict[str, Any]:
    try:
        padded = raw + ("=" * (-len(raw) % 4))
        payload = json.loads(base64.urlsafe_b64decode(padded).decode())
    except Exception as exc:
        raise OAIError("badResumptionToken", "invalid resumptionToken") from exc
    if not isinstance(payload, dict):
        raise OAIError("badResumptionToken", "invalid resumptionToken payload")
    return payload


@dataclass(frozen=True)
class APIConfig:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    api_username: str
    api_password: str
    article_table: str
    source_table: str
    event_table: str
    snapshot_table: str
    oai_page_size: int
    public_base_url: str | None = None

    @property
    def article_sql(self) -> str:
        return quote_identifier(self.article_table)

    @property
    def source_sql(self) -> str:
        return quote_identifier(self.source_table)

    @property
    def event_sql(self) -> str:
        return quote_identifier(self.event_table)

    @property
    def snapshot_sql(self) -> str:
        return quote_identifier(self.snapshot_table)


@dataclass(frozen=True)
class OAIError(Exception):
    code: str
    message: str


class SQLBackend:
    def __init__(self, config: APIConfig) -> None:
        self.config = config

    def _connect(self) -> pymysql.connections.Connection:
        return pymysql.connect(
            host=self.config.db_host,
            port=self.config.db_port,
            user=self.config.db_user,
            password=self.config.db_password,
            database=self.config.db_name,
            charset="utf8mb4",
            cursorclass=DictCursor,
            autocommit=True,
            read_timeout=120,
            write_timeout=120,
        )

    def _fetchone(
        self,
        sql: str,
        params: list[Any] | None = None,
    ) -> dict[str, Any] | None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, params or [])
                return cursor.fetchone()

    def _fetchall(
        self,
        sql: str,
        params: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, params or [])
                return list(cursor.fetchall())

    @staticmethod
    def article_projection(alias: str = "a", include_xml: bool = True) -> str:
        fields = [f"{alias}.{name}" for name in ARTICLE_COLUMNS]
        fields.extend(
            (
                f"LOWER(HEX({alias}.data_hash)) AS data_hash",
                f"LOWER(HEX({alias}.provenance_hash)) AS provenance_hash",
            )
        )
        if include_xml:
            fields.append(f"{alias}.metadata_xml")
        return ", ".join(fields)

    def assert_ready(self) -> None:
        if not self._fetchone("SELECT 1 LIMIT 1"):
            raise RuntimeError("database readiness query returned no result")

    def meta(self) -> dict[str, Any]:
        latest = self._fetchone(
            f"""
            SELECT *
            FROM {self.config.snapshot_sql}
            ORDER BY snapshot_date DESC
            LIMIT 1
            """
        )
        watermark = self._fetchone(
            f"SELECT COALESCE(MAX(event_id), 0) AS event_id "
            f"FROM {self.config.event_sql}"
        )
        if not latest:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="no published snapshot",
            )
        return {
            "latest_snapshot": latest,
            "high_watermark_event_id": int((watermark or {}).get("event_id") or 0),
        }

    def snapshots(self) -> list[dict[str, Any]]:
        return self._fetchall(
            f"""
            SELECT *
            FROM {self.config.snapshot_sql}
            ORDER BY snapshot_date
            """
        )

    def list_articles(
        self,
        *,
        after_id: int,
        limit: int,
        article_status: str,
        changed_since: str | None,
        issn: str | None,
        doi: str | None,
        ids: list[int] | None,
        structured_metadata: bool,
        include_metadata_xml: bool,
    ) -> dict[str, Any]:
        conditions = ["a.article_id > %s"]
        params: list[Any] = [after_id]
        if article_status != "all":
            conditions.append("a.status = %s")
            params.append(article_status)
        if changed_since is not None:
            conditions.append("a.date_modified >= %s")
            params.append(changed_since)
        if issn is not None:
            conditions.append("a.journal_issn = %s")
            params.append(issn)
        if doi is not None:
            conditions.append("a.doi = %s")
            params.append(doi.strip().lower())
        if ids:
            placeholders = ", ".join(["%s"] * len(ids))
            conditions.append(f"a.article_id IN ({placeholders})")
            params.extend(ids)
        params.append(limit + 1)
        rows = self._fetchall(
            f"""
            SELECT {self.article_projection(
                "a",
                include_xml=structured_metadata or include_metadata_xml,
            )}
            FROM {self.config.article_sql} a
            WHERE {" AND ".join(conditions)}
            ORDER BY a.article_id
            LIMIT %s
            """,
            params,
        )
        has_more = len(rows) > limit
        rows = rows[:limit]
        records = [
            normalize_article(
                row,
                structured_metadata=structured_metadata,
                include_metadata_xml=include_metadata_xml,
            )
            for row in rows
        ]
        return {
            "data": records,
            "next_after_id": (
                int(rows[-1]["article_id"]) if has_more and rows else None
            ),
        }

    def article(
        self,
        article_id: int,
        *,
        structured_metadata: bool = True,
        include_metadata_xml: bool = False,
    ) -> dict[str, Any] | None:
        row = self._fetchone(
            f"""
            SELECT {self.article_projection(
                "a",
                include_xml=structured_metadata or include_metadata_xml,
            )}
            FROM {self.config.article_sql} a
            WHERE a.article_id = %s
            """,
            [article_id],
        )
        if not row:
            return None
        return normalize_article(
            row,
            structured_metadata=structured_metadata,
            include_metadata_xml=include_metadata_xml,
        )

    def sources(
        self,
        article_id: int,
        *,
        after_id: int,
        limit: int,
    ) -> dict[str, Any]:
        rows = self._fetchall(
            f"""
            SELECT
                source_record_id,
                article_id,
                context_id,
                endpoint_id,
                source_identifier,
                application,
                journal_issn,
                journal_title,
                endpoint_oai_url,
                source_oai_identifier,
                record_update_date,
                record_publish_date,
                record_created_at,
                record_modified_at,
                source_removed_at,
                is_present,
                is_active,
                date_added,
                date_modified,
                date_removed,
                title,
                first_creator,
                publication_year,
                doi,
                article_url,
                LOWER(HEX(metadata_hash)) AS metadata_hash
            FROM {self.config.source_sql}
            WHERE article_id = %s
              AND source_record_id > %s
            ORDER BY source_record_id
            LIMIT %s
            """,
            [article_id, after_id, limit + 1],
        )
        has_more = len(rows) > limit
        rows = rows[:limit]
        for row in rows:
            coerce_ints(row, SOURCE_INT_COLUMNS)
            row["is_present"] = bool(row["is_present"])
            row["is_active"] = bool(row["is_active"])
        return {
            "data": rows,
            "next_after_id": (
                int(rows[-1]["source_record_id"]) if has_more and rows else None
            ),
        }

    def changes(
        self,
        *,
        after_event_id: int,
        limit: int,
        structured_metadata: bool,
        include_metadata_xml: bool,
    ) -> dict[str, Any]:
        rows = self._fetchall(
            f"""
            SELECT
                e.event_id,
                e.snapshot_date AS event_snapshot_date,
                e.event_type,
                e.operation AS event_operation,
                e.version_number AS event_version_number,
                e.redirect_to_article_id AS event_redirect_to_article_id,
                LOWER(HEX(e.previous_data_hash)) AS previous_data_hash,
                LOWER(HEX(e.data_hash)) AS event_data_hash,
                LOWER(HEX(e.previous_provenance_hash))
                    AS previous_provenance_hash,
                LOWER(HEX(e.provenance_hash)) AS event_provenance_hash,
                e.reason,
                {self.article_projection(
                    "a",
                    include_xml=structured_metadata or include_metadata_xml,
                )}
            FROM {self.config.event_sql} e
            INNER JOIN {self.config.article_sql} a
                ON a.article_id = e.article_id
            WHERE e.event_id > %s
            ORDER BY e.event_id
            LIMIT %s
            """,
            [after_event_id, limit + 1],
        )
        has_more = len(rows) > limit
        rows = rows[:limit]
        output: list[dict[str, Any]] = []
        event_fields = {
            "event_id",
            "event_snapshot_date",
            "event_type",
            "event_operation",
            "event_version_number",
            "event_redirect_to_article_id",
            "previous_data_hash",
            "event_data_hash",
            "previous_provenance_hash",
            "event_provenance_hash",
            "reason",
        }
        for row in rows:
            event = {key: row[key] for key in event_fields}
            event["event_id"] = int(event["event_id"])
            event["event_version_number"] = int(event["event_version_number"])
            if event["event_redirect_to_article_id"] is not None:
                event["event_redirect_to_article_id"] = int(
                    event["event_redirect_to_article_id"]
                )
            event["reason"] = decode_json(event["reason"], {})
            article_row = {
                key: value for key, value in row.items() if key not in event_fields
            }
            article = normalize_article(
                article_row,
                structured_metadata=structured_metadata,
                include_metadata_xml=include_metadata_xml,
            )
            current_operation = (
                "upsert" if article["status"] == "active" else "delete"
            )
            output.append(
                {
                    "event": event,
                    "current_operation": current_operation,
                    "article": article,
                }
            )
        return {
            "data": output,
            "next_after_event_id": (
                int(rows[-1]["event_id"]) if has_more and rows else None
            ),
        }

    def oai_records(
        self,
        *,
        from_date: str | None,
        until_date: str | None,
        issn: str | None,
        after_date: str | None,
        after_id: int,
        limit: int,
    ) -> tuple[list[dict[str, Any]], bool]:
        conditions: list[str] = []
        params: list[Any] = []
        if from_date is not None:
            conditions.append("a.date_modified >= %s")
            params.append(from_date)
        if until_date is not None:
            conditions.append("a.date_modified <= %s")
            params.append(until_date)
        if issn is not None:
            conditions.append("a.journal_issn = %s")
            params.append(issn)
        if after_date is not None:
            conditions.append(
                "(a.date_modified > %s OR "
                "(a.date_modified = %s AND a.article_id > %s))"
            )
            params.extend((after_date, after_date, after_id))
        where_sql = (
            "WHERE " + " AND ".join(conditions) if conditions else ""
        )
        params.append(limit + 1)
        rows = self._fetchall(
            f"""
            SELECT {self.article_projection("a", include_xml=True)}
            FROM {self.config.article_sql} a
            {where_sql}
            ORDER BY a.date_modified, a.article_id
            LIMIT %s
            """,
            params,
        )
        return rows[:limit], len(rows) > limit

    def oai_sets(
        self,
        *,
        after_issn: str,
        limit: int,
    ) -> tuple[list[dict[str, Any]], bool]:
        rows = self._fetchall(
            f"""
            SELECT
                journal_issn,
                MIN(journal_title) AS journal_title,
                COUNT(*) AS article_count
            FROM {self.config.article_sql}
            WHERE journal_issn IS NOT NULL
              AND journal_issn > %s
            GROUP BY journal_issn
            ORDER BY journal_issn
            LIMIT %s
            """,
            [after_issn, limit + 1],
        )
        for row in rows:
            row["article_count"] = int(row["article_count"])
        return rows[:limit], len(rows) > limit


def oai_root(request_url: str, attrs: dict[str, str | None]) -> ET.Element:
    root = ET.Element(f"{{{OAI_NS}}}OAI-PMH")
    ET.SubElement(root, f"{{{OAI_NS}}}responseDate").text = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    request_node = ET.SubElement(root, f"{{{OAI_NS}}}request")
    for key, value in attrs.items():
        if value is not None:
            request_node.set(key, value)
    request_node.text = request_url
    return root


def oai_response(root: ET.Element) -> Response:
    return Response(
        content=ET.tostring(root, encoding="utf-8", xml_declaration=True),
        media_type="text/xml; charset=utf-8",
    )


def oai_identifier(article_id: int) -> str:
    return f"{OAI_IDENTIFIER_PREFIX}{article_id}"


def parse_oai_identifier(raw: str) -> int:
    if not raw.startswith(OAI_IDENTIFIER_PREFIX):
        raise OAIError("idDoesNotExist", "unknown identifier")
    suffix = raw[len(OAI_IDENTIFIER_PREFIX) :]
    if not suffix.isdigit():
        raise OAIError("idDoesNotExist", "invalid identifier")
    return int(suffix)


def parse_oai_set(raw: str | None) -> str | None:
    if raw is None:
        return None
    if not raw.startswith(OAI_SET_PREFIX):
        raise OAIError("badArgument", "set must be issn:<ISSN>")
    normalized = ISSN_CLEAN_RE.sub("", raw[len(OAI_SET_PREFIX) :]).upper()
    if len(normalized) != 8:
        raise OAIError("badArgument", "invalid ISSN set")
    return normalized


def append_oai_header(parent: ET.Element, row: dict[str, Any]) -> ET.Element:
    deleted = row["status"] != "active"
    header = ET.SubElement(parent, f"{{{OAI_NS}}}header")
    if deleted:
        header.set("status", "deleted")
    ET.SubElement(header, f"{{{OAI_NS}}}identifier").text = oai_identifier(
        int(row["article_id"])
    )
    ET.SubElement(header, f"{{{OAI_NS}}}datestamp").text = str(
        row["date_modified"]
    )
    if row.get("journal_issn"):
        ET.SubElement(header, f"{{{OAI_NS}}}setSpec").text = (
            f"{OAI_SET_PREFIX}{row['journal_issn']}"
        )
    return header


def append_fallback_dc(parent: ET.Element, row: dict[str, Any]) -> None:
    dc = ET.SubElement(parent, f"{{{OAI_DC_NS}}}dc")
    mapping = (
        ("title", "title"),
        ("creator", "creators"),
        ("subject", "subjects"),
        ("description", "description"),
        ("publisher", "publisher"),
        ("date", "published"),
        ("type", "types"),
        ("format", "formats"),
        ("identifier", "identifiers"),
        ("source", "source_title"),
        ("language", "languages"),
        ("relation", "relations"),
        ("coverage", "coverage"),
        ("rights", "rights"),
    )
    for dc_name, column in mapping:
        value = row.get(column)
        if value:
            ET.SubElement(dc, f"{{{DC_NS}}}{dc_name}").text = str(value)


def append_oai_metadata(parent: ET.Element, row: dict[str, Any]) -> None:
    metadata = ET.SubElement(parent, f"{{{OAI_NS}}}metadata")
    raw = row.get("metadata_xml")
    if raw:
        try:
            source_record = ET.fromstring(raw)
            source_metadata = source_record.find(".//{*}metadata")
            if source_metadata is not None and len(source_metadata):
                metadata.append(source_metadata[0])
                return
        except ET.ParseError:
            pass
    append_fallback_dc(metadata, row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Serve the temporal PKP Beacon article catalogue."
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--db-host", default=os.getenv("OJS_DB_HOST", DEFAULT_DB_HOST))
    parser.add_argument(
        "--db-port",
        type=int,
        default=env_int("OJS_DB_PORT", DEFAULT_DB_PORT),
    )
    parser.add_argument("--db-name", default=os.getenv("OJS_DB_NAME", DEFAULT_DB_NAME))
    parser.add_argument("--db-user", default=os.getenv("OJS_DB_USER", DEFAULT_DB_USER))
    parser.add_argument("--db-password", default=os.getenv("OJS_DB_PASSWORD", ""))
    parser.add_argument(
        "--oai-page-size",
        type=int,
        default=env_int("OJS_OAI_PAGE_SIZE", 500),
    )
    args = parser.parse_args()
    if not 1 <= args.port <= 65535 or not 1 <= args.db_port <= 65535:
        parser.error("ports must be between 1 and 65535")
    if args.oai_page_size <= 0:
        parser.error("--oai-page-size must be positive")
    return args


def read_private_file(path: Path, description: str) -> str:
    try:
        file_stat = path.stat()
    except OSError as exc:
        raise ValueError(f"{description} file does not exist: {path}") from exc
    if not path.is_file():
        raise ValueError(f"{description} file is not a regular file: {path}")
    if file_stat.st_mode & 0o077:
        raise ValueError(
            f"{description} file permissions are too open: {path}; "
            "run chmod 600 on it"
        )
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"could not read {description} file: {path}") from exc


def read_db_password(path: Path) -> str:
    password = read_private_file(path, "database password").rstrip("\r\n")
    if not password:
        raise ValueError(f"database password file is empty: {path}")
    if "\n" in password or "\r" in password:
        raise ValueError(
            f"database password file must contain exactly one line: {path}"
        )
    return password


def read_api_credentials(path: Path) -> tuple[str, str]:
    contents = read_private_file(path, "API credentials")

    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(contents.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(
                f"invalid API credentials line {line_number}: {path}"
            )
        name, value = line.split("=", 1)
        name = name.strip()
        value = value.strip()
        if name not in {"OJS_API_USERNAME", "OJS_API_KEY"}:
            raise ValueError(
                f"unknown API credentials setting {name!r}: {path}"
            )
        if not value or not CREDENTIAL_NAME_RE.fullmatch(value):
            raise ValueError(
                f"invalid value for {name} in API credentials file: {path}"
            )
        values[name] = value

    username = values.get("OJS_API_USERNAME", "")
    api_key = values.get("OJS_API_KEY", "")
    if not username or not api_key:
        raise ValueError(
            "API credentials file must set OJS_API_USERNAME and OJS_API_KEY"
        )
    return username, api_key


def normalize_public_base_url(raw: str | None) -> str | None:
    if raw is None or not raw.strip():
        return None
    value = raw.strip().rstrip("/")
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "OJS_PUBLIC_BASE_URL must be an absolute HTTP(S) URL without "
            "credentials, query parameters, or a fragment"
        )
    return value


def resolve_config(args: argparse.Namespace) -> APIConfig:
    credentials_file = os.getenv("OJS_API_CREDENTIALS_FILE")
    if credentials_file:
        api_username, api_password = read_api_credentials(
            Path(credentials_file).expanduser()
        )
    else:
        api_username = os.getenv("OJS_API_USERNAME", "")
        api_password = os.getenv("OJS_API_KEY") or os.getenv(
            "OJS_API_PASSWORD",
            "",
        )
    if not api_username or not api_password:
        raise ValueError(
            "set OJS_API_CREDENTIALS_FILE, or set OJS_API_USERNAME and "
            "OJS_API_KEY"
        )
    password_file = os.getenv("OJS_DB_PASSWORD_FILE")
    db_password = (
        read_db_password(Path(password_file).expanduser())
        if password_file
        else args.db_password
    )
    return APIConfig(
        db_host=args.db_host,
        db_port=args.db_port,
        db_name=args.db_name,
        db_user=args.db_user,
        db_password=db_password,
        api_username=api_username,
        api_password=api_password,
        article_table=DEFAULT_ARTICLE_TABLE,
        source_table=DEFAULT_SOURCE_TABLE,
        event_table=DEFAULT_EVENT_TABLE,
        snapshot_table=DEFAULT_SNAPSHOT_TABLE,
        oai_page_size=args.oai_page_size,
        public_base_url=normalize_public_base_url(
            os.getenv("OJS_PUBLIC_BASE_URL")
        ),
    )


def create_app(config: APIConfig) -> FastAPI:
    backend = SQLBackend(config)
    security = HTTPBasic()
    app = FastAPI(
        title="PKP Beacon Temporal Articles API",
        description="Current deduplicated records, provenance, and change events.",
        version="2.0.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    def require_auth(
        credentials: HTTPBasicCredentials = Depends(security),
    ) -> str:
        valid_user = secrets.compare_digest(
            credentials.username,
            config.api_username,
        )
        valid_password = secrets.compare_digest(
            credentials.password,
            config.api_password,
        )
        if not (valid_user and valid_password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid credentials",
                headers={"WWW-Authenticate": "Basic"},
            )
        return credentials.username

    @app.get("/health", tags=["system"])
    def health() -> dict[str, str]:
        try:
            backend.assert_ready()
        except Exception:
            LOGGER.exception("database readiness check failed")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="service unavailable",
            ) from None
        return {"status": "ok"}

    @app.get("/", tags=["system"])
    def root(_: str = Depends(require_auth)) -> dict[str, Any]:
        return {
            "service": "pkp-beacon-temporal-articles",
            "version": app.version,
            "endpoints": [
                "/meta",
                "/snapshots",
                "/articles",
                "/articles/{article_id}",
                "/articles/{article_id}/sources",
                "/changes",
                "/oai",
            ],
        }

    @app.get("/meta", tags=["metadata"])
    def meta(_: str = Depends(require_auth)) -> dict[str, Any]:
        return backend.meta()

    @app.get("/snapshots", tags=["metadata"])
    def snapshots(_: str = Depends(require_auth)) -> dict[str, Any]:
        return {"data": backend.snapshots()}

    @app.get("/articles", tags=["articles"])
    def articles(
        after_id: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=1000),
        article_status: str = Query("active", alias="status"),
        changed_since: str | None = Query(None),
        issn: str | None = Query(None),
        doi: str | None = Query(None),
        ids: str | None = Query(
            None,
            description="Comma-separated article IDs, at most 100.",
        ),
        structured_metadata: bool = Query(True),
        include_metadata_xml: bool = Query(False),
        _: str = Depends(require_auth),
    ) -> dict[str, Any]:
        if article_status not in {"active", "removed", "merged", "all"}:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="status must be active, removed, merged, or all",
            )
        parsed_ids: list[int] | None = None
        if ids:
            try:
                parsed_ids = sorted(
                    {int(value) for value in ids.split(",") if value}
                )
            except ValueError as exc:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="ids must be comma-separated integers",
                ) from exc
            if len(parsed_ids) > 100 or any(value <= 0 for value in parsed_ids):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="ids must contain 1 to 100 positive integers",
                )
        result = backend.list_articles(
            after_id=after_id,
            limit=limit,
            article_status=article_status,
            changed_since=validate_date(changed_since, "changed_since"),
            issn=normalize_issn(issn) if issn else None,
            doi=doi,
            ids=parsed_ids,
            structured_metadata=structured_metadata,
            include_metadata_xml=include_metadata_xml,
        )
        result.update(backend.meta())
        return result

    @app.get("/articles/{article_id}", tags=["articles"])
    def article(
        article_id: int,
        structured_metadata: bool = Query(True),
        include_metadata_xml: bool = Query(False),
        _: str = Depends(require_auth),
    ) -> dict[str, Any]:
        row = backend.article(
            article_id,
            structured_metadata=structured_metadata,
            include_metadata_xml=include_metadata_xml,
        )
        if row is None:
            raise HTTPException(status_code=404, detail="article not found")
        return row

    @app.get("/articles/{article_id}/sources", tags=["provenance"])
    def sources(
        article_id: int,
        after_id: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=1000),
        _: str = Depends(require_auth),
    ) -> dict[str, Any]:
        if backend.article(
            article_id,
            structured_metadata=False,
            include_metadata_xml=False,
        ) is None:
            raise HTTPException(status_code=404, detail="article not found")
        return backend.sources(
            article_id,
            after_id=after_id,
            limit=limit,
        )

    @app.get("/changes", tags=["changes"])
    def changes(
        after_event_id: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=1000),
        structured_metadata: bool = Query(True),
        include_metadata_xml: bool = Query(False),
        _: str = Depends(require_auth),
    ) -> dict[str, Any]:
        result = backend.changes(
            after_event_id=after_event_id,
            limit=limit,
            structured_metadata=structured_metadata,
            include_metadata_xml=include_metadata_xml,
        )
        result.update(backend.meta())
        return result

    @app.get("/oai", tags=["oai"])
    def oai(
        request: Request,
        verb: str | None = Query(None),
        metadata_prefix: str | None = Query(None, alias="metadataPrefix"),
        identifier: str | None = Query(None),
        set_spec: str | None = Query(None, alias="set"),
        from_date: str | None = Query(None, alias="from"),
        until_date: str | None = Query(None, alias="until"),
        resumption_token: str | None = Query(None, alias="resumptionToken"),
        _: str = Depends(require_auth),
    ) -> Response:
        request_url = (
            f"{config.public_base_url}/oai"
            if config.public_base_url
            else str(request.url.replace(query=""))
        )
        attrs = {
            "verb": verb,
            "metadataPrefix": metadata_prefix,
            "identifier": identifier,
            "set": set_spec,
            "from": from_date,
            "until": until_date,
            "resumptionToken": resumption_token,
        }
        try:
            root_node = oai_root(request_url, attrs)
            if verb is None:
                raise OAIError("badVerb", "verb is required")
            if verb == "Identify":
                if any(
                    value is not None
                    for value in (
                        metadata_prefix,
                        identifier,
                        set_spec,
                        from_date,
                        until_date,
                        resumption_token,
                    )
                ):
                    raise OAIError(
                        "badArgument",
                        "Identify accepts only verb",
                    )
                metadata = backend.meta()
                snapshot = metadata["latest_snapshot"]
                all_snapshots = backend.snapshots()
                node = ET.SubElement(root_node, f"{{{OAI_NS}}}Identify")
                ET.SubElement(
                    node,
                    f"{{{OAI_NS}}}repositoryName",
                ).text = "PKP Beacon Temporal Articles"
                ET.SubElement(node, f"{{{OAI_NS}}}baseURL").text = request_url
                ET.SubElement(
                    node,
                    f"{{{OAI_NS}}}protocolVersion",
                ).text = "2.0"
                ET.SubElement(
                    node,
                    f"{{{OAI_NS}}}adminEmail",
                ).text = os.getenv("OJS_ADMIN_EMAIL", "admin@localhost")
                ET.SubElement(
                    node,
                    f"{{{OAI_NS}}}earliestDatestamp",
                ).text = str(all_snapshots[0]["snapshot_date"])
                ET.SubElement(
                    node,
                    f"{{{OAI_NS}}}deletedRecord",
                ).text = "persistent"
                ET.SubElement(
                    node,
                    f"{{{OAI_NS}}}granularity",
                ).text = "YYYY-MM-DD"
                description = ET.SubElement(
                    node,
                    f"{{{OAI_NS}}}description",
                )
                description.text = (
                    f"Latest snapshot: {snapshot['snapshot_date']}"
                )
                return oai_response(root_node)

            if verb == "ListMetadataFormats":
                query_names = [
                    name for name, _ in request.query_params.multi_items()
                ]
                if (
                    any(
                        name not in {"verb", "identifier"}
                        for name in query_names
                    )
                    or len(query_names) != len(set(query_names))
                    or any(
                        value is not None
                        for value in (
                            metadata_prefix,
                            set_spec,
                            from_date,
                            until_date,
                            resumption_token,
                        )
                    )
                ):
                    raise OAIError(
                        "badArgument",
                        "ListMetadataFormats accepts only verb and identifier",
                    )
                if identifier is not None:
                    row = backend.article(
                        parse_oai_identifier(identifier),
                        structured_metadata=False,
                        include_metadata_xml=False,
                    )
                    if row is None:
                        raise OAIError(
                            "idDoesNotExist",
                            "article does not exist",
                        )
                node = ET.SubElement(
                    root_node,
                    f"{{{OAI_NS}}}ListMetadataFormats",
                )
                fmt = ET.SubElement(node, f"{{{OAI_NS}}}metadataFormat")
                ET.SubElement(
                    fmt,
                    f"{{{OAI_NS}}}metadataPrefix",
                ).text = OAI_METADATA_PREFIX
                ET.SubElement(fmt, f"{{{OAI_NS}}}schema").text = (
                    "http://www.openarchives.org/OAI/2.0/oai_dc.xsd"
                )
                ET.SubElement(
                    fmt,
                    f"{{{OAI_NS}}}metadataNamespace",
                ).text = OAI_DC_NS
                return oai_response(root_node)

            if verb == "ListSets":
                after_issn = ""
                if resumption_token:
                    if any(
                        value is not None
                        for value in (
                            metadata_prefix,
                            identifier,
                            set_spec,
                            from_date,
                            until_date,
                        )
                    ):
                        raise OAIError(
                            "badArgument",
                            "resumptionToken must be the only argument besides verb",
                        )
                    token = decode_token(resumption_token)
                    if token.get("verb") != verb:
                        raise OAIError(
                            "badResumptionToken",
                            "token verb does not match",
                        )
                    after_issn = str(token.get("afterIssn") or "")
                elif any(
                    value is not None
                    for value in (
                        metadata_prefix,
                        identifier,
                        set_spec,
                        from_date,
                        until_date,
                    )
                ):
                    raise OAIError(
                        "badArgument",
                        "ListSets accepts only verb or resumptionToken",
                    )
                rows, has_more = backend.oai_sets(
                    after_issn=after_issn,
                    limit=config.oai_page_size,
                )
                if not rows:
                    raise OAIError("noSetHierarchy", "no ISSN sets are available")
                node = ET.SubElement(root_node, f"{{{OAI_NS}}}ListSets")
                for row in rows:
                    set_node = ET.SubElement(node, f"{{{OAI_NS}}}set")
                    ET.SubElement(
                        set_node,
                        f"{{{OAI_NS}}}setSpec",
                    ).text = f"{OAI_SET_PREFIX}{row['journal_issn']}"
                    title = row.get("journal_title") or row["journal_issn"]
                    ET.SubElement(
                        set_node,
                        f"{{{OAI_NS}}}setName",
                    ).text = f"{title} ({row['article_count']} records)"
                token_node = ET.SubElement(
                    node,
                    f"{{{OAI_NS}}}resumptionToken",
                )
                if has_more:
                    token_node.text = encode_token(
                        {
                            "verb": verb,
                            "afterIssn": rows[-1]["journal_issn"],
                        }
                    )
                else:
                    token_node.text = ""
                return oai_response(root_node)

            if verb == "GetRecord":
                if (
                    identifier is None
                    or metadata_prefix != OAI_METADATA_PREFIX
                    or any(
                        value is not None
                        for value in (
                            set_spec,
                            from_date,
                            until_date,
                            resumption_token,
                        )
                    )
                ):
                    raise OAIError(
                        "badArgument",
                        "GetRecord requires identifier and metadataPrefix=oai_dc",
                    )
                row = backend.article(
                    parse_oai_identifier(identifier),
                    structured_metadata=False,
                    include_metadata_xml=True,
                )
                if row is None:
                    raise OAIError("idDoesNotExist", "article does not exist")
                node = ET.SubElement(root_node, f"{{{OAI_NS}}}GetRecord")
                record_node = ET.SubElement(node, f"{{{OAI_NS}}}record")
                append_oai_header(record_node, row)
                if row["status"] == "active":
                    append_oai_metadata(record_node, row)
                return oai_response(root_node)

            if verb not in {"ListIdentifiers", "ListRecords"}:
                raise OAIError("badVerb", "unsupported verb")

            after_date: str | None = None
            after_id = 0
            if resumption_token:
                if any(
                    value is not None
                    for value in (
                        metadata_prefix,
                        identifier,
                        set_spec,
                        from_date,
                        until_date,
                    )
                ):
                    raise OAIError(
                        "badArgument",
                        "resumptionToken must be the only argument besides verb",
                    )
                token = decode_token(resumption_token)
                if token.get("verb") != verb:
                    raise OAIError(
                        "badResumptionToken",
                        "token verb does not match",
                    )
                metadata_prefix = token.get("metadataPrefix")
                set_spec = token.get("set")
                from_date = token.get("from")
                until_date = token.get("until")
                after_date = token.get("afterDate")
                after_id = int(token.get("afterId") or 0)
            elif identifier is not None:
                raise OAIError(
                    "badArgument",
                    "identifier is not valid for this verb",
                )

            if metadata_prefix != OAI_METADATA_PREFIX:
                raise OAIError(
                    "cannotDisseminateFormat",
                    "metadataPrefix must be oai_dc",
                )
            from_date = validate_date(from_date, "from")
            until_date = validate_date(until_date, "until")
            if from_date and until_date and from_date > until_date:
                raise OAIError("badArgument", "from must not exceed until")
            normalized_set = parse_oai_set(set_spec)
            rows, has_more = backend.oai_records(
                from_date=from_date,
                until_date=until_date,
                issn=normalized_set,
                after_date=after_date,
                after_id=after_id,
                limit=config.oai_page_size,
            )
            if not rows:
                raise OAIError("noRecordsMatch", "no records match")
            node = ET.SubElement(root_node, f"{{{OAI_NS}}}{verb}")
            for row in rows:
                if verb == "ListIdentifiers":
                    append_oai_header(node, row)
                else:
                    record_node = ET.SubElement(
                        node,
                        f"{{{OAI_NS}}}record",
                    )
                    append_oai_header(record_node, row)
                    if row["status"] == "active":
                        append_oai_metadata(record_node, row)
            token_node = ET.SubElement(
                node,
                f"{{{OAI_NS}}}resumptionToken",
            )
            if has_more:
                last = rows[-1]
                token_node.text = encode_token(
                    {
                        "verb": verb,
                        "metadataPrefix": metadata_prefix,
                        "set": set_spec,
                        "from": from_date,
                        "until": until_date,
                        "afterDate": str(last["date_modified"]),
                        "afterId": int(last["article_id"]),
                    }
                )
            else:
                token_node.text = ""
            return oai_response(root_node)
        except HTTPException as exc:
            error_root = oai_root(request_url, attrs)
            error = ET.SubElement(error_root, f"{{{OAI_NS}}}error")
            error.set("code", "badArgument")
            error.text = str(exc.detail)
            return oai_response(error_root)
        except OAIError as exc:
            error_root = oai_root(request_url, attrs)
            error = ET.SubElement(error_root, f"{{{OAI_NS}}}error")
            error.set("code", exc.code)
            error.text = exc.message
            return oai_response(error_root)

    return app


def main() -> int:
    args = parse_args()
    config = resolve_config(args)
    uvicorn.run(create_app(config), host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
