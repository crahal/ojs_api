#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import secrets
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import duckdb
import pyarrow.parquet as pq
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials


DEFAULT_USERNAME = "admin"
DEFAULT_PASSWORD = "OJSpassword"
DEFAULT_INPUT = Path("data/clean/deduplicated.parquet")
DEFAULT_TEMP_DIR = Path("/tmp/ojs_api_api_duckdb_tmp")
ISSN_CLEAN_RE = re.compile(r"[^0-9Xx]+")
OAI_NS = "http://www.openarchives.org/OAI/2.0/"
OAI_ENTITY_NS = "https://ojs.api/oai/entity"
OAI_ISSN_NS = "https://ojs.api/oai/issn"
OAI_METADATA_PREFIX_ENTITY = "ojs_entity"
OAI_METADATA_PREFIX_ISSN = "ojs_issn"
OAI_IDENTIFIER_ENTITY_PREFIX = "oai:ojs-api:entity:"
OAI_IDENTIFIER_ISSN_PREFIX = "oai:ojs-api:issn:"
OAI_SET_ISSN_PREFIX = "issn:"
ET.register_namespace("", OAI_NS)
ET.register_namespace("ojs", OAI_ENTITY_NS)
ET.register_namespace("ojs_issn", OAI_ISSN_NS)
VALID_SORT_COLUMNS = {
    "dedupe_id",
    "canonical_id",
    "canonical_name",
    "canonical_issn",
    "source_row_count",
    "journal_observation_count",
}
ENTITY_COLUMNS = (
    "dedupe_id",
    "canonical_id",
    "canonical_issn",
    "canonical_name",
    "canonical_issn_key",
    "canonical_name_key",
    "source_row_count",
    "journal_observation_count",
    "is_merged_cluster",
    "matched_rules",
    "matched_observation_ids",
    "source_ids",
    "all_issns",
    "all_names",
)


def timestamp_now() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def normalize_issn(raw: str) -> str:
    normalized = ISSN_CLEAN_RE.sub("", raw).upper()
    if len(normalized) != 8:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="issn must normalize to exactly 8 characters (digits or X).",
        )
    return normalized


@dataclass(frozen=True)
class APIConfig:
    input_path: Path
    summary_path: Path
    username: str
    password: str
    memory_limit: str
    threads: int
    temp_dir: Path
    oai_page_size: int
    oai_datestamp: str


@dataclass(frozen=True)
class OAIError(Exception):
    code: str
    message: str


def encode_resumption_token(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_resumption_token(token: str) -> dict[str, Any]:
    padding = "=" * (-len(token) % 4)
    try:
        decoded = base64.urlsafe_b64decode(token + padding).decode("utf-8")
        payload = json.loads(decoded)
    except Exception as exc:  # pragma: no cover - defensive parsing
        raise OAIError("badResumptionToken", "Invalid resumptionToken.") from exc
    if not isinstance(payload, dict):
        raise OAIError("badResumptionToken", "Invalid resumptionToken payload.")
    return payload


def oai_root(request_url: str, request_attrs: dict[str, str | None]) -> ET.Element:
    root = ET.Element(f"{{{OAI_NS}}}OAI-PMH")
    response_date = ET.SubElement(root, f"{{{OAI_NS}}}responseDate")
    response_date.text = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    request_node = ET.SubElement(root, f"{{{OAI_NS}}}request")
    for key, value in request_attrs.items():
        if value is not None:
            request_node.set(key, value)
    request_node.text = request_url
    return root


def oai_xml_response(root: ET.Element) -> Response:
    xml_bytes = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    return Response(content=xml_bytes, media_type="text/xml; charset=utf-8")


def normalize_issn_opt(raw: str | None) -> str | None:
    if raw is None:
        return None
    return ISSN_CLEAN_RE.sub("", raw).upper() or None


def coerce_scalar(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    return json.dumps(value, ensure_ascii=False)


def parse_oai_set_spec(set_spec: str | None) -> str | None:
    if set_spec is None:
        return None
    if not set_spec.startswith(OAI_SET_ISSN_PREFIX):
        raise OAIError("badArgument", "Only set specs of the form 'issn:<ISSN>' are supported.")
    raw = set_spec[len(OAI_SET_ISSN_PREFIX) :]
    normalized = normalize_issn_opt(raw)
    if normalized is None or len(normalized) != 8:
        raise OAIError("badArgument", "Invalid ISSN set spec. Expected 'issn:<ISSN>'.")
    return normalized


def parse_entity_identifier(identifier: str) -> int:
    if not identifier.startswith(OAI_IDENTIFIER_ENTITY_PREFIX):
        raise OAIError("idDoesNotExist", "Unknown entity identifier.")
    raw = identifier[len(OAI_IDENTIFIER_ENTITY_PREFIX) :]
    if not raw.isdigit():
        raise OAIError("idDoesNotExist", "Entity identifier is invalid.")
    return int(raw)


def parse_issn_identifier(identifier: str) -> str:
    if not identifier.startswith(OAI_IDENTIFIER_ISSN_PREFIX):
        raise OAIError("idDoesNotExist", "Unknown ISSN identifier.")
    raw = identifier[len(OAI_IDENTIFIER_ISSN_PREFIX) :]
    normalized = normalize_issn_opt(raw)
    if normalized is None or len(normalized) != 8:
        raise OAIError("idDoesNotExist", "ISSN identifier is invalid.")
    return normalized

class DedupeBackend:
    def __init__(self, config: APIConfig) -> None:
        self.config = config
        self.input_sql = sql_literal(str(config.input_path))

    def _connect(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(database=":memory:")
        conn.execute(f"SET memory_limit = {sql_literal(self.config.memory_limit)}")
        conn.execute(f"SET threads = {self.config.threads}")
        conn.execute(f"SET temp_directory = {sql_literal(str(self.config.temp_dir))}")
        conn.execute("SET preserve_insertion_order = false")
        return conn

    def _run_fetchone(self, sql: str, params: list[Any] | None = None) -> tuple[Any, ...]:
        conn = self._connect()
        try:
            result = conn.execute(sql, params or []).fetchone()
            return result if result is not None else tuple()
        finally:
            conn.close()

    def _run_fetchall(
        self, sql: str, params: list[Any] | None = None
    ) -> tuple[list[tuple[Any, ...]], list[str]]:
        conn = self._connect()
        try:
            cursor = conn.execute(sql, params or [])
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            return rows, columns
        finally:
            conn.close()

    @staticmethod
    def _rows_to_dicts(rows: list[tuple[Any, ...]], columns: list[str]) -> list[dict[str, Any]]:
        return [dict(zip(columns, row)) for row in rows]

    def _build_where(
        self,
        *,
        q: str | None,
        issn: str | None,
        merged_only: bool | None,
        match_rule: str | None,
    ) -> tuple[str, list[Any]]:
        clauses: list[str] = []
        params: list[Any] = []

        if q:
            q_like = f"%{q.lower()}%"
            clauses.append(
                "(LOWER(COALESCE(canonical_name, '')) LIKE ? OR LOWER(COALESCE(canonical_issn, '')) LIKE ?)"
            )
            params.extend([q_like, q_like])

        if issn:
            clauses.append("canonical_issn_key = ?")
            params.append(normalize_issn(issn))

        if merged_only is True:
            clauses.append("is_merged_cluster = TRUE")
        elif merged_only is False:
            clauses.append("is_merged_cluster = FALSE")

        if match_rule:
            clauses.append("list_contains(matched_rules, ?)")
            params.append(match_rule)

        if not clauses:
            return "", params
        return "WHERE " + " AND ".join(clauses), params

    def get_meta(self) -> dict[str, Any]:
        parquet_file = pq.ParquetFile(self.config.input_path)
        schema = parquet_file.schema_arrow
        row_count = parquet_file.metadata.num_rows

        merged_row = self._run_fetchone(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE is_merged_cluster) AS merged_cluster_count,
                COALESCE(SUM(journal_observation_count - 1) FILTER (WHERE is_merged_cluster), 0) AS merged_observation_count,
                COALESCE(SUM(source_row_count), 0) AS total_source_rows
            FROM read_parquet({self.input_sql})
            """
        )
        merged_cluster_count = int(merged_row[0]) if merged_row else 0
        merged_observation_count = int(merged_row[1]) if merged_row else 0
        total_source_rows = int(merged_row[2]) if merged_row else 0

        rule_rows, _ = self._run_fetchall(
            f"""
            SELECT DISTINCT rule
            FROM (
                SELECT UNNEST(matched_rules) AS rule
                FROM read_parquet({self.input_sql})
            )
            ORDER BY 1
            """
        )
        match_rules = [str(row[0]) for row in rule_rows]

        summary_payload: dict[str, Any] | None = None
        if self.config.summary_path.exists():
            summary_payload = json.loads(self.config.summary_path.read_text(encoding="utf-8"))

        return {
            "generated_at": timestamp_now(),
            "input_path": str(self.config.input_path),
            "summary_path": str(self.config.summary_path),
            "summary_exists": self.config.summary_path.exists(),
            "shape": {"rows": row_count, "columns": len(schema.names)},
            "row_groups": parquet_file.metadata.num_row_groups,
            "file_size_bytes": self.config.input_path.stat().st_size,
            "merged_cluster_count": merged_cluster_count,
            "merged_observation_count": merged_observation_count,
            "total_source_rows": total_source_rows,
            "available_match_rules": match_rules,
            "columns": [
                {"name": field.name, "type": str(field.type)}
                for field in schema
            ],
            "summary_json": summary_payload,
        }

    def list_entities(
        self,
        *,
        q: str | None,
        issn: str | None,
        merged_only: bool | None,
        match_rule: str | None,
        limit: int,
        offset: int,
        sort_by: str,
        sort_order: str,
    ) -> dict[str, Any]:
        if sort_by not in VALID_SORT_COLUMNS:
            allowed = ", ".join(sorted(VALID_SORT_COLUMNS))
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"sort_by must be one of: {allowed}",
            )
        normalized_order = sort_order.lower()
        if normalized_order not in {"asc", "desc"}:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="sort_order must be 'asc' or 'desc'.",
            )

        where_sql, where_params = self._build_where(
            q=q,
            issn=issn,
            merged_only=merged_only,
            match_rule=match_rule,
        )

        count_row = self._run_fetchone(
            f"""
            SELECT COUNT(*) AS total
            FROM read_parquet({self.input_sql})
            {where_sql}
            """,
            where_params,
        )
        total = int(count_row[0]) if count_row else 0

        query_params = [*where_params, limit, offset]
        rows, columns = self._run_fetchall(
            f"""
            SELECT
                {", ".join(ENTITY_COLUMNS)}
            FROM read_parquet({self.input_sql})
            {where_sql}
            ORDER BY {sort_by} {normalized_order}, dedupe_id ASC
            LIMIT ? OFFSET ?
            """,
            query_params,
        )

        items = [dict(zip(columns, row)) for row in rows]
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "sort_by": sort_by,
            "sort_order": normalized_order,
            "items": items,
        }

    def get_entity(self, dedupe_id: int) -> dict[str, Any]:
        rows, columns = self._run_fetchall(
            f"""
            SELECT {", ".join(ENTITY_COLUMNS)}
            FROM read_parquet({self.input_sql})
            WHERE dedupe_id = ?
            """,
            [dedupe_id],
        )
        if not rows:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Entity not found.")
        return dict(zip(columns, rows[0]))

    def list_oai_entities(
        self,
        *,
        limit: int,
        offset: int,
        issn_key: str | None,
    ) -> tuple[int, list[dict[str, Any]]]:
        where_sql = ""
        params: list[Any] = []
        if issn_key is not None:
            where_sql = "WHERE canonical_issn_key = ?"
            params.append(issn_key)

        total_row = self._run_fetchone(
            f"""
            SELECT COUNT(*)
            FROM read_parquet({self.input_sql})
            {where_sql}
            """,
            params,
        )
        total = int(total_row[0]) if total_row else 0

        rows, columns = self._run_fetchall(
            f"""
            SELECT {", ".join(ENTITY_COLUMNS)}
            FROM read_parquet({self.input_sql})
            {where_sql}
            ORDER BY dedupe_id ASC
            LIMIT ? OFFSET ?
            """,
            [*params, limit, offset],
        )
        return total, self._rows_to_dicts(rows, columns)

    def get_oai_entity(self, dedupe_id: int) -> dict[str, Any] | None:
        rows, columns = self._run_fetchall(
            f"""
            SELECT {", ".join(ENTITY_COLUMNS)}
            FROM read_parquet({self.input_sql})
            WHERE dedupe_id = ?
            """,
            [dedupe_id],
        )
        if not rows:
            return None
        return dict(zip(columns, rows[0]))

    def list_oai_issns(self, *, limit: int, offset: int) -> tuple[int, list[dict[str, Any]]]:
        total_row = self._run_fetchone(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT canonical_issn_key
                FROM read_parquet({self.input_sql})
                WHERE canonical_issn_key IS NOT NULL
            )
            """
        )
        total = int(total_row[0]) if total_row else 0
        rows, columns = self._run_fetchall(
            f"""
            SELECT
                canonical_issn_key AS issn_key,
                MIN(canonical_issn) AS issn,
                COUNT(*) AS entity_count
            FROM read_parquet({self.input_sql})
            WHERE canonical_issn_key IS NOT NULL
            GROUP BY canonical_issn_key
            ORDER BY canonical_issn_key ASC
            LIMIT ? OFFSET ?
            """,
            [limit, offset],
        )
        return total, self._rows_to_dicts(rows, columns)

    def get_oai_issn(self, issn_key: str) -> dict[str, Any] | None:
        rows, columns = self._run_fetchall(
            f"""
            SELECT
                canonical_issn_key AS issn_key,
                MIN(canonical_issn) AS issn,
                COUNT(*) AS entity_count
            FROM read_parquet({self.input_sql})
            WHERE canonical_issn_key = ?
            GROUP BY canonical_issn_key
            """,
            [issn_key],
        )
        if not rows:
            return None
        return dict(zip(columns, rows[0]))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Serve deduplicated parquet entities via a read-only FastAPI service."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Path to deduplicated parquet. Default: %(default)s",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Bind host. Default: %(default)s",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Bind port. Default: %(default)s",
    )
    parser.add_argument(
        "--memory-limit",
        default="1GB",
        help="DuckDB memory limit for API queries. Default: %(default)s",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="DuckDB threads for API queries. Default: %(default)s",
    )
    parser.add_argument(
        "--temp-dir",
        type=Path,
        default=DEFAULT_TEMP_DIR,
        help="DuckDB temp spill directory. Default: %(default)s",
    )
    parser.add_argument(
        "--oai-page-size",
        type=int,
        default=500,
        help="Max records per OAI response page. Default: %(default)s",
    )
    args = parser.parse_args()
    if args.threads <= 0:
        parser.error("--threads must be a positive integer")
    if args.port <= 0 or args.port > 65535:
        parser.error("--port must be between 1 and 65535")
    if args.oai_page_size <= 0:
        parser.error("--oai-page-size must be a positive integer")
    return args


def resolve_config(args: argparse.Namespace) -> APIConfig:
    input_path = args.input.resolve()
    if not input_path.exists():
        raise FileNotFoundError(
            f"Input parquet not found: {input_path}. Run `python src/2_deduplicate.py` first."
        )

    summary_path = input_path.with_name(f"{input_path.name}.summary.json")
    username = os.getenv("OJS_API_USERNAME", DEFAULT_USERNAME)
    password = os.getenv("OJS_API_PASSWORD", DEFAULT_PASSWORD)
    if not username or not password:
        raise ValueError("OJS_API_USERNAME and OJS_API_PASSWORD must be non-empty.")

    temp_dir = args.temp_dir.resolve()
    temp_dir.mkdir(parents=True, exist_ok=True)
    oai_datestamp = datetime.fromtimestamp(
        input_path.stat().st_mtime, tz=timezone.utc
    ).strftime("%Y-%m-%d")

    return APIConfig(
        input_path=input_path,
        summary_path=summary_path,
        username=username,
        password=password,
        memory_limit=args.memory_limit,
        threads=args.threads,
        temp_dir=temp_dir,
        oai_page_size=args.oai_page_size,
        oai_datestamp=oai_datestamp,
    )


def create_app(config: APIConfig) -> FastAPI:
    backend = DedupeBackend(config)
    security = HTTPBasic(auto_error=True)
    app = FastAPI(
        title="OJS Deduplicated Entities API",
        description="Read-only API over deduplicated OJS journal entities parquet.",
        version="1.0.0",
    )

    def require_auth(credentials: HTTPBasicCredentials = Depends(security)) -> str:
        valid_user = secrets.compare_digest(credentials.username, config.username)
        valid_pass = secrets.compare_digest(credentials.password, config.password)
        if not (valid_user and valid_pass):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials.",
                headers={"WWW-Authenticate": "Basic"},
            )
        return credentials.username

    def append_oai_header(
        parent: ET.Element, *, identifier: str, set_spec: str | None = None
    ) -> ET.Element:
        header = ET.SubElement(parent, f"{{{OAI_NS}}}header")
        ET.SubElement(header, f"{{{OAI_NS}}}identifier").text = identifier
        ET.SubElement(header, f"{{{OAI_NS}}}datestamp").text = config.oai_datestamp
        if set_spec is not None:
            ET.SubElement(header, f"{{{OAI_NS}}}setSpec").text = set_spec
        return header

    def append_oai_entity_metadata(parent: ET.Element, entity: dict[str, Any]) -> None:
        metadata = ET.SubElement(parent, f"{{{OAI_NS}}}metadata")
        entity_node = ET.SubElement(metadata, f"{{{OAI_ENTITY_NS}}}entity")
        for key in ENTITY_COLUMNS:
            value = entity.get(key)
            child = ET.SubElement(entity_node, f"{{{OAI_ENTITY_NS}}}{key}")
            if isinstance(value, list):
                for item in value:
                    item_node = ET.SubElement(child, f"{{{OAI_ENTITY_NS}}}item")
                    item_node.text = coerce_scalar(item)
            elif value is not None:
                child.text = coerce_scalar(value)

    def append_oai_issn_metadata(parent: ET.Element, issn_record: dict[str, Any]) -> None:
        metadata = ET.SubElement(parent, f"{{{OAI_NS}}}metadata")
        issn_node = ET.SubElement(metadata, f"{{{OAI_ISSN_NS}}}issn_record")
        ET.SubElement(issn_node, f"{{{OAI_ISSN_NS}}}issn_key").text = coerce_scalar(
            issn_record.get("issn_key")
        )
        ET.SubElement(issn_node, f"{{{OAI_ISSN_NS}}}issn").text = coerce_scalar(
            issn_record.get("issn")
        )
        ET.SubElement(issn_node, f"{{{OAI_ISSN_NS}}}entity_count").text = coerce_scalar(
            issn_record.get("entity_count")
        )

    def append_resumption_token(
        parent: ET.Element,
        *,
        next_offset: int,
        total: int,
        cursor: int,
        payload: dict[str, Any],
    ) -> None:
        token_node = ET.SubElement(parent, f"{{{OAI_NS}}}resumptionToken")
        token_node.set("cursor", str(cursor))
        token_node.set("completeListSize", str(total))
        if next_offset >= total:
            token_node.text = ""
            return
        payload = {**payload, "offset": next_offset}
        token_node.text = encode_resumption_token(payload)

    def oai_error_response(
        *,
        request_url: str,
        request_attrs: dict[str, str | None],
        code: str,
        message: str,
    ) -> Response:
        root = oai_root(request_url, request_attrs)
        error_node = ET.SubElement(root, f"{{{OAI_NS}}}error")
        error_node.set("code", code)
        error_node.text = message
        return oai_xml_response(root)

    @app.get("/health", tags=["system"])
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "generated_at": timestamp_now(),
            "input_path": str(config.input_path),
            "summary_exists": config.summary_path.exists(),
        }

    @app.get("/", tags=["system"])
    def root(_: str = Depends(require_auth)) -> dict[str, Any]:
        return {
            "service": "ojs-deduplicated-entities-api",
            "version": app.version,
            "docs": "/docs",
            "endpoints": ["/health", "/meta", "/entities", "/entities/{dedupe_id}", "/oai"],
            "rate_limit": None,
        }

    @app.get("/meta", tags=["metadata"])
    def meta(_: str = Depends(require_auth)) -> dict[str, Any]:
        return backend.get_meta()

    @app.get("/entities", tags=["entities"])
    def entities(
        q: str | None = Query(None, description="Substring search over canonical name and ISSN."),
        issn: str | None = Query(None, description="Exact ISSN filter after normalization."),
        merged_only: bool | None = Query(
            None, description="True for merged clusters only, false for singletons only."
        ),
        match_rule: str | None = Query(None, description="Filter when matched_rules contains this value."),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        sort_by: str = Query("dedupe_id"),
        sort_order: str = Query("asc"),
        _: str = Depends(require_auth),
    ) -> dict[str, Any]:
        return backend.list_entities(
            q=q,
            issn=issn,
            merged_only=merged_only,
            match_rule=match_rule,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            sort_order=sort_order,
        )

    @app.get("/entities/{dedupe_id}", tags=["entities"])
    def entity_by_id(dedupe_id: int, _: str = Depends(require_auth)) -> dict[str, Any]:
        return backend.get_entity(dedupe_id)

    @app.get("/oai", tags=["oai"])
    def oai_endpoint(
        request: Request,
        verb: str = Query(..., description="OAI-PMH verb."),
        metadataPrefix: str | None = Query(None),
        identifier: str | None = Query(None),
        set_spec: str | None = Query(None, alias="set"),
        resumptionToken: str | None = Query(None),
    ) -> Response:
        request_url = str(request.url.replace(query=""))
        request_attrs = {
            "verb": verb,
            "metadataPrefix": metadataPrefix,
            "identifier": identifier,
            "set": set_spec,
            "resumptionToken": resumptionToken,
        }
        try:
            allowed_verbs = {
                "Identify",
                "ListMetadataFormats",
                "ListSets",
                "ListIdentifiers",
                "ListRecords",
                "GetRecord",
            }
            if verb not in allowed_verbs:
                raise OAIError("badVerb", "Unsupported verb.")

            offset = 0
            if resumptionToken is not None:
                if any(value is not None for value in (metadataPrefix, identifier, set_spec)):
                    raise OAIError(
                        "badArgument",
                        "When resumptionToken is supplied, do not send metadataPrefix, identifier, or set.",
                    )
                token_payload = decode_resumption_token(resumptionToken)
                if token_payload.get("verb") != verb:
                    raise OAIError("badResumptionToken", "resumptionToken verb does not match request verb.")
                metadataPrefix = token_payload.get("metadataPrefix")
                set_spec = token_payload.get("set")
                offset = int(token_payload.get("offset", 0))

            root = oai_root(
                request_url,
                {
                    "verb": verb,
                    "metadataPrefix": metadataPrefix,
                    "identifier": identifier,
                    "set": set_spec,
                    "resumptionToken": resumptionToken,
                },
            )

            if verb == "Identify":
                if any(value is not None for value in (metadataPrefix, identifier, set_spec, resumptionToken)):
                    raise OAIError("badArgument", "Identify only accepts the verb argument.")
                node = ET.SubElement(root, f"{{{OAI_NS}}}Identify")
                ET.SubElement(node, f"{{{OAI_NS}}}repositoryName").text = "OJS Deduplicated Repository"
                ET.SubElement(node, f"{{{OAI_NS}}}baseURL").text = request_url
                ET.SubElement(node, f"{{{OAI_NS}}}protocolVersion").text = "2.0"
                ET.SubElement(node, f"{{{OAI_NS}}}adminEmail").text = "admin@localhost"
                ET.SubElement(node, f"{{{OAI_NS}}}earliestDatestamp").text = config.oai_datestamp
                ET.SubElement(node, f"{{{OAI_NS}}}deletedRecord").text = "no"
                ET.SubElement(node, f"{{{OAI_NS}}}granularity").text = "YYYY-MM-DD"
                return oai_xml_response(root)

            if verb == "ListMetadataFormats":
                node = ET.SubElement(root, f"{{{OAI_NS}}}ListMetadataFormats")
                formats = (
                    (
                        OAI_METADATA_PREFIX_ENTITY,
                        "https://ojs.api/oai/ojs_entity.xsd",
                        OAI_ENTITY_NS,
                    ),
                    (
                        OAI_METADATA_PREFIX_ISSN,
                        "https://ojs.api/oai/ojs_issn.xsd",
                        OAI_ISSN_NS,
                    ),
                )
                for prefix, schema_url, namespace in formats:
                    fmt = ET.SubElement(node, f"{{{OAI_NS}}}metadataFormat")
                    ET.SubElement(fmt, f"{{{OAI_NS}}}metadataPrefix").text = prefix
                    ET.SubElement(fmt, f"{{{OAI_NS}}}schema").text = schema_url
                    ET.SubElement(fmt, f"{{{OAI_NS}}}metadataNamespace").text = namespace
                return oai_xml_response(root)

            if verb == "ListSets":
                if any(value is not None for value in (metadataPrefix, identifier, set_spec)):
                    raise OAIError("badArgument", "ListSets accepts only verb or resumptionToken.")
                total, rows = backend.list_oai_issns(limit=config.oai_page_size, offset=offset)
                if total == 0:
                    raise OAIError("noSetHierarchy", "No ISSN sets are available.")
                node = ET.SubElement(root, f"{{{OAI_NS}}}ListSets")
                for row in rows:
                    set_node = ET.SubElement(node, f"{{{OAI_NS}}}set")
                    set_spec_value = f"{OAI_SET_ISSN_PREFIX}{row['issn_key']}"
                    ET.SubElement(set_node, f"{{{OAI_NS}}}setSpec").text = set_spec_value
                    display_issn = row.get("issn") or row["issn_key"]
                    ET.SubElement(set_node, f"{{{OAI_NS}}}setName").text = (
                        f"ISSN {display_issn} ({row['entity_count']} entities)"
                    )
                append_resumption_token(
                    node,
                    next_offset=offset + len(rows),
                    total=total,
                    cursor=offset,
                    payload={"verb": verb},
                )
                return oai_xml_response(root)

            if verb in {"ListIdentifiers", "ListRecords"}:
                if metadataPrefix is None:
                    raise OAIError("badArgument", "metadataPrefix is required.")
                if identifier is not None:
                    raise OAIError("badArgument", "identifier is not valid for this verb.")
                if metadataPrefix not in {OAI_METADATA_PREFIX_ENTITY, OAI_METADATA_PREFIX_ISSN}:
                    raise OAIError("cannotDisseminateFormat", "Unsupported metadataPrefix.")

                node = ET.SubElement(root, f"{{{OAI_NS}}}{verb}")
                if metadataPrefix == OAI_METADATA_PREFIX_ENTITY:
                    issn_key_filter = parse_oai_set_spec(set_spec)
                    total, records = backend.list_oai_entities(
                        limit=config.oai_page_size,
                        offset=offset,
                        issn_key=issn_key_filter,
                    )
                    if total == 0:
                        raise OAIError("noRecordsMatch", "No records match this request.")
                    for record in records:
                        entity_id = int(record["dedupe_id"])
                        set_value = (
                            f"{OAI_SET_ISSN_PREFIX}{record['canonical_issn_key']}"
                            if record.get("canonical_issn_key")
                            else None
                        )
                        identifier_value = f"{OAI_IDENTIFIER_ENTITY_PREFIX}{entity_id}"
                        if verb == "ListIdentifiers":
                            append_oai_header(node, identifier=identifier_value, set_spec=set_value)
                        else:
                            record_node = ET.SubElement(node, f"{{{OAI_NS}}}record")
                            append_oai_header(record_node, identifier=identifier_value, set_spec=set_value)
                            append_oai_entity_metadata(record_node, record)
                    append_resumption_token(
                        node,
                        next_offset=offset + len(records),
                        total=total,
                        cursor=offset,
                        payload={
                            "verb": verb,
                            "metadataPrefix": metadataPrefix,
                            "set": set_spec,
                        },
                    )
                    return oai_xml_response(root)

                if set_spec is not None:
                    raise OAIError(
                        "badArgument",
                        "set filtering is only supported with metadataPrefix=ojs_entity.",
                    )
                total, rows = backend.list_oai_issns(limit=config.oai_page_size, offset=offset)
                if total == 0:
                    raise OAIError("noRecordsMatch", "No ISSN records available.")
                for row in rows:
                    issn_key = row["issn_key"]
                    set_value = f"{OAI_SET_ISSN_PREFIX}{issn_key}"
                    identifier_value = f"{OAI_IDENTIFIER_ISSN_PREFIX}{issn_key}"
                    if verb == "ListIdentifiers":
                        append_oai_header(node, identifier=identifier_value, set_spec=set_value)
                    else:
                        record_node = ET.SubElement(node, f"{{{OAI_NS}}}record")
                        append_oai_header(record_node, identifier=identifier_value, set_spec=set_value)
                        append_oai_issn_metadata(record_node, row)
                append_resumption_token(
                    node,
                    next_offset=offset + len(rows),
                    total=total,
                    cursor=offset,
                    payload={
                        "verb": verb,
                        "metadataPrefix": metadataPrefix,
                    },
                )
                return oai_xml_response(root)

            if verb == "GetRecord":
                if metadataPrefix is None or identifier is None:
                    raise OAIError("badArgument", "GetRecord requires both identifier and metadataPrefix.")
                if set_spec is not None:
                    raise OAIError("badArgument", "set is not valid for GetRecord.")
                if metadataPrefix not in {OAI_METADATA_PREFIX_ENTITY, OAI_METADATA_PREFIX_ISSN}:
                    raise OAIError("cannotDisseminateFormat", "Unsupported metadataPrefix.")

                node = ET.SubElement(root, f"{{{OAI_NS}}}GetRecord")
                record_node = ET.SubElement(node, f"{{{OAI_NS}}}record")
                if metadataPrefix == OAI_METADATA_PREFIX_ENTITY:
                    dedupe_id = parse_entity_identifier(identifier)
                    record = backend.get_oai_entity(dedupe_id)
                    if record is None:
                        raise OAIError("idDoesNotExist", "Entity identifier not found.")
                    set_value = (
                        f"{OAI_SET_ISSN_PREFIX}{record['canonical_issn_key']}"
                        if record.get("canonical_issn_key")
                        else None
                    )
                    append_oai_header(
                        record_node,
                        identifier=f"{OAI_IDENTIFIER_ENTITY_PREFIX}{dedupe_id}",
                        set_spec=set_value,
                    )
                    append_oai_entity_metadata(record_node, record)
                else:
                    issn_key = parse_issn_identifier(identifier)
                    row = backend.get_oai_issn(issn_key)
                    if row is None:
                        raise OAIError("idDoesNotExist", "ISSN identifier not found.")
                    append_oai_header(
                        record_node,
                        identifier=f"{OAI_IDENTIFIER_ISSN_PREFIX}{issn_key}",
                        set_spec=f"{OAI_SET_ISSN_PREFIX}{issn_key}",
                    )
                    append_oai_issn_metadata(record_node, row)
                return oai_xml_response(root)

            raise OAIError("badVerb", "Unsupported verb.")
        except OAIError as exc:
            return oai_error_response(
                request_url=request_url,
                request_attrs=request_attrs,
                code=exc.code,
                message=exc.message,
            )

    return app


def main() -> int:
    args = parse_args()
    try:
        config = resolve_config(args)
    except Exception as exc:
        print(f"[{timestamp_now()}] API startup failed: {exc}", file=sys.stderr, flush=True)
        return 1

    print(f"[{timestamp_now()}] Serving API from {config.input_path}", file=sys.stderr, flush=True)
    print(
        (
            f"[{timestamp_now()}] host={args.host} port={args.port} "
            f"memory_limit={config.memory_limit} threads={config.threads} "
            f"temp_dir={config.temp_dir} oai_page_size={config.oai_page_size}"
        ),
        file=sys.stderr,
        flush=True,
    )
    print(
        (
            f"[{timestamp_now()}] Auth username={config.username} "
            f"password={'*' * len(config.password)}"
        ),
        file=sys.stderr,
        flush=True,
    )
    app = create_app(config)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
