from __future__ import annotations

import importlib.util
import os
import sys
import types
import unittest
from argparse import Namespace
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock
from xml.etree import ElementTree as ET

from fastapi import HTTPException
from fastapi.security import HTTPBasicCredentials
from starlette.requests import Request


def load_api_module():
    if importlib.util.find_spec("pymysql") is None:
        pymysql = types.ModuleType("pymysql")
        pymysql.connect = lambda **kwargs: None
        pymysql.connections = types.SimpleNamespace(Connection=object)
        cursors = types.ModuleType("pymysql.cursors")
        cursors.DictCursor = object
        pymysql.cursors = cursors
        sys.modules["pymysql"] = pymysql
        sys.modules["pymysql.cursors"] = cursors
    path = Path(__file__).parents[1] / "src" / "3_build_api.py"
    spec = importlib.util.spec_from_file_location("ojs_build_api", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


api = load_api_module()


class FakeBackend:
    def __init__(self, config) -> None:
        self.config = config

    def assert_ready(self) -> None:
        return None

    def meta(self):
        return {
            "latest_snapshot": {"snapshot_date": "2026-07-01"},
            "high_watermark_event_id": 1,
        }

    def snapshots(self):
        return [{"snapshot_date": "2026-01-01"}]

    def article(self, article_id, **kwargs):
        if article_id != 1:
            return None
        return self._row()

    def oai_sets(self, **kwargs):
        return (
            [
                {
                    "journal_issn": "1234567X",
                    "journal_title": "Fixture Journal",
                    "article_count": 1,
                }
            ],
            False,
        )

    def oai_records(self, **kwargs):
        return [self._row()], False

    @staticmethod
    def _row():
        return {
            "article_id": 1,
            "status": "active",
            "date_modified": "2026-07-01",
            "journal_issn": "1234567X",
            "title": "Fixture article",
            "metadata_xml": None,
        }


class APITest(unittest.TestCase):
    def setUp(self):
        self.original_backend = api.SQLBackend
        api.SQLBackend = FakeBackend
        config = api.APIConfig(
            db_host="localhost",
            db_port=3306,
            db_name="fixture",
            db_user="fixture",
            db_password="fixture",
            api_username="fixture-user",
            api_password="fixture-key",
            article_table="ojs_articles",
            source_table="ojs_article_sources",
            event_table="ojs_article_events",
            snapshot_table="ojs_snapshots",
            oai_page_size=100,
        )
        self.config = config
        self.app = api.create_app(self.config)
        self.oai_route = next(
            route for route in self.app.routes if route.path == "/oai"
        )
        self.oai = self.oai_route.endpoint
        self.auth_dependency = next(
            dependency.call
            for dependency in self.oai_route.dependant.dependencies
            if dependency.call.__name__ == "require_auth"
        )

    def tearDown(self):
        api.SQLBackend = self.original_backend

    @staticmethod
    def _request(query_string: bytes = b"") -> Request:
        return Request(
            {
                "type": "http",
                "http_version": "1.1",
                "method": "GET",
                "scheme": "https",
                "path": "/oai",
                "raw_path": b"/oai",
                "query_string": query_string,
                "headers": [],
                "client": ("127.0.0.1", 12345),
                "server": ("api.example.org", 443),
                "root_path": "",
            }
        )

    def test_oai_requires_user_and_key(self):
        with self.assertRaises(HTTPException) as raised:
            self.auth_dependency(
                HTTPBasicCredentials(username="fixture-user", password="wrong")
            )
        self.assertEqual(raised.exception.status_code, 401)
        self.assertEqual(
            raised.exception.headers["WWW-Authenticate"],
            "Basic",
        )
        self.assertEqual(
            self.auth_dependency(
                HTTPBasicCredentials(
                    username="fixture-user",
                    password="fixture-key",
                )
            ),
            "fixture-user",
        )

    def test_all_six_oai_verbs_return_xml(self):
        cases = (
            ("Identify", {}),
            ("ListMetadataFormats", {}),
            ("ListSets", {}),
            (
                "GetRecord",
                {
                    "metadataPrefix": "oai_dc",
                    "identifier": "oai:pkp-beacon:article:1",
                },
            ),
            ("ListIdentifiers", {"metadataPrefix": "oai_dc"}),
            ("ListRecords", {"metadataPrefix": "oai_dc"}),
        )
        namespace = {"oai": api.OAI_NS}
        request = Request(
            {
                "type": "http",
                "http_version": "1.1",
                "method": "GET",
                "scheme": "https",
                "path": "/oai",
                "raw_path": b"/oai",
                "query_string": b"",
                "headers": [],
                "client": ("127.0.0.1", 12345),
                "server": ("api.example.org", 443),
                "root_path": "",
            }
        )
        for verb, extra in cases:
            with self.subTest(verb=verb):
                response = self.oai(
                    request=request,
                    verb=verb,
                    metadata_prefix=extra.get("metadataPrefix"),
                    identifier=extra.get("identifier"),
                    set_spec=None,
                    from_date=None,
                    until_date=None,
                    resumption_token=None,
                    _="fixture-user",
                )
                response_text = response.body.decode("utf-8")
                self.assertEqual(response.status_code, 200, response_text)
                self.assertTrue(
                    response.headers["content-type"].startswith("text/xml")
                )
                root = ET.fromstring(response.body)
                self.assertIsNotNone(root.find(f"oai:{verb}", namespace))

    def test_missing_oai_verb_returns_protocol_xml_instead_of_json_422(self):
        verb_parameter = next(
            parameter
            for parameter in self.oai_route.dependant.query_params
            if parameter.name == "verb"
        )
        self.assertIsNone(verb_parameter.default)
        response = self.oai(
            request=self._request(),
            verb=None,
            metadata_prefix=None,
            identifier=None,
            set_spec=None,
            from_date=None,
            until_date=None,
            resumption_token=None,
            _="fixture-user",
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.headers["content-type"].startswith("text/xml"))
        root = ET.fromstring(response.body)
        error = root.find(f"{{{api.OAI_NS}}}error")
        self.assertIsNotNone(error)
        self.assertEqual(error.get("code"), "badVerb")

    def test_list_metadata_formats_rejects_extraneous_arguments(self):
        extraneous_arguments = (
            ("metadata_prefix", "oai_dc"),
            ("set_spec", "issn:1234567X"),
            ("from_date", "2026-01-01"),
            ("until_date", "2026-07-01"),
            ("resumption_token", "unexpected"),
        )
        for name, value in extraneous_arguments:
            with self.subTest(argument=name):
                arguments = {
                    "metadata_prefix": None,
                    "identifier": None,
                    "set_spec": None,
                    "from_date": None,
                    "until_date": None,
                    "resumption_token": None,
                }
                arguments[name] = value
                response = self.oai(
                    request=self._request(),
                    verb="ListMetadataFormats",
                    **arguments,
                    _="fixture-user",
                )
                self.assertEqual(response.status_code, 200)
                root = ET.fromstring(response.body)
                error = root.find(f"{{{api.OAI_NS}}}error")
                self.assertIsNotNone(error)
                self.assertEqual(error.get("code"), "badArgument")

        illegal_query_strings = (
            b"verb=ListMetadataFormats&unexpected=value",
            b"verb=ListMetadataFormats&verb=Identify",
        )
        for query_string in illegal_query_strings:
            with self.subTest(query_string=query_string):
                response = self.oai(
                    request=self._request(query_string),
                    verb="ListMetadataFormats",
                    metadata_prefix=None,
                    identifier=None,
                    set_spec=None,
                    from_date=None,
                    until_date=None,
                    resumption_token=None,
                    _="fixture-user",
                )
                root = ET.fromstring(response.body)
                error = root.find(f"{{{api.OAI_NS}}}error")
                self.assertIsNotNone(error)
                self.assertEqual(error.get("code"), "badArgument")

    def test_list_metadata_formats_accepts_its_optional_identifier(self):
        response = self.oai(
            request=self._request(),
            verb="ListMetadataFormats",
            metadata_prefix=None,
            identifier="oai:pkp-beacon:article:1",
            set_spec=None,
            from_date=None,
            until_date=None,
            resumption_token=None,
            _="fixture-user",
        )
        self.assertEqual(response.status_code, 200)
        root = ET.fromstring(response.body)
        self.assertIsNotNone(
            root.find(f"{{{api.OAI_NS}}}ListMetadataFormats")
        )

    def test_credentials_file_must_be_private(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "api-client.env"
            path.write_text(
                "OJS_API_USERNAME=fixture-user\nOJS_API_KEY=fixture-key\n",
                encoding="utf-8",
            )
            path.chmod(0o600)
            self.assertEqual(
                api.read_api_credentials(path),
                ("fixture-user", "fixture-key"),
            )
            path.chmod(0o644)
            with self.assertRaisesRegex(ValueError, "permissions are too open"):
                api.read_api_credentials(path)

    def test_database_password_file_and_public_base_url_are_resolved(self):
        with TemporaryDirectory() as directory:
            password_path = Path(directory) / "database-password"
            password_path.write_text("secret with punctuation!\n", encoding="utf-8")
            password_path.chmod(0o600)
            args = Namespace(
                db_host="database",
                db_port=3306,
                db_name="fixture",
                db_user="ojs_api",
                db_password="direct-value-must-not-win",
                oai_page_size=100,
            )
            environment = {
                "OJS_API_USERNAME": "fixture-user",
                "OJS_API_KEY": "fixture-key",
                "OJS_DB_PASSWORD_FILE": str(password_path),
                "OJS_PUBLIC_BASE_URL": "https://api.example.org/catalogue/",
            }
            with mock.patch.dict(os.environ, environment, clear=True):
                config = api.resolve_config(args)

            self.assertEqual(config.db_password, "secret with punctuation!")
            self.assertEqual(
                config.public_base_url,
                "https://api.example.org/catalogue",
            )

    def test_database_password_file_must_be_private(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "database-password"
            path.write_text("fixture-password\n", encoding="utf-8")
            path.chmod(0o644)
            with self.assertRaisesRegex(ValueError, "permissions are too open"):
                api.read_db_password(path)

    def test_default_database_user_is_unprivileged_api_user(self):
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(
            sys,
            "argv",
            ["3_build_api.py"],
        ):
            self.assertEqual(api.parse_args().db_user, "ojs_api")

    def test_public_base_url_is_used_in_oai_xml(self):
        app = api.create_app(
            replace(
                self.config,
                public_base_url="https://public.example.org/catalogue",
            )
        )
        oai = next(route for route in app.routes if route.path == "/oai").endpoint
        request = Request(
            {
                "type": "http",
                "http_version": "1.1",
                "method": "GET",
                "scheme": "http",
                "path": "/oai",
                "raw_path": b"/oai",
                "query_string": b"verb=Identify",
                "headers": [],
                "client": ("127.0.0.1", 12345),
                "server": ("internal", 8000),
                "root_path": "",
            }
        )
        response = oai(
            request=request,
            verb="Identify",
            metadata_prefix=None,
            identifier=None,
            set_spec=None,
            from_date=None,
            until_date=None,
            resumption_token=None,
            _="fixture-user",
        )
        root = ET.fromstring(response.body)
        namespace = {"oai": api.OAI_NS}
        expected = "https://public.example.org/catalogue/oai"
        self.assertEqual(root.find("oai:request", namespace).text, expected)
        self.assertEqual(
            root.find("oai:Identify/oai:baseURL", namespace).text,
            expected,
        )

    def test_health_probe_is_constant_time(self):
        queries = []

        class ProbeBackend(self.original_backend):
            def _fetchone(inner_self, sql, params=None):
                queries.append(sql)
                return {"1": 1}

        ProbeBackend(self.config).assert_ready()
        self.assertEqual(queries, ["SELECT 1 LIMIT 1"])

    def test_health_failure_does_not_expose_database_error(self):
        class FailingBackend(FakeBackend):
            def assert_ready(self):
                raise RuntimeError("host=db.internal password=do-not-leak")

        with mock.patch.object(api, "SQLBackend", FailingBackend):
            app = api.create_app(self.config)
        health = next(
            route for route in app.routes if route.path == "/health"
        ).endpoint
        with self.assertLogs(api.LOGGER, level="ERROR"), self.assertRaises(
            HTTPException
        ) as raised:
            health()
        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(raised.exception.detail, "service unavailable")

    def test_list_and_change_queries_skip_unrequested_metadata_xml(self):
        queries = []

        class RecordingBackend(self.original_backend):
            def _fetchall(inner_self, sql, params=None):
                queries.append(sql)
                return []

        backend = RecordingBackend(self.config)
        backend.list_articles(
            after_id=0,
            limit=10,
            article_status="active",
            changed_since=None,
            issn=None,
            doi=None,
            ids=None,
            structured_metadata=False,
            include_metadata_xml=False,
        )
        backend.changes(
            after_event_id=0,
            limit=10,
            structured_metadata=False,
            include_metadata_xml=False,
        )
        self.assertEqual(len(queries), 2)
        for sql in queries:
            self.assertNotIn("a.metadata_xml", sql)

        queries.clear()
        backend.list_articles(
            after_id=0,
            limit=10,
            article_status="active",
            changed_since=None,
            issn=None,
            doi=None,
            ids=None,
            structured_metadata=True,
            include_metadata_xml=False,
        )
        backend.changes(
            after_event_id=0,
            limit=10,
            structured_metadata=False,
            include_metadata_xml=True,
        )
        for sql in queries:
            self.assertIn("a.metadata_xml", sql)


if __name__ == "__main__":
    unittest.main()
