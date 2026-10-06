from __future__ import annotations

import configparser
import math
import re
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).parents[1]


def configuration(path: str) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None, strict=False)
    parser.optionxform = str
    parser.read(PROJECT_ROOT / path)
    return parser


def size_bytes(value: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)([KMG])", value)
    if match is None:
        raise ValueError(value)
    return int(match[1]) * 1024 ** ("KMG".index(match[2]) + 1)


class LoggingDeploymentTest(unittest.TestCase):
    def test_service_captures_both_streams_and_accounts_for_builder_resources(self):
        service = configuration("deploy/ojs-api-update.service")["Service"]
        self.assertEqual(service["StandardOutput"], "journal")
        self.assertEqual(service["StandardError"], "journal")
        self.assertTrue(service.getboolean("CPUAccounting"))
        self.assertTrue(service.getboolean("MemoryAccounting"))
        self.assertTrue(service.getboolean("IOAccounting"))

    def test_persistent_journal_has_small_host_size_and_time_limits(self):
        journal = configuration("deploy/journald-ojs-api.conf")["Journal"]
        self.assertEqual(journal["Storage"], "persistent")
        persistent_limit = size_bytes(journal["SystemMaxUse"])
        runtime_limit = size_bytes(journal["RuntimeMaxUse"])
        reserve = size_bytes(journal["SystemKeepFree"])
        self.assertLessEqual(persistent_limit, 128 * 1024 ** 2)
        self.assertLessEqual(runtime_limit, 32 * 1024 ** 2)
        self.assertLess(runtime_limit, persistent_limit)
        self.assertGreaterEqual(reserve, 1024 ** 3)
        retention = re.fullmatch(r"([1-9][0-9]*)day", journal["MaxRetentionSec"])
        rotation = re.fullmatch(r"([1-9][0-9]*)day", journal["MaxFileSec"])
        self.assertIsNotNone(retention)
        self.assertIsNotNone(rotation)
        self.assertLessEqual(int(retention[1]), 14)
        self.assertLessEqual(int(rotation[1]), int(retention[1]))

    def test_example_has_a_finite_bounded_progress_interval(self):
        environment = dict(
            line.split("=", 1)
            for line in (PROJECT_ROOT / ".env.example").read_text().splitlines()
            if line and not line.startswith("#") and "=" in line
        )
        interval = float(environment["OJS_PROGRESS_SECONDS"])
        self.assertTrue(math.isfinite(interval))
        self.assertGreaterEqual(interval, 1)
        self.assertLessEqual(interval, 3600)


if __name__ == "__main__":
    unittest.main()
