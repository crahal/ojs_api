from __future__ import annotations

import os
import re
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
import run_pipeline
import compact_update


def example_environment() -> dict[str, str]:
    return dict(
        line.split("=", 1)
        for line in (PROJECT_ROOT / ".env.example").read_text().splitlines()
        if line and not line.startswith("#") and "=" in line
    )


def memory_mib(value: str) -> int:
    match = re.fullmatch(r"(\d+)([gm])", value.lower())
    if not match:
        raise ValueError(value)
    return int(match[1]) * (1024 if match[2] == "g" else 1)


class SmallHostConfigTest(unittest.TestCase):
    def test_combined_hard_limits_leave_os_headroom_on_four_gb_host(self):
        env = example_environment()
        service = (PROJECT_ROOT / "deploy/ojs-api-update.service").read_text()
        builder = re.search(r"^MemoryMax=(\S+)$", service, re.MULTILINE).group(1)
        maximum = sum(memory_mib(value) for value in (
            builder, env["OJS_SERVING_MYSQL_MEMORY_LIMIT"],
            env["OJS_API_MEMORY_LIMIT"],
        ))
        self.assertLessEqual(maximum, 4 * 1024 - 768)
        self.assertLess(
            memory_mib(env["OJS_MYSQL_BUFFER_POOL_SIZE"]),
            memory_mib(builder),
        )
        self.assertLess(
            memory_mib(env["OJS_SERVING_MYSQL_BUFFER_POOL_SIZE"]),
            memory_mib(env["OJS_SERVING_MYSQL_MEMORY_LIMIT"]),
        )

    def test_direct_pipeline_default_uses_one_small_worker(self):
        with patch.dict(os.environ, {}, clear=True):
            args = run_pipeline.parser().parse_args([])
        self.assertEqual(args.metadata_workers, 1)
        self.assertLessEqual(memory_mib(args.mysql_buffer_pool_size), 1024)

    def test_daily_cron_uses_the_memory_limited_service(self):
        cron = (PROJECT_ROOT / "deploy/ojs-api.cron").read_text()
        self.assertRegex(
            cron, r"(?m)^17 3 \* \* \* root /usr/bin/systemctl start --no-block ojs-api-update.service$"
        )
        self.assertNotIn("automatic_update.sh", cron)

    def test_service_timeout_exceeds_the_coordinator_default(self):
        env = example_environment()
        with patch.dict(os.environ, {}, clear=True):
            defaults = compact_update.parser().parse_args([])
        self.assertEqual(float(env["OJS_MAX_RUNTIME_HOURS"]), defaults.max_runtime_hours)
        service = (PROJECT_ROOT / "deploy/ojs-api-update.service").read_text()
        days = int(re.search(r"^TimeoutStartSec=(\d+)d$", service, re.MULTILINE).group(1))
        self.assertGreater(days * 24, defaults.max_runtime_hours)


if __name__ == "__main__":
    unittest.main()
