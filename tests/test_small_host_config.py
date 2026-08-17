from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
import run_pipeline


def example_environment() -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in (PROJECT_ROOT / ".env.example").read_text(
        encoding="utf-8"
    ).splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        values[name] = value
    return values


class SmallHostConfigTest(unittest.TestCase):
    def test_checked_in_defaults_fit_the_16_gb_profile(self):
        values = example_environment()
        self.assertEqual(values["OJS_OAI_PAGE_SIZE"], "100")
        self.assertEqual(values["OJS_METADATA_WORKERS"], "1")
        self.assertEqual(values["OJS_MYSQL_BUFFER_POOL_SIZE"], "2G")
        self.assertEqual(
            values["OJS_SERVING_MYSQL_BUFFER_POOL_SIZE"],
            "5G",
        )
        self.assertEqual(values["OJS_SERVING_MYSQL_MEMORY_LIMIT"], "7g")
        self.assertEqual(values["OJS_API_MEMORY_LIMIT"], "512m")
        self.assertEqual(values["OJS_MIN_DATA_FILESYSTEM_GB"], "900")
        self.assertEqual(values["OJS_MIN_FREE_GB"], "300")
        self.assertEqual(values["OJS_MIN_AVAILABLE_MEMORY_MB"], "2048")
        self.assertEqual(values["OJS_UPDATE_WORKING_SET_PERCENT"], "125")

    def test_cli_defaults_are_conservative_without_an_env_file(self):
        with patch.dict(os.environ, {}, clear=True):
            args = run_pipeline.parser().parse_args([])
        self.assertEqual(args.metadata_workers, 1)
        self.assertEqual(args.mysql_buffer_pool_size, "2G")

    def test_update_gate_checks_the_attached_data_filesystem_and_memory(self):
        wrapper = (PROJECT_ROOT / "scripts" / "automatic_update.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn('df -Pk "$data_root"', wrapper)
        self.assertIn('du -sk -- "$current_database"', wrapper)
        self.assertIn("OJS_UPDATE_WORKING_SET_PERCENT", wrapper)
        self.assertIn("OJS_MIN_AVAILABLE_MEMORY_MB", wrapper)
        self.assertIn("/^MemAvailable:/", wrapper)

    def test_lightsail_runbook_and_preflight_are_checked_in(self):
        guide = (PROJECT_ROOT / "LIGHTSAIL_DEPLOYMENT.md").read_text(
            encoding="utf-8"
        )
        for required_text in (
            "attached SSD disk",
            "emergency swap",
            "Lightsail firewalls",
            "Build the first release",
            "Enable the daily update timer",
            "Keep the attached disk bounded",
        ):
            self.assertIn(required_text, guide)
        preflight = PROJECT_ROOT / "scripts" / "lightsail_preflight.sh"
        self.assertTrue(preflight.stat().st_mode & 0o100)


if __name__ == "__main__":
    unittest.main()
