from __future__ import annotations

import configparser
import re
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).parents[1]


def configuration(path: str) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None, strict=False)
    parser.optionxform = str
    parser.read(PROJECT_ROOT / path)
    return parser


class EmailDeploymentTest(unittest.TestCase):
    def test_reporter_is_independent_of_update_and_recovery(self):
        unit = configuration("deploy/ojs-api-progress-email.service")
        self.assertEqual(unit["Service"]["Type"], "oneshot")
        self.assertEqual(
            unit["Service"]["ExecStart"],
            "/usr/bin/python3 /srv/ojs_api/src/email_progress.py",
        )
        for value in unit["Unit"].values():
            self.assertNotIn("ojs-api-update", value)
        for key in ("ExecStartPre", "ExecStartPost", "ExecStop", "ExecStopPost"):
            self.assertNotIn(key, unit["Service"])
        self.assertEqual(unit["Service"].get("Restart", "no"), "no")
        update = (PROJECT_ROOT / "deploy/ojs-api-update.service").read_text()
        self.assertNotIn("progress-email", update)

    def test_password_uses_systemd_credential_not_environment(self):
        unit_text = (PROJECT_ROOT / "deploy/ojs-api-progress-email.service").read_text()
        service = configuration("deploy/ojs-api-progress-email.service")["Service"]
        self.assertEqual(
            service["LoadCredential"],
            "gmail-app-password:/etc/ojs-api/progress-email-password",
        )
        self.assertEqual(service["EnvironmentFile"], "/etc/ojs-api/progress-email.env")
        for line in unit_text.splitlines():
            if line.startswith(("Environment=", "EnvironmentFile=", "ExecStart=")):
                self.assertNotIn("password", line.lower())
                self.assertNotIn("/srv/ojs_api/.env", line)
        self.assertEqual(service["UMask"], "0077")

    def test_reporter_sandbox_has_service_local_journal_access_and_bounded_state(self):
        service = configuration("deploy/ojs-api-progress-email.service")["Service"]
        self.assertEqual(service["User"], "ojs")
        self.assertEqual(service["Group"], "ojs")
        self.assertEqual(service["SupplementaryGroups"], "systemd-journal")
        self.assertEqual(service["ProtectSystem"], "strict")
        self.assertTrue(service.getboolean("NoNewPrivileges"))
        self.assertEqual(service["CapabilityBoundingSet"], "")
        self.assertEqual(service["StateDirectory"], "ojs-api-report")
        self.assertEqual(service["StateDirectoryMode"], "0700")
        self.assertNotIn("ReadWritePaths", service)
        hidden_paths = {
            path.removeprefix("-") for path in service["InaccessiblePaths"].split()
        }
        self.assertTrue({
            "/run/docker.sock", "/var/run/docker.sock",
            "/srv/ojs_api/.env", "/srv/ojs_api/.secrets",
        }.issubset(hidden_paths))
        self.assertLessEqual(int(service["TimeoutStartSec"]), 180)
        self.assertLessEqual(int(service["TasksMax"]), 16)
        memory_limit = re.fullmatch(r"([1-9][0-9]*)M", service["MemoryMax"])
        self.assertIsNotNone(memory_limit)
        self.assertLessEqual(int(memory_limit[1]), 128)

    def test_twelve_hour_timer_has_no_backlog_or_retry_schedule(self):
        timer = configuration("deploy/ojs-api-progress-email.timer")
        self.assertEqual(timer["Timer"]["OnActiveSec"], "12h")
        self.assertEqual(timer["Timer"]["OnUnitActiveSec"], "12h")
        self.assertEqual(timer["Timer"]["Unit"], "ojs-api-progress-email.service")
        self.assertNotIn("OnCalendar", timer["Timer"])
        self.assertNotIn("OnBootSec", timer["Timer"])
        self.assertNotIn("OnStartupSec", timer["Timer"])
        self.assertFalse(timer["Timer"].getboolean("Persistent", fallback=False))
        self.assertEqual(timer["Install"]["WantedBy"], "timers.target")

    def test_documented_timer_activation_is_gated_on_successful_test_send(self):
        guide = (PROJECT_ROOT / "LIGHTSAIL_DEPLOYMENT.md").read_text()
        blocks = re.findall(r"```bash\n(.*?)\n```", guide, re.DOTALL)
        activation = "sudo systemctl enable --now ojs-api-progress-email.timer"
        matching_blocks = [block for block in blocks if activation in block]
        self.assertEqual(len(matching_blocks), 1)
        self.assertRegex(
            matching_blocks[0],
            r"configure_progress_email\.py \\\n\s*--sender YOUR_GMAIL "
            r"--recipient YOUR_EMAIL &&\n"
            r"sudo systemctl start ojs-api-progress-email\.service &&\n"
            + re.escape(activation),
        )


if __name__ == "__main__":
    unittest.main()
