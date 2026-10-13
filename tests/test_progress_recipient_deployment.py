from __future__ import annotations

import configparser
import shlex
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).parents[1]
BASE_SERVICE = "deploy/ojs-api-progress-email.service"
SUBSCRIBER_SERVICE = "deploy/ojs-api-progress-email@.service"
SUBSCRIBER_TIMER = "deploy/ojs-api-progress-email@.timer"


def configuration(path: str) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None, strict=False)
    parser.optionxform = str
    parser.read(PROJECT_ROOT / path)
    return parser


def directives(path: str, key: str) -> list[str]:
    prefix = key + "="
    return [
        line.removeprefix(prefix)
        for line in (PROJECT_ROOT / path).read_text().splitlines()
        if line.startswith(prefix)
    ]


class ProgressRecipientDeploymentTests(unittest.TestCase):
    def test_subscriber_inherits_base_hardening_resources_and_credentials(self):
        base = configuration(BASE_SERVICE)
        subscriber = configuration(SUBSCRIBER_SERVICE)
        self.assertEqual(subscriber.sections(), base.sections())
        permitted_changes = {
            ("Unit", "Description"),
            ("Service", "EnvironmentFile"),
            ("Service", "ExecStart"),
            ("Service", "TimeoutStartSec"),
        }
        for section in base.sections():
            self.assertEqual(set(subscriber[section]), set(base[section]))
            for key, value in base[section].items():
                if (section, key) not in permitted_changes:
                    with self.subTest(section=section, key=key):
                        self.assertEqual(subscriber[section][key], value)
        # ConfigParser keeps only the last repeated directive; compare these
        # separately so a silently removed environment flag cannot go unnoticed.
        self.assertEqual(
            directives(SUBSCRIBER_SERVICE, "Environment"),
            directives(BASE_SERVICE, "Environment"),
        )
        self.assertEqual(directives(SUBSCRIBER_SERVICE, "LoadCredential"), [
            "gmail-app-password:/etc/ojs-api/progress-email-password",
        ])

    def test_recipient_override_is_required_and_loaded_after_shared_settings(self):
        self.assertEqual(directives(SUBSCRIBER_SERVICE, "EnvironmentFile"), [
            "/etc/ojs-api/progress-email.env",
            "/etc/ojs-api/progress-email-%i.env",
        ])

    def test_subscriber_has_independent_state_under_shared_private_state_parent(self):
        service = configuration(SUBSCRIBER_SERVICE)["Service"]
        self.assertEqual(shlex.split(service["ExecStart"]), [
            "/usr/bin/python3",
            "/srv/ojs_api/src/email_progress.py",
            "--state-file",
            "/var/lib/ojs-api-report/subscriber-%i.json",
            "--min-send-interval-hours=72",
            "--lock-wait-seconds=60",
        ])
        self.assertEqual(service["StateDirectory"], "ojs-api-report")
        self.assertEqual(service["StateDirectoryMode"], "0700")
        self.assertEqual(service["UMask"], "0077")
        self.assertNotIn("ReadWritePaths", service)

    def test_service_timeout_preserves_work_allowance_after_bounded_lock_wait(self):
        base = configuration(BASE_SERVICE)["Service"]
        subscriber = configuration(SUBSCRIBER_SERVICE)["Service"]
        self.assertEqual(int(subscriber["TimeoutStartSec"]), 240)
        self.assertEqual(
            int(subscriber["TimeoutStartSec"]), int(base["TimeoutStartSec"]) + 60,
        )

    def test_three_day_timer_waits_after_completion_without_replaying_backlog(self):
        timer = configuration(SUBSCRIBER_TIMER)
        self.assertEqual(dict(timer["Timer"]), {
            "OnActiveSec": "72h",
            "OnUnitInactiveSec": "72h",
            "AccuracySec": "1min",
            "Unit": "ojs-api-progress-email@%i.service",
        })
        self.assertEqual(timer["Install"]["WantedBy"], "timers.target")

    def test_original_recipient_schedule_remains_twelve_hours(self):
        timer = configuration("deploy/ojs-api-progress-email.timer")["Timer"]
        self.assertEqual(timer["OnActiveSec"], "12h")
        self.assertEqual(timer["OnUnitActiveSec"], "12h")
        self.assertEqual(timer["Unit"], "ojs-api-progress-email.service")
        self.assertNotIn("OnUnitInactiveSec", timer)
        self.assertEqual(directives(BASE_SERVICE, "EnvironmentFile"), [
            "/etc/ojs-api/progress-email.env",
        ])

    def test_template_does_not_enable_paid_ai_or_expose_credentials(self):
        service_text = (PROJECT_ROOT / SUBSCRIBER_SERVICE).read_text()
        self.assertNotIn("openai", service_text.lower())
        self.assertNotIn("OJS_PROGRESS_LLM_ENABLED", service_text)
        for key in ("Environment", "EnvironmentFile", "ExecStart"):
            for value in directives(SUBSCRIBER_SERVICE, key):
                with self.subTest(key=key, value=value):
                    self.assertNotIn("password", value.lower())
                    self.assertNotIn("/srv/ojs_api/.env", value)

    def test_subscriber_cannot_mutate_recovery_or_install_secrets(self):
        unit = configuration(SUBSCRIBER_SERVICE)
        service = unit["Service"]
        self.assertEqual(service["Type"], "oneshot")
        self.assertEqual(service["User"], "ojs")
        self.assertEqual(service["ProtectSystem"], "strict")
        self.assertTrue(service.getboolean("NoNewPrivileges"))
        self.assertEqual(service["CapabilityBoundingSet"], "")
        self.assertEqual(service.get("Restart", "no"), "no")
        self.assertEqual(
            [key for key in service if key.startswith("Exec")], ["ExecStart"],
        )
        self.assertNotIn("ojs-api-update", (PROJECT_ROOT / SUBSCRIBER_SERVICE).read_text())
        self.assertNotIn("ojs-api-update", (PROJECT_ROOT / SUBSCRIBER_TIMER).read_text())
        self.assertNotIn("progress-email@", (PROJECT_ROOT / "deploy/ojs-api-update.service").read_text())


if __name__ == "__main__":
    unittest.main()
