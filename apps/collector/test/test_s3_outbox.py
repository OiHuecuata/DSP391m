"""Crash/retry boundaries of the opt-in durable acquisition outbox."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dsp_collector.runtime.artifacts import write_json_atomic
from dsp_collector.runtime.outbox import drain, enqueue, jobs


class S3OutboxTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="dsp-synthetic-outbox-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.queue = self.root / "pending"

    def artifact(self, source="youtube", run="synthetic-one"):
        path = self.root / "bronze" / source / (run + ".json")
        with patch.dict("os.environ", {"DSP_S3_OUTBOX_DIR": str(self.queue)}):
            write_json_atomic(
                path,
                {
                    "source": source,
                    "run_id": run,
                    "collected_at": "2026-10-28T00:00:00Z",
                },
            )
        return path

    def test_all_collectors_enqueue_without_cloud_or_spark(self):
        for source in ("pbplus", "threads", "youtube"):
            path = self.artifact(source)
            enqueue(path, self.queue)
        self.assertEqual(len(list(jobs(self.queue))), 3)

    def test_outage_retains_envelope_backoff_then_commit(self):
        original = self.artifact()
        state = next(jobs(self.queue))[1]

        def outage(_):
            raise ConnectionError("offline")

        result = drain(self.queue, outage, now=100)
        self.assertEqual(result[0]["status"], "failed")
        self.assertTrue(original.exists())
        self.assertTrue(Path(state["artifact"]).exists())
        committed = []

        def commit(s):
            committed.append(s["payload_sha256"])
            return {"status": "committed", "input_versions": {"posts": 0}}

        drain(self.queue, commit, now=101)
        self.assertEqual(committed, [])
        drain(self.queue, commit, now=103)
        drain(self.queue, commit, now=104)
        self.assertEqual(len(committed), 1)

    def test_notification_failure_never_reverts_committed_bronze(self):
        self.artifact()

        def offline(_):
            raise ConnectionError("Kafka unavailable")

        drain(self.queue, lambda _: {"status": "committed"}, notify=offline)
        state = next(jobs(self.queue))[1]
        self.assertEqual(state["status"], "committed")
        self.assertEqual(state["notification_status"], "pending")
        with patch("builtins.print"):
            drain(
                self.queue,
                lambda _: self.fail("must not ingest twice"),
                notify=lambda _: None,
            )
        self.assertEqual(next(jobs(self.queue))[1]["notification_status"], "sent")

    def test_conflicting_identity_and_changed_pending_payload_are_rejected(self):
        path = self.artifact()
        payload = json.loads(path.read_text())
        payload["collected_at"] = "2026-10-28T00:01:00Z"
        path.write_text(json.dumps(payload))
        with self.assertRaisesRegex(ValueError, "conflicting"):
            enqueue(path, self.queue)
        state = next(jobs(self.queue))[1]
        Path(state["artifact"]).write_text(json.dumps(payload))
        result = drain(
            self.queue, lambda _: self.fail("changed payload must not commit")
        )
        self.assertEqual(result[0]["status"], "failed")

    def test_false_success_receipt_is_not_committed(self):
        self.artifact()
        drain(self.queue, lambda _: {"status": "pending"})
        self.assertEqual(next(jobs(self.queue))[1]["status"], "failed")
