"""Control-plane safety tests, without contacting AWS."""

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dsp_collector.runtime.outbox import drain, enqueue, jobs
from dsp_spark.io.lakehouse_control import LocalControl, S3Control, writable_root
from dsp_spark.io.s3 import s3a_options
from dsp_spark.jobs.batch.s3_lakehouse import (
    authorize_cloud_write,
    commit_envelope,
    require_phase_one,
    require_policy,
)


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, **kw):
        key = kw["Key"]
        if kw.get("IfNoneMatch") == "*" and key in self.objects:
            raise RuntimeError("PreconditionFailed")
        if kw.get("IfMatch") and self.objects[key][1] != kw["IfMatch"]:
            raise RuntimeError("PreconditionFailed")
        etag = str(hash(kw["Body"]))
        self.objects[key] = (kw["Body"], etag)
        return {"ETag": etag}

    def delete_object(self, **kw):
        if self.objects[kw["Key"]][1] != kw["IfMatch"]:
            raise RuntimeError("PreconditionFailed")
        del self.objects[kw["Key"]]

    def get_object(self, **kw):
        data, etag = self.objects[kw["Key"]]
        return {"Body": io.BytesIO(data), "ETag": etag}


class LakehouseControlTests(unittest.TestCase):
    def test_root_can_never_run_unattended_watch_even_with_supervised_flag(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "phase-one.json"
            report.write_text(
                json.dumps(
                    {
                        "status": "PASS",
                        "remote_root": "s3a://bucket/bootstrap/x",
                        "tables": {str(n): {"content_match": True} for n in range(44)},
                    }
                )
            )
            with patch("boto3.Session") as session:
                session.return_value.client.return_value.get_caller_identity.return_value = {
                    "Arn": "arn:aws:iam::000000000000:root"
                }
                with self.assertRaisesRegex(ValueError, "Root"):
                    authorize_cloud_write(
                        "s3a://bucket/staging/test",
                        allow=True,
                        supervised_root=True,
                        command="watch",
                        synthetic=True,
                        profile="fixture",
                        region="ap-southeast-2",
                        verification_report=report,
                        bootstrap_root="s3a://bucket/bootstrap/x",
                    )

    def test_bootstrap_is_immutable_including_encoded_path(self):
        for root in (
            "s3a://bucket/bootstrap/snapshot",
            "s3a://bucket/%62ootstrap/snapshot",
        ):
            with self.assertRaisesRegex(ValueError, "immutable"):
                writable_root(root)

    def test_single_writer_cannot_expire_or_steal_another_owner(self):
        client = FakeS3()
        a = S3Control("s3a://bucket/staging/test", client)
        b = S3Control("s3a://bucket/staging/test", client)
        with a.writer(), self.assertRaises(RuntimeError), b.writer():
            self.fail("second writer entered")
        with b.writer():
            self.assertEqual(len(client.objects), 1)
        self.assertEqual(client.objects, {})

    def test_local_pointer_cas_and_immutable_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            control = LocalControl(Path(directory).as_uri())
            etag = control.put("current.json", {"generation_id": "first"}, absent=True)
            control.put("current.json", {"generation_id": "second"}, expected=etag)
            with self.assertRaises(RuntimeError):
                control.put("current.json", {"generation_id": "stale"}, expected=etag)
            self.assertEqual(control.get("current.json")[0]["generation_id"], "second")

    def test_profile_uses_sdk_v2_without_embedding_session_credentials(self):
        options = s3a_options(profile="dsp391m-spark", region="ap-southeast-2")
        self.assertIn(
            "software.amazon.awssdk",
            options["spark.hadoop.fs.s3a.aws.credentials.provider"],
        )
        self.assertEqual(options["spark.executorEnv.AWS_PROFILE"], "dsp391m-spark")
        self.assertFalse(
            any("secret.key" in n or "session.token" in n for n in options)
        )
        with self.assertRaises(ValueError):
            s3a_options(profile="name -Dother=bad")

    def test_policies_are_reviewed_and_test_permissions_cannot_authorize_real_data(
        self,
    ):
        policy = {"youtube": {"storage_permission": True}}
        with self.assertRaises(ValueError):
            require_policy(policy, "youtube")
        policy["youtube"].update(
            policy_reviewed_at="2026-10-28T00:00:00Z",
            synthetic_only=True,
            retention_days=None,
        )
        require_policy(policy, "youtube", synthetic=True)
        with self.assertRaises(ValueError):
            require_policy(policy, "youtube")
        with self.assertRaises(ValueError):
            require_policy(policy, "youtube", synthetic=True, derivatives=True)

    def test_metadata_only_report_does_not_unlock_phase_two(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            path.write_text(
                json.dumps(
                    {
                        "status": "PASS",
                        "remote_root": "s3a://bucket/bootstrap/x",
                        "tables": {str(n): {} for n in range(44)},
                    }
                )
            )
            with self.assertRaisesRegex(ValueError, "content"):
                require_phase_one(path, "s3a://bucket/bootstrap/x")

    def test_invalid_contract_stays_failed_without_a_bronze_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "invalid.json"
            artifact.write_text(
                json.dumps(
                    {
                        "source": "youtube",
                        "run_id": "synthetic-invalid",
                        "collected_at": "2026-10-28T00:00:00Z",
                    }
                )
            )
            enqueue(artifact, root / "outbox")
            policies = {
                "youtube": {
                    "storage_permission": True,
                    "synthetic_only": True,
                    "policy_reviewed_at": "2026-10-28",
                    "retention_days": None,
                }
            }
            control = LocalControl((root / "lake").as_uri())
            result = drain(
                root / "outbox",
                lambda s: commit_envelope(
                    None, None, control, s, policies=policies, synthetic=True
                ),
            )
            self.assertEqual(result[0]["status"], "failed")
            self.assertNotIn("receipt", next(jobs(root / "outbox"))[1])
            self.assertFalse((root / "lake" / "receipts").exists())
