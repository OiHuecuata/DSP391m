import io
import tempfile
import unittest
from pathlib import Path

from dsp_collector.publishers.observation import build, publish, validate
from dsp_collector.runtime.durable import ArtifactStore, sha256

from tests.threads_vertical.fixtures import post
from tests.threads_vertical.test_features import envelope, native


class Message:
    def topic(self):
        return "dsp.social.observations.v2"

    def partition(self):
        return 0

    def offset(self):
        return 3


class Producer:
    def __init__(self, _config):
        self.callbacks = []

    def produce(self, *_args, **kwargs):
        self.callbacks.append(kwargs["on_delivery"])

    def poll(self, _timeout):
        pass

    def flush(self, _timeout):
        for callback in self.callbacks:
            callback(None, Message())
        return 0


class TransportStorageTests(unittest.TestCase):
    def event(self):
        row = native(envelope([post(8)]))[0]
        return build(
            row,
            {
                "record_path": "file:///tmp/fixture/records",
                "record_version": 0,
                "artifact_path": "file:///tmp/fixture/artifacts",
                "artifact_version": 0,
                "artifact_id": "sha256:" + "b" * 64,
            },
        )

    def test_acknowledgement_is_required_not_just_flush(self):
        event = self.event()
        self.assertEqual(
            publish([event], bootstrap="fixture:9092", producer_factory=Producer)[
                event["event_id"]
            ]["offset"],
            3,
        )

        class Unknown(Producer):
            def flush(self, _timeout):
                return 0

        with self.assertRaises(RuntimeError):
            publish([event], bootstrap="fixture:9092", producer_factory=Unknown)

    def test_partial_delivery_does_not_acknowledge_handoff(self):
        class Failed(Producer):
            def flush(self, _timeout):
                for callback in self.callbacks:
                    callback("failure", None)
                return 0

        with self.assertRaises(RuntimeError):
            publish([self.event()], bootstrap="fixture:9092", producer_factory=Failed)

    def test_strict_versioned_event_rejects_added_fields(self):
        event = self.event()
        event["fake_metric"] = 0
        with self.assertRaises(Exception):
            validate(event)

    def test_s3_conditional_bytes_and_readback_sdk_contract(self):
        import boto3
        from botocore.stub import Stubber

        client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="synthetic",
            aws_secret_access_key="synthetic",
        )
        raw = b'{"native": 0}\n'
        with Stubber(client) as stub:
            stub.add_response(
                "put_object",
                {},
                {
                    "Bucket": "fixture-bucket",
                    "Key": "raw/fetch.json",
                    "Body": raw,
                    "IfNoneMatch": "*",
                    "Metadata": {"sha256": sha256(raw)},
                },
            )
            stub.add_response(
                "get_object",
                {"Body": io.BytesIO(raw)},
                {"Bucket": "fixture-bucket", "Key": "raw/fetch.json"},
            )
            result = ArtifactStore("s3a://fixture-bucket/raw", s3_client=client).put(
                "fetch.json", raw
            )
            self.assertEqual(result["content_sha256"], sha256(raw))
            stub.assert_no_pending_responses()

    def test_s3_uncertain_existing_creation_reconciles(self):
        import boto3
        from botocore.stub import Stubber

        client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="synthetic",
            aws_secret_access_key="synthetic",
        )
        raw = b"accepted"
        with Stubber(client) as stub:
            stub.add_client_error(
                "put_object", "PreconditionFailed", http_status_code=412
            )
            stub.add_response("get_object", {"Body": io.BytesIO(raw)})
            self.assertEqual(
                ArtifactStore("s3a://fixture-bucket/raw", s3_client=client).put(
                    "fetch.json", raw
                )["byte_count"],
                len(raw),
            )

    def test_encoded_traversal_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArtifactStore(Path(directory).as_uri())
            with self.assertRaises(ValueError):
                store.put("%2e%2e/other.json", b"denied")


if __name__ == "__main__":
    unittest.main()
