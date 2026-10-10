"""Permitted synthetic source artifacts; never fetch or replay operational data."""

import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from jsonschema import ValidationError

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "apps/collector/src"))

from dsp_collector.jobs.collect_courts import process_pbplus_envelope
from dsp_collector.jobs.collect_social import process_threads_envelope
from dsp_collector.jobs.collect_youtube import process_youtube_envelope
from dsp_collector.normalizers.observation import (
    canonical_json,
    parse_json,
    prepare_envelope,
    project_envelope,
    run_envelope_context,
    validate_observation,
)
from dsp_collector.publishers.kafka import publish_social_batch

FIXTURE = ROOT / "tests/fixtures/pipeline/native-source-artifacts.v1.json"


class ObservationNormalizerTests(unittest.TestCase):
    def setUp(self):
        self.artifacts = json.loads(FIXTURE.read_text())
        self.context = run_envelope_context(
            permission_reference="fixture:" + str(FIXTURE.relative_to(ROOT)),
            retention_policy_id="synthetic-local-testing",
        )

    def rows(
        self,
        source="threads",
        *,
        envelope=None,
        context=None,
        uri="file:///tmp/native-source.json",
    ):
        value = self.artifacts[source] if envelope is None else envelope
        return project_envelope(
            prepare_envelope(value, context or self.context),
            artifact_uri=uri,
            artifact_sha256="0" * 64,
        )

    def test_all_source_fields_and_exact_payload_pointer(self):
        for source, count in [("pbplus", 1), ("threads", 2), ("youtube", 2)]:
            for row in self.rows(source):
                value = self.artifacts[source]
                for key in row["source_payload_path"].split("/")[1:]:
                    value = value[int(key)] if isinstance(value, list) else value[key]
                self.assertEqual(parse_json(row["raw_payload"]), value)
                self.assertEqual(
                    row["payload_hash"],
                    hashlib.sha256(canonical_json(value).encode()).hexdigest(),
                )
                self.assertEqual(
                    row["collection_run_id"], self.artifacts[source]["run_id"]
                )
                validate_observation(row)
            self.assertEqual(len(self.rows(source)), count)

    def test_legacy_known_identity_vector_stable_under_uri_and_payload_change(self):
        envelope = json.loads(
            (ROOT / "tests/fixtures/pipeline/pbplus.json").read_text()
        )
        before = self.rows("pbplus", envelope=envelope)
        known = next(r for r in before if r["source_record_id"].endswith("hanoi"))
        self.assertEqual(
            known["observation_id"],
            "sha256:d32a51b5d6f626aeb2356363f8b24b26f0fd7a49818a552e1199be41f6b02c90",
        )
        envelope["posts"][0]["number_of_courts"] = 12
        changed = next(
            r
            for r in self.rows(
                "pbplus", envelope=envelope, uri="file:///tmp/copied.json"
            )
            if r["source_record_id"].endswith("hanoi")
        )
        self.assertEqual(known["observation_id"], changed["observation_id"])
        self.assertNotEqual(known["payload_hash"], changed["payload_hash"])

    def test_run_query_fetch_identity_disjoint_and_native_content_stable(self):
        envelope = self.artifacts["pbplus"]
        one = self.rows("pbplus")[0]
        other = copy.deepcopy(envelope)
        other["run_id"] = "another-run"
        variants = [one, self.rows("pbplus", envelope=other)[0]]
        for query, batch in [("q1", "b1"), ("q2", "b1"), ("q1", "b2")]:
            context = {
                **self.context,
                "provenance_scope": "FETCH_BATCH",
                "query_id": query,
                "fetch_batch_id": batch,
                "observed_at": "2026-10-05T11:59:00.000000Z",
            }
            variants.append(self.rows("pbplus", context=context)[0])
        self.assertEqual(len({r["observation_id"] for r in variants}), 5)
        self.assertEqual(len({r["native_record_key"] for r in variants}), 1)

    def test_merged_query_evidence_does_not_invent_single_query_or_page(self):
        row = next(r for r in self.rows() if r["record_type"] == "Post")
        self.assertIsNone(row["query_id"])
        self.assertEqual(row["provenance_scope"], "RUN_ENVELOPE")
        self.assertEqual(
            parse_json(row["query_provenance"])["query_contexts"],
            self.artifacts["threads"]["posts"][0]["query_contexts"],
        )
        self.assertEqual(
            parse_json(row["query_provenance"])["matched_queries"],
            ["pickleball Hà Nội", "pickleball"],
        )
        self.assertEqual(row["observed_at"], row["collected_at"])

    def test_metric_missing_zero_disabled_and_legacy_uncertainty(self):
        post = next(r for r in self.rows() if r["record_type"] == "Post")
        reply = next(r for r in self.rows() if r["record_type"] == "Comment")
        self.assertIsNone(post["like_count"])
        self.assertEqual(post["metric_availability"]["like_count"], "UNAVAILABLE")
        self.assertEqual(reply["metric_availability"]["like_count"], "UNKNOWN")
        value = copy.deepcopy(self.artifacts["threads"])
        value["posts"][0]["raw_payload"]["like_and_view_counts_disabled"] = False
        row = next(r for r in self.rows(envelope=value) if r["record_type"] == "Post")
        self.assertEqual(row["like_count"], 0)
        self.assertEqual(row["metric_availability"]["like_count"], "AVAILABLE")
        video = next(r for r in self.rows("youtube") if r["record_type"] == "Video")
        self.assertEqual(video["view_count"], 0)
        self.assertIsNone(video["like_count"])
        self.assertEqual(video["comment_count"], 2)
        self.assertIsNone(video["reply_count"])
        self.assertEqual(video["metric_availability"]["share_count"], "NOT_PROVIDED")

    def test_venue_and_content_fields_preserved(self):
        venue = self.rows("pbplus")[0]
        self.assertEqual(venue["court_count_raw"], "6")
        self.assertEqual(venue["price_raw"], "100k–150k/giờ")
        self.assertEqual(parse_json(venue["amenities_raw"]), ["Đèn", "Bãi xe"])
        video = next(r for r in self.rows("youtube") if r["record_type"] == "Video")
        self.assertEqual(video["title_raw"], "Synthetic Hà Nội venue")
        self.assertEqual(video["language_raw"], "vi")
        self.assertIsNotNone(video["description_raw"])

    def test_comment_own_text_update_and_captured_reply_parent(self):
        comment = next(r for r in self.rows("youtube") if r["record_type"] == "Comment")
        self.assertEqual(comment["native_post_id"], "native_video_1")
        self.assertEqual(comment["comment_text_raw"], "Synthetic: tìm sân ở Huế")
        self.assertEqual(comment["updated_at"], "2026-10-05T01:01:00.000000Z")
        self.assertIsNone(comment["parent_comment_id"])
        reply = next(r for r in self.rows() if r["record_type"] == "Comment")
        self.assertEqual(reply["native_post_id"], "123")
        self.assertTrue(reply["is_reply"])

    def test_duplicate_exact_dedup_conflicting_native_payload_rejected(self):
        envelope = copy.deepcopy(self.artifacts["pbplus"])
        envelope["posts"] *= 2
        envelope["count"] = 2
        self.assertEqual(len(self.rows("pbplus", envelope=envelope)), 1)
        envelope["posts"][1] = {**envelope["posts"][1], "court_name": "different"}
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            self.rows("pbplus", envelope=envelope)

    def test_hash_identity_type_availability_forgery_rejected(self):
        row = self.rows("pbplus")[0]
        for change in [
            {"payload_hash": "f" * 64},
            {"observation_id": "sha256:" + "f" * 64},
            {"native_record_key": "sha256:" + "f" * 64},
            {"source_record_id": "https://wrong.example/venue"},
            {"view_count": True},
            {
                "metric_availability": {
                    **row["metric_availability"],
                    "view_count": "AVAILABLE",
                }
            },
        ]:
            with (
                self.subTest(change=change),
                self.assertRaises((ValueError, ValidationError)),
            ):
                validate_observation({**row, **change})

    def test_bad_json_and_unknown_fields_rejected(self):
        for value in ['{"a":1,"a":2}', '{"x":NaN}', '{"x":9223372036854775808}']:
            with self.assertRaises(ValueError):
                parse_json(value)
        with self.assertRaises(ValidationError):
            validate_observation({**self.rows("pbplus")[0], "unexpected": 1})

    def test_context_required_and_replay_cannot_relabel_it(self):
        with self.assertRaisesRegex(ValueError, "explicit"):
            project_envelope(
                self.artifacts["pbplus"],
                artifact_uri="file:///tmp/f",
                artifact_sha256="0" * 64,
            )
        value = prepare_envelope(self.artifacts["pbplus"], self.context)
        with self.assertRaisesRegex(ValueError, "conflicts"):
            prepare_envelope(value, {**self.context, "permission_reference": "another"})

    def test_timezone_normalization_does_not_invent_publication(self):
        envelope = copy.deepcopy(self.artifacts["pbplus"])
        envelope["collected_at"] = "2026-10-05T19:00:00+07:00"
        row = self.rows("pbplus", envelope=envelope)[0]
        self.assertEqual(row["collected_at"], "2026-10-05T12:00:00.000000Z")
        self.assertIsNone(row["published_at"])

    def test_credential_or_missing_identity_rejected_before_artifact_write(self):
        for change in [{"api_key": "synthetic-denied-key"}, {"id": None, "url": None}]:
            with tempfile.TemporaryDirectory() as tmp:
                envelope = copy.deepcopy(self.artifacts["pbplus"])
                envelope["posts"][0].update(change)
                with self.assertRaises(ValueError):
                    process_pbplus_envelope(
                        envelope, artifact_root=tmp, observation_context=self.context
                    )
                self.assertEqual(list(Path(tmp).rglob("*.json")), [])

    def test_expired_context_denied_before_raw_write(self):
        with (
            tempfile.TemporaryDirectory() as tmp,
            self.assertRaisesRegex(ValueError, "expired"),
        ):
            process_pbplus_envelope(
                self.artifacts["pbplus"],
                artifact_root=tmp,
                observation_context={
                    **self.context,
                    "retention_expires_at": "2020-01-01T00:00:00.000000Z",
                },
            )

    def test_current_processors_emit_opt_in_lineage_metadata_only(self):
        for source, processor in [
            ("pbplus", process_pbplus_envelope),
            ("threads", process_threads_envelope),
            ("youtube", process_youtube_envelope),
        ]:
            with self.subTest(source=source), tempfile.TemporaryDirectory() as tmp:
                result = processor(
                    self.artifacts[source],
                    artifact_root=tmp,
                    observation_context=self.context,
                )
                raw = result.bronze_path.read_bytes()
                batch = parse_json(result.silver_path.read_bytes())
                self.assertTrue(batch["native_bronze_required"])
                self.assertEqual(
                    batch["native_bronze_artifact"]["content_sha256"],
                    hashlib.sha256(raw).hexdigest(),
                )
                self.assertEqual(
                    parse_json(raw)["run_id"], self.artifacts[source]["run_id"]
                )

    def test_youtube_replay_run_cannot_be_reassigned(self):
        with (
            tempfile.TemporaryDirectory() as tmp,
            self.assertRaisesRegex(ValueError, "conflicts"),
        ):
            process_youtube_envelope(
                self.artifacts["youtube"], run_id="different", artifact_root=tmp
            )

    def test_native_publication_requires_verifier_before_producer(self):
        factory = Mock()
        batch = {
            "schema_version": "1",
            "source": "threads",
            "run_id": "native",
            "count": 0,
            "records": [],
            "native_bronze_required": True,
        }
        with self.assertRaisesRegex(ValueError, "verified durable Bronze"):
            publish_social_batch(batch, publisher_factory=factory)
        verifier = Mock(side_effect=ValueError("commit/readback failure"))
        with self.assertRaisesRegex(ValueError, "commit/readback"):
            publish_social_batch(
                batch, publisher_factory=factory, bronze_verifier=verifier
            )
        factory.assert_not_called()
        verifier.assert_called_once_with(batch)


if __name__ == "__main__":
    unittest.main()
