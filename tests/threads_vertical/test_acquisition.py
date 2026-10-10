import json
import tempfile
import unittest
from pathlib import Path

from dsp_collector.normalizers.observation_v2 import project
from dsp_collector.runtime.durable import ArtifactStore, sha256
from dsp_collector.sources.crawler.threads.incremental import collect_incremental
from dsp_collector.sources.crawler.threads.rich_fields import metrics

from tests.threads_vertical.fixtures import POLICY, FixtureFetcher


class AcquisitionTests(unittest.TestCase):
    def test_unknown_policy_prevents_fetch(self):
        with tempfile.TemporaryDirectory() as directory:
            fetcher = FixtureFetcher()
            with self.assertRaises(PermissionError):
                collect_incremental(
                    {}, run_id="denied", directory=directory, policy={}, fetcher=fetcher
                )
            self.assertEqual(fetcher.calls, 0)

    def test_distinct_queries_resume_and_native_parity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = {
                "raw_base_uri": (root / "raw").as_uri(),
                "unique_target": 120,
                "request_budget": 8,
                "queries": ["pickleball", "pickleball giá thuê"],
                "pacing_seconds": 0.5,
            }
            fetcher = FixtureFetcher()
            state = collect_incremental(
                config,
                run_id="synthetic",
                directory=root / "state",
                policy=POLICY,
                fetcher=fetcher,
                fixture=True,
            )
            self.assertEqual(len(state["seen"]), 120)
            self.assertEqual(len(state["artifacts"]), 2)
            resumed = collect_incremental(
                config,
                run_id="synthetic",
                directory=root / "state",
                policy=POLICY,
                fetcher=fetcher,
                fixture=True,
            )
            self.assertEqual(resumed, state)
            self.assertEqual(fetcher.calls, 2)
            store = ArtifactStore(config["raw_base_uri"])
            rows = []
            for reference in state["artifacts"]:
                raw = store.read(reference["artifact_uri"])
                self.assertEqual(sha256(raw), reference["content_sha256"])
                rows.extend(
                    project(
                        json.loads(raw),
                        artifact_uri=reference["artifact_uri"],
                        artifact_sha256=sha256(raw),
                    )
                )
            self.assertEqual(len(rows), 126)
            self.assertEqual(sum(row["record_type"] == "Comment" for row in rows), 6)
            nested = next(r for r in rows if r["source_record_id"] == "10002")
            self.assertEqual(nested["parent_comment_id"], "10001")
            self.assertEqual(nested["native_post_id"], "1")
            zero = next(r for r in rows if r["source_record_id"] == "10001")
            self.assertEqual(zero["like_count"], 0)
            self.assertIsNone(zero["language_raw"])
            post = next(r for r in rows if r["source_record_id"] == "1")
            self.assertEqual(post["view_count"], 10)
            self.assertEqual(post["language_raw"], "vi")

    def test_raw_write_before_checkpoint_recovers_without_fetch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = {
                "raw_base_uri": (root / "raw").as_uri(),
                "unique_target": 100,
                "request_budget": 4,
                "queries": ["pickleball"],
            }
            state = collect_incremental(
                config,
                run_id="crash",
                directory=root / "state",
                policy=POLICY,
                fetcher=FixtureFetcher(),
                fixture=True,
            )
            checkpoint = root / "state/fetch-checkpoint.json"
            value = json.loads(checkpoint.read_text())
            value.update(seen=[], queries={}, artifacts=[], requests=0)
            checkpoint.write_text(json.dumps(value))

            def denied_fetch(*_args):
                raise AssertionError("Resume should use durable original fetch")

            recovered = collect_incremental(
                config,
                run_id="crash",
                directory=root / "state",
                policy=POLICY,
                fetcher=denied_fetch,
                fixture=True,
            )
            self.assertEqual(recovered["artifacts"], state["artifacts"])

    def test_metrics_missing_zero_invalid_and_disabled(self):
        values, states = metrics({"like_count": 0, "view_count": "bad"})
        self.assertEqual((values["like_count"], states["like_count"]), (0, "AVAILABLE"))
        self.assertEqual(
            (values["reply_count"], states["reply_count"]), (None, "NOT_PROVIDED")
        )
        self.assertEqual(states["view_count"], "INVALID")
        self.assertEqual(
            metrics({"like_count": 5, "like_and_view_counts_disabled": True})[0][
                "like_count"
            ],
            None,
        )

    def test_conflicting_raw_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArtifactStore(Path(directory).as_uri())
            first = store.put("immutable.json", b"first")
            with self.assertRaises(ValueError):
                store.put("immutable.json", b"second")
            self.assertEqual(store.read(first["artifact_uri"]), b"first")


if __name__ == "__main__":
    unittest.main()
