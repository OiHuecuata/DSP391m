"""Measured live-run reporting must survive native Spark metadata types."""

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "live_accumulation", ROOT / "scripts/collect_live_dataset.py"
)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)


class AccumulationReportingTests(unittest.TestCase):
    def test_real_uuid_progress_is_serializable_and_retains_metrics(self):
        identity = uuid4()
        progress = module.progress_json(
            [{"id": identity, "numInputRows": 25, "batchId": 2}]
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "processing.json"
            module.pipeline.write_report(path, {"progress": progress})
            result = json.loads(path.read_text())
        self.assertEqual(result["progress"][0]["id"], str(identity))
        self.assertEqual(result["progress"][0]["numInputRows"], 25)

    def test_business_content_excludes_lineage_and_normalization_clock(self):
        a = {
            "text": "pickleball",
            "run_id": "first",
            "normalized_at": "earlier",
            "detected_locations": ["Huế", "Hà Nội"],
        }
        b = {
            **a,
            "run_id": "second",
            "normalized_at": "later",
            "detected_locations": ["Hà Nội", "Huế"],
        }
        self.assertEqual(
            module.content(a, ("text", "detected_locations")),
            module.content(b, ("text", "detected_locations")),
        )

    def test_disk_guard_stops_before_live_collection(self):
        from types import SimpleNamespace
        from unittest.mock import patch

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(
                module.shutil, "disk_usage", return_value=SimpleNamespace(free=0)
            ),
            patch("dsp_collector.jobs.collect_social.collect_threads") as collector,
            self.assertRaisesRegex(RuntimeError, "Disk capacity guard"),
        ):
            module.collect_one(
                Path(directory), 1, "threads", {"minimum_free_bytes": 100}
            )
        collector.assert_not_called()


if __name__ == "__main__":
    unittest.main()
