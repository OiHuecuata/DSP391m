"""Pure contract checks; no AWS, model downloads or synthetic benchmark claims."""

import hashlib
import json
import tempfile
import unittest
from dataclasses import asdict, replace
from pathlib import Path

from dsp_spark.ml.config import EncoderConfig, TrainingConfig, staging_uri
from dsp_spark.ml.data import Snapshot, normalize_text
from dsp_spark.ml.embedding import checked_vectors
from dsp_spark.ml.explain import safe_tokens
from dsp_spark.ml.registry import Registry, runtime_versions


class DemandContracts(unittest.TestCase):
    def test_strict_encoder_config_and_cache_identity(self):
        base = EncoderConfig(revision="a" * 40)
        for other in (
            replace(base, revision="b" * 40),
            replace(base, max_seq_length=256),
            replace(base, version="v2"),
        ):
            self.assertNotEqual(base.key, other.key)
        with self.assertRaisesRegex(ValueError, "immutable"):
            EncoderConfig(revision="main")
        with self.assertRaises(ValueError):
            TrainingConfig(base, ks=(8,), primary_candidate=5)
        with self.assertRaises(TypeError):
            TrainingConfig.from_dict({"encoder": asdict(base), "unknown": True})

    def test_text_and_vectors(self):
        self.assertEqual(normalize_text(" Ａ\t  sân\u00a0 \n"), "A sân")
        self.assertEqual(normalize_text(None), "")
        self.assertEqual(checked_vectors([[1, 0]], 2, normalized=True).shape, (1, 2))
        for vectors, dim in (([[float("nan"), 0]], 2), ([[1]], 2), ([[0, 0]], 2)):
            with self.assertRaises(ValueError):
                checked_vectors(vectors, dim, normalized=True)

    def test_staging_and_snapshot_guards(self):
        for path in (
            "s3a://bucket/production/models",
            "s3a://staging/production/models",
            "file:///tmp/models",
            "s3a://bucket/staging/../silver",
        ):
            with self.assertRaises(ValueError):
                staging_uri(path)
        self.assertEqual(
            staging_uri("s3a://bucket/staging/ml"), "s3a://bucket/staging/ml"
        )
        with self.assertRaises(ValueError):
            Snapshot("file:///tmp/x", -1, "unknown", "fixture")
        with self.assertRaises(ValueError):
            Snapshot("file:///tmp/x", 0, "pretend", "fixture")
        with self.assertRaisesRegex(ValueError, "schema"):
            Snapshot("file:///tmp/x", 0, "unknown", "fixture", schema_version="999")

    def test_registry_immutable_checksum_and_runtime(self):
        with tempfile.TemporaryDirectory() as temp:
            registry = Registry(str(Path(temp) / "staging"))
            registry.put("runs/r/umap.joblib", b"fixture")
            manifest = {
                "umap_sha256": hashlib.sha256(b"fixture").hexdigest(),
                "artifact_version": "demand-clustering-v1",
                "runtime": runtime_versions(),
                "status": "candidate",
            }
            registry.commit("r", manifest)
            self.assertEqual(registry.manifest("r")["status"], "candidate")
            self.assertEqual(registry.load_umap("r", manifest), b"fixture")
            with self.assertRaises(FileExistsError):
                registry.commit("r", manifest)
            with self.assertRaisesRegex(ValueError, "checksum"):
                registry.load_umap("r", {**manifest, "umap_sha256": "wrong"})
            manifest["runtime"]["numpy"] = "incompatible"
            Path(temp, "staging/runs/r/manifest.json").write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "runtime"):
                registry.manifest("r")

    def test_pii_filtered_from_lexical_tokens(self):
        tokens = safe_tokens(
            "Tìm sân 0912 345 678 person@example.com https://private.invalid @someone"
        )
        self.assertEqual(tokens, ["tìm", "sân"])


if __name__ == "__main__":
    unittest.main()
