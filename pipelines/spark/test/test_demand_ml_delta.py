"""Real local Spark/Delta + UMAP. Synthetic vectors do not measure encoder quality."""

import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import numpy as np
from dsp_spark.ml.config import EncoderConfig, ReductionConfig, TrainingConfig
from dsp_spark.ml.data import Snapshot, load_posts
from dsp_spark.ml.embedding import embed
from dsp_spark.ml.evaluation import adjusted_rand, evaluate
from dsp_spark.ml.extensions import spatial_aggregations
from dsp_spark.ml.pipeline import infer, train
from dsp_spark.ml.publish import assignment_path, publish, rollback
from dsp_spark.ml.reduction import fit_umap
from dsp_spark.ml.registry import Registry
from dsp_spark.runtime.config import LakehousePaths, SparkSettings
from dsp_spark.runtime.spark import create_spark
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.linalg import Vectors
from pyspark.sql import functions as F
from sklearn.metrics import (
    adjusted_rand_score,
    calinski_harabasz_score,
    silhouette_score,
)


def synthetic_partition(rows, config):
    """Deliberate synthetic injection, unavailable from the production CLI."""
    import hashlib

    import numpy as np

    for row in rows:
        seed = int(hashlib.sha256(row.text.encode()).hexdigest()[:8], 16)
        vector = np.random.default_rng(seed).normal(size=config.dimension)
        vector[int(row.content_id.split("-")[-1]) % 3] += 6.0
        vector /= np.linalg.norm(vector)
        yield row.content_id, row.text_hash_ml, config.key, vector.tolist()


class DemandDeltaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(
            SparkSettings(LakehousePaths("file:///tmp/dsp-ml-session"))
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def fixture(self, root):
        schema = "post_id string, content_id string, text_normalized string, platform string, schema_version string, lineage_ids array<string>, intent_labels array<string>, topic_labels array<string>, is_duplicate boolean, last_seen_at string"
        rows = [
            (
                f"post-{i}",
                f"synthetic-{i}",
                f"Synthetic pickleball demand group {i%3} example {i}",
                "synthetic",
                "3",
                [f"synthetic-lineage-{i}"],
                [f"intent-{i%3}"],
                ["synthetic-topic"],
                False,
                "2026-10-28T00:00:00Z",
            )
            for i in range(66)
        ]
        path = (root / "silver/social_posts").as_uri()
        self.spark.createDataFrame(rows, schema).write.format("delta").save(path)
        return Snapshot(path, 0, "synthetic", "synthetic-ml-integration"), rows, schema

    def test_metric_parity_with_sklearn(self):
        matrix = np.array(
            [[1.0, 0.0], [0.9, 0.2], [-1.0, 0.0], [-0.8, -0.1], [0.8, 0.1]]
        )
        semantic = matrix / np.linalg.norm(matrix, axis=1, keepdims=True)
        labels = [0, 0, 1, 1, 0]
        predictions = self.spark.createDataFrame(
            [
                (Vectors.dense(x), Vectors.dense(e), int(label))
                for x, e, label in zip(matrix, semantic, labels)
            ],
            ["features", "semantic_features", "cluster_id"],
        )
        metrics = evaluate(predictions)
        self.assertAlmostEqual(
            metrics["cosine_silhouette_original_embeddings"],
            silhouette_score(semantic, labels, metric="cosine"),
            places=7,
        )
        self.assertAlmostEqual(
            metrics["calinski_harabasz_umap_euclidean"],
            calinski_harabasz_score(matrix, labels),
            places=7,
        )
        other = [1, 0, 0, 1, 0]
        pairs = self.spark.createDataFrame(
            list(zip(labels, other)), "reference_cluster int, cluster_id int"
        )
        self.assertAlmostEqual(
            adjusted_rand(pairs), adjusted_rand_score(labels, other), places=12
        )

    def test_loader_pinning_schema_empty_and_fanout(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot, rows, schema = self.fixture(root)
            first, report = load_posts(self.spark, snapshot)
            self.assertEqual(report["rows"], 66)
            self.spark.createDataFrame([rows[0]], schema).write.format("delta").mode(
                "append"
            ).save(snapshot.path)
            self.assertEqual(load_posts(self.spark, snapshot)[1]["rows"], 66)
            with self.assertRaisesRegex(ValueError, "duplicate"):
                load_posts(self.spark, replace(snapshot, version=1))
            broken = self.spark.createDataFrame([("x",)], "content_id string")
            bad = (root / "bad").as_uri()
            broken.write.format("delta").save(bad)
            with self.assertRaisesRegex(ValueError, "schema"):
                load_posts(self.spark, replace(snapshot, path=bad))
            self.assertEqual(
                load_posts(self.spark, snapshot, after="2026-10-29T00:00:00Z")[1][
                    "rows"
                ],
                0,
            )
            self.assertEqual(first.select("content_id").distinct().count(), 66)

    def test_null_empty_content_fanout_and_model_save_load_parity(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot, rows, schema = self.fixture(root)
            bad_rows = [("new-post", rows[0][1], *rows[0][2:])]
            self.spark.createDataFrame(bad_rows, schema).write.format("delta").mode(
                "append"
            ).save(snapshot.path)
            with self.assertRaisesRegex(ValueError, "fanout"):
                load_posts(self.spark, replace(snapshot, version=1))
            nullable = list(rows[0])
            nullable[0] = "null-content-post"
            nullable[1] = None
            null_path = (root / "null").as_uri()
            self.spark.createDataFrame([tuple(nullable)], schema).write.format(
                "delta"
            ).save(null_path)
            with self.assertRaisesRegex(ValueError, "null"):
                load_posts(self.spark, replace(snapshot, path=null_path))
            empty = list(rows[0])
            empty[2] = "\u3000 \n"
            empty_path = (root / "empty").as_uri()
            self.spark.createDataFrame([tuple(empty)], schema).write.format(
                "delta"
            ).save(empty_path)
            self.assertEqual(
                load_posts(self.spark, replace(snapshot, path=empty_path))[1][
                    "empty_text_excluded"
                ],
                1,
            )
            data = self.spark.createDataFrame(
                [
                    (str(i), Vectors.dense(v))
                    for i, v in enumerate(
                        ([0.0, 1.0], [0.1, 1.1], [5.0, 6.0], [5.1, 6.1])
                    )
                ],
                ["id", "features"],
            )
            model = KMeans(k=2, seed=42).fit(data)
            model_path = (root / "model").as_uri()
            model.save(model_path)
            self.assertEqual(
                model.transform(data).orderBy("id").collect(),
                KMeansModel.load(model_path).transform(data).orderBy("id").collect(),
            )

    def test_embedding_cache_invalidates_changed_text(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot, _, _ = self.fixture(root)
            posts, _ = load_posts(self.spark, snapshot)
            one = posts.filter(F.col("content_id") == "synthetic-1")
            encoder = EncoderConfig(
                model="synthetic-injected-not-MiniLM", revision="a" * 40, dimension=18
            )
            cache = str(root / "staging/cache")
            first = (
                embed(
                    one,
                    encoder,
                    cache,
                    partitions=1,
                    partition_encoder=synthetic_partition,
                )
                .first()
                .embedding
            )
            changed = one.withColumn(
                "text", F.lit("a changed synthetic post")
            ).withColumn("text_hash_ml", F.sha2("text", 256))
            second = (
                embed(
                    changed,
                    encoder,
                    cache,
                    partitions=1,
                    partition_encoder=synthetic_partition,
                )
                .first()
                .embedding
            )
            self.assertNotEqual(first, second)
            self.assertEqual(self.spark.read.format("delta").load(cache).count(), 2)

    def test_full_artifact_inference_cache_idempotency_and_rollback(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot, rows, schema = self.fixture(root)
            config = TrainingConfig(
                EncoderConfig(
                    model="synthetic-injected-not-MiniLM",
                    revision="a" * 40,
                    dimension=18,
                ),
                reduction=ReductionConfig(n_components=15, n_neighbors=30),
                ks=(3,),
                primary_candidate=3,
                stability_seeds=(17, 29, 43),
                partitions=2,
            )
            registry = Registry(str(root / "staging/registry"))
            cache = str(root / "staging/embedding_cache_v1")
            report = train(
                self.spark,
                snapshot,
                config,
                registry,
                cache,
                run_id="synthetic-test",
                partition_encoder=synthetic_partition,
            )
            self.assertEqual(report["status"], "candidate")
            self.assertEqual(
                len(report["candidates"]["3"]["metrics"]["stability"]["runs"]), 3
            )
            registry.manifest("synthetic-test")
            first, _ = infer(
                self.spark,
                snapshot,
                registry,
                "synthetic-test",
                3,
                cache,
                partition_encoder=synthetic_partition,
            )
            first = first.persist()
            self.assertEqual(first.count(), 66)
            from datetime import UTC

            from jsonschema import Draft202012Validator, FormatChecker

            contract = json.loads(
                (
                    Path(__file__).resolve().parents[3]
                    / "contracts/data/gold/demand_cluster_assignments_v1.schema.json"
                ).read_text()
            )
            example = first.first().asDict()
            example["inference_timestamp"] = (
                example["inference_timestamp"].replace(tzinfo=UTC).isoformat()
            )
            Draft202012Validator(contract, format_checker=FormatChecker()).validate(
                example
            )
            second, _ = infer(
                self.spark,
                snapshot,
                registry,
                "synthetic-test",
                3,
                cache,
                partition_encoder=synthetic_partition,
            )
            columns = ["content_id", "cluster_id", "distance_to_centroid"]
            self.assertEqual(
                first.select(*columns).orderBy("content_id").collect(),
                second.select(*columns).orderBy("content_id").collect(),
            )
            output = str(root / "staging/publish")
            publish(first, output)
            publish(second, output)
            self.assertEqual(
                self.spark.read.format("delta").load(assignment_path(output)).count(),
                66,
            )
            new_row = list(rows[0])
            new_row[0] = "post-99"
            new_row[1] = "synthetic-99"
            new_row[2] = "Entirely new synthetic pickleball post"
            new_row[-1] = "2026-10-29T00:00:00Z"
            self.spark.createDataFrame([tuple(new_row)], schema).write.format(
                "delta"
            ).mode("append").save(snapshot.path)
            new_snapshot = replace(snapshot, version=1)
            new, _ = infer(
                self.spark,
                new_snapshot,
                registry,
                "synthetic-test",
                3,
                cache,
                after="2026-10-28T00:00:00Z",
                partition_encoder=synthetic_partition,
            )
            self.assertEqual(new.count(), 1)
            parity, _ = infer(
                self.spark,
                new_snapshot,
                registry,
                "synthetic-test",
                3,
                cache,
                after="2026-10-28T00:00:00Z",
                partition_encoder=synthetic_partition,
            )
            self.assertEqual(
                new.select(*columns).collect(), parity.select(*columns).collect()
            )
            publish(new, output)
            self.assertEqual(
                self.spark.read.format("delta").load(assignment_path(output)).count(),
                67,
            )
            rollback(self.spark, output, 0)
            self.assertEqual(
                self.spark.read.format("delta").load(assignment_path(output)).count(),
                66,
            )
            posts, _ = load_posts(self.spark, snapshot)
            encoded = embed(
                posts,
                config.encoder,
                cache,
                partitions=2,
                partition_encoder=synthetic_partition,
            )
            with self.assertRaisesRegex(ValueError, "bounded"):
                fit_umap(encoded, replace(config.reduction, fit_max_rows=32), 18)
            self.assertEqual(self.spark.read.format("delta").load(cache).count(), 67)
            first.unpersist()
            Path(".local/ml").mkdir(parents=True, exist_ok=True)
            Path(".local/ml/synthetic-integration-metrics.json").write_text(
                json.dumps(report, indent=2)
            )

    def test_spatial_distinct_counts_no_evidence_fanout(self):
        venues = self.spark.createDataFrame(
            [("v1", "a"), ("v2", "a")], "venue_id string, old_province_id string"
        )
        mentions = self.spark.createDataFrame(
            [("p1", "a"), ("p1", "a"), ("p2", "a"), ("orphan", "a")],
            "content_id string, old_province_id string",
        )
        posts = self.spark.createDataFrame([("p1",), ("p2",)], "content_id string")
        result = spatial_aggregations(
            venues, mentions, posts, "old_province_id"
        ).first()
        self.assertEqual(
            (result.venue_count, result.interested_post_count, result.posts_per_venue),
            (2, 2, 1.0),
        )


if __name__ == "__main__":
    unittest.main()
