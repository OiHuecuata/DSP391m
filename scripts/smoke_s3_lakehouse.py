"""Finite synthetic collector -> managed Delta -> two validated generations.

Default execution writes only an isolated local directory. S3 requires prior
operator approval, Phase 1 evidence, an isolated staging prefix and explicit
flags. No crawling, service startup, ClickHouse, cleanup, or production cutover.
"""

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "apps/collector/src"), str(ROOT / "pipelines/spark/src")]

from dsp_collector.jobs.collect_courts import process_pbplus_envelope
from dsp_collector.jobs.collect_social import process_threads_envelope
from dsp_collector.jobs.collect_youtube import process_youtube_envelope
from dsp_collector.runtime.outbox import atomic_json, enqueue, jobs
from dsp_spark.io.lakehouse_control import control_store, writable_root
from dsp_spark.jobs.batch.lakehouse_generation import (
    rollback,
    verify_generation,
    version_vector,
)
from dsp_spark.jobs.batch.s3_lakehouse import authorize_cloud_write, process_once
from dsp_spark.runtime.config import LakehousePaths, SparkSettings
from dsp_spark.runtime.spark import create_spark

BOOTSTRAP = "s3a://dsp391m-data-lake/bootstrap/20261028T064025Z/lakehouse"


def synthetic_youtube(run, at, video="synthetic-video-1"):
    return {
        "schema_version": "1",
        "source": "youtube",
        "run_id": run,
        "collected_at": at,
        "query_runs": [],
        "responses": [],
        "videos": [
            {
                "id": video,
                "snippet": {"title": "Synthetic pickleball Hà Nội", "publishedAt": at},
                "statistics": {"viewCount": "0"},
            }
        ],
        "comments": [
            {
                "video_id": video,
                "payload": {
                    "id": "synthetic-comment-" + video,
                    "snippet": {
                        "textOriginal": "Synthetic tìm sân Hà Nội",
                        "likeCount": 0,
                    },
                },
            }
        ],
        "video_queries": {},
        "requests": {},
        "errors": [],
    }


def run(args):
    work = args.workdir.resolve()
    base = writable_root(args.base or (work / "lake").as_uri())
    authorize_cloud_write(
        base,
        allow=args.allow_cloud_write,
        supervised_root=args.supervised_root,
        command="synthetic-smoke",
        synthetic=True,
        profile=args.profile,
        region="ap-southeast-2",
        verification_report=args.verification_report,
        bootstrap_root=BOOTSTRAP,
    )
    control = control_store(base, profile=args.profile, region="ap-southeast-2")
    if control.get("current.json")[0] or control.get("bootstrap-plan.json")[0]:
        raise ValueError("smoke requires a new isolated namespace")
    queue = work / "outbox"
    if list(jobs(queue)):
        raise ValueError("smoke requires a new isolated workdir")
    policies = json.loads(
        (ROOT / "configs/collection/s3_synthetic_policy.example.json").read_text()
    )
    at = datetime.now(UTC).isoformat()
    token = "synthetic-" + uuid4().hex[:12]
    artifacts = []
    with patch.dict(os.environ, {"DSP_S3_OUTBOX_DIR": str(queue)}):
        for source, processor in (
            ("pbplus", process_pbplus_envelope),
            ("threads", process_threads_envelope),
        ):
            raw = json.loads(
                (ROOT / "tests/fixtures/pipeline" / (source + ".json")).read_text()
            )
            raw.update(run_id=token + "-" + source, collected_at=at)
            artifacts.append(
                processor(raw, artifact_root=work / "artifacts").bronze_path
            )
        artifacts.append(
            process_youtube_envelope(
                synthetic_youtube(token + "-youtube", at),
                artifact_root=work / "artifacts",
            ).bronze_path
        )
    assert len(list(jobs(queue))) == 3, "collectors did not automatically enqueue"
    settings = SparkSettings(
        LakehousePaths(base), s3_profile=args.profile, s3_region="ap-southeast-2"
    )
    spark = create_spark(settings)
    try:
        first = process_once(
            spark, settings, control, queue, policies=policies, synthetic=True, as_of=at
        )
        assert first["status"] == "VALIDATED", first
        before = version_vector(spark, settings.paths)
        for artifact in artifacts:
            enqueue(artifact, queue)
        replay = process_once(
            spark, settings, control, queue, policies=policies, synthetic=True, as_of=at
        )
        assert (
            replay["generation"]["generation_id"]
            == first["generation"]["generation_id"]
        )
        assert (
            version_vector(spark, settings.paths) == before
        ), "retry created Bronze commits"
        new_at = datetime.now(UTC).isoformat()
        with patch.dict(os.environ, {"DSP_S3_OUTBOX_DIR": str(queue)}):
            process_youtube_envelope(
                synthetic_youtube(token + "-new", new_at, "synthetic-video-2"),
                artifact_root=work / "artifacts",
            )
        second = process_once(
            spark,
            settings,
            control,
            queue,
            policies=policies,
            synthetic=True,
            as_of=new_at,
        )
        assert second["status"] == "VALIDATED", second
        assert (
            second["generation"]["generation_id"]
            != first["generation"]["generation_id"]
        )
        assert (
            second["generation"]["after"]["silver_social_posts"]["rows"]
            > first["generation"]["after"]["silver_social_posts"]["rows"]
        )
        verify_generation(spark, settings, second["generation"])
        with control.writer():
            rollback(spark, settings, control, first["generation"]["generation_id"])
        result = {
            "status": "PASS",
            "synthetic_only": True,
            "base": base,
            "checks": {
                "collector_auto_enqueue": True,
                "all_three_bronze_commits": True,
                "replay_no_duplicates_or_commits": True,
                "automatic_new_generation": True,
                "all_23_outputs_read_back": True,
                "rollback_verified_and_paused": True,
            },
            "first": first,
            "second": second,
            "current": control.get("current.json")[0],
        }
        atomic_json(args.report or work / "smoke-report.json", result)
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "base": base,
                    "generation_id": second["generation"]["generation_id"],
                }
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--base")
    parser.add_argument("--profile", default="dsp391m-spark")
    parser.add_argument(
        "--verification-report",
        type=Path,
        default=ROOT / ".local/s3-lakehouse/snapshot-verification.json",
    )
    parser.add_argument("--report", type=Path)
    parser.add_argument("--allow-cloud-write", action="store_true")
    parser.add_argument("--supervised-root", action="store_true")
    run(parser.parse_args())
