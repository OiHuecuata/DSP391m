"""Contract-driven Delta Bronze persistence and evidence-based source changes."""

import os
from datetime import UTC, datetime, timedelta

from delta.tables import DeltaTable
from dsp_collector.runtime.bronze import (
    DATASETS,
    apply_source_policy,
    digest,
    encode,
    load_policies,
    timestamp,
    validate_observation,
)
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ...io.delta import merge_delta, read_delta
from .contracts import spark_schema

REJECTION_SCHEMA = T.StructType(
    [
        T.StructField("rejection_id", T.StringType(), False),
        T.StructField("source", T.StringType(), False),
        T.StructField("run_id", T.StringType(), True),
        T.StructField("artifact_uri", T.StringType(), True),
        T.StructField("error_json", T.StringType(), False),
        T.StructField("rejected_at", T.TimestampType(), False),
    ]
)


def reject_bronze(spark, settings, source, run_id, artifact_uri, errors):
    rows = [
        (
            digest([source, run_id, e]),
            source,
            run_id,
            artifact_uri,
            encode(e),
            datetime.now(UTC),
        )
        for e in errors
    ]
    merge_delta(
        spark.createDataFrame(rows, REJECTION_SCHEMA),
        settings.paths.rejected_bronze,
        keys=("rejection_id",),
    )


def observation_frame(spark, dataset, records):
    schema = spark_schema(dataset)
    timestamps = [f.name for f in schema if isinstance(f.dataType, T.TimestampType)]
    converted = [
        {
            **row,
            **{
                name: (
                    datetime.fromisoformat(row[name]) if row[name] is not None else None
                )
                for name in timestamps
            },
        }
        for row in records
    ]
    return spark.createDataFrame(converted, schema)


def _source_changes(spark, frame, path):
    """Use actual preceding payload evidence, never absence or failed collection.

    Out-of-order replay compares only earlier source observations. First-seen
    means new to this lake, not newly created at the source. Metrics do not
    produce duplicate lifecycle events for their associated content.
    """
    history = (
        read_delta(spark, path)
        if DeltaTable.isDeltaTable(spark, path)
        else spark.createDataFrame([], frame.schema)
    )
    rows = history.unionByName(frame).dropDuplicates(["observation_id"])
    window = Window.partitionBy(
        "source_platform", "record_type", "source_record_id"
    ).orderBy(F.to_timestamp("collected_at"), "collection_run_id", "observation_id")
    compared = rows.withColumn(
        "_previous_hash", F.lag("payload_hash").over(window)
    ).withColumn("_previous_id", F.lag("observation_id").over(window))
    # Do not reinterpret rows already ingested; replay remains deterministic.
    unseen = frame.join(
        history.select("observation_id"), "observation_id", "left_anti"
    ).select("observation_id")
    changed = compared.join(unseen, "observation_id").filter(
        F.col("_previous_hash").isNull()
        | (F.col("payload_hash") != F.col("_previous_hash"))
    )
    # Epoch microseconds preserve instants independently of both the SQL
    # session timezone and the Python driver's local timezone.
    timestamp_fields = {
        f.name for f in changed.schema if isinstance(f.dataType, T.TimestampType)
    }
    changed = changed.select(
        *(
            (
                F.unix_micros(F.col(f.name)).alias(f.name)
                if isinstance(f.dataType, T.TimestampType)
                else F.col(f.name)
            )
            for f in changed.schema
        )
    )
    result = []
    for r in changed.toLocalIterator():
        row = {
            k: (
                timestamp(datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=v))
                if k in timestamp_fields and v is not None
                else v
            )
            for k, v in r.asDict().items()
        }
        previous_hash, previous_id = row.pop("_previous_hash"), row.pop("_previous_id")
        entity_type = row["record_type"]
        payload = {
            "observation_id": row["observation_id"],
            "payload_hash": row["payload_hash"],
            "previous_payload_hash": previous_hash,
        }
        row.update(
            observation_id=digest(
                [
                    "bronze_source_changes",
                    row["source_platform"],
                    row["source_record_id"],
                    row["collection_run_id"],
                    timestamp(row["collected_at"]),
                ]
            ),
            record_type="Change",
            raw_payload=encode(payload),
            payload_hash=digest(payload),
            change_type="changed" if previous_hash else "new",
            previous_payload_hash=previous_hash,
            current_payload_hash=payload["payload_hash"],
            previous_observation_id=previous_id,
            entity_record_type=entity_type,
            evidence_json=encode(
                {
                    "method": (
                        "observed_payload_comparison"
                        if previous_hash
                        else "first_observed_in_lake"
                    ),
                    **payload,
                }
            ),
        )
        fields = spark_schema("bronze_source_changes").names
        result.append({k: row[k] for k in fields})
    return result


def persist_collection_runs(frame, path):
    """One row per source/run; retain chronological status updates in Delta history."""
    keys = ("source_platform", "collection_run_id")
    window = Window.partitionBy(*keys).orderBy(
        F.col("ended_at").desc_nulls_last(), F.col("payload_hash").desc()
    )
    unique = (
        frame.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    if not DeltaTable.isDeltaTable(frame.sparkSession, path):
        merge_delta(unique, path, keys=keys, initialize_empty=True)
        return
    existing = read_delta(frame.sparkSession, path)
    if [(f.name, f.dataType.simpleString()) for f in existing.schema] != [
        (f.name, f.dataType.simpleString()) for f in unique.schema
    ]:
        raise ValueError("Collection run schema mismatch; migration required")
    if unique.isEmpty():
        return
    (
        DeltaTable.forPath(frame.sparkSession, path)
        .alias("target")
        .merge(
            unique.alias("incoming"),
            "target.source_platform = incoming.source_platform AND target.collection_run_id = incoming.collection_run_id",
        )
        .whenMatchedUpdateAll(
            condition="incoming.ended_at > target.ended_at OR target.ended_at IS NULL"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def persist_observations(spark, datasets, settings, *, policies=None):
    """One writer per lake; retry all tables after any cross-table failure.

    Write change evidence before observations, so an interrupted batch does not
    lose comparison evidence on retry. Delta commits preserve existing history.
    Schema JSON is authoritative; JSON-valued fields use encoded STRING columns.
    """
    policies = (
        policies
        if policies is not None
        else load_policies(os.getenv("DSP_SOURCE_POLICY_REGISTRY"))
    )
    frames = {}
    prepared = {}
    for dataset in DATASETS:
        records = [
            {**r, "ingested_at": r.get("ingested_at") or timestamp(datetime.now(UTC))}
            for r in datasets.get(dataset, [])
        ]
        for row in records:
            validate_observation(dataset, row)
        records = [apply_source_policy(row, policies) for row in records]
        prepared[dataset] = records
        frames[dataset] = observation_frame(spark, dataset, records)
    explicit_changes = prepared["bronze_source_changes"]
    changes = []
    for name in DATASETS[:3]:
        if datasets.get(name):
            changes.extend(
                _source_changes(spark, frames[name], getattr(settings.paths, name))
            )
    changes = list(
        {row["observation_id"]: row for row in changes + explicit_changes}.values()
    )
    for row in changes:
        validate_observation("bronze_source_changes", row)
    merge_delta(
        observation_frame(spark, "bronze_source_changes", changes),
        settings.paths.bronze_source_changes,
        keys=("observation_id",),
        initialize_empty=True,
    )
    for dataset in DATASETS[:-1]:
        if dataset == "bronze_collection_runs":
            persist_collection_runs(frames[dataset], getattr(settings.paths, dataset))
            continue
        merge_delta(
            frames[dataset],
            getattr(settings.paths, dataset),
            keys=("observation_id",),
            initialize_empty=True,
        )
    return {
        name: (
            len(datasets.get(name, []))
            if name != "bronze_source_changes"
            else len(changes)
        )
        for name in DATASETS
    }
