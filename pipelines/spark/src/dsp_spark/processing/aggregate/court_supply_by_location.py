"""Latest logical venue supply and address-resolution quality, without I/O."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ..transform.location import location_expression


def latest_courts(silver: DataFrame) -> DataFrame:
    # Collection completion is the source observation. Enrichment/normalization
    # only break same-collection ties; ingestion cannot promote an older replay.
    window = Window.partitionBy("record_id").orderBy(
        F.to_timestamp("collected_at").desc_nulls_last(),
        F.to_timestamp("geocoded_at").desc_nulls_last(),
        F.to_timestamp("normalized_at").desc_nulls_last(),
        F.col("lakehouse_ingested_at").desc_nulls_last(),
        F.col("run_id").desc(),
    )
    return (
        silver.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def resolved_courts(silver: DataFrame) -> DataFrame:
    latest = latest_courts(silver).withColumn(
        "_location",
        location_expression(F.col("address"), address_source=F.col("address_source")),
    )
    return latest.select("*", "_location.*").drop("_location")


def court_supply_by_location(resolved: DataFrame, *, generated_at: str) -> DataFrame:
    return (
        resolved.filter(F.col("location_id").isNotNull())
        .groupBy("location_id", "location_name")
        .agg(
            F.count("record_id").alias("venue_count"),
            F.sum("number_of_courts").alias("known_court_count"),
            F.count("number_of_courts").alias("venues_with_known_court_count"),
            F.sum(F.when(F.col("number_of_courts").isNull(), 1).otherwise(0)).alias(
                "venues_without_known_court_count"
            ),
            F.date_format(
                F.max(F.to_timestamp("collected_at")), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"
            ).alias("latest_source_observed_at"),
        )
        .withColumn("schema_version", F.lit("1"))
        .withColumn("gold_generated_at", F.lit(generated_at))
    )
