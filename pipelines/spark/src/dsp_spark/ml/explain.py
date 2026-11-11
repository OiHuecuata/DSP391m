"""Descriptive c-TF-IDF and post-hoc label lift; neither is ground truth."""

import re

from pyspark.sql import Window
from pyspark.sql import functions as F


def safe_tokens(text):
    text = re.sub(r"https?://\S+|\S+@\S+|@\w+|(?:\+?\d[\s().-]*){7,}", " ", text or "")
    return [w.lower() for w in re.findall(r"[^\W\d_]{2,}", text, re.UNICODE)]


def explain(predictions, posts, *, top_n=15, representatives=False):
    from .data import unique_keys

    unique_keys(posts, ("content_id",))
    # The only label/text join is after prediction, at a verified 1:1 grain.
    joined = predictions.select("content_id", "cluster_id").join(
        posts.select("content_id", "text", "intent_labels", "topic_labels"),
        "content_id",
    )
    n = joined.count()
    size = (
        joined.groupBy("cluster_id").count().withColumnRenamed("count", "cluster_posts")
    )
    tokens = joined.select(
        "cluster_id",
        F.explode(F.udf(safe_tokens, "array<string>")("text")).alias("term"),
    )
    counts = tokens.groupBy("cluster_id", "term").count()
    totals = counts.groupBy("cluster_id").agg(F.sum("count").alias("cluster_words"))
    frequency = counts.groupBy("term").agg(F.sum("count").alias("term_frequency"))
    a = totals.agg(F.avg("cluster_words")).first()[0]
    weights = (
        counts.join(totals, "cluster_id")
        .join(frequency, "term")
        .withColumn(
            "c_tf_idf",
            F.col("count")
            / F.col("cluster_words")
            * F.log1p(F.lit(a or 0.0) / F.col("term_frequency")),
        )
    )
    rank = Window.partitionBy("cluster_id").orderBy(F.desc("c_tf_idf"), "term")
    keywords = [
        r.asDict()
        for r in weights.withColumn("rank", F.row_number().over(rank))
        .filter(F.col("rank") <= top_n)
        .orderBy("cluster_id", "rank")
        .select("cluster_id", "term", "c_tf_idf")
        .collect()
    ]
    lifts = {}
    for field in ("intent_labels", "topic_labels"):
        labels = joined.select(
            "content_id",
            "cluster_id",
            F.explode(F.array_distinct(field)).alias("label"),
        )
        global_counts = (
            labels.groupBy("label").count().withColumnRenamed("count", "global_count")
        )
        enrichment = (
            labels.groupBy("cluster_id", "label")
            .count()
            .join(global_counts, "label")
            .join(size, "cluster_id")
            .withColumn(
                "lift",
                (F.col("count") / F.col("cluster_posts"))
                / (F.col("global_count") / F.lit(max(n, 1))),
            )
        )
        ranked = enrichment.withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("cluster_id").orderBy(F.desc("lift"), "label")
            ),
        ).filter(F.col("rank") <= top_n)
        lifts[field] = [
            r.asDict()
            for r in ranked.orderBy("cluster_id", "rank").drop("rank").collect()
        ]
    examples = []
    if representatives:
        ranked = predictions.withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("cluster_id").orderBy(
                    "distance_to_centroid", "content_id"
                )
            ),
        ).filter("rank <= 3")
        examples = [
            {
                "content_id": r.content_id,
                "cluster_id": r.cluster_id,
                "distance_to_centroid": r.distance_to_centroid,
                "text": "[WITHHELD: full text suppressed to avoid exposing PII]",
            }
            for r in ranked.select(
                "content_id", "cluster_id", "distance_to_centroid"
            ).collect()
        ]
    return {
        "method": "c-TF-IDF: cluster term proportions * log(1 + mean cluster word count / corpus term frequency)",
        "interpretation": "descriptive lexical associations; not SHAP or supervised importance; labels are post-hoc and not independent truth",
        "keywords": keywords,
        "posthoc_lift": lifts,
        "representative_posts": examples,
        "clusters": [
            {
                "cluster_id": r.cluster_id,
                "posts": r.cluster_posts,
                "share": r.cluster_posts / max(n, 1),
                "name": None,
                "interpretation": "UNREVIEWED",
                "interpretation_version": 1,
            }
            for r in size.orderBy("cluster_id").collect()
        ],
    }
