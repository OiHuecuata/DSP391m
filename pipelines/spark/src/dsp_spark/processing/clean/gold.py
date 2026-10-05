"""Contract-derived projection and validation for Gold rebuilds."""

from functools import partial

from pyspark.sql import functions as F
from pyspark.sql import types as T

from .contracts import contract_documents, spark_schema, validation_reason


def validated_gold(frame, kind):
    schema = spark_schema(kind)
    projected = frame.select(
        *(F.col(f.name).cast(f.dataType).alias(f.name) for f in schema)
    )
    reason = F.udf(
        partial(validation_reason, kind=kind, documents=contract_documents()),
        T.StringType(),
    )
    invalid = (
        projected.withColumn(
            "_contract_error",
            reason(
                F.to_json(
                    F.struct(*projected.columns), options={"ignoreNullFields": "false"}
                )
            ),
        )
        .filter(F.col("_contract_error").isNotNull())
        .select("_contract_error")
        .limit(1)
        .collect()
    )
    if invalid:
        raise ValueError("Gold contract violation: " + invalid[0]["_contract_error"])
    return projected
