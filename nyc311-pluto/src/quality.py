"""Quarantine, BBL enrichment and final checks for NYC311_Pluto Silver."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def validate_nyc311_fields(df):
    """Quarantine invalid IDs and extra duplicate-ID rows.

    Other field problems (BBL/ZIP/Community Board) stay flagged in Silver.
    """
    checked = df.withColumn(
        "quality_reason",
        F.when(F.col("request_id_status") == "missing", "missing_request_id")
        .when(F.col("request_id_status") == "invalid", "invalid_request_id")
        .otherwise(F.lit(None).cast("string")),
    )
    invalid = checked.filter(F.col("request_id_status") != "valid")
    candidates = checked.filter(F.col("request_id_status") == "valid")

    # Stable ordering: same input always selects the same duplicate candidate.
    # A hash does not prove which conflicting source record is authoritative.
    source_columns = sorted(candidates.columns)
    ranked = (
        candidates.withColumn(
            "_source_hash",
            F.sha2(F.to_json(F.struct(*[F.col(c) for c in source_columns])), 256),
        )
        .withColumn(
            "_row_number",
            F.row_number().over(
                Window.partitionBy("request_id").orderBy("_source_hash")
            ),
        )
    )
    valid = ranked.filter(F.col("_row_number") == 1).drop(
        "_source_hash", "_row_number", "quality_reason"
    )
    extra = (
        ranked.filter(F.col("_row_number") > 1)
        .drop("_source_hash", "_row_number")
        .withColumn("quality_reason", F.lit("duplicate_request_id"))
    )
    quarantine = invalid.unionByName(extra)
    return valid, quarantine


def validate_nyc311_lifecycle(df):
    """Quarantine unusable Created Dates; preserve open requests."""
    bad = (
        F.coalesce(F.col("created_at_missing"), F.lit(False))
        | F.coalesce(F.col("created_at_parse_failed"), F.lit(False))
        | F.coalesce(F.col("created_at_before_1900"), F.lit(False))
    )
    valid = df.filter(~bad)
    quarantine = df.filter(bad).withColumn(
        "quality_reason", F.col("lifecycle_quality_status")
    )
    return valid, quarantine


def validate_pluto(df):
    """Quarantine missing/invalid BBLs and *all* ambiguous duplicate BBL rows."""
    valid_bbl = df.filter(F.col("bbl_status") == "valid")
    invalid_bbl = (
        df.filter(F.col("bbl_status") != "valid")
        .withColumn("quality_reason", F.col("bbl_status"))
    )

    duplicate_keys = (
        valid_bbl.groupBy("bbl")
        .count()
        .filter(F.col("count") > 1)
        .select("bbl")
    )
    duplicate_rows = (
        valid_bbl.join(duplicate_keys, "bbl", "inner")
        .withColumn("quality_reason", F.lit("duplicate_bbl"))
    )
    valid = valid_bbl.join(duplicate_keys, "bbl", "left_anti")
    quarantine = invalid_bbl.unionByName(duplicate_rows)
    return valid, quarantine


def join_nyc_pluto(nyc_df, pluto_df):
    """Left join requests with validated, unique PLUTO BBLs."""
    # Select only needed PLUTO fields, avoiding ambiguous NYC/PLUTO names.
    pluto_selected = pluto_df.select(
        "bbl",
        F.lit(True).alias("_matched_pluto"),
        F.col("pluto_borough"),
        F.col("land_use"),
        F.col("building_class"),
        F.col("residential_units"),
        F.col("total_units"),
        F.col("number_of_buildings"),
        F.col("lotarea").alias("pluto_lotarea"),
        F.col("bldgarea").alias("pluto_bldgarea"),
        F.col("yearbuilt").alias("pluto_yearbuilt"),
    )
    return (
        nyc_df.join(pluto_selected, on="bbl", how="left")
        .withColumn(
            "pluto_match_status",
            F.when(F.col("_matched_pluto").isNotNull(), "matched")
            .otherwise("unmatched"),
        )
        .drop("_matched_pluto")
    )


def validate_joined_silver(silver_df, expected_count):
    """Fail if join loses or multiplies NYC requests; report match coverage."""
    count = silver_df.count()
    if count != expected_count:
        raise ValueError(
            f"Join changed NYC row count: before={expected_count}, after={count}"
        )

    duplicate_requests = (
        silver_df.groupBy("request_id")
        .count().filter(F.col("count") > 1).limit(1).count()
    )
    if duplicate_requests:
        raise ValueError("Final Silver contains duplicate request_id values")

    print(f"Silver validation passed: {count} unique NYC requests")
    silver_df.groupBy("pluto_match_status").count().show()
