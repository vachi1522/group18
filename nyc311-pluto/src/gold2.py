"""Member B Gold tables: Q3 repeated problems, Q4 peer comparison, Q5 property-type variety.

Developed in notebooks/gold2.ipynb. Called by gold_combined.py:
    run_property_gold(spark, base, output_dir)

Caching: uses the Level-1 cached `base` from gold_combined.py, plus its own
Level-2 cache `property_profile` (one row per BBL, reused by Q3 summary, Q4, Q5).
"""

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.window import Window


REFERENCE_YEAR = 2026
REPEAT_MIN_REQUESTS = 3
REPEAT_MIN_MONTHS = 2
PEER_MIN_PROPERTIES = 10
HIGH_ACTIVITY_RATIO = 3.0
VARIETY_MIN_PROPERTIES = 30
PROPERTY_TABLES = [
    "property_profile",
    "property_repeat_problems",
    "property_repeat_summary",
    "property_peer_comparison",
    "property_type_variety",
]
PEER_COLS = ["peer_borough", "peer_land_use", "unit_band"]


def write_table(df, path, partition_by=None):
    """Write one Gold table as Parquet (overwrite)."""
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)
    print(f"  wrote {path}", flush=True)


def unit_band(units_col):
    """Size band from residential units (NULL/0 kept as their own band)."""
    return (
        F.when(units_col.isNull(), "unknown")
        .when(units_col == 0, "0 (non-res)")
        .when(units_col <= 2, "1-2")
        .when(units_col <= 10, "3-10")
        .when(units_col <= 50, "11-50")
        .otherwise("51+")
    )


def get_matched_requests(base):
    """Requests whose valid BBL matched a PLUTO tax lot.

    No extra cache: this is a cheap filter on the already-cached base.
    """
    return base.filter(F.col("pluto_match_status") == "matched")


def build_property_profile(matched):
    """One row per matched property with request totals and PLUTO attributes."""
    return (
        matched.groupBy("bbl").agg(
            F.count("*").alias("total_requests"),
            F.countDistinct("complaint_type").alias("unique_complaint_types"),
            F.countDistinct("agency").alias("unique_agencies"),
            F.countDistinct("request_year", "request_month").alias("active_months"),
            F.min("created_at").alias("first_request"),
            F.max("created_at").alias("last_request"),
            F.round(F.avg("resolution_hours"), 2).alias("avg_resolution_hours"),
            F.first("borough", ignorenulls=True).alias("borough"),
            F.first("pluto_land_use", ignorenulls=True).alias("land_use"),
            F.first("pluto_building_class", ignorenulls=True).alias("building_class"),
            F.first("pluto_residential_units", ignorenulls=True).alias("residential_units"),
            F.first("pluto_yearbuilt", ignorenulls=True).alias("year_built"),
        )
        .withColumn("unit_band", unit_band(F.col("residential_units")))
        .withColumn(
            "requests_per_res_unit",   # safe division
            F.when(F.col("residential_units") > 0,
                   F.round(F.col("total_requests") / F.col("residential_units"), 2)),
        )
        .withColumn(
            "building_age",            # year 0 / NULL = unknown
            F.when(F.col("year_built") > 0, F.lit(REFERENCE_YEAR) - F.col("year_built")),
        )
    )


def build_repeat_problems(matched):
    """One row per (property, complaint type) that keeps coming back."""
    return (
        matched.filter(F.col("complaint_type").isNotNull())
        .groupBy("bbl", "complaint_type").agg(
            F.count("*").alias("request_count"),
            F.countDistinct("request_year", "request_month").alias("active_months"),
            F.min("created_at").alias("first_request"),
            F.max("created_at").alias("last_request"),
        )
        .withColumn("span_days", F.datediff("last_request", "first_request"))
        .filter(
            (F.col("request_count") >= REPEAT_MIN_REQUESTS)
            & (F.col("active_months") >= REPEAT_MIN_MONTHS)
        )
    )


def build_repeat_summary(repeat_problems, profile):
    """One row per property with repeated problems, plus its top repeated type."""
    # Lab 3 window function: rank complaint types inside each property
    w = Window.partitionBy("bbl").orderBy(F.desc("request_count"), "complaint_type")
    top_type = (
        repeat_problems.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("bbl",
                F.col("complaint_type").alias("top_repeated_type"),
                F.col("request_count").alias("top_repeated_count"))
    )
    summary = repeat_problems.groupBy("bbl").agg(
        F.count("*").alias("repeated_problem_types"),
        F.sum("request_count").alias("repeated_requests"),
        F.max("span_days").alias("longest_span_days"),
    )
    return (
        summary.join(top_type, "bbl")
        .join(profile.select("bbl", "total_requests", "borough", "land_use",
                             "building_class", "residential_units"), "bbl", "left")
        .withColumn("repeat_share",
                    F.round(F.col("repeated_requests") / F.col("total_requests") * 100, 2))
        .orderBy(F.desc("repeated_requests"))
    )


def build_peer_comparison(profile):
    """Compare each property with its peer group (borough + land use + size band)."""
    with_peer = (
        profile
        .withColumn("peer_borough", F.coalesce("borough", F.lit("UNKNOWN")))
        .withColumn("peer_land_use", F.coalesce("land_use", F.lit("UNKNOWN")))
    )

    # Peer statistics (small table: one row per peer group)
    peer_stats = with_peer.groupBy(*PEER_COLS).agg(
        F.count("*").alias("peer_properties"),
        F.percentile_approx("total_requests", 0.5).alias("peer_median_requests"),
        F.percentile_approx("total_requests", 0.9).alias("peer_p90_requests"),
        F.round(F.avg("total_requests"), 2).alias("peer_avg_requests"),
    )

    # Lab 3: broadcast the small lookup table in the join
    # Lab 3: window function percent_rank inside the peer group
    w = Window.partitionBy(*PEER_COLS).orderBy("total_requests")
    return (
        with_peer.join(F.broadcast(peer_stats), PEER_COLS, "left")
        .withColumn("peer_percentile", F.round(F.percent_rank().over(w) * 100, 2))
        .withColumn(
            "activity_ratio",          # safe division
            F.when(F.col("peer_median_requests") > 0,
                   F.round(F.col("total_requests") / F.col("peer_median_requests"), 2)),
        )
        .withColumn(
            "is_unusually_high",
            (F.col("peer_properties") >= PEER_MIN_PROPERTIES)
            & (F.col("total_requests") > F.col("peer_p90_requests"))
            & (F.col("activity_ratio") >= HIGH_ACTIVITY_RATIO),
        )
        .select("bbl", *PEER_COLS, "building_class", "residential_units",
                "total_requests", "peer_properties", "peer_median_requests",
                "peer_p90_requests", "peer_avg_requests", "peer_percentile",
                "activity_ratio", "is_unusually_high")
    )


def build_property_type_variety(matched, profile):
    """One row per property type (land use + building class)."""
    # Average variety per property (from the cached profile)
    per_property = (
        profile
        .groupBy("land_use", "building_class").agg(
            F.count("*").alias("properties"),
            F.sum("total_requests").alias("total_requests"),
            F.round(F.avg("unique_complaint_types"), 2).alias("avg_types_per_property"),
            F.round(F.avg("unique_agencies"), 2).alias("avg_agencies_per_property"),
        )
    )
    # Total variety across the whole property type (needs request-level rows)
    whole_type = (
        matched.groupBy(
            F.col("pluto_land_use").alias("land_use"),
            F.col("pluto_building_class").alias("building_class"),
        ).agg(
            F.countDistinct("complaint_type").alias("distinct_complaint_types"),
            F.countDistinct("agency").alias("distinct_agencies"),
        )
    )
    return (
        per_property.join(whole_type, ["land_use", "building_class"], "left")
        .withColumn("enough_properties", F.col("properties") >= VARIETY_MIN_PROPERTIES)
        .orderBy(F.desc("enough_properties"), F.desc("avg_types_per_property"))
    )


def run_property_gold(spark, base, output_dir):
    """Build, write, and clean up all Member B Gold tables."""
    print("\n--- Member B: property tables (Q3, Q4, Q5) ---", flush=True)
    matched = get_matched_requests(base)

    # LEVEL-2 CACHE: profile is an expensive groupBy reused by Q3 summary, Q4, Q5
    profile = build_property_profile(matched).persist(StorageLevel.MEMORY_AND_DISK)
    print(f"[Level-2 cache] property_profile: {profile.count()} properties", flush=True)

    try:
        write_table(profile, f"{output_dir}/property_profile")

        repeat_problems = build_repeat_problems(matched)
        write_table(repeat_problems, f"{output_dir}/property_repeat_problems")

        # Read Q3 back from Parquet instead of recomputing it
        repeat_problems = spark.read.parquet(f"{output_dir}/property_repeat_problems")
        write_table(build_repeat_summary(repeat_problems, profile),
                    f"{output_dir}/property_repeat_summary")

        write_table(build_peer_comparison(profile),
                    f"{output_dir}/property_peer_comparison")

        write_table(build_property_type_variety(matched, profile),
                    f"{output_dir}/property_type_variety")
    finally:
        profile.unpersist()

    return PROPERTY_TABLES
