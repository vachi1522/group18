"""Gold tables: Q1 demand concentration, Q2 resolution performance.

Developed in notebooks/gold1.ipynb. Called by gold.py:
    run_demand_gold(spark, base, output_dir)

Caching: only uses the Level-1 cached 'base' from gold_combined.py.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


MIN_CLOSED_FOR_STATS = 30
DEMAND_TABLES = [
    "demand_by_area_month",
    "demand_by_complaint_borough",
    "resolution_by_agency_complaint",
    "resolution_by_borough_month",
]


def write_table(df, path, partition_by=None):
    """Write one Gold table as Parquet (overwrite)."""
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)
    print(f"  wrote {path}", flush=True)


def build_demand_by_area_month(base):
    """Requests per borough + community board per month."""
    return (
        base.groupBy("borough", "community_board", "request_year", "request_month")
        .agg(
            F.count("*").alias("total_requests"),
            F.countDistinct("complaint_type").alias("unique_complaint_types"),
            F.countDistinct("bbl").alias("unique_properties"),
        )
    )


def build_demand_by_complaint_borough(base):
    """Requests per complaint type in each borough, with rank and share."""
    counts = (
        base.filter(F.col("complaint_type").isNotNull())
        .groupBy("borough", "complaint_type")
        .agg(F.count("*").alias("total_requests"))
    )
    # Window functions: rank and share inside each borough
    w_rank = Window.partitionBy("borough").orderBy(F.desc("total_requests"))
    w_total = Window.partitionBy("borough")
    return (
        counts
        .withColumn("rank_in_borough", F.row_number().over(w_rank))
        .withColumn("share_of_borough_pct",
                    F.round(F.col("total_requests") / F.sum("total_requests").over(w_total) * 100, 2))
    )


def resolution_stats():
    """Shared aggregation list: slow = high median/p90, inconsistent = high spread."""
    return [
        F.count("*").alias("total_requests"),
        F.count("resolution_hours").alias("closed_with_valid_time"),
        F.sum(F.when(F.col("status") != "Closed", 1).otherwise(0)).alias("num_unresolved"),
        F.round(F.percentile_approx("resolution_hours", 0.5), 2).alias("median_hours"),
        F.round(F.percentile_approx("resolution_hours", 0.9), 2).alias("p90_hours"),
        F.round(F.avg("resolution_hours"), 2).alias("avg_hours"),
        F.round(F.stddev("resolution_hours"), 2).alias("stddev_hours"),
    ]


def add_resolution_ratios(df):
    return (
        df
        .withColumn("pct_unresolved",
                    F.round(F.col("num_unresolved") / F.col("total_requests") * 100, 2))
        .withColumn(   # coefficient of variation = inconsistency (safe division)
            "cv_hours",
            F.when(F.col("avg_hours") > 0, F.round(F.col("stddev_hours") / F.col("avg_hours"), 2)))
        .withColumn("enough_data", F.col("closed_with_valid_time") >= MIN_CLOSED_FOR_STATS)
    )


def build_resolution_by_agency_complaint(base):
    return add_resolution_ratios(
        base.groupBy("agency", "complaint_type").agg(*resolution_stats())
    )


def build_resolution_by_borough_month(base):
    return add_resolution_ratios(
        base.groupBy("borough", "request_year", "request_month").agg(*resolution_stats())
    )


def run_demand_gold(spark, base, output_dir):
    """Build and write all demand & resolution Gold tables."""
    print("\n--- demand & resolution tables (Q1, Q2) ---", flush=True)

    write_table(build_demand_by_area_month(base),
                f"{output_dir}/demand_by_area_month", partition_by=["request_year"])
    write_table(build_demand_by_complaint_borough(base),
                f"{output_dir}/demand_by_complaint_borough")
    write_table(build_resolution_by_agency_complaint(base),
                f"{output_dir}/resolution_by_agency_complaint")
    write_table(build_resolution_by_borough_month(base),
                f"{output_dir}/resolution_by_borough_month", partition_by=["request_year"])

    return DEMAND_TABLES
