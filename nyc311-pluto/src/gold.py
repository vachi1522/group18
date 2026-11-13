"""Run the DAT535 Gold stage; run with spark-submit gold.py

    read Silver -> cache once -> build Gold tables -> write Parquet -> unpersist -> summary

    gold.py   Spark session, Level-1 cache (load_gold_base), run + summary
    gold1.py           Q1, Q2      -> run_demand_gold()
    gold2.py           Q3, Q4, Q5  -> run_property_gold()
                                      (own Level-2 cache: property_profile)
"""

import findspark
findspark.init()

import argparse
import sys

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gold1 import run_demand_gold        
from gold2 import run_property_gold      


DEFAULT_OUTPUT_DIR = "/home/ubuntu/spark-project-data/dev"
DEFAULT_SILVER_PATH = f"{DEFAULT_OUTPUT_DIR}/silver/nyc311_enriched"
DEFAULT_GOLD_OUTPUT = f"{DEFAULT_OUTPUT_DIR}/gold"
GOLD_BASE_COLUMNS = [
    # request
    "request_id", "created_at", "closed_at", "request_year", "request_month",
    "status", "resolution_hours", "lifecycle_quality_status",
    # what / who
    "complaint_type", "agency",
    # where
    "borough", "community_board", "incident_zip",
    # property
    "bbl", "pluto_match_status", "pluto_land_use", "pluto_building_class",
    "pluto_residential_units", "pluto_yearbuilt",
]


def create_spark_session(app_name="DAT535-NYC311-PLUTO-Gold", cores_max="12"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://192.168.18.168:7077")
        .config("spark.driver.host", "192.168.18.168")
        .config("spark.cores.max", cores_max)
        .config("spark.executor.cores", "4")
        .config("spark.executor.memory", "6g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    return spark


def load_gold_base(spark, silver_path=DEFAULT_SILVER_PATH, sample_pct=None):
    """LEVEL-1 CACHE: read Silver once, select Gold columns, persist.

    sample_pct: e.g. 1 = deterministic 1% of requests (same rows every run),
                useful in notebooks. None = full data (use in gold.py).
    Returns (base_df, row_count). The count() fills the cache and doubles as
    the "Silver is not empty" check.
    """
    base = (
        spark.read.parquet(silver_path)
        .select(*GOLD_BASE_COLUMNS)
        .withColumn("request_date", F.to_date("created_at"))
    )

    if sample_pct is not None:
        base = base.filter(F.abs(F.hash("request_id")) % 100 < sample_pct)

    base = base.persist(StorageLevel.MEMORY_AND_DISK)
    row_count = base.count()
    if row_count == 0:
        raise ValueError(f"Silver is empty: {silver_path}")
    print(f"[Level-1 cache] Gold base cached: {row_count} rows", flush=True)
    return base, row_count


def run_gold(spark, input_path, output_dir):
    """Run the Gold stage: one cached base, then both members' tables."""
    print("=" * 50, flush=True)
    print("GOLD LAYER: NYC 311 + PLUTO", flush=True)
    print("=" * 50, flush=True)

    base, silver_count = load_gold_base(spark, input_path)   # Level-1 cache
    try:
        tables = []
        tables += run_demand_gold(spark, base, output_dir)     
        tables += run_property_gold(spark, base, output_dir)   
    finally:
        base.unpersist()

    generate_summary(spark, silver_count, output_dir, tables)
    return 0


def generate_summary(spark, silver_count, output_dir, tables):
    """Print row counts of every Gold table (read back from Parquet; cheap)."""
    print("\n" + "=" * 50, flush=True)
    print("GOLD SUMMARY", flush=True)
    print("=" * 50, flush=True)
    print(f"{'silver_requests':<35} {silver_count}", flush=True)
    for name in tables:
        rows = spark.read.parquet(f"{output_dir}/{name}").count()
        if rows == 0:
            raise ValueError(f"Gold table is empty: {name}")
        print(f"{name:<35} {rows}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="NYC 311 + PLUTO Gold Layer")
    parser.add_argument("--input", default=DEFAULT_SILVER_PATH, help="Silver input path")
    parser.add_argument("--output", default=DEFAULT_GOLD_OUTPUT, help="Gold output directory")
    args, _ = parser.parse_known_args()

    spark = None
    try:
        spark = create_spark_session()
        return run_gold(spark, args.input, args.output.rstrip("/"))
    except Exception as error:
        print(f"\nERROR: Gold stage failed: {error}", flush=True)
        return 1
    finally:
        if spark is not None:
            spark.stop()
            print("Spark session stopped.")


if __name__ == "__main__":
    sys.exit(main())
