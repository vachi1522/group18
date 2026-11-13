"""Placeholder for the NYC 311 + PLUTO Gold layer."""

import findspark
findspark.init()

import argparse
import os
import sys

from pyspark.sql import SparkSession

DEFAULT_SILVER_PATH = "/home/ubuntu/spark-project-data/dev/silver/nyc311_enriched"
DEFAULT_GOLD_OUTPUT = "/home/ubuntu/spark-project-data/dev/gold"


def create_spark_session():
    """Create a Spark session with Parquet date/timestamp compatibility."""

    spark = (
        SparkSession.builder
        .appName("DAT535-NYC311-PLUTO-Gold")
        .master("spark://192.168.18.168:7077")
        .config("spark.driver.host", "192.168.18.168")
        .config("spark.cores.max", "12")
        .config("spark.executor.cores", "4")
        .config("spark.executor.memory", "6g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )
    
    # Only show errors in the console
    spark.sparkContext.setLogLevel("ERROR")

    # Parquet date/timestamp compatibility
    spark.conf.set(
        "spark.sql.parquet.int96RebaseModeInWrite",
        "CORRECTED"
    )

    spark.conf.set(
        "spark.sql.parquet.datetimeRebaseModeInWrite",
        "CORRECTED"
    )
    return spark


def run_gold(spark, input_path, output_dir):
    """Run the Gold stage: read Silver data, transform and save Gold outputs."""

    print("\nStarting Gold transformations...", flush=True)

    # TODO: Implement Gold transformations

    print("Gold transformations completed.", flush=True)

    return 0


def main():
    """Run the Gold stage with command-line arguments for input path, output dir."""

    parser = argparse.ArgumentParser(description="NYC 311 + PLUTO Gold Layer")
    parser.add_argument("--input", help="Path to the Silver input data", default=DEFAULT_SILVER_PATH)
    parser.add_argument("--output", help="Path to the Gold output directory", default=DEFAULT_GOLD_OUTPUT)

    args = parser.parse_args()
    
    try:
        spark = create_spark_session()
        result = run_gold(spark, args.input, args.output.rstrip("/"))
        return result

    except Exception as error:
        print(f"\nERROR: Gold stage failed: {error}", flush=True)
        return 1

    finally:
        if spark is not None:
            spark.stop()
            print("Spark session stopped.")


if __name__ == "__main__":
    sys.exit(main())