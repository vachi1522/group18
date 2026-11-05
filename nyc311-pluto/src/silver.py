"""Run the DAT535 Silver stage; run with spark-submit silver.py"""

import argparse
import os

from pyspark.sql import SparkSession

from bronze import NYC311_CSV, PLUTO_CSV, load_nyc311, load_pluto
from nyc311_cleaning1 import clean_nyc311_fields
from nyc311_cleaning2 import clean_nyc311_lifecycle
from pluto_cleaning import clean_pluto
from quality import (
    validate_nyc311_fields,
    validate_nyc311_lifecycle,
    validate_pluto,
    join_nyc_pluto,
    validate_joined_silver,
)

DEFAULT_OUTPUT = "/home/ubuntu/spark-project-data/dev"


def create_spark_session():
    """Create a Spark session with Parquet date/timestamp compatibility."""
    
    spark = SparkSession.builder.appName("DAT535-NYC311-PLUTO-Silver").getOrCreate()
    
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


def run_silver(spark, nyc_path, pluto_path, output_dir):
    """Run the Silver stage: read Bronze CSVs, clean, validate, join, and write outputs."""
    
    # 1. Read unchanged Bronze CSV files
    nyc_raw = load_nyc311(spark, nyc_path)
    pluto_raw = load_pluto(spark, pluto_path)

    # 2. Clean NYC fields, then quarantine unusable IDs
    nyc = clean_nyc311_fields(nyc_raw)
    nyc, fields_quarantine = validate_nyc311_fields(nyc)

    # 3. Add lifecycle fields, then quarantine unusable Created Dates
    nyc = clean_nyc311_lifecycle(nyc)
    nyc, lifecycle_quarantine = validate_nyc311_lifecycle(nyc)

    # 4. Clean PLUTO, quarantine unusable/duplicate BBLs
    pluto = clean_pluto(pluto_raw)
    pluto, pluto_quarantine = validate_pluto(pluto)

    # 5. Join, validate, then save all outputs
    accepted_nyc_count = nyc.count()
    silver_df = join_nyc_pluto(nyc, pluto)
    validate_joined_silver(silver_df, accepted_nyc_count)

    # Writes are intentionally after the validation gate.
    # IMPORTANT: overwrite replaces the destination directories.
    silver_path = os.path.join(output_dir, "silver", "nyc311_enriched")
    silver_df.write.mode("overwrite").parquet(silver_path)
    fields_quarantine.write.mode("overwrite").parquet(
        os.path.join(output_dir, "quarantine", "nyc311_fields")
    )
    lifecycle_quarantine.write.mode("overwrite").parquet(
        os.path.join(output_dir, "quarantine", "nyc311_lifecycle")
    )
    pluto_quarantine.write.mode("overwrite").parquet(
        os.path.join(output_dir, "quarantine", "pluto")
    )
    print("Silver saved:", silver_path)


def main():
    """Run the Silver stage with command-line arguments for input/output paths."""
    
    parser = argparse.ArgumentParser(description="NYC 311 + PLUTO Silver")
    parser.add_argument("--nyc", default=NYC311_CSV)
    parser.add_argument("--pluto", default=PLUTO_CSV)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    spark = create_spark_session()
    
    try:
        run_silver(spark, args.nyc, args.pluto, args.output.rstrip("/"))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
