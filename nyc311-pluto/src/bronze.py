
import os

BRONZE_DIR = os.path.join("/home/ubuntu/spark-project-data", "bronze")
NYC311_CSV = os.path.join(BRONZE_DIR, "nyc311", "nyc311_2020_present.csv")
PLUTO_CSV = os.path.join(BRONZE_DIR, "pluto", "pluto.csv")


def read_csv(spark, path):
    """Read raw CSV fields as strings, as in the DAT535 notebooks."""
    
    if not path:
        raise ValueError(f"A Bronze path is missing.")
    
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .option("escape", '"')
        .csv(path)
    )


def load_nyc311(spark, path=NYC311_CSV):
    return read_csv(spark, path)


def load_pluto(spark, path=PLUTO_CSV):
    return read_csv(spark, path)
