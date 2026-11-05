
NYC311_CSV = "/home/ubuntu/spark-project-data/bronze/nyc311/nyc311_2020_present.csv"
PLUTO_CSV = "/home/ubuntu/spark-project-data/bronze/pluto/pluto.csv"


def read_csv(spark, path):
    """Read raw CSV fields as strings, as in the DAT535 notebooks."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .option("escape", '"')
        .csv(path)
    )


def load_nyc311(spark, path=NYC311_CSV):
    return read_csv(spark, path)


def load_pluto(spark, path=PLUTO_CSV):
    return read_csv(spark, path)
