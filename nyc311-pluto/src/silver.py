
# ============================================================
# DAT535 - NYC 311 + PLUTO
# SILVER LAYER
# ============================================================

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ------------------------------------------------------------
# 1. NYC 311 FIELD CLEANING
# ------------------------------------------------------------

def clean_nyc311_fields(df: DataFrame) -> DataFrame:

    # We will add the cleaning logic from
    # nyc311_fields.ipynb here.

    return df


# ------------------------------------------------------------
# 2. NYC 311 LIFECYCLE CLEANING
# ------------------------------------------------------------

def clean_nyc311_lifecycle(df: DataFrame) -> DataFrame:

    # We will add lifecycle transformations
    # from nyc311_lifecycle.ipynb here.

    return df


# ------------------------------------------------------------
# 3. PLUTO CLEANING
# ------------------------------------------------------------

def clean_pluto(df: DataFrame) -> DataFrame:

    # We will add transformations from
    # pluto_silver.ipynb here.

    return df


# ------------------------------------------------------------
# 4. JOIN NYC 311 WITH PLUTO
# ------------------------------------------------------------

def join_nyc_pluto(
    nyc_df: DataFrame,
    pluto_df: DataFrame
) -> DataFrame:

    # We will add the validated BBL left join here.

    return nyc_df
