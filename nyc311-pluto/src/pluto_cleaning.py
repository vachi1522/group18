# ============================================================
# pluto_silver.py
# ============================================================

# To use: run apply_pluto_silver(input_df) to obtain the cleaned DataFrame.
# This module contains transformations only. It does not create a Spark
# session, read or write files, or perform quarantine/deduplication.

# 1. Imports
from pyspark.sql.functions import (
    col,
    lit,
    when,
    trim,
    upper,
    regexp_replace,
)


# 2. Source Column Configuration
PLUTO_REQUIRED_COLUMNS = [
    "BBL",
    "borough",
    "landuse",
    "bldgclass",
    "unitsres",
    "unitstotal",
    "numbldgs",
    "lotarea",
    "bldgarea",
    "yearbuilt",
]


# 3. Original Value Preservation
def preserve_original_column(df, source_column: str, raw_column: str):
    """Preserve the original source value without overwriting an existing raw column."""

    if raw_column not in df.columns:
        df = df.withColumn(raw_column, col(source_column))

    return df


# 4. BBL Cleaning and Quality Status
def clean_bbl(df):
    """Preserve the source BBL and create a normalized BBL with a quality status."""

    df = preserve_original_column(df, "BBL", "bbl_raw")

    cleaned_bbl = trim(col("bbl_raw").cast("string"))

    return (
        df
        .withColumn(
            "bbl",
            when(
                cleaned_bbl.rlike(r"^[1-5][0-9]{9}$"),
                cleaned_bbl,
            ).otherwise(lit(None).cast("string")),
        )
        .withColumn(
            "bbl_status",
            when(
                col("bbl_raw").isNull() | (trim(col("bbl_raw")) == ""),
                lit("missing"),
            )
            .when(col("bbl").isNotNull(), lit("valid"))
            .otherwise(lit("invalid")),
        )
    )


# 5. Borough Cleaning
def clean_borough(df):
    """Preserve the original borough code and map it to its full borough name."""

    df = preserve_original_column(df, "borough", "borough_raw")
    borough_key = upper(trim(col("borough_raw").cast("string")))

    return df.withColumn(
        "pluto_borough",
        when(borough_key == "MN", lit("MANHATTAN"))
        .when(borough_key == "BX", lit("BRONX"))
        .when(borough_key == "BK", lit("BROOKLYN"))
        .when(borough_key == "QN", lit("QUEENS"))
        .when(borough_key == "SI", lit("STATEN ISLAND"))
        .otherwise(lit(None).cast("string")),
    )


# 6. Land Use Cleaning and Quality Status
def clean_land_use(df):
    """Preserve the original land-use code and retain the documented codes 1 to 11."""

    df = preserve_original_column(df, "landuse", "land_use_raw")
    cleaned_land_use = trim(col("land_use_raw").cast("string"))
    valid_land_use_codes = [str(value) for value in range(1, 12)]

    return (
        df
        .withColumn(
            "land_use",
            when(
                cleaned_land_use.isin(*valid_land_use_codes),
                cleaned_land_use,
            ).otherwise(lit(None).cast("string")),
        )
        .withColumn(
            "land_use_status",
            when(
                col("land_use_raw").isNull()
                | (trim(col("land_use_raw")) == ""),
                lit("missing"),
            )
            .when(col("land_use").isNotNull(), lit("valid"))
            .otherwise(lit("invalid")),
        )
    )


# 7. Building Class Cleaning and Quality Status
def clean_building_class(df):
    """Preserve the original building class and normalize its text representation."""

    df = preserve_original_column(df, "bldgclass", "building_class_raw")
    source_value = trim(col("building_class_raw").cast("string"))

    return (
        df
        .withColumn(
            "building_class",
            when(
                source_value.isNull() | (source_value == ""),
                lit(None).cast("string"),
            ).otherwise(upper(source_value)),
        )
        .withColumn(
            "building_class_status",
            when(
                col("building_class").isNull(),
                lit("missing"),
            ).otherwise(lit("present")),
        )
    )


# 8. Residential and Total Unit Conversion
def clean_unit_counts(df):
    """Convert residential and total unit counts to long integers."""

    return (
        df
        .withColumn(
            "residential_units",
            regexp_replace(trim(col("unitsres")), ",", "").cast("long"),
        )
        .withColumn(
            "total_units",
            regexp_replace(trim(col("unitstotal")), ",", "").cast("long"),
        )
    )


# 9. Number of Buildings Conversion
def clean_number_of_buildings(df):
    """Convert the source number-of-buildings field to a long integer."""

    return df.withColumn(
        "number_of_buildings",
        regexp_replace(trim(col("numbldgs")), ",", "").cast("long"),
    )


# 10. Lot Area Cleaning and Quality Status
def clean_lot_area(df):
    """Preserve the original lot area and convert valid integer values to long."""

    df = preserve_original_column(df, "lotarea", "lotarea_raw")
    cleaned_value = regexp_replace(trim(col("lotarea_raw")), ",", "")

    return (
        df
        .withColumn("_lotarea_cleaned", cleaned_value)
        .withColumn(
            "lotarea",
            when(
                col("_lotarea_cleaned").rlike(r"^[0-9]+$"),
                col("_lotarea_cleaned").cast("long"),
            ).otherwise(lit(None).cast("long")),
        )
        .withColumn(
            "lotarea_status",
            when(
                col("lotarea_raw").isNull()
                | (trim(col("lotarea_raw")) == ""),
                lit("missing"),
            )
            .when(col("lotarea").isNotNull(), lit("valid"))
            .otherwise(lit("invalid")),
        )
        .drop("_lotarea_cleaned")
    )


# 11. Building Area Cleaning and Quality Status
def clean_building_area(df):
    """Preserve the original building area and convert valid integer values to long."""

    df = preserve_original_column(df, "bldgarea", "bldgarea_raw")
    cleaned_value = regexp_replace(trim(col("bldgarea_raw")), ",", "")

    return (
        df
        .withColumn("_bldgarea_cleaned", cleaned_value)
        .withColumn(
            "bldgarea",
            when(
                col("_bldgarea_cleaned").rlike(r"^[0-9]+$"),
                col("_bldgarea_cleaned").cast("long"),
            ).otherwise(lit(None).cast("long")),
        )
        .withColumn(
            "bldgarea_status",
            when(
                col("bldgarea_raw").isNull()
                | (trim(col("bldgarea_raw")) == ""),
                lit("missing"),
            )
            .when(col("bldgarea").isNotNull(), lit("valid"))
            .otherwise(lit("invalid")),
        )
        .drop("_bldgarea_cleaned")
    )


# 12. Year Built Cleaning and Quality Status
def clean_year_built(df):
    """Preserve the original year built and classify missing, valid, and invalid values."""

    df = preserve_original_column(df, "yearbuilt", "yearbuilt_raw")
    cleaned_value = trim(col("yearbuilt_raw").cast("string"))

    return (
        df
        .withColumn("_yearbuilt_cleaned", cleaned_value)
        .withColumn(
            "yearbuilt",
            when(
                col("_yearbuilt_cleaned").rlike(r"^[0-9]{4}$")
                & (col("_yearbuilt_cleaned") != "0000"),
                col("_yearbuilt_cleaned").cast("long"),
            ).otherwise(lit(None).cast("long")),
        )
        .withColumn(
            "yearbuilt_status",
            when(
                col("_yearbuilt_cleaned").isNull()
                | (col("_yearbuilt_cleaned") == "")
                | col("_yearbuilt_cleaned").rlike(r"^0+$"),
                lit("missing"),
            )
            .when(col("yearbuilt").isNotNull(), lit("valid"))
            .otherwise(lit("invalid")),
        )
        .drop("_yearbuilt_cleaned")
    )


# 13. Main Transformation Function
def clean_pluto(input_df):
    """
    Apply all PLUTO Silver cleaning transformations in sequence.

    Keeps all original input columns, preserves selected raw values,
    and adds cleaned columns and quality-status columns.

    This function does not quarantine records or deduplicate BBL values.
    """

    missing_columns = [
        name for name in PLUTO_REQUIRED_COLUMNS
        if name not in input_df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"PLUTO input is missing required columns: {missing_columns}"
        )

    df = clean_bbl(input_df)
    df = clean_borough(df)
    df = clean_land_use(df)
    df = clean_building_class(df)
    df = clean_unit_counts(df)
    df = clean_number_of_buildings(df)
    df = clean_lot_area(df)
    df = clean_building_area(df)
    df = clean_year_built(df)

    return df
