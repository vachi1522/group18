"""NYC 311 request-lifecycle transformations for the DAT535 Silver layer."""


# 1. Imports
from pyspark.sql.functions import (
    col,
    when,
    lit,
    coalesce,
    month,
    year,
    round as spark_round,
    trim,
    upper,
    regexp_replace,
    to_timestamp,
    current_timestamp
)


# 2. Source and Target Column Configuration
LIFECYCLE_SOURCE_TO_TARGET = {
    "Created Date": "created_at_raw",
    "Closed Date": "closed_at_raw",
    "Status": "status_raw",
}


# 3. Lifecycle Input Column Preparation
def add_lifecycle_input_columns(df):
    """
    Add Lifecycle input columns without removing original columns.
    Existing columns from Fields and other sources are preserved.
    """

    missing_columns = [
        source
        for source, target in LIFECYCLE_SOURCE_TO_TARGET.items()
        if source not in df.columns and target not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Lifecycle source columns not found: {missing_columns}"
        )

    for source, target in LIFECYCLE_SOURCE_TO_TARGET.items():

        # Do not overwrite an existing Lifecycle input column
        if target not in df.columns:

            source_value = trim(
                col(f"`{source}`").cast("string")
            )

            df = df.withColumn(
                target,
                when(
                    source_value.isNull() | (source_value == ""),
                    lit(None).cast("string")
                ).otherwise(source_value)
            )

    return df


# 4. Timestamp Parsing
def parse_nyc311_timestamp(column_name: str):
    """Parse common NYC 311 timestamp formats into a Spark timestamp."""
    value = trim(col(column_name))

    # Main format used by NYC 311
    primary = to_timestamp(value, "MM/dd/yyyy hh:mm:ss a")

    # Backup formats in case the format changes in the future
    fallback = coalesce(
        to_timestamp(value, "M/d/yyyy h:mm:ss a"),
        to_timestamp(value, "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(value, "yyyy-MM-dd'T'HH:mm:ss"),
    )

    return coalesce(primary, fallback)


# 5. Status Normalization
def normalize_lifecycle_status(df):
    """Normalize status values while preserving status_raw."""
    status_key = upper(
        trim(regexp_replace(col("status_raw"), r"\s+", " "))
    )

    return df.withColumn(
        "status",
        when(
            status_key.isNull() | (status_key == ""),
            lit(None).cast("string"),
        )
        .when(status_key == "CLOSED", lit("Closed"))
        .when(status_key == "OPEN", lit("Open"))
        .when(status_key == "IN PROGRESS", lit("In Progress"))
        .when(status_key == "PENDING", lit("Pending"))
        .when(status_key == "ASSIGNED", lit("Assigned"))
        .when(status_key == "UNSPECIFIED", lit("Unspecified"))
        .when(status_key == "CANCEL", lit("Cancel"))
        .when(status_key == "EMAIL SENT", lit("Email Sent"))
        .when(status_key == "STARTED", lit("Started"))
        .when(status_key == "COMPLETED", lit("Completed"))
        .otherwise(
            trim(regexp_replace(col("status_raw"), r"\s+", " "))
        ),
    )


# 6. Lifecycle Date Parsing and Validation
def add_lifecycle_dates(df):
    """Parse timestamps and add date-quality flags."""

    created_parsed = parse_nyc311_timestamp("created_at_raw")
    closed_parsed = parse_nyc311_timestamp("closed_at_raw")

    raw_created_blank = (
        col("created_at_raw").isNull()
        | (trim(col("created_at_raw")) == "")
    )
    raw_closed_blank = (
        col("closed_at_raw").isNull()
        | (trim(col("closed_at_raw")) == "")
    )

    before_1900 = lit("1900-01-01 00:00:00").cast("timestamp")

    df = (
        df
        .withColumn("_created_at_parsed", created_parsed)
        .withColumn("_closed_at_parsed", closed_parsed)

        .withColumn("created_at_missing", raw_created_blank)
        .withColumn("closed_at_missing", raw_closed_blank)

        .withColumn(
            "created_at_parse_failed",
            (~raw_created_blank) & col("_created_at_parsed").isNull(),
        )
        .withColumn(
            "closed_at_parse_failed",
            (~raw_closed_blank) & col("_closed_at_parsed").isNull(),
        )

        .withColumn(
            "created_at_before_1900",
            coalesce(
                col("_created_at_parsed").isNotNull()
                & (col("_created_at_parsed") < before_1900),
                lit(False),
            ),
        )
        .withColumn(
            "closed_at_before_1900",
            coalesce(
                col("_closed_at_parsed").isNotNull()
                & (col("_closed_at_parsed") < before_1900),
                lit(False),
            ),
        )

        .withColumn(
            "created_at_is_future",
            coalesce(
                col("_created_at_parsed") > current_timestamp(),
                lit(False),
            ),
        )
        .withColumn(
            "closed_at_is_future",
            coalesce(
                col("_closed_at_parsed") > current_timestamp(),
                lit(False),
            ),
        )

        .withColumn(
            "closed_before_created",
            coalesce(
                col("_closed_at_parsed").isNotNull()
                & col("_created_at_parsed").isNotNull()
                & (
                    col("_closed_at_parsed")
                    < col("_created_at_parsed")
                )
                & (~col("closed_at_before_1900")),
                lit(False),
            ),
        )

        .withColumn(
            "created_at",
            when(
                col("created_at_before_1900"),
                lit(None).cast("timestamp"),
            ).otherwise(col("_created_at_parsed")),
        )
        .withColumn(
            "closed_at",
            when(
                col("closed_at_before_1900"),
                lit(None).cast("timestamp"),
            ).otherwise(col("_closed_at_parsed")),
        )
    )

    return df


# 7. Status and Date Consistency Checks
def check_status_date_consistency(df):
    """Flag inconsistencies between status and closing date."""

    df = (
        df
        .withColumn(
            "closed_status_missing_closed_date",
            coalesce(
                (col("status") == "Closed")
                & col("closed_at_missing"),
                lit(False),
            ),
        )
        .withColumn(
            "non_closed_status_has_closed_date",
            coalesce(
                col("status").isNotNull()
                & (col("status") != "Closed")
                & col("_closed_at_parsed").isNotNull()
                & (~col("created_at_before_1900"))
                & (~col("closed_at_before_1900")),
                lit(False),
            ),
        )
        .withColumn(
            "status_date_consistency",
            when(
                col("status").isNull(),
                lit("missing_status"),
            )
            .when(
                col("closed_at_before_1900"),
                lit("closed_at_before_1900"),
            )
            .when(
                col("closed_at_parse_failed"),
                lit("invalid_closed_date"),
            )
            .when(
                col("closed_at_is_future"),
                lit("future_closed_at"),
            )
            .when(
                col("closed_status_missing_closed_date"),
                lit("closed_status_missing_closed_date"),
            )
            .when(
                col("non_closed_status_has_closed_date"),
                lit("non_closed_status_has_closed_date"),
            )
            .otherwise(lit("consistent")),
        )
    )

    return df


# 8. Request Date Components Extraction
def add_request_date_parts(df):
    """Derive year and month from the cleaned creation timestamp."""

    return (
        df
        .withColumn(
            "request_year",
            year(col("created_at")).cast("int"),
        )
        .withColumn(
            "request_month",
            month(col("created_at")).cast("int"),
        )
    )


# 9. Resolution Time Calculation
def calculate_resolution_hours(df):
    """Calculate duration only for valid, closed requests."""

    valid_resolution = (
        (col("status") == "Closed")
        & col("created_at").isNotNull()
        & col("closed_at").isNotNull()
        & (~col("created_at_is_future"))
        & (~col("closed_at_is_future"))
        & (~col("closed_before_created"))
        & (col("closed_at") > col("created_at"))
    )

    return df.withColumn(
        "resolution_hours",
        when(
            valid_resolution,
            spark_round(
                (
                    col("closed_at").cast("long")
                    - col("created_at").cast("long")
                ) / lit(3600.0),
                2,
            ).cast("double"),
        ).otherwise(lit(None).cast("double")),
    )


# 10. Lifecycle Quality Status Assignment
def add_lifecycle_quality_status(df):
    """Assign one primary quality label to each request."""

    return df.withColumn(
        "lifecycle_quality_status",
        when(
            col("created_at_missing"),
            lit("missing_created_at"),
        )
        .when(
            col("created_at_parse_failed"),
            lit("invalid_created_at"),
        )
        .when(
            col("created_at_before_1900"),
            lit("created_at_before_1900"),
        )
        .when(
            col("created_at_is_future"),
            lit("future_created_at"),
        )
        .when(
            col("closed_at_before_1900"),
            lit("closed_at_before_1900"),
        )
        .when(
            col("closed_at_parse_failed"),
            lit("invalid_closed_at"),
        )
        .when(
            col("closed_at_is_future"),
            lit("future_closed_at"),
        )
        .when(
            col("closed_before_created"),
            lit("closed_before_created"),
        )
        .when(
            col("status").isNull(),
            lit("missing_status"),
        )
        .when(
            col("closed_status_missing_closed_date"),
            lit("closed_status_missing_closed_date"),
        )
        .when(
            col("non_closed_status_has_closed_date"),
            lit("non_closed_status_has_closed_date"),
        )
        .otherwise(lit("ok")),
    )


# 11. Main Transformation Function
def clean_nyc311_lifecycle(input_df):
    """
    Apply all Lifecycle transformations in sequence.

    Keeps all original input columns and adds lifecycle columns.
    """
    
    # Add Lifecycle source columns if they do not already exist
    df = add_lifecycle_input_columns(input_df)
    
    required_columns = [
        "created_at_raw",
        "closed_at_raw",
        "status_raw",
    ]

    missing_columns = [
        name
        for name in required_columns
        if name not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Lifecycle input is missing required columns: {missing_columns}"
        )

    df = normalize_lifecycle_status(df)
    df = add_lifecycle_dates(df)
    df = check_status_date_consistency(df)
    df = add_request_date_parts(df)
    df = calculate_resolution_hours(df)
    df = add_lifecycle_quality_status(df)

    # Remove only temporary columns created by Lifecycle
    df = df.drop(
        "_created_at_parsed",
        "_closed_at_parsed"
    )

    return df