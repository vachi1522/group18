"""NYC 311 request-field transformations for the DAT535 Silver layer.

This module contains only reusable transformations. Validation/quarantine
and persistence are coordinated by the pipeline and quality modules.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_nyc311_fields(df: DataFrame) -> DataFrame:
    """Standardize NYC 311 request fields without filtering source rows."""
    # ------------------------------------------------------------
    # 1. Define valid reference values
    # ------------------------------------------------------------

    valid_boroughs = [
        "BRONX", "BROOKLYN", "MANHATTAN",
        "QUEENS", "STATEN ISLAND"
    ]

    # Special Community Board codes retained from exploration
    special_areas = [
        "64 MANHATTAN",
        "55 BROOKLYN", "56 BROOKLYN",
        "26 BRONX", "27 BRONX", "28 BRONX",
        "80 QUEENS", "81 QUEENS", "82 QUEENS",
        "83 QUEENS", "84 QUEENS",
        "95 STATEN ISLAND"
    ]

    # Maximum numbered community districts by borough
    max_board_numbers = {
        "BRONX": 12,
        "BROOKLYN": 18,
        "MANHATTAN": 12,
        "QUEENS": 14,
        "STATEN ISLAND": 3
    }

    # ------------------------------------------------------------
    # 2. Reusable column expressions
    # ------------------------------------------------------------

    bbl_text = F.trim(F.col("BBL"))

    board_text = F.upper(
        F.trim(F.col("Community Board"))
    )

    zip_text = F.trim(F.col("Incident Zip"))

    detail_text = F.trim(
        F.col("Problem Detail (formerly Descriptor)")
    )

    # ------------------------------------------------------------
    # 3. Rebuild the cleaned NYC 311 DataFrame
    # ------------------------------------------------------------

    nyc311_fields_df = (
        df

        # 3.1 Request identifier
        .withColumn(
            "request_id",
            F.trim(F.col("Unique Key"))
        )
        .withColumn(
            "request_id_status",
            F.when(
                F.col("request_id").isNull() |
                (F.col("request_id") == ""),
                "missing"
            ).when(
                F.col("request_id").rlike(r"^[0-9]+$"),
                "valid"
            ).otherwise("invalid")
        )

        # 3.2 BBL
        .withColumn("bbl_raw", F.col("BBL"))
        .withColumn(
            "bbl",
            F.when(
                bbl_text.rlike(r"^[1-5][0-9]{9}$"),
                bbl_text
            ).otherwise(F.lit(None).cast("string"))
        )
        .withColumn(
            "bbl_status",
            F.when(
                F.col("bbl_raw").isNull() |
                (bbl_text == ""),
                "missing"
            ).when(
                F.col("bbl").isNotNull(),
                "valid"
            ).otherwise("invalid")
        )

        # 3.3 Borough
        .withColumn("borough_raw", F.col("Borough"))
        .withColumn(
            "borough",
            F.when(
                F.col("Borough").isNull() |
                (F.trim(F.col("Borough")) == ""),
                F.lit(None).cast("string")
            ).otherwise(
                F.upper(F.trim(F.col("Borough")))
            )
        )
        .withColumn(
            "borough_status",
            F.when(
                F.col("borough").isNull(), "missing"
            ).when(
                F.col("borough") == "UNSPECIFIED",
                "unspecified"
            ).when(
                F.col("borough").isin(valid_boroughs),
                "valid"
            ).otherwise("unexpected")
        )

        # 3.4 Community Board
        .withColumn(
            "community_board_raw",
            F.col("Community Board")
        )
        .withColumn(
            "community_board",
            F.when(
                board_text.rlike(
                    r"^[0-9]{2} (BRONX|BROOKLYN|MANHATTAN|QUEENS|STATEN ISLAND)$"
                ),
                board_text
            ).otherwise(F.lit(None).cast("string"))
        )
        .withColumn(
            "_board_number",
            F.substring("community_board", 1, 2).cast("int")
        )
        .withColumn(
            "_board_borough",
            F.regexp_extract(
                F.col("community_board"),
                r"^[0-9]{2} (.+)$",
                1
            )
        )
    )

    # Build maximum Community Board number expression
    max_board_expr = F.lit(0)

    for borough_name, maximum in max_board_numbers.items():
        max_board_expr = F.when(
            F.col("_board_borough") == borough_name,
            F.lit(maximum)
        ).otherwise(max_board_expr)

    nyc311_fields_df = (
        nyc311_fields_df

        .withColumn(
            "_max_board_number",
            max_board_expr
        )
        .withColumn(
            "community_board_status",
            F.when(
                F.col("community_board_raw").isNull() |
                (board_text == ""),
                "missing"
            ).when(
                board_text.contains("UNSPECIFIED"),
                "unspecified"
            ).when(
                F.col("community_board").isin(special_areas),
                "special_area"
            ).when(
                F.col("community_board").isNotNull() &
                (F.col("_board_number") >= 1) &
                (F.col("_board_number") <= F.col("_max_board_number")),
                "valid"
            ).when(
                F.col("community_board").isNotNull(),
                "invalid_number"
            ).otherwise("unexpected")
        )

        # Remove helper columns
        .drop(
            "_board_number",
            "_board_borough",
            "_max_board_number"
        )

        # 3.5 Incident ZIP
        .withColumn(
            "incident_zip_raw",
            F.col("Incident Zip")
        )
        .withColumn(
            "incident_zip",
            F.when(
                zip_text.rlike(r"^[0-9]{5}$"),
                zip_text
            ).otherwise(F.lit(None).cast("string"))
        )
        .withColumn(
            "incident_zip_status",
            F.when(
                F.col("Incident Zip").isNull() |
                (zip_text == ""),
                "missing"
            ).when(
                F.col("incident_zip").isNotNull(),
                "valid"
            ).otherwise("invalid_format")
        )

        # 3.6 Complaint type
        .withColumn(
            "complaint_type_raw",
            F.col("Problem (formerly Complaint Type)")
        )
        .withColumn(
            "complaint_type",
            F.when(
                F.col("complaint_type_raw").isNull() |
                (F.trim(F.col("complaint_type_raw")) == ""),
                F.lit(None).cast("string")
            ).otherwise(
                F.trim(F.col("complaint_type_raw"))
            )
        )

        # 3.7 Complaint detail
        .withColumn(
            "complaint_detail_raw",
            F.col("Problem Detail (formerly Descriptor)")
        )
        .withColumn(
            "complaint_detail",
            F.when(
                F.col("complaint_detail_raw").isNull() |
                (detail_text == "") |
                (F.upper(detail_text) == "N/A"),
                F.lit(None).cast("string")
            ).otherwise(detail_text)
        )

        # 3.8 Agency
        .withColumn(
            "agency_raw",
            F.col("Agency")
        )
        .withColumn(
            "agency",
            F.when(
                F.col("agency_raw").isNull() |
                (F.trim(F.col("agency_raw")) == ""),
                F.lit(None).cast("string")
            ).otherwise(
                F.upper(F.trim(F.col("agency_raw")))
            )
        )

        # 3.9 BBL join eligibility
        .withColumn(
            "bbl_join_eligible",
            F.col("bbl_status") == "valid"
        )

        # 3.10 Overall invalid field flag
        .withColumn(
            "has_invalid_field",
            (F.col("request_id_status") == "invalid") |
            (F.col("bbl_status") == "invalid") |
            (F.col("borough_status") == "unexpected") |
            (F.col("community_board_status").isin(
                "invalid_number", "unexpected"
            )) |
            (F.col("incident_zip_status") == "invalid_format")
        )
    )

    return nyc311_fields_df
