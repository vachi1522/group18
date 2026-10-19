
# Source Data Profiling — NYC 311 and PLUTO

## 1. Purpose

This document records the structure, important columns, data-quality findings, and planned cleaning rules for the two datasets used in our DAT535 project.

Datasets:
1. NYC 311 Service Requests - main dataset
2. NYC PLUTO - property reference dataset

Architecture: Bronze → Silver → Gold

Silver grain: One row per unique usable NYC 311 service request.

## 2. NYC 311 Dataset — Member A

To be completed by Member A after NYC 311 exploration.

Required information:
- Source and raw file details
- Exact column names and types
- Request identifier quality
- Created Date and Closed Date formats
- Open and closed request handling
- Missing and duplicate values
- BBL availability and format

## 3. NYC PLUTO Dataset

### 3.1 Dataset Information

Dataset: Primary Land Use Tax Lot Output (PLUTO)

Official source:
https://data.cityofnewyork.us/City-Government/Primary-Land-Use-Tax-Lot-Output-PLUTO-/64uk-42ks

Raw format: CSV

Raw file on Spark VM:
/home/ubuntu/spark-project-data/bronze/pluto/pluto.csv

Raw file size: 463,577,106 bytes (approximately 442 MiB)

Total records: 858,284

Snapshot/download date: 12 October 2026

Purpose: Enrich NYC 311 complaints with tax-lot and property characteristics.

### 3.2 Important Columns

| Column | Description | Planned use |
|---|---|---|
| BBL | Borough-Block-Lot identifier | Join key |
| borough | Borough code | Location |
| Tax block | Tax block number | Reference |
| Tax lot | Tax lot number | Reference |
| address | Property address | Interpretation |
| postcode | Postal code | Location |
| landuse | Land-use category | Property comparisons |
| bldgclass | Building class | Property comparisons |
| unitsres | Residential unit count | Complaint-rate normalization |
| unitstotal | Total unit count | Reference |
| numbldgs | Number of buildings | Property characteristics |
| bldgarea | Total building floor area | Property-size comparisons |
| lotarea | Land area | Reference |
| yearbuilt | Construction year | Building age |

### 3.3 BBL Validation

- Expected format: 10-digit BBL.
- All 858,284 records have a 10-digit BBL matching the tested pattern.
- NULL or invalid BBL formats found: 0.
- Duplicate BBL values found: 0.
- Observed PLUTO grain: one row per BBL.

Conclusion:
BBL is a suitable candidate key for the PLUTO lookup.

A valid format does not independently prove that a tax lot exists.

### 3.4 Residential Units - unitsres

Findings:
- NULL values: 649
- Empty strings: 0
- Comma-formatted numeric values: 92
- Other invalid numeric formats: 0
- Zero residential units: 90,072

Examples:
- "1,614" → 1614
- "10,914" → 10914

Proposed Silver handling:
- Remove thousands separators.
- Convert values to an integer type.
- Preserve NULL values.
- Preserve zero values.
- Use positive residential-unit counts when calculating complaints per residential unit.

### 3.5 Construction Year - yearbuilt

Findings:
- yearbuilt = 0: 39,850 records
- NULL values: 467
- Empty strings: 0
- Invalid year formats: 0
- Earliest reported nonzero year: 1652
- Latest reported year: 2026

Proposed Silver handling:
- Preserve original year.
- Convert valid year values to integers.
- Treat zero and NULL as unavailable for age calculations.
- Flag unusually old years for review rather than automatically deleting records.
- Calculate building age only when appropriate and clearly define the reference date.

### 3.6 Building Area - bldgarea

Findings:
- NULL values: 506
- Empty strings: 0
- Invalid numeric formats: 0
- Zero building area: 40,966 records
- Building area above 1 million sq. ft.: 268 records
- Maximum reported building area: 24,000,000 sq. ft.

Examples:
- "1,864" → 1864
- "24,000,000" → 24000000

Investigation:
- Many zero-area records belong to vacant-land, parking, open-space, or transportation-related categories.
- 381 residential-category records have zero building area.
- Of these 381, 371 have building class Z0.
- Some records report positive residential-unit counts but zero building area.

Proposed Silver handling:
- Remove thousands separators.
- Convert building area to a numeric type.
- Preserve zero and NULL values.
- Flag potentially inconsistent property attributes.
- Do not divide by zero when calculating area-normalized complaint rates.
- Do not automatically delete extremely large areas.

### 3.7 Land Use and Building Class

The dataset contains multiple land-use and building-class categories.

Important observations:
- Land-use classifications allow comparisons between property types.
- Building classes provide more detailed property categories.
- Zero building area can be legitimate for some property types.
- Tax lots may contain multiple buildings.

### 3.8 PLUTO Data Quality Summary

| Check | Finding |
|---|---|
| BBL format | All records pass the tested format rule |
| Duplicate BBLs | None found |
| Residential-unit NULLs | 649 |
| Construction-year NULLs | 467 |
| Construction year equal to zero | 39,850 |
| Building-area NULLs | 506 |
| Building area equal to zero | 40,966 |
| Invalid numeric formats in checked columns | None found |

### 3.9 Important Limitations

- PLUTO describes tax lots, not necessarily individual buildings.
- One tax lot may contain several buildings.
- PLUTO may reflect a newer property snapshot than historical NYC 311 complaints.
- Missing values and zero values must be interpreted according to the column.
- Complaint counts alone do not measure the number of affected residents.
- Observed relationships do not establish causation.

## 4. Shared Silver Decisions - Both Members

To be finalized together after both source profiles are reviewed.

Topics to agree:
- Exact 311-to-PLUTO BBL mapping
- Required and optional column mappings
- Timestamp and date formats
- Treatment of missing values
- Accepted, flagged, and quarantined records
- Reproducible development sample
- Duplicate service-request handling
- LEFT JOIN behavior
- Prevention of row multiplication
- Preservation of legitimate open requests
