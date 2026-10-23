
# Source Data Profiling — NYC 311 and PLUTO

## 1. Purpose

This document records the structure, important columns, data-quality findings, and planned cleaning rules for the two datasets used in our DAT535 project.

Datasets:
1. NYC 311 Service Requests - main dataset
2. NYC PLUTO - property reference dataset

Architecture: Bronze → Silver → Gold

Silver grain: One row per unique usable NYC 311 service request.

## 2. NYC 311 Dataset — Member A

### 2.1 Dataset Information

Dataset: 311 Service Requests from 2020 to Present (NYC 311)

Official source:
https\://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9

Raw format: CSV

Raw file on Spark VM:
/home/ubuntu/spark-project-data/bronze/nyc311/nyc311_2020_present.csv

Raw file size: 16,037,001,359 bytes (approximately 14.94 GiB)

Total records: 22,761,196

Snapshot/download date: 12 October 2026

Purpose: Analyze NYC 311 service demand, resolution performance, and complaint patterns.

### 3.2 Important Columns

| Column | Description | Planned use |
|---|---|---|
| Unique Key | Unique service request identifier | Record identification |
| Created Date | Request creation date and time | Demand trends |
| Closed Date | Request closure date and time | Resolution time |
| Status | Current request status | Unresolved requests |
| Problem (formerly Complaint Type) | Main complaint category | Demand and problem diversity |
| Problem Detail (formerly Descriptor) | Detailed complaint description | Recurring problems |
| Agency | Code of the responsible city agency | Agency comparison |
| Agency Name | Full name of the responsible agency | Reporting |
| BBL | Borough-Block-Lot identifier | Join key |
| Borough | NYC borough associated with the request | Geographic demand |
| Community Board | Community district associated with the request | Local hotspots |
| Incident Zip | ZIP code associated with the incident | Geographic grouping |
| Incident Address | Street address associated with the incident | Property identification |
| Address Type | Type of address reference | Location quality |
| Location Type | Type of incident location | Location comparison |
| Resolution Action Updated Date | Date of the latest resolution-action update | Request progression |
| Resolution Description | Description of the resolution action | Resolution analysis |
| Due Date | Recorded due date | Deadline analysis |

### 3.3 Unique Key

Findings:
- Missing or blank IDs: 0.
- Invalid non-numeric ID formats: 0.
- Duplicate `Unique Key` values: None found.

### 3.4 Created Date and Closed Date

Observed format: `MM/dd/yyyy hh:mm:ss a`.

| Check | Created Date | Closed Date |
|---|---:|---:|
| Missing or blank values | 0 | 439,146 |
| Unparseable date values | 0 | 0 |
| Earliest parsed date | 2020-01-01 00:00:00 | 1899-12-31 19:00:00 |
| Latest parsed date | 2026-10-10 03:03:53 | 2033-03-01 00:00:00 |

Additional findings:
- 47,207 records have a `Closed Date` earlier than their `Created Date`.
- Some requests have identical creation and closing timestamps.
- Several records have the closing date `12/31/1899 07:00:00 PM`, despite being created in 2020 or later. This may be a default or placeholder value and requires investigation.
- Other records contain closing dates in the future, including December 2026 and March 2033, relative to the October 2026 snapshot.
- Some requests marked `In Progress`, `Open`, `Pending`, or `Assigned` have a `Closed Date` despite their current status.

Proposed Silver handling:
- Parse dates using the observed timestamp format.
- Distinguish missing dates from unparseable values and chronologically inconsistent dates.
- Retain legitimate open requests with a missing `Closed Date`.
- Flag implausible dates, future closing dates, and reversed timestamps for validation.
- Do not calculate resolution duration from invalid or inconsistent timestamps.

### 3.5 Current Status Distribution

| Status | Records |
|---|---:|
| Closed | 22,294,838 |
| In Progress | 286,243 |
| Open | 84,638 |
| Pending | 63,306 |
| Assigned | 24,590 |
| Started | 4,739 |
| Unspecified | 2,841 |
| Cancel | 1 |
| **Total** | **22,761,196** |

Observations:
- Approximately 97.95% of requests are marked `Closed`.
- `Unspecified` is a recorded status value, not a missing value.
- Some non-closed statuses have populated closing dates, indicating potential inconsistencies between status and dates.
- Only one record has the status `Cancel`.

Proposed Silver handling:
- Preserve the original status and create a standardized status field.
- Retain legitimate unresolved requests.
- Flag unexpected status/date combinations for validation rather than automatically changing the status.

### 3.6 BBL Validation and Availability

Expected format: a 10-digit BBL beginning with a borough code from 1 to 5.

Validation pattern: `^[1-5][0-9]{9}$`

Findings:

| Check | Records |
|---|---:|
| Valid-format BBLs | 20,090,144 |
| Missing BBLs | 2,668,612 |
| Invalid-format BBLs | 2,440 |
| Distinct non-missing BBL values | 770,925 |

The 2,440 invalid BBL records consist of:
- `0000000000`: 2,438 records.
- `24`: 1 record.
- `14`: 1 record.

Proposed Silver handling:
- Preserve the original BBL value.
- Normalize BBLs only when the value meets the agreed validation rules.
- Distinguish missing, invalid, and valid-but-unmatched BBLs.
- Retain 311 requests without a usable BBL.
- Use BBL as the property-level join key to PLUTO.
- Do not deduplicate 311 records by BBL, because multiple requests can legitimately refer to the same tax lot.

### 3.7 Complaint Categories

Most frequent complaint types:

| Complaint Type | Records |
|---|---:|
| Illegal Parking | 2,964,367 |
| Noise - Residential | 2,579,719 |
| HEAT/HOT WATER | 1,658,600 |
| Noise - Street/Sidewalk | 1,177,444 |
| Blocked Driveway | 1,086,462 |
| UNSANITARY CONDITION | 716,077 |
| Request Large Bulky Item Collection | 632,148 |
| Street Condition | 519,781 |
| Water System | 422,631 |
| Noise - Vehicle | 417,036 |

Findings:
- Distinct non-blank complaint types: 276.
- Missing complaint types: 0.
- Inconsistent naming convention (lowercase, uppercase)
- Duplicate categories (Abandoned Vehicle and Derelict Vehicles)
- Overlaping categories (General Construction/Plumbing and PLUMBING)

Proposed Silver handling:
- Preserve the original complaint type.
- Standardize whitespace and capitalization only where appropriate.
- Retain meaningful distinctions between complaint categories.
- Use complaint types to analyze demand concentration, recurring problems, and service-needs diversity.

### Responsible Agencies

The `Agency` column has no missing values.

Data quality observations:
- Agency codes should be interpreted with caution because some may reflect historical naming changes. For example, `DCA` was renamed `DCWP` in 2020, and `DOITT` was consolidated into `OTI` in 2022.
- `NYC311-PRD` (371 records) and `3-1-1` (1 record) are unusual values that require investigation before standardization.

Proposed Silver handling:
- Preserve the original agency code.
- Document and validate any historical code mappings before merging categories.
- Rare codes should be investigated before being flag as invalid.

### Borough and Geographic Quality

Borough distribution:

| Borough value | Records |
|---|---:|
| BROOKLYN | 6,833,840 |
| QUEENS | 5,476,485 |
| BRONX | 4,834,551 |
| MANHATTAN | 4,580,930 |
| STATEN ISLAND | 955,909 |
| Unspecified | 41,046 |
| Missing | 38,433 |
| Precinct 112 | 1 |
| Precinct 52 | 1 |

Data quality observations:
- `Precinct 112` and `Precinct 52` are unexpected values in the `Borough` column because they refer to police precincts rather than borough names.
- `Unspecified` and missing borough values should be treated separately.

Proposed Silver handling:
- Standardize borough names and whitespace.
- Flag unexpected borough values and inspect other location fields before attempting corrections.
- Preserve records with missing geographic information.

### Address Type

| Address Type | Records |
|---|---:|
| ADDRESS | 18,101,481 |
| Missing | 2,801,263 |
| INTERSECTION | 1,480,500 |
| BLOCKFACE | 255,647 |
| UNRECOGNIZED | 72,989 |
| PLACE | 36,274 |
| PLACENAME | 13,040 |
| BBL | 2 |

Observations:
- Two records have `Address Type = BBL` while `Incident Address`, `Street Name`, and `Incident Zip` are missing, although the BBL field is populated.
- These two records should be reviewed before deciding whether the address type should be recoded or retained as a BBL-based location reference.

### Location Type

The `Location Type` column contains 2,974,990 missing values.

Common values include:
- `Street/Sidewalk`: 6,521,378 records.
- `RESIDENTIAL BUILDING`: 4,486,297 records.
- `Residential Building/House`: 2,771,441 records.
- `Street`: 2,082,960 records.
- `Sidewalk`: 1,654,407 records.

Data quality issues:
- Inconsistent capitalization, such as `RESIDENTIAL BUILDING` and `Residential Building/House`.
- Potentially overlapping categories, such as `Street/Sidewalk`, `Street`, and `Sidewalk`.
- Different levels of granularity, with broad and highly specific location categories.
- Compound categories such as `Store/Commercial` and `Club/Bar/Restaurant`.
- Ambiguous or generic labels, including `Above Address`, `Other (Explain Below)`, and `N/A`.

Proposed Silver handling:
- Preserve original values for traceability.
- Standardize whitespace and capitalization where appropriate.
- Define mappings only for confirmed equivalent categories.
- Missing values and placeholders should not automatically be considered equivalent without defining a rule.

### NYC 311 Data Quality Summary

| Check | Finding |
|---|---|
| Source records | 22,761,196 |
| Expected columns | 44 |
| Missing `Unique Key` | 0 |
| Invalid `Unique Key` format | 0 |
| Duplicate `Unique Key` values | None found |
| Missing `Created Date` | 0 |
| Unparseable creation dates | 0 |
| Missing `Closed Date` | 439,146 |
| Unparseable closing dates | 0 |
| Closing date earlier than creation date | 47,207 |
| Valid-format BBLs | 20,090,144 |
| Missing BBLs | 2,668,612 |
| Invalid BBLs | 2,440 |
| Missing borough values | 38,433 |
| Unexpected borough values | 2 |

### Important Limitations

- NYC 311 requests represent reported service needs, not all problems experienced by residents.
- A BBL identifies a tax lot, not necessarily a single building.
- Multiple requests associated with the same BBL are legitimate and central to the repeated-problem and property-burden analyses.
- A missing `Closed Date` does not automatically indicate an error.
- Parsed dates may still be semantically invalid, as shown by the 1899 closing dates, future dates, and records with closing dates before creation.
- PLUTO may represent a newer snapshot than some historical 311 requests.
- Requests without a valid or matching BBL must not be dropped during the PLUTO join.
- Observed relationships between property characteristics and complaints indicate associations, not causation.

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
