
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

**Checkpoint:** Shared Source Profiling / Silver Design (DAT535 two-member plan)  
**Status:** Proposed contract — requires review and approval by both members before Silver implementation.  
**Source evidence:** Sections 2 and 3 above; NYC 311 and PLUTO snapshots downloaded on **2026-10-12**.  
**Scope:** Batch processing; unchanged Bronze inputs; Silver has **one row per unique usable NYC 311 service request**, not one row per property.

### 4.1 Column mappings and target types

Column names below reflect the source-profile tables. The code must check the actual input headers before selecting or casting columns. Keep original input values available for validation and lineage; do not silently overwrite raw strings before checks.

**NYC 311 — request-level fields**

| Raw NYC 311 column | Silver name | Target type | Use / treatment |
|---|---|---|---|
| `Unique Key` | `request_id` | String | Required; trim and validate; unique identifier for each request. |
| `Created Date` | `created_at` | Timestamp | Required; parse from observed format; request trend/time grain. |
| `Closed Date` | `closed_at` | Timestamp (nullable) | Parse if present; NULL is allowed for unresolved requests. |
| `Status` | `status` | String | Trim and standardize known values; retain `status_raw` for audit. |
| `Problem (formerly Complaint Type)` | `complaint_type` | String | Main service category; trim carefully without collapsing distinct types. |
| `Problem Detail (formerly Descriptor)` | `complaint_detail` | String (nullable) | Repeated types and detailed complaint analysis. |
| `Agency` | `agency` | String | Preserve source codes; mappings require evidence. |
| `Agency Name` | `agency_name` | String (nullable) | Readable agency label. |
| `BBL` | `bbl` | String (nullable) | Validated 10-digit property join key; preserve `bbl_raw`. |
| `Borough` | `borough` | String (nullable) | Standardized borough name; preserve unknown/unexpected values for review. |
| `Community Board` | `community_board` | String (nullable) | Local demand / hotspot reporting. |
| `Incident Zip` | `incident_zip` | String (nullable) | Geographic grouping; keep as string (leading zeros possible). |
| `Incident Address` | `incident_address` | String (nullable) | Context only; **not** the primary join key. |
| `Address Type` | `address_type` | String (nullable) | Location-quality context. |
| `Location Type` | `location_type` | String (nullable) | Location comparisons; do not merge ambiguous categories. |
| `Resolution Action Updated Date` | `resolution_action_updated_at` | Timestamp (nullable) | Optional audit/context; not a substitute for Closed Date. |
| `Resolution Description` | `resolution_description` | String (nullable) | Optional descriptive context. |
| `Due Date` | `due_at` | Timestamp (nullable) | Optional, only if valid and relevant to an agreed analysis. |

**PLUTO — one property lookup row per valid BBL**

| Raw PLUTO column | Silver lookup name | Target type | Use / treatment |
|---|---|---|---|
| `BBL` | `bbl` | String | Required PLUTO lookup key; must be unique after validation. |
| `borough` | `pluto_borough` | String (nullable) | Source borough code (e.g., `BK`); do not confuse with NYC 311 borough names. |
| `Tax block` | `tax_block` | String (nullable) | Reference; retains source spelling/key representation. |
| `Tax lot` | `tax_lot` | String (nullable) | Reference; retains source spelling/key representation. |
| `address` | `property_address` | String (nullable) | Property address. |
| `postcode` | `property_postcode` | String (nullable) | Property ZIP; use string type. |
| `landuse` | `land_use` | String (nullable) | Land-use category code, **not** a continuous numeric measure. |
| `bldgclass` | `building_class` | String (nullable) | Detailed property class code. |
| `unitsres` | `residential_units` | Long (nullable) | Remove thousands separators before cast; normalization denominator if > 0. |
| `unitstotal` | `total_units` | Long (nullable) | Compare property size; format/NULL checks pending. |
| `numbldgs` | `number_of_buildings` | Long (nullable) | Number of buildings on tax lot; checks pending. |
| `bldgarea` | `building_area_sqft` | Long (nullable) | Remove thousands separators before cast; no area-based division if <= 0 or NULL. |
| `lotarea` | `lot_area_sqft` | Long (nullable) | Land area; format/NULL checks pending. |
| `yearbuilt` | `year_built` | Integer (nullable) | Parse four-digit years; `0` is unusable for derived age, but preserve raw. |

Optional PLUTO attributes: `resarea` → `residential_area_sqft`; `comarea` → `commercial_area_sqft` (numeric conversion and quality checks required if used). Optional NYC 311 latitude/longitude must be checked against the actual 44-column snapshot before mapping; these are not confirmed in Section 2's selected-column table.

**Additional derived Silver fields** (final names to be retained in `docs/02_silver_contract.md`):

| Field | Proposed definition |
|---|---|
| `bbl_status` | `valid_bbl`, `missing_bbl`, or `invalid_bbl`. |
| `pluto_match_status` | `matched`, `missing_bbl`, `invalid_bbl`, or `unmatched_valid_bbl`. |
| `is_closed_status` | True only if standardized source status is `Closed`; does not by itself imply a valid closure duration. |
| `resolution_hours` | `(closed_at - created_at)` in hours **only** for status `Closed` and valid, non-negative timestamps; otherwise NULL. |
| `request_year`, `request_month` | Extracted from valid `created_at`; used for trend reporting and possibly output partitioning. |
| `quality_status` | `accepted`, `flagged`, or `quarantined`. |
| `quality_reason_codes` | One or more explicit reason codes; preserve all applicable reasons. |

### 4.2 Source timestamp and timezone rules

- **Observed raw NYC 311 date format:** `MM/dd/yyyy hh:mm:ss a` (example pattern only; parse actual source strings with Spark's equivalent pattern). This is documented in Section 2.4.
- **Confirmed profile counts:** 0 missing/unparseable Created Dates; 439,146 missing Closed Dates; 47,207 records with Closed Date earlier than Created Date.
- **Additional confirmed issues:** 1899 placeholder-like closing dates, dates as late as 2033, and cases with non-Closed status but a populated Closed Date.
- **Before coding:** Member A checks `spark.sql.session.timeZone`, captures 3–5 representative raw date strings, and confirms the intended interpretation of timestamps lacking an explicit offset. Do **not** assume UTC or silently shift timezones.
- Parse with explicit patterns. Retain original raw timestamp strings or an audit copy where needed; record parse-failure and chronology flags separately.
- Preserve `Closed Date = NULL` for legitimate unresolved requests. Never blanket-drop these rows.
- If `closed_at < created_at`, a 1899 placeholder appears, or a closing date falls after the documented snapshot date **2026-10-12**, keep the request when its required keys/dates are usable, **flag** the date issue, and set `resolution_hours = NULL`. The download date only provides day-level precision; a same-day closing time cannot be ruled invalid without the extraction timestamp.
- If `Status` is not `Closed` but `Closed Date` is present, flag `status_close_mismatch` and do not automatically change the source status or assume a reliable resolution duration.
- If status is `Closed` but Closed Date is missing or invalid, flag this and do not calculate a duration.
- Keep unusual but parsable timestamps visible in quality counts; do not substitute `Resolution Action Updated Date` for `Closed Date` without a separate documented rule.

### 4.3 BBL representation, grain, and joining

**Join key rules**

1. Treat `BBL` as a **string**, not a number or floating-point value. Trim surrounding whitespace.
2. Accept only 10-character strings matching `^[1-5][0-9]{9}$` (borough digit + 5-digit block + 4-digit lot). No arbitrary padding/truncation or conversion from values such as `24` / `14`.
3. Preserve original 311 BBL values as `bbl_raw`; normalize to `bbl` only if valid. The observed NYC 311 invalids include `0000000000`, `24`, and `14`.
4. PLUTO's 858,284 rows were observed to have valid 10-digit BBLs with **no duplicates**. Recheck uniqueness in the pipeline before any join; any future duplicate PLUTO keys must be resolved by a documented deterministic rule or quarantined from the lookup.
5. **311 grain:** one request per `request_id`. **PLUTO grain:** one **tax lot**, not necessarily one building, per valid `bbl`.
6. Use a **LEFT JOIN**, with cleaned accepted/flagged 311 requests on the left and a unique validated PLUTO lookup on the right. Never use an INNER JOIN that drops missing/unmatched BBL requests.
7. Distinguish `missing_bbl`, `invalid_bbl`, `unmatched_valid_bbl`, and `matched` in the final output. Missing/invalid/unmatched BBL **alone** is not a reason to quarantine a usable service request.
8. After joining, verify `joined_request_rows == eligible_311_request_rows` and that every surviving `request_id` appears exactly once. Stop downstream Gold publication on unexplained row multiplication.
9. `pluto_borough` uses PLUTO borough codes while `borough` uses 311 names; retain separate columns unless a verified code-to-name mapping is implemented.

### 4.4 Required, optional, and missing-data rules

| Data condition | Proposed decision | Quality state / downstream use |
|---|---|---|
| `Unique Key` missing, blank, or unusable | Preserve raw record separately with reason; do not publish as valid Silver request. | `quarantined` |
| Duplicate `Unique Key` | Investigate and apply a **deterministic** rule, e.g., exact duplicate collapse or explicit version precedence; count removed/isolated copies. Do not use BBL for 311 deduplication. | Duplicate copies quarantined/reconciled; one usable request remains |
| `Created Date` missing or unparseable | Retain raw row in quarantine for diagnosis; no valid request time grain. | `quarantined` |
| Non-Closed status and NULL Closed Date | Preserve as legitimate unresolved request. | `accepted` if otherwise valid |
| Closed Date invalid, reversed, beyond snapshot date, or inconsistent with status | Preserve usable request, add reason, and leave derived duration NULL. | `flagged` |
| Unexpected source status value | Keep raw text, record reason; don't force it to Closed. | `flagged` |
| Missing, zero-filled, or malformed 311 BBL | Preserve request; property fields remain NULL. | `flagged` for key quality; no PLUTO match |
| Valid-format BBL absent from PLUTO | Preserve request; property fields remain NULL. | `accepted` or `flagged` per agreed monitoring policy; `unmatched_valid_bbl` recorded |
| Missing borough or `Unspecified` borough | Preserve request; keep missing vs literal category distinct. | `flagged` if geography is unreliable for a requested aggregation |
| Unexpected borough (e.g., precinct label) | Preserve original and flag; do not guess a borough. | `flagged` |
| Missing complaint detail/location type | Preserve request; these are not mandatory identifiers. | `accepted` unless an analysis explicitly requires the field |
| PLUTO `residential_units` NULL or zero | Preserve tax lot; do **not** calculate residential-unit-normalized complaint rate. | Not a reason to exclude service request |
| PLUTO `year_built` zero or NULL | Preserve original; derived building age NULL. | Optional property-quality flag |
| PLUTO `building_area_sqft` zero or NULL | Preserve record; area-normalized metrics unavailable. | Optional property-quality flag |
| Very large or unusually old PLUTO values | Preserve; flag only if a reviewed, evidence-based threshold has been agreed. | Review flag, not automatic deletion |
| Failed numeric conversion of a selected PLUTO field | Preserve original string and record conversion failure; avoid silently replacing with zero. | Property quality flag; quarantine lookup key only if BBL unusable |

**Quality precedence:** `quarantined` (essential request fields unusable) takes priority over `flagged` (usable request with noncritical quality issues); otherwise `accepted`. Maintain separate counts for input, duplicates, accepted, flagged, and quarantined rows, with explicit `quality_reason_codes`. The team must agree whether flagged rows reside in the main Silver Parquet or in a clearly identified companion output; **do not lose them silently**.

### 4.5 Reproducible development sample

**Proposed method — pending both members' confirmation:**

- Sample approximately **1% of NYC 311 request IDs** using a deterministic hash of the trimmed `Unique Key` (fixed modulus/seed or other reproducible strategy). Unlike an unseeded random sample, this preserves the selection on repeated runs and keeps duplicate versions of the same ID together for testing.
- Document the exact selection expression, Spark version, sample source snapshot, resulting row count, and distinct request-ID count. Do not treat 1% as an exact row-count guarantee.
- Keep the **complete PLUTO lookup** for early development joins (PLUTO is much smaller); otherwise sampling PLUTO independently creates false unmatched BBLs. Later optional pruning to the sample's BBLs must be documented and tested.
- Add tiny hand-constructed test fixtures for cases too rare to guarantee in a random/hash sample: missing request ID, duplicate request ID, 1899 placeholder, future/reversed Closed Date, open request with NULL Closed Date, malformed BBL, valid-but-unmatched BBL, and duplicate PLUTO BBL.
- Write development and production results to separate directories. The original Bronze CSVs remain unchanged.

### 4.6 Reconciliation and Silver acceptance checks

Both members must confirm the following **before** accepting the shared Silver output:

1. Required input column names match the actual source headers; optional fields may be absent without silently changing the contract.
2. Raw 311 and PLUTO files are still unchanged; source paths, snapshot dates, file sizes, and available checksums are recorded.
3. Report input 311 count, deduplicated usable request count, `accepted`, `flagged`, `quarantined`, and duplicate counts. Explain any difference from the raw **22,761,196** rows.
4. No repeated `request_id` survives in usable Silver; every eligible request remains one row after the PLUTO LEFT JOIN.
5. Report counts/rates for valid, missing, invalid, matched, and unmatched BBLs. The source-profile figures are **input baselines**, not assumed output counts.
6. Verify known open requests with NULL Closed Date remain available and have NULL resolution duration.
7. Verify invalid resolution timestamps never produce negative or misleading `resolution_hours`.
8. Verify `residential_units`, `building_area_sqft`, and other converted numeric columns have correct Spark types; commas were stripped without changing the raw Bronze files.
9. Save representative before/after examples, data-quality summaries, schema, and LEFT JOIN cardinality test results.
10. Publish Gold only after critical Silver checks pass (notably unique request ID, no join row multiplication, valid output schema, and non-empty usable Silver).

### 4.7 Outstanding checks identified in Sections 2–3

These are **follow-up validation tasks**, not claims already proven by the source profile. They can be completed while implementing Bronze and before final Silver approval. **Sections 1–3 have intentionally not been edited.**

| Owner | Check to complete | Evidence to add or record |
|---|---|---|
| Member A | Correct NYC 311 subsection numbering (currently begins at `3.2` in Part 2) if document cleanup is authorized. | Cosmetic correction deferred to preserve Parts 1–3. |
| Member A | Count 1899 placeholder dates, future dates against snapshot, and distinct reversed-date categories; inspect Spark session timezone. | Date-quality counts and timezone setting. |
| Member A | Cross-tab standardized Status by present/missing Closed Date, including `Closed` with NULL and non-Closed with populated date. | Status × closing-date table. |
| Member A | Check NULL/blank coverage of `Community Board`, `Problem Detail`, and other Gold grouping fields. | Completeness counts and usable-category notes. |
| Member B | Check `unitstotal`, `numbldgs`, `lotarea` for NULL/blank, comma formatting, castability, zeros, and extremes. | PLUTO numeric-profile results. |
| Both | Retain known **2026-10-12** download dates and available SHA-256 checksums; record actual extraction timestamps or source versions only if evidence exists. | Verified manifest/source metadata; do not invent missing timestamps. |
| Both | Approve exact deterministic sampling method, date timezone policy, duplicate-ID tie-breaking, and final quality-state output locations. | Joint decision documented in `docs/02_silver_contract.md`. |

### 4.8 Joint sign-off / implementation handoff

- [ ] **Member A** verifies NYC 311 raw headers, timestamp behavior, and follow-up profile checks.
- [ ] **Member B** verifies PLUTO optional numeric fields and proposed property mapping.
- [ ] Both agree on BBL format, unique PLUTO lookup, LEFT JOIN, and cardinality tests.
- [ ] Both agree on deterministic sample, duplicate handling, and accepted/flagged/quarantined semantics.
- [ ] Both move the **approved** mapping and quality logic into `docs/02_silver_contract.md` before coding `src/silver.py` and `src/quality.py`.

**Decision status:** **Not yet jointly approved.** This section is a documented proposal based on the existing source-profile results; any change to the agreed rules should be recorded before implementation.
