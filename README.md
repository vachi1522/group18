# NYC 311 + PLUTO: Urban Service Demand and Response Reliability Analytics

DAT535 Data Engineering project (group 18).

A batch data pipeline built with **PySpark** that combines NYC 311 service requests with NYC PLUTO property data and produces summary tables that answer five questions about city service demand and problem properties.

The pipeline follows the **medallion architecture**: **Bronze → Silver → Gold**.

---

## Questions answered

| # | Question | Gold module |
|---|---|---|
| Q1 | Where is service demand concentrated? | `gold1.py` |
| Q2 | Where is resolution performance slow or inconsistent? | `gold1.py` |
| Q3 | Which properties have repeated service problems? | `gold2.py` |
| Q4 | Which properties have unusually high complaint activity compared with similar buildings? | `gold2.py` |
| Q5 | Which property types have a wide variety of city-service needs? | `gold2.py` |

---

## Datasets

| Dataset | Role | Records | Raw size | Format |
|---|---|---|---|---|
| [NYC 311 Service Requests (2020 to Present)](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9) | Main dataset: one row per service request | 22,761,196 | ~15 GB | CSV |
| [NYC PLUTO](https://data.cityofnewyork.us/City-Government/Primary-Land-Use-Tax-Lot-Output-PLUTO-/64uk-42ks) | Property reference data: one row per tax lot | 858,284 | ~442 MB | CSV |

- Snapshot / download date: **12 October 2026**
- The two datasets are joined on **BBL** (Borough-Block-Lot), a 10-digit property identifier.
- Profiling results and the agreed cleaning rules are documented in [`docs/source_metadata.md`](nyc311-pluto/docs/source_metadata.md).

The raw data is not stored in this repository. On the Spark VM it is located at:

```
/home/ubuntu/spark-project-data/bronze/nyc311/nyc311_2020_present.csv
/home/ubuntu/spark-project-data/bronze/pluto/pluto.csv
```

---

## Architecture

```
BRONZE   raw CSV files, unchanged
   │
   ▼
SILVER   clean → validate → quarantine → LEFT JOIN on BBL → validate → write
   │      output: one row per unique, usable NYC 311 request, enriched with PLUTO
   ▼
GOLD     read Silver once (cached) → build 9 summary tables → write
```

### Bronze
`bronze.py` reads both CSV files with every column as a string (`inferSchema=False`), keeps malformed rows (`PERMISSIVE` mode) and handles quoted text (`escape='"'`). The raw files are never modified.

### Silver
`silver.py` runs ten steps:

| Step | What happens | Module |
|---|---|---|
| 1 | Load NYC 311 and PLUTO from Bronze | `bronze.py` |
| 2 | Clean request fields (ID, BBL, borough, community board, ZIP, complaint type, agency), then quarantine missing, invalid and duplicate request IDs | `nyc311_cleaning1.py`, `quality.py` |
| 3 | Clean lifecycle fields (created/closed timestamps, status, `resolution_hours`), then quarantine unusable Created Dates | `nyc311_cleaning2.py`, `quality.py` |
| 4 | Clean PLUTO (BBL, borough, land use, building class, units, areas, year built), then quarantine missing, invalid and duplicate BBLs | `pluto_cleaning.py`, `quality.py` |
| 5 | LEFT JOIN NYC 311 with PLUTO on `bbl` | `quality.py` |
| 6 | Validation gate: the row count must not change and every `request_id` must be unique | `quality.py` |
| 7 | Write the Silver dataset | `silver.py` |
| 8–10 | Write the three quarantine datasets | `silver.py` |

Nothing is written unless the validation gate in step 6 passes.

### Gold
`gold.py` reads Silver once, keeps the 19 columns Gold needs and caches them (`persist(MEMORY_AND_DISK)`). It then calls the two Gold modules and checks that every table is non-empty.

| Table | One row per | Question |
|---|---|---|
| `demand_by_area_month` | borough + community board + year + month | Q1 |
| `demand_by_complaint_borough` | borough + complaint type (with rank and share in borough) | Q1 |
| `resolution_by_agency_complaint` | agency + complaint type | Q2 |
| `resolution_by_borough_month` | borough + year + month | Q2 |
| `property_profile` | property (BBL) | Q3–Q5 base |
| `property_repeat_problems` | property + complaint type that repeats | Q3 |
| `property_repeat_summary` | property with repeated problems | Q3 |
| `property_peer_comparison` | property compared with its peer group | Q4 |
| `property_type_variety` | property type (land use + building class) | Q5 |

- `demand_by_area_month` and `resolution_by_borough_month` are partitioned by `request_year`.
- Resolution tables report median, p90, average, standard deviation, coefficient of variation and % unresolved, with an `enough_data` flag (at least 30 validly closed requests).
- `gold2.py` caches `property_profile` a second time (Level-2 cache) because it is reused by Q3, Q4 and Q5.

---

## Data quality approach

| Situation | Action |
|---|---|
| Request ID missing, invalid or duplicated | **Quarantine** (one copy of a duplicated ID is kept, chosen deterministically by a SHA-256 hash) |
| Created Date missing, unreadable, before 1900 or in the future | **Quarantine** |
| PLUTO BBL missing, invalid or duplicated | **Quarantine** (all copies of a duplicated BBL) |
| Invalid BBL, ZIP, borough or community board in NYC 311 | **Flag**: keep the row and add a status column |
| Closed Date problems (1899 placeholder, closed before created, future date) | **Flag**: keep the row, `resolution_hours` set to NULL |
| Request without a matching PLUTO property | **Keep**: `pluto_match_status = "unmatched"` |

Original values are preserved in `*_raw` columns, and every quarantined row has a `quality_reason`.

---

## Output locations

All outputs are written as Parquet under `/home/ubuntu/spark-project-data/dev/`:

```
dev/
├── silver/nyc311_enriched/       Silver dataset
├── quarantine/
│   ├── nyc311_fields/            rejected request IDs
│   ├── nyc311_lifecycle/         rejected Created Dates
│   └── pluto/                    rejected PLUTO BBLs
└── gold/                         the 9 Gold tables
```

---

## Repository structure

```
group18/
├── .github/workflows/
│   ├── pipeline.yml          manual run + monthly schedule
│   ├── dat535-dev.yml        runs on push / pull request to dev
│   └── dat535-prod.yml       runs on push to main
└── nyc311-pluto/
    ├── docs/
    │   └── source_metadata.md        source profiling and Silver rules
    ├── notebooks/
    │   ├── 02_pluto_profiling.ipynb  PLUTO exploration
    │   ├── 03_nyc311_profiling.ipynb NYC 311 exploration
    │   ├── nyc311_cleaning1.ipynb    development of request-field cleaning
    │   ├── nyc311_cleaning2.ipynb    development of lifecycle cleaning
    │   ├── pluto_cleaning.ipynb      development of PLUTO cleaning
    │   ├── gold1.ipynb               development of Q1, Q2
    │   └── gold2.ipynb               development of Q3, Q4, Q5
    └── src/
        ├── pipeline.py               runs Silver, then Gold
        ├── bronze.py                 raw CSV paths and reader
        ├── silver.py                 Silver orchestrator (10 steps)
        ├── nyc311_cleaning1.py       NYC 311 request-field cleaning
        ├── nyc311_cleaning2.py       NYC 311 lifecycle cleaning
        ├── pluto_cleaning.py         PLUTO cleaning
        ├── quality.py                quarantine, join and validation
        ├── gold.py                   Gold driver and Level-1 cache
        ├── gold1.py                  Q1, Q2 tables
        └── gold2.py                  Q3, Q4, Q5 tables
```

---

## How to run

The code runs on the course Spark cluster (Spark standalone master at `spark://192.168.18.168:7077`) from the Python environment `~/spark-env`.

```bash
source ~/spark-env/bin/activate
cd nyc311-pluto/src
```

**Full pipeline (Silver, then Gold):**
```bash
python pipeline.py
```
`pipeline.py` prints a summary and returns exit code `0` on success or `1` if a stage failed.

**Single stages:**
```bash
python silver.py      # optional: --nyc <csv> --pluto <csv> --output <dir>
python gold.py        # optional: --input <silver path> --output <gold dir>
```
Without options, the default paths listed above are used.

---

## CI/CD (GitHub Actions)

All three workflows run on a **self-hosted runner**, activate `~/spark-env` and execute `nyc311-pluto/src/pipeline.py`.

| Workflow | Trigger |
|---|---|
| `pipeline.yml` | manual (`workflow_dispatch`) and monthly schedule (1st of the month, 00:00 UTC) |
| `dat535-dev.yml` | push or pull request to `dev`, or manual |
| `dat535-prod.yml` | push to `main`, or manual |

---

## Technologies

Python · Apache Spark (PySpark) · Parquet · GitHub Actions

---
