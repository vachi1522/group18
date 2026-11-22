"""Run the DAT535 Bronze stage; run with spark-submit bronze.py"""

import os
import sys
import argparse
import csv
import json
import glob
import hashlib
import requests

from datetime import datetime, timezone

BRONZE_DIR = os.path.join("/home/ubuntu/spark-project-data", "bronze")
NYC311_CSV = os.path.join(BRONZE_DIR, "nyc311", "nyc311_2020_present.csv")
PLUTO_CSV = os.path.join(BRONZE_DIR, "pluto", "pluto.csv")

# API settings
DOMAIN = "https://data.cityofnewyork.us"
DATASETS = {"nyc311": "erm2-nwe9", "pluto": "64uk-42ks"}
WATERMARK_FILE = os.path.join(BRONZE_DIR, "nyc311", "_state", "watermark.json")
csv.field_size_limit(sys.maxsize)  # some 311 text fields are very long


# full download
def ingest_full(name):
    """Download the full CSV of one dataset into bronze/<name>/snapshot=<date>/
    and write a receipt (_manifest.json) with row count and SHA-256 checksum."""
    
    started_at = datetime.now(timezone.utc).replace(microsecond=0)
    out_dir = os.path.join(BRONZE_DIR, name, f"snapshot={started_at:%Y-%m-%d}")
    out_file = os.path.join(out_dir, f"{name}.csv")
    os.makedirs(out_dir, exist_ok=True)

    url = f"{DOMAIN}/api/views/{DATASETS[name]}/rows.csv?accessType=DOWNLOAD"
    headers = {"X-App-Token": os.environ.get("SOCRATA_APP_TOKEN", "")}
    print(f"[{name}] downloading {url}", flush=True)

    # Download to .part first, rename only when complete
    with requests.get(url, headers=headers, stream=True, timeout=(30, 600)) as response:
        response.raise_for_status()
        with open(out_file + ".part", "wb") as f:
            for chunk in response.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    os.replace(out_file + ".part", out_file)

    # Receipt: checksum + number of rows
    sha = hashlib.sha256()
    with open(out_file, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            sha.update(chunk)
    with open(out_file, newline="", encoding="utf-8") as f:
        rows = sum(1 for _ in csv.reader(f)) - 1

    with open(os.path.join(out_dir, "_manifest.json"), "w") as f:
        json.dump({
            "dataset": name,
            "url": url,
            "downloaded_utc": started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "rows": rows,
            "bytes": os.path.getsize(out_file),
            "sha256": sha.hexdigest(),
        }, f, indent=2)
    print(f"[{name}] done: {rows:,} rows", flush=True)

    # Incremental downloads continue from this time
    if name == "nyc311":
        os.makedirs(os.path.dirname(WATERMARK_FILE), exist_ok=True)
        with open(WATERMARK_FILE, "w") as f:
            json.dump({"watermark_utc": started_at.strftime("%Y-%m-%dT%H:%M:%SZ")}, f, indent=2)


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


# find the newest downloaded snapshot =====
def latest_snapshot(name):
    """Newest snapshot that has a receipt, or None."""
    manifests = sorted(glob.glob(os.path.join(BRONZE_DIR, name, "snapshot=*", "_manifest.json")))
    return os.path.join(os.path.dirname(manifests[-1]), f"{name}.csv") if manifests else None


# read newest snapshot, fall back to old file =====
def load_nyc311(spark, path=None):
    return read_csv(spark, path or latest_snapshot("nyc311") or NYC311_CSV)

def load_pluto(spark, path=None):
    return read_csv(spark, path or latest_snapshot("pluto") or PLUTO_CSV)


# run with "python bronze.py --full" to download the full datasets
def main(args=None):
    """Run the Bronze stage with command-line arguments to download full datasets NYC 311 and PLUTO or incremental NYC 311."""
    
    parser = argparse.ArgumentParser(description="NYC 311 + PLUTO Bronze Layer")
    parser.add_argument("--full", action="store_true", help="full download of NYC 311 + PLUTO")
    args = parser.parse_args(args)
    
    if args.full:
        for name in DATASETS:
            ingest_full(name)


if __name__ == "__main__":
    sys.exit(main())
