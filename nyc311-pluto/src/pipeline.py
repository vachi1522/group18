"""
NYC 311 + PLUTO Pipeline Orchestrator

Usage:
    python pipeline.py

Execution order:
    1. Silver   (silver.py)
    2. Gold     (gold_combined.py -> gold1.py, gold2.py)

Each stage runs in its OWN Python process (subprocess). This gives every stage
a fresh Spark driver JVM, so memory used by Silver is fully released before Gold
starts, and spark.driver.memory set in a stage's SparkSession actually applies.
"""

import os
import subprocess
import sys


# --------------------------------------------------
# Configuration
# --------------------------------------------------

# Directory containing pipeline.py, silver.py, gold_combined.py, gold1.py, gold2.py
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

STAGES = [
    ("Silver", "silver.py"),
    ("Gold", "gold_combined.py"),
]

# Free the master VM's memory for the Gold driver:
# stop the Spark worker on this machine (the master) just before Gold,
# and start it again after Gold - always, even if Gold fails.
# Set to False to turn this off.
PAUSE_MASTER_WORKER_FOR_GOLD = True
SPARK_HOME = os.environ.get("SPARK_HOME", "/opt/spark")
MASTER_URL = "spark://192.168.18.168:7077"


def local_worker_running():
    """True if a Spark Worker process runs on this machine."""
    try:
        out = subprocess.run(["jps", "-l"], capture_output=True, text=True).stdout
    except FileNotFoundError:
        return False
    return "org.apache.spark.deploy.worker.Worker" in out


def stop_local_worker():
    """Stop this machine's Spark worker. Returns True only if we stopped it."""
    if not local_worker_running():
        print("No Spark worker on this machine - nothing to pause.")
        return False
    print("Pausing the Spark worker on the master for Gold...", flush=True)
    subprocess.run([f"{SPARK_HOME}/sbin/stop-worker.sh"])
    return True


def start_local_worker():
    """Start this machine's Spark worker again."""
    print("Restarting the Spark worker on the master...", flush=True)
    subprocess.run([f"{SPARK_HOME}/sbin/start-worker.sh", MASTER_URL])
    if local_worker_running():
        print("Master worker is running again.")
    else:
        print("WARNING: master worker did not restart - run start-worker.sh manually.")


# --------------------------------------------------
# Execute a pipeline stage in a separate process
# --------------------------------------------------

def run_stage(stage_name, script_name):

    print("\n" + "=" * 60)
    print(f"RUNNING {stage_name.upper()} LAYER")
    print("=" * 60, flush=True)

    script_path = os.path.join(SCRIPT_DIR, script_name)

    if not os.path.isfile(script_path):
        print(f"ERROR: {script_name} does not exist.")
        return "failed"

    try:
        completed = subprocess.run(
            [sys.executable, script_path],
            cwd=SCRIPT_DIR,
        )
    except Exception as error:
        print(f"\nERROR: {stage_name} could not start: {error}")
        return "failed"

    if completed.returncode == 0:
        print(f"\n{stage_name} completed successfully.")
        return "success"

    print(f"\nERROR: {stage_name} exited with exit code: {completed.returncode}")
    return "failed"


# --------------------------------------------------
# Main pipeline
# --------------------------------------------------

def main():

    results = {}

    print("=" * 60)
    print("NYC 311 + PLUTO PIPELINE")
    print("SILVER -> GOLD")
    print("=" * 60)

    for stage_name, script_name in STAGES:

        paused = False
        if stage_name == "Gold" and PAUSE_MASTER_WORKER_FOR_GOLD:
            paused = stop_local_worker()

        try:
            results[stage_name] = run_stage(stage_name, script_name)
        finally:
            if paused:
                start_local_worker()      # always switch back, even on failure

        # Do not run Gold on top of a failed Silver
        if results[stage_name] == "failed":
            break

    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)

    for stage_name, _ in STAGES:
        status = results.get(stage_name, "skipped")
        print(f"{stage_name:<10} {status.upper()}")

    print("=" * 60)

    if "failed" in results.values():
        print("Pipeline finished with errors.")
        return 1

    print("Pipeline finished successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
