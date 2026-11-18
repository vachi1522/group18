"""
NYC 311 + PLUTO Pipeline Orchestrator

Usage:
    python pipeline.py

Execution order:
    1. Silver
    2. Gold
"""

import os
import sys
import time


# --------------------------------------------------
# Configuration
# --------------------------------------------------

# Directory containing pipeline.py, silver.py and gold.py (+ gold1.py, gold2.py)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


# --------------------------------------------------
# Execute a pipeline stage
# --------------------------------------------------

def run_stage(stage_name, module):

    print("\n" + "=" * 60)
    print(f"RUNNING {stage_name.upper()} LAYER")
    print("=" * 60)

    stage_main = getattr(module, "main", None)

    if not callable(stage_main):
        print(f"ERROR: {stage_name}.py must define a main() function.")
        return "failed", 0.0

    started_at = time.perf_counter()
    try:
        # Ensure execution from the script directory
        os.chdir(SCRIPT_DIR) 

        result = stage_main()

        if result is None or result == 0 or result == "success":
            print(f"\n{stage_name.title()} completed successfully in {time.perf_counter() - started_at:.2f} seconds.")
            return "success", time.perf_counter() - started_at
        else:
            print(f"\nERROR: {stage_name.title()} returned failure: {result} (elapsed: {time.perf_counter() - started_at:.2f} seconds)")
            return "failed", time.perf_counter() - started_at

    except SystemExit as error:

        if error.code is None or error.code == 0:
            print(f"\n{stage_name.title()} completed successfully in {time.perf_counter() - started_at:.2f} seconds.")
            return "success", time.perf_counter() - started_at
        else:
            print(f"\nERROR: {stage_name.title()} exited with exit code: {error.code} (elapsed: {time.perf_counter() - started_at:.2f} seconds)")
            return "failed", time.perf_counter() - started_at

    except Exception as error:

        print(f"\nERROR: {stage_name.title()} failed: {error} (elapsed: {time.perf_counter() - started_at:.2f} seconds)")
        return "failed", time.perf_counter() - started_at


# --------------------------------------------------
# Main pipeline
# --------------------------------------------------

def main():

    results = {}
    timings = {}

    print("=" * 60)
    print("NYC 311 + PLUTO PIPELINE")
    print("SILVER -> GOLD")
    print("=" * 60)

    silver_path = os.path.join(SCRIPT_DIR, "silver.py")
    gold_path = os.path.join(SCRIPT_DIR, "gold.py")

    # Check that both scripts exist in the same directory

    silver_exists = os.path.isfile(silver_path)
    gold_exists = os.path.isfile(gold_path)

    if not silver_exists:
        print("ERROR: silver.py does not exist.")
    if not gold_exists:
        print("ERROR: gold.py does not exist.")


    # ----------------------------------------------
    # 1. Silver
    # ----------------------------------------------

    if silver_exists:

        try:
            import silver

            results["Silver"], timings["Silver"] = run_stage("Silver", silver)

        except Exception as error:
            print(f"\nERROR: Could not import silver.py: {error}")
            results["Silver"] = "failed"

    else:
        print("\nERROR: silver.py does not exist.")
        results["Silver"] = "failed"


    # ----------------------------------------------
    # 2. Gold
    # ----------------------------------------------

    if gold_exists:

        try:
            import gold

            results["Gold"], timings["Gold"] = run_stage("Gold", gold)

        except Exception as error:
            print(f"\nERROR: Could not import gold.py: {error}")
            results["Gold"] = "failed"

    else:
        results["Gold"] = "failed"


    # ----------------------------------------------
    # Summary
    # ---------------------------------------------- 
    
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)

    print(f"{'Bronze':<10} {'PRE-STAGED':<10} not timed")
    for stage, status in results.items():
        elapsed = f"{timings[stage]:.2f} seconds" if stage in timings else "not run"
        print(f"{stage:<10} {status.upper():<10} {elapsed}")

    print("=" * 60)

    # Return failure if an existing stage failed
    if "failed" in results.values():
        print("Pipeline finished with errors.")
        return 1
    else:
        print("Pipeline finished successfully.")
        return 0


# --------------------------------------------------
# Entry point
# -------------------------------------------------- 

if __name__ == "__main__":
    sys.exit(main())
