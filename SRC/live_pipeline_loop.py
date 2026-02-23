import argparse
import subprocess
import sys
import time
from pathlib import Path


def run_script(script_path):
    return subprocess.run([sys.executable, str(script_path)], check=False).returncode


def main():
    parser = argparse.ArgumentParser(description="Run live ingest + transform loop.")
    parser.add_argument("--sleep-seconds", type=float, default=2.0, help="Pause between loop cycles.")
    parser.add_argument(
        "--load-bigquery",
        action="store_true",
        help="Also run BigQuery load after each transform cycle.",
    )
    parser.add_argument(
        "--bigquery-mode",
        default="append",
        choices=["append", "replace"],
        help="BigQuery load mode when --load-bigquery is enabled.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    ingest_script = root / "SRC" / "ingest_live_events.py"
    transform_script = root / "SRC" / "transform" / "main.py"
    bigquery_script = root / "SRC" / "load_to_bigquery.py"

    print(f"Starting loop with sleep={args.sleep_seconds}s")
    print(f"Ingest script: {ingest_script}")
    print(f"Transform script: {transform_script}")
    if args.load_bigquery:
        print(f"BigQuery script: {bigquery_script} mode={args.bigquery_mode}")

    while True:
        ingest_code = run_script(ingest_script)
        transform_code = run_script(transform_script)
        bq_code = None
        if args.load_bigquery:
            bq_code = subprocess.run(
                [sys.executable, str(bigquery_script), "--mode", args.bigquery_mode],
                check=False,
            ).returncode
        if bq_code is None:
            print(f"cycle ingest_exit={ingest_code} transform_exit={transform_code}")
        else:
            print(
                f"cycle ingest_exit={ingest_code} transform_exit={transform_code} "
                f"bigquery_exit={bq_code}"
            )
        time.sleep(max(args.sleep_seconds, 0.0))


if __name__ == "__main__":
    main()
