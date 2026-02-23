import argparse
import os
import re
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_DIM_DATASET = os.getenv("BQ_DIM_DATASET")
BQ_FACT_DATASET = os.getenv("BQ_FACT_DATASET")
WAREHOUSE_OUT_DIR = os.getenv("WAREHOUSE_OUT_DIR", "data/warehouse")


def _resolve_write_disposition(mode):
    mode = mode.lower().strip()
    from google.cloud import bigquery

    if mode == "replace":
        return bigquery.WriteDisposition.WRITE_TRUNCATE
    if mode == "append":
        return bigquery.WriteDisposition.WRITE_APPEND
    raise ValueError("mode must be either 'append' or 'replace'")


def _is_valid_dataset_id(value):
    if not value:
        return False
    text = value.strip()
    return re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,1023}", text) is not None


def _resolve_dataset_ids():
    dim_dataset = (BQ_DIM_DATASET or "").strip()
    fact_dataset = (BQ_FACT_DATASET or "").strip()
    shared_dataset = (BQ_DATASET or "").strip()

    if dim_dataset and not _is_valid_dataset_id(dim_dataset):
        raise ValueError(
            f"Invalid BQ_DIM_DATASET='{dim_dataset}'. Use only a BigQuery dataset id, e.g. Commerce_pulse."
        )
    if fact_dataset and not _is_valid_dataset_id(fact_dataset):
        raise ValueError(
            f"Invalid BQ_FACT_DATASET='{fact_dataset}'. Use only a BigQuery dataset id, e.g. Commerce_pulse."
        )
    if shared_dataset and not _is_valid_dataset_id(shared_dataset):
        raise ValueError(
            f"Invalid BQ_DATASET='{shared_dataset}'. Use only a BigQuery dataset id, e.g. Commerce_pulse."
        )

    resolved_dim = dim_dataset or shared_dataset
    resolved_fact = fact_dataset or shared_dataset
    if not resolved_dim or not resolved_fact:
        raise ValueError(
            "Missing dataset config. Set BQ_DATASET or set both BQ_DIM_DATASET and BQ_FACT_DATASET."
        )

    return resolved_dim, resolved_fact


def _load_csv_to_table(client, csv_path, table_id, write_disposition):
    from google.cloud import bigquery

    if not csv_path.exists():
        print(f"skip missing file: {csv_path}")
        return

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=write_disposition,
    )

    with csv_path.open("rb") as file_obj:
        load_job = client.load_table_from_file(file_obj, table_id, job_config=job_config)
    load_job.result()
    table = client.get_table(table_id)
    print(f"loaded {csv_path} -> {table_id} rows={table.num_rows}")


def main():
    parser = argparse.ArgumentParser(description="Load warehouse CSV outputs into BigQuery.")
    parser.add_argument(
        "--mode",
        default="append",
        choices=["append", "replace"],
        help="append=add rows, replace=truncate and reload each table",
    )
    parser.add_argument("--project-id", default=GCP_PROJECT_ID, help="GCP project id")
    parser.add_argument("--warehouse-dir", default=WAREHOUSE_OUT_DIR, help="Warehouse output directory")
    args = parser.parse_args()

    try:
        from google.cloud import bigquery
    except Exception as exc:
        raise RuntimeError(
            "google-cloud-bigquery is not installed. Run: pip install google-cloud-bigquery"
        ) from exc

    project_id = args.project_id
    if not project_id:
        raise ValueError("Missing project id. Set GCP_PROJECT_ID in .env or pass --project-id.")

    dim_dataset, fact_dataset = _resolve_dataset_ids()

    write_disposition = _resolve_write_disposition(args.mode)
    warehouse_dir = Path(args.warehouse_dir)

    table_map = [
        (warehouse_dir / "dimensions" / "dim_customer.csv", f"{project_id}.{dim_dataset}.dim_customer"),
        (warehouse_dir / "dimensions" / "dim_product.csv", f"{project_id}.{dim_dataset}.dim_product"),
        (warehouse_dir / "dimensions" / "dim_date.csv", f"{project_id}.{dim_dataset}.dim_date"),
        (warehouse_dir / "facts" / "facts_order.csv", f"{project_id}.{fact_dataset}.facts_order"),
        (warehouse_dir / "facts" / "facts_payment.csv", f"{project_id}.{fact_dataset}.facts_payment"),
        (warehouse_dir / "facts" / "facts_refunds.csv", f"{project_id}.{fact_dataset}.facts_refunds"),
        (warehouse_dir / "facts" / "facts_shipment.csv", f"{project_id}.{fact_dataset}.facts_shipment"),
        (warehouse_dir / "facts" / "facts_order_daily.csv", f"{project_id}.{fact_dataset}.facts_order_daily"),
    ]

    client = bigquery.Client(project=project_id)
    for csv_path, table_id in table_map:
        _load_csv_to_table(client, csv_path, table_id, write_disposition)

    print("BigQuery load complete.")


if __name__ == "__main__":
    main()
