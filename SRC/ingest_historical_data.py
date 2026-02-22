import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne


load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")
HISTORICAL_DATA_DIR = os.getenv("HISTORICAL_DATA_DIR")
HISTORICAL_DATA_SOURCE = os.getenv("HISTORICAL_DATA_SOURCE")


"""
Historical data ingestion script.

This script ingests historical JSON files and loads them into MongoDB
as standardized event records. It is designed to be idempotent, meaning
it can be safely re-run without creating duplicate events.
"""

def get_ingestion_time():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
"""
This timestamp is used to track when a record was ingested
    into the database, not when the original event occurred.
"""

def generate_event_id(source, file_name, index, record):  ## Generates a deterministic event ID based on the record content to ensure idempotency. It combines the source, file name, record index, and a canonical JSON representation of the record, then hashes it using SHA-1.
    canonical_payload = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    raw = f"{source}|{file_name}|{index}|{canonical_payload}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def generate_event_time_from_record(record):    
# This function attempts to extract a timestamp from the record by checking for common fields that may contain event time information.
    for key in (
        "event_time",
        "created_at",
        "paid_at",
        "refunded_at",
        "created",
        "paidAt",
        "refundedAt",
        "timestamp",
        "ts",
    ):
        if key in record:
            return record[key]
    return None


"""
This function attempts to identify the vendor based on specific fields in the record. 
It checks for known field patterns that are unique to each vendor and returns a standardized vendor name. 
If no known patterns are found, it defaults to "unknown". 
"""
def identify_vendor_from_record(record):    
    if "vendor" in record:
        return record["vendor"]
    if "orderRef" in record:
        return "vendor_a"
    if "order_id" in record:
        return "vendor_b"
    if "order" in record:
        return "vendor_c"
    return "unknown"


def main():
    """
    This function scans the configured directory for historical JSON files,
    normalizes each record into a common event structure, and upserts the
    events into MongoDB. A unique event_id is used to prevent duplicate
    inserts when the script is re-run.

    The script also prints a summary of how many records were read, inserted, and already existed in the database for each file, 
    as well as a final summary at the end.
    """

    
    data_dir = Path(HISTORICAL_DATA_DIR)
    if not data_dir.exists():
        print(f"Folder not found: {data_dir}")
        return

    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB][MONGO_COLLECTION]
    collection.create_index("event_id", unique=True)

    total_records = 0
    total_upserted = 0
    total_existing = 0

    for file_path in sorted(data_dir.glob("*.json")):
        with file_path.open("r", encoding="utf-8") as handle:
            content = json.load(handle)

        if isinstance(content, list):
            records = [item for item in content if isinstance(item, dict)]
        elif isinstance(content, dict):
            records = [content]
        else:
            records = []

        operations = []
        event_type = f"{file_path.stem.split('_')[0]}_historical"

        for idx, record in enumerate(records):
            event_id = generate_event_id(HISTORICAL_DATA_SOURCE, file_path.name, idx, record)
            event_doc = {
                "event_id": event_id,
                "event_type": event_type,
                "event_time": generate_event_time_from_record(record),
                "vendor": identify_vendor_from_record(record),
                "payload": record,
                "ingested_at": get_ingestion_time(),
            }
            operations.append(
                UpdateOne(
                    {"event_id": event_id},
                    {"$setOnInsert": event_doc},
                    upsert=True,
                )
            )

        if not operations:
            continue

        result = collection.bulk_write(operations, ordered=False)
        total_records += len(operations)
        total_upserted += result.upserted_count
        total_existing += len(operations) - result.upserted_count
        print(
            f"{file_path.name}: read={len(operations)} "
            f"inserted={result.upserted_count} existing={len(operations) - result.upserted_count}"
        )

    print(
        f"done total_read={total_records} total_inserted={total_upserted} total_existing={total_existing}"
    )


if __name__ == "__main__":
    main()
