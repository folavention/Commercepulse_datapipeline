import json
import os
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne


load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")
LIVE_EVENTS_DIR = os.getenv("LIVE_EVENTS_DIR")


def parse_live_event(line): 
    """ 
    Parses a single line of JSONL input, extracting the event information and returning a standardized event document. 
    It handles potential parsing errors and ensures that the required fields are present before returning the event document.
    """
    
    line = line.strip()
    if not line:
        return None

    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        return None

    if not isinstance(data, dict):
        return None

    event_id = data.get("event_id")
    if not event_id:
        return None

    return {
        "event_id": event_id,
        "event_type": data.get("event_type"),
        "event_time": data.get("event_time"),
        "vendor": data.get("vendor"),
        "payload": data.get("payload"),
        "ingested_at": data.get("ingested_at"),
    }


def read_events_from_file(file_path):  
    """
    Reads a JSON Lines file and returns only valid live events. 
    It uses the parse_live_event function to filter out any invalid lines, 
    and ensures that only well-formed event documents are returned for further processing.
    """
    events = []
    with file_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            event_doc = parse_live_event(line)
            if event_doc:
                events.append(event_doc)
    return events


def build_upserts(event_docs): 
    """
    Builds mongoDB upsert operations for a list of event documents, 
    to ensure that only new events are inserted into the database, 
    while existing events are left unchanged.
    """
    operations = []
    for event_doc in event_docs:
        operations.append(
            UpdateOne(           
                {"event_id": event_doc["event_id"]},     # Use event_id as the unique identifier for upsert
                {"$setOnInsert": event_doc},
                upsert=True,
            )
        )
    return operations


def main():
    live_root = Path(LIVE_EVENTS_DIR)
    if not live_root.exists():
        print(f"Folder not found: {live_root}")
        return

    mongo = MongoClient(MONGO_URI)
    events_raw = mongo[MONGO_DB][MONGO_COLLECTION]
    events_raw.create_index("event_id", unique=True)    # Enforced idempotency at the database level using a unique index on event_id

    total_read = 0
    total_inserted = 0
    total_existing = 0

    for jsonl_path in sorted(live_root.glob("*/events.jsonl")):
        event_docs = read_events_from_file(jsonl_path)
        operations = build_upserts(event_docs)

        if not operations:
            continue

        result = events_raw.bulk_write(operations, ordered=False)     # Perform bulk upsert operations to efficiently insert new events while ignoring existing ones
        read_count = len(operations)
        inserted_count = result.upserted_count
        existing_count = read_count - inserted_count

        total_read += read_count
        total_inserted += inserted_count
        total_existing += existing_count

        print(f"{jsonl_path}: read={read_count} inserted={inserted_count} existing={existing_count}")

    print(f"done total_read={total_read} total_inserted={total_inserted} total_existing={total_existing}")


if __name__ == "__main__":   
    main()
