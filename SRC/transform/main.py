import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from normalize import normalize_events
from rules import apply_rules
from warehouse import build_dimensions, build_facts, write_outputs


load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "events_raw")
WAREHOUSE_OUT_DIR = os.getenv("WAREHOUSE_OUT_DIR", "data/warehouse")
LATE_ARRIVAL_HOURS = int(os.getenv("LATE_ARRIVAL_HOURS", "24"))




def main():
    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB][MONGO_COLLECTION]

    docs = list(
        collection.find(
            {},
            {
                "_id": 0,
                "event_id": 1,
                "event_type": 1,
                "event_time": 1,
                "vendor": 1,
                "payload": 1,
                "ingested_at": 1,
            },
        )
    )
    if not docs:
        print("No events found in MongoDB collection.")
        return

    events_df = pd.DataFrame(docs)
    events_df = normalize_events(events_df)
    events_df = apply_rules(events_df, late_arrival_hours=LATE_ARRIVAL_HOURS)

    dim_customer, dim_product, dim_date = build_dimensions(events_df)
    facts_order, facts_payment, facts_refunds, facts_shipment, facts_order_daily = build_facts(events_df)

    out_dir = Path(WAREHOUSE_OUT_DIR)
    write_outputs(
        out_dir,
        dim_customer,
        dim_product,
        dim_date,
        facts_order,
        facts_payment,
        facts_refunds,
        facts_shipment,
        facts_order_daily,
    )

    print(f"Warehouse outputs written to: {out_dir}")
    print(
        f"events={len(events_df)} facts_order={len(facts_order)} "
        f"facts_payment={len(facts_payment)} facts_refunds={len(facts_refunds)} "
        f"facts_shipment={len(facts_shipment)} facts_order_daily={len(facts_order_daily)}"
    )


if __name__ == "__main__":
    main()
