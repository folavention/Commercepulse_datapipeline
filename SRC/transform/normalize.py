import pandas as pd
from utils import extract_order_id, pick, to_ts

def normalize_events(df):
    df = df.copy()
    df["payload"] = df["payload"].where(df["payload"].notna(), None)

    df["event_time_ts"] = df["event_time"].apply(to_ts)
    df["ingested_at_ts"] = df["ingested_at"].apply(to_ts)
    df["order_id"] = df["payload"].apply(extract_order_id)
    df["transaction_id"] = df["payload"].apply(lambda p: pick(p, "transaction_id", "txRef", "txn"))
    df["tracking_id"] = df["payload"].apply(lambda p: pick(p, "tracking_code", "tracking"))
    df["amount"] = (
        df["payload"].apply(lambda p: pick(p, "amountPaid", "amount", "refundAmount", "amt", "totalAmount", "total"))
    )
    df["currency"] = df["payload"].apply(lambda p: pick(p, "currencyCode", "currency", "ccy"))
    df["status"] = df["payload"].apply(lambda p: pick(p, "payment_status", "status", "state", "shipment_status"))
    df["region"] = df["payload"].apply(
        lambda p: pick(
            p,
            "state",
            "region",
        )
        or (p.get("geo", {}).get("region") if isinstance(p, dict) and isinstance(p.get("geo"), dict) else None)
        or (p.get("address", {}).get("city") if isinstance(p, dict) and isinstance(p.get("address"), dict) else None)
    )

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    return df