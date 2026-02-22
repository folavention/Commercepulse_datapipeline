import pandas as pd


def apply_rules(df, late_arrival_hours=24):
    df = df.copy()
    event_type = df["event_type"].fillna("").astype(str)

    order_required_events = {
        "payment_succeeded",
        "refund_issued",
        "shipment_updated",
        "order_updated",
    }

    df["is_late_arrival"] = (
        df["event_time_ts"].notna()
        & df["ingested_at_ts"].notna()
        & ((df["ingested_at_ts"] - df["event_time_ts"]).dt.total_seconds() > late_arrival_hours * 3600)
    )
    df["is_missing_event_id"] = df["event_id"].isna() | (df["event_id"].astype(str).str.strip() == "")
    df["is_missing_order_id"] = df["order_id"].isna()
    df["is_unknown_vendor"] = df["vendor"].isna() | (df["vendor"].astype(str).str.strip() == "")
    df["is_negative_amount"] = df["amount"].notna() & (df["amount"] < 0)
    df["is_invalid_currency"] = df["currency"].notna() & ~df["currency"].isin(["NGN", "USD"])
    df["is_bad_event_time"] = df["event_time_ts"].isna()
    df["is_bad_ingested_at"] = df["ingested_at_ts"].isna()
    df["is_order_required_event"] = event_type.isin(order_required_events)
    df["is_missing_order_id_critical"] = df["is_missing_order_id"] & df["is_order_required_event"]
    df["is_missing_order_id_warning"] = df["is_missing_order_id"] & ~df["is_order_required_event"]

    df["is_critical"] = (
        df["is_missing_event_id"]
        | df["is_unknown_vendor"]
        | df["is_negative_amount"]
        | df["is_invalid_currency"]
        | df["is_bad_event_time"]
        | df["is_bad_ingested_at"]
        | df["is_missing_order_id_critical"]
    )
    df["is_warning"] = df["is_late_arrival"] | df["is_missing_order_id_warning"]
    df["is_anomaly"] = df["is_critical"] | df["is_warning"]

    df["quality_bucket"] = "valid"
    df.loc[df["is_warning"], "quality_bucket"] = "warning"
    df.loc[df["is_critical"], "quality_bucket"] = "critical"
   
    return df
