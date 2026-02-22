import re

import pandas as pd


def _pick(payload, *keys):
    if not isinstance(payload, dict):
        return None
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


def _normalize_phone(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.startswith("+"):
        return "+" + re.sub(r"\D", "", text[1:])
    digits = re.sub(r"\D", "", text)
    return digits or None


def _extract_customer_fields(payload):
    if not isinstance(payload, dict):
        return None, None, None

    customer = payload.get("customer") if isinstance(payload.get("customer"), dict) else {}
    buyer = payload.get("buyer") if isinstance(payload.get("buyer"), dict) else {}

    email = (
        _pick(payload, "buyerEmail", "email")
        or _pick(customer, "email")
        or _pick(buyer, "email")
    )
    if isinstance(email, str):
        email = email.strip().lower() or None

    phone = (
        _pick(payload, "buyerPhone", "phone", "phoneNumber", "msisdn")
        or _pick(customer, "phone", "phoneNumber", "msisdn")
        or _pick(buyer, "phone", "phoneNumber", "msisdn")
    )
    phone = _normalize_phone(phone)

    customer_id = (
        _pick(payload, "customerId", "customer_id")
        or _pick(customer, "customerId", "customer_id", "id")
        or _pick(buyer, "customerId", "customer_id", "id")
    )
    if customer_id is None and email:
        customer_id = f"EMAIL::{email}"
    elif customer_id is None and phone:
        customer_id = f"PHONE::{phone}"

    return customer_id, email, phone


def _extract_primary_sku(payload):
    if not isinstance(payload, dict):
        return None

    items = payload.get("items")
    if isinstance(items, list) and items:
        first_item = items[0]
        if isinstance(first_item, dict):
            return first_item.get("sku") or first_item.get("productSku")

    items = payload.get("line_items")
    if isinstance(items, list) and items:
        first_item = items[0]
        if isinstance(first_item, dict):
            return first_item.get("sku")

    refunded_items = payload.get("refunded_items")
    if isinstance(refunded_items, list) and refunded_items:
        first_item = refunded_items[0]
        if isinstance(first_item, dict):
            return first_item.get("sku")

    items_refunded = payload.get("items_refunded")
    if isinstance(items_refunded, list) and items_refunded:
        first_item = items_refunded[0]
        if isinstance(first_item, dict):
            return first_item.get("sku")

    return None


def build_dimensions(df):
    customer_df = df.copy()
    customer_fields = customer_df["payload"].apply(_extract_customer_fields).apply(pd.Series)
    customer_fields.columns = ["customer_id", "email", "phone_number"]
    customer_df = pd.concat([customer_df, customer_fields], axis=1)

    dim_customer = (
        customer_df[customer_df["customer_id"].notna()]
        .sort_values(["customer_id", "event_time_ts", "ingested_at_ts"])
        .drop_duplicates(subset=["customer_id"], keep="last")
        [
            [
                "customer_id",
                "email",
                "phone_number",
                "vendor",
                "event_time_ts",
                "ingested_at_ts",
            ]
        ]
        .rename(
            columns={
                "vendor": "source_vendor",
                "event_time_ts": "last_event_time",
                "ingested_at_ts": "last_ingested_at",
            }
        )
        .reset_index(drop=True)
    )

    product_df = df.copy()
    product_df["product_id"] = product_df["payload"].apply(_extract_primary_sku)
    dim_product = (
        product_df[product_df["product_id"].notna()]
        .groupby("product_id", dropna=False)
        .agg(
            first_event_time=("event_time_ts", "min"),
            last_event_time=("event_time_ts", "max"),
            event_count=("event_id", "count"),
            order_count=("order_id", "nunique"),
        )
        .reset_index()
    )

    dim_date = (
        df[df["event_time_ts"].notna()]
        .assign(date_key=lambda x: x["event_time_ts"].dt.date)
        .groupby("date_key", dropna=False)
        .agg(event_count=("event_id", "count"))
        .reset_index()
    )

    return dim_customer, dim_product, dim_date


def build_facts(df):
    base_cols = [
        "event_id",
        "event_type",
        "event_time",
        "vendor",
        "order_id",
        "transaction_id",
        "tracking_id",
        "amount",
        "currency",
        "status",
        "region",
        "ingested_at",
        "quality_bucket",
        "is_late_arrival",
        "is_warning",
        "is_critical",
        "is_anomaly",
    ]
    facts_events = df[base_cols].copy()

    facts_payment = facts_events[facts_events["event_type"] == "payment_succeeded"].copy()
    facts_refunds = facts_events[facts_events["event_type"] == "refund_issued"].copy()
    facts_shipment = facts_events[facts_events["event_type"] == "shipment_updated"].copy()

    order_candidates = facts_events[facts_events["order_id"].notna()].copy()
    order_candidates["event_time_ts"] = pd.to_datetime(order_candidates["event_time"], errors="coerce", utc=True)
    facts_order = (
        order_candidates.sort_values(["order_id", "event_time_ts", "ingested_at"])
        .drop_duplicates(subset=["order_id"], keep="last")
        .drop(columns=["event_time_ts"])
        .reset_index(drop=True)
    )

    facts_order_daily = (
        order_candidates[order_candidates["event_time_ts"].notna()]
        .assign(event_date=lambda x: x["event_time_ts"].dt.date)
        .groupby(["event_date", "order_id"], dropna=False)
        .agg(
            event_count=("event_id", "count"),
            total_amount=("amount", "sum"),
            payment_events=("event_type", lambda s: (s == "payment_succeeded").sum()),
            refund_events=("event_type", lambda s: (s == "refund_issued").sum()),
            shipment_events=("event_type", lambda s: (s == "shipment_updated").sum()),
            warning_count=("is_warning", "sum"),
            critical_count=("is_critical", "sum"),
        )
        .reset_index()
    )

    return facts_order, facts_payment, facts_refunds, facts_shipment, facts_order_daily


def write_outputs(
    out_dir,
    dim_customer,
    dim_product,
    dim_date,
    facts_order,
    facts_payment,
    facts_refunds,
    facts_shipment,
    facts_order_daily,
):
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "dimensions").mkdir(exist_ok=True)
    (out_dir / "facts").mkdir(exist_ok=True)

    dim_customer.to_csv(out_dir / "dimensions" / "dim_customer.csv", index=False)
    dim_product.to_csv(out_dir / "dimensions" / "dim_product.csv", index=False)
    dim_date.to_csv(out_dir / "dimensions" / "dim_date.csv", index=False)

    facts_order.to_csv(out_dir / "facts" / "facts_order.csv", index=False)
    facts_payment.to_csv(out_dir / "facts" / "facts_payment.csv", index=False)
    facts_refunds.to_csv(out_dir / "facts" / "facts_refunds.csv", index=False)
    facts_shipment.to_csv(out_dir / "facts" / "facts_shipment.csv", index=False)
    facts_order_daily.to_csv(out_dir / "facts" / "facts_order_daily.csv", index=False)
