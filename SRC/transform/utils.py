import pandas as pd


def to_ts(value):
    if value is None:
        return pd.NaT
    return pd.to_datetime(value, errors="coerce", utc=True)


def pick(payload, *keys):
    if not isinstance(payload, dict):
        return None
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None


def extract_order_id(payload):
    if not isinstance(payload, dict):
        return None
    if "order_id" in payload:
        return payload["order_id"]
    if "orderRef" in payload:
        return payload["orderRef"]
    if "order" in payload:
        order = payload["order"]
        if isinstance(order, dict):
            return order.get("id")
        return order
    return None