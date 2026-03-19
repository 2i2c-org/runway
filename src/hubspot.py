"""HubSpot deals: fetch, add derived columns, and categorize."""

import os
from datetime import datetime

import numpy as np
import pandas as pd
from hubspot import HubSpot
from hubspot.crm.deals import ApiException as DealsApiException
from hubspot.crm.pipelines import ApiException as PipelinesApiException

from src.assumptions import AVG_DAYS_PER_MONTH, PIPELINE_STAGES

# HubSpot returns raw stage IDs (not labels) - "closedlost" is the internal ID
EXCLUDED_DEALSTAGES = {"closedlost"}
DEAL_PROPERTIES = [
    "dealname",
    "dealstage",
    "closedate",
    "hs_deal_stage_probability",
    "amount",
    "amount_collected",
    "target_start_date",
    "target_end_date",
    "contract_start_date",
    "contract_end_date",
    "notes_last_updated",
]

# Preferred column order for deal tabs (most useful first)
PREFERRED_COLUMNS = [
    "dealname",
    "amount",
    "amount_collected",
    "monthly_revenue",
    "hs_deal_stage_probability",
    "use_start_date",
    "use_end_date",
    "dealstage",
]


def _month_start(dt):
    return pd.Timestamp(dt.year, dt.month, 1)


def _months_between(start, end):
    days = (end - start).dt.days.fillna(0)
    return np.maximum((days / AVG_DAYS_PER_MONTH).round(), 1)


def _reorder_columns(df, front):
    front = [c for c in front if c in df.columns]
    rest = [c for c in df.columns if c not in set(front)]
    return df[front + rest]


def _get_dealstage_labels(client):
    pipelines = client.crm.pipelines.pipelines_api.get_all("deals")
    labels = {}
    for pipeline in pipelines.results:
        for stage in pipeline.stages:
            labels[str(stage.id)] = stage.label
    return labels


def fetch_deals():
    """Fetch all deals, filter out Closed Lost, resolve stage labels."""
    token = os.environ.get("HUBSPOT_ACCESS_TOKEN") or os.environ.get("HUBSPOT_TOKEN")
    if not token:
        raise RuntimeError("Missing HUBSPOT_ACCESS_TOKEN. Add it to your .env file.")
    client = HubSpot(access_token=token)

    try:
        deals = client.crm.deals.get_all(properties=DEAL_PROPERTIES)
    except DealsApiException as err:
        raise RuntimeError(f"HubSpot API error: {err}") from err

    # HubSpot SDK returns nested dicts like {"properties.dealname": "..."}
    # we flatten them to make them easier to work with
    df = pd.json_normalize([deal.to_dict() for deal in deals])
    df.columns = df.columns.str.replace("properties.", "", regex=False)

    keep_cols = ["id"] + DEAL_PROPERTIES
    df = df[[c for c in keep_cols if c in df.columns]]

    original_count = len(df)
    df = df[~df["dealstage"].isin(EXCLUDED_DEALSTAGES)]
    filtered_out = original_count - len(df)

    stages = _get_dealstage_labels(client)
    df["dealstage"] = df["dealstage"].map(stages).fillna(df["dealstage"])

    return df, {"total": len(df), "filtered_out": filtered_out}


def add_columns(df, projection_start):
    """Add use_start/end_date and monthly_revenue columns.

    projection_start: first month of projections. For partially-collected deals,
    remaining revenue is spread from this date onwards.
    """
    df = df.copy()

    # HubSpot sometimes returns "" instead of null
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    # Contract dates trump target dates
    df["use_start_date"] = df["contract_start_date"].fillna(df["target_start_date"])
    df["use_end_date"] = df["contract_end_date"].fillna(df["target_end_date"])

    # monthly_revenue = amount / contract_months normally.
    # If amount_collected > 0, spread remaining amount over months from
    # projection_start onwards and shift use_start_date to match.
    start = pd.to_datetime(df["use_start_date"], errors="coerce")
    end = pd.to_datetime(df["use_end_date"], errors="coerce")
    amount = pd.to_numeric(df["amount"], errors="coerce")
    collected = pd.to_numeric(df.get("amount_collected", 0), errors="coerce").fillna(0)

    has_data = start.notna() & end.notna() & amount.notna()
    monthly = amount / _months_between(start, end)

    has_collected = has_data & (collected > 0)
    if has_collected.any():
        remaining = (amount - collected).clip(lower=0)
        months_left = _months_between(pd.Series(projection_start, index=df.index), end)
        # .where keeps values where condition is True, replaces where False
        monthly = monthly.where(~has_collected, remaining / months_left)
        df.loc[has_collected, "use_start_date"] = str(projection_start.date())

    df["monthly_revenue"] = monthly.where(has_data).clip(lower=0).round(0)

    # Normalize dates to YYYY-MM-DD strings
    for col in [c for c in df.columns if "date" in c]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    return _reorder_columns(df, PREFERRED_COLUMNS)


def categorize_deals(df, projection_start):
    """Split deals into active, removed, and inactive. Returns a dict."""
    start = pd.to_datetime(df["use_start_date"], errors="coerce")
    end = pd.to_datetime(df["use_end_date"], errors="coerce")
    amount = pd.to_numeric(df["amount"], errors="coerce")
    complete = start.notna() & end.notna() & amount.notna()
    is_pipeline = df["dealstage"].isin(PIPELINE_STAGES)

    # Active = Closed Won (not expired) + pipeline deals with complete data
    is_committed = (
        (df["dealstage"] == "Closed Won")
        & (end >= pd.Timestamp(projection_start))
        & complete
    )
    active = df[is_committed | (is_pipeline & complete)].sort_values(
        "monthly_revenue", ascending=False, na_position="last"
    )

    # Removed = pipeline deals missing data (action item: fix in HubSpot)
    removed = df[is_pipeline & ~complete].copy()
    removed["missing_fields"] = removed.apply(
        lambda row: ", ".join(
            col
            for col in ["use_start_date", "use_end_date", "amount"]
            if pd.isna(row.get(col))
        ),
        axis=1,
    )
    removed = _reorder_columns(
        removed,
        [
            "missing_fields",
            "dealname",
            "dealstage",
            "amount",
            "use_start_date",
            "use_end_date",
        ],
    )

    # Inactive = everything not active or removed (expired deals, etc.)
    used_ids = set(active["id"].astype(str)) | set(removed["id"].astype(str))
    inactive = df[~df["id"].astype(str).isin(used_ids)]

    return {"active": active, "removed": removed, "inactive": inactive}
