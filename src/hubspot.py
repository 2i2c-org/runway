"""HubSpot deals: load from CSV, add derived columns, and categorize."""

from pathlib import Path

import numpy as np
import pandas as pd

from src.assumptions import PIPELINE_STAGES

# Preferred column order for deal tabs (most useful first)
PREFERRED_COLUMNS = [
    "dealname",
    "revenue_type",
    "amount",
    "amount_collected",
    "monthly_revenue",
    "hs_deal_stage_probability",
    "use_start_date",
    "use_end_date",
    "dealstage",
]

DATA_DIR = Path(__file__).parent.parent / "data"


def load_deals():
    """Load deals from the pre-built CSV (downloaded from data-private repo)."""
    path = DATA_DIR / "deals_raw.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found. Run the download step first "
            "(gh release download hubspot-latest --repo 2i2c-org/data-private --dir data/)."
        )
    return pd.read_csv(path)


def months_between(start, end):
    """Count calendar months a deal spans (inclusive of both start and end months)."""
    return np.maximum(
        (end.dt.year - start.dt.year) * 12 + (end.dt.month - start.dt.month) + 1, 1
    )


def _reorder_columns(df, front):
    front = [c for c in front if c in df.columns]
    rest = [c for c in df.columns if c not in set(front)]
    return df[front + rest]


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
    monthly = amount / months_between(start, end)

    has_collected = has_data & (collected > 0)
    if has_collected.any():
        remaining = (amount - collected).clip(lower=0)
        months_left = months_between(pd.Series(projection_start, index=df.index), end)
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
    not_expired = end >= pd.Timestamp(projection_start)
    active = df[is_committed | (is_pipeline & complete & not_expired)].sort_values(
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
