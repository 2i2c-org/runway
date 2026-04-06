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
    "projection_monthly_revenue",
    "hs_deal_stage_probability",
    "effective_start_date",
    "effective_end_date",
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
    """Count calendar months a deal spans (inclusive of both start and end months).

    Parameters
    ----------
    start : pd.Series
        Start dates.
    end : pd.Series
        End dates.

    Returns
    -------
    pd.Series
        Number of months (minimum 1).
    """
    return np.maximum(
        (end.dt.year - start.dt.year) * 12 + (end.dt.month - start.dt.month) + 1, 1
    )


def _reorder_columns(df, front):
    front = [c for c in front if c in df.columns]
    rest = [c for c in df.columns if c not in set(front)]
    return df[front + rest]


def add_columns(df, projection_start):
    """Add effective dates, monthly_revenue, and projection_monthly_revenue.

    - effective_start/end_date: best available contract date
      (contract dates preferred over target dates)
    - monthly_revenue: full contract rate (amount / full duration)
    - projection_monthly_revenue: remaining revenue over remaining months

    Parameters
    ----------
    df : pd.DataFrame
        Raw deals from HubSpot.
    projection_start : pd.Timestamp
        First day of the projection start month.

    Returns
    -------
    pd.DataFrame
        Deals with derived columns added, columns reordered.
    """
    df = df.copy()

    # HubSpot sometimes returns "" instead of null
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    # Contract dates trump estimated target dates
    df["effective_start_date"] = df["contract_start_date"].fillna(
        df["target_start_date"]
    )
    df["effective_end_date"] = df["contract_end_date"].fillna(df["target_end_date"])

    start = pd.to_datetime(df["effective_start_date"], errors="coerce")
    end = pd.to_datetime(df["effective_end_date"], errors="coerce")
    amount = pd.to_numeric(df["amount"], errors="coerce")
    raw_collected = pd.to_numeric(
        df.get("amount_collected", 0), errors="coerce"
    )
    # null means HubSpot has no data; explicit $0 means nothing collected yet
    collected_is_missing = raw_collected.isna()
    raw_collected = raw_collected.fillna(0)

    has_data = start.notna() & end.notna() & amount.notna()
    total_months = months_between(start, end)

    # How much time has passed in the contract?
    # Use this to estimate collection when HubSpot has no data, and to calculate remaining months.
    ps = pd.Series(projection_start, index=df.index)
    months_elapsed = np.maximum(
        (ps.dt.year - start.dt.year) * 12 + (ps.dt.month - start.dt.month), 0
    )
    months_elapsed = np.minimum(months_elapsed, total_months)

    # When HubSpot has no collection data, assume we've collected proportional to how far through the contract we are.
    elapsed_fraction = (months_elapsed / total_months).clip(0, 1)
    estimated_collected = (amount * elapsed_fraction).round(0)
    missing_collected = has_data & collected_is_missing
    collected = raw_collected.where(~missing_collected, estimated_collected)
    df["amount_collected_is_estimated"] = missing_collected & has_data

    # Full contract rate: what the deal is worth per month over its full duration
    monthly = amount / total_months
    df["monthly_revenue"] = monthly.where(has_data).clip(lower=0).round(0)

    # Projection rate: what we expect to receive per month going forward
    remaining = (amount - collected).clip(lower=0)
    months_left = np.maximum(total_months - months_elapsed, 1)
    proj_monthly = remaining / months_left
    df["projection_monthly_revenue"] = (
        proj_monthly.where(has_data).clip(lower=0).round(0)
    )

    # Normalize dates to YYYY-MM-DD strings
    for col in [c for c in df.columns if "date" in c]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    return _reorder_columns(df, PREFERRED_COLUMNS)


def categorize_deals(df, projection_start):
    """Split deals into active, removed, and inactive.

    Parameters
    ----------
    df : pd.DataFrame
        Deals with derived columns (from `add_columns`).
    projection_start : pd.Timestamp
        First day of the projection start month.

    Returns
    -------
    dict
        Keys: "active", "removed", "inactive", each a DataFrame.
    """
    start = pd.to_datetime(df["effective_start_date"], errors="coerce")
    end = pd.to_datetime(df["effective_end_date"], errors="coerce")
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
            for col in ["effective_start_date", "effective_end_date", "amount"]
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
            "effective_start_date",
            "effective_end_date",
        ],
    )

    # Inactive = everything not active or removed (expired deals, etc.)
    used_ids = set(active["id"].astype(str)) | set(removed["id"].astype(str))
    inactive = df[~df["id"].astype(str).isin(used_ids)]

    return {"active": active, "removed": removed, "inactive": inactive}
