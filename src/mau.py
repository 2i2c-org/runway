"""MAU data: fetch, clean, and calculate revenue from monthly active users."""

import pandas as pd

from src.assumptions import MAU_EXCLUDED_CLUSTER_SUBSTRINGS

# Re-built daily via CRON — unique user counts per cluster
MAUS_CSV_URL = "https://github.com/2i2c-org/data-maus/releases/download/latest/maus-unique-by-cluster.csv"


def build_mau_table(df=None):
    """Fetch and build monthly unique users table by cluster/month."""
    if df is None:
        df = pd.read_csv(MAUS_CSV_URL)
    required = {"cluster", "date", "unique_users"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"MAU CSV missing required columns: {sorted(missing)}")

    # Drop excluded clusters (e.g. prometheus, hhmi)
    for sub in MAU_EXCLUDED_CLUSTER_SUBSTRINGS:
        df = df[~df["cluster"].astype(str).str.contains(sub, case=False, na=False)]

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["unique_users"] = pd.to_numeric(df["unique_users"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["date"] = df["date"].dt.to_period("M").dt.to_timestamp()

    out = (
        df.groupby(["cluster", "date"], as_index=False)["unique_users"]
        .max()
        .sort_values(["cluster", "date"])
        .reset_index(drop=True)
    )
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[["cluster", "date", "unique_users"]]


def cluster_revenue(mau_count):
    """Tiered MAU pricing per cluster. Mirrors the spreadsheet formula.

    Effective rates: $10/user up to 10, $5 up to 100, $2.50 up to 1k,
    $1.25 up to 10k, free above 10k.

    Ref: https://compass.2i2c.org/business-development/pricing-strategy/#usage-costs
    """
    if mau_count <= 0:
        return 0.0
    revenue = (
        max(0, mau_count) * 10
        + max(0, mau_count - 10) * -5
        + max(0, mau_count - 100) * -2.5
        + max(0, mau_count - 1000) * -1.25
        + max(0, mau_count - 10000) * -1.25
    )
    return max(revenue, 0.0)


def calculate_revenue(mau_df):
    """Add per-cluster revenue column and return mean monthly total (last 12 months)."""
    df = mau_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["unique_users"] = pd.to_numeric(df["unique_users"], errors="coerce").fillna(0)
    df["cluster_revenue"] = df["unique_users"].apply(cluster_revenue)

    # Sum revenue across clusters for each month, then average the last 12 months
    monthly_totals = df.groupby("date")["cluster_revenue"].sum().sort_index()
    last_12 = monthly_totals.iloc[-12:]
    mean_monthly = round(last_12.mean(), 0)
    return df, mean_monthly
