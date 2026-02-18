"""Fetch and transform KPI monthly users data from the 2i2c cloud KPIs page."""

from __future__ import annotations

import re
import urllib.parse
import urllib.request
from typing import Optional

import pandas as pd


KPI_CLOUD_URL = "https://2i2c.org/kpis/cloud/"
HUB_ACTIVITY_CSV_FILENAME = "hub-activity.csv"
HUB_ACTIVITY_CSV_LINK_RE = re.compile(
    r'href=["\']([^"\']*hub-activity\.csv[^"\']*)["\']', re.IGNORECASE
)


def fetch_html(url: str = KPI_CLOUD_URL, timeout: int = 30) -> str:
    """Fetch HTML content from the KPI page."""
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8")


def _resolve_hub_activity_csv_url(html: str, page_url: str) -> str:
    matches = HUB_ACTIVITY_CSV_LINK_RE.findall(html)
    candidates = [urllib.parse.urljoin(page_url, link) for link in matches]
    if not candidates:
        raise RuntimeError(
            f"No links to {HUB_ACTIVITY_CSV_FILENAME} found in KPI page HTML."
        )
    return candidates[-1]


def _load_hub_activity_csv(csv_url: str) -> pd.DataFrame:
    return pd.read_csv(csv_url)


def build_mau_table(df: pd.DataFrame) -> pd.DataFrame:
    """Build monthly users table aligned with KPI dashboard source logic."""
    required = {"cluster", "hub", "date", "users", "timescale"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"MAU CSV missing required columns: {sorted(missing)}")

    df = df.copy()

    # Match KPI dashboard filtering logic
    df = df[~df["hub"].astype(str).str.contains("staging", case=False, na=False)]
    df = df[~df["cluster"].astype(str).str.contains("prometheus", case=False, na=False)]
    df = df[~((df["cluster"] == "utoronto") & (df["hub"] == "highmem"))]

    # Use monthly users and collapse daily rows to one row per month
    df = df[df["timescale"] == "monthly"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["users"] = pd.to_numeric(df["users"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["date"] = df["date"].dt.to_period("M").dt.to_timestamp()

    out = (
        df.groupby(["cluster", "hub", "date"], as_index=False)["users"]
        .max()
        .sort_values(["cluster", "hub", "date"])
        .reset_index(drop=True)
    )
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[["cluster", "hub", "date", "users"]]


def fetch_mau_table(
    url: str = KPI_CLOUD_URL,
    csv_url: Optional[str] = None,
    timeout: int = 30,
) -> pd.DataFrame:
    """Fetch KPI HTML and return a monthly users table by cluster/hub/month."""
    if csv_url is None:
        html = fetch_html(url=url, timeout=timeout)
        csv_url = _resolve_hub_activity_csv_url(html, page_url=url)
    df = _load_hub_activity_csv(csv_url)
    return build_mau_table(df)
