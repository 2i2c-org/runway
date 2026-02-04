"""Fetch and transform KPI MAU data from the 2i2c cloud KPIs page."""

from __future__ import annotations

import re
import urllib.parse
import urllib.request
from typing import Optional

import pandas as pd


KPI_CLOUD_URL = "https://2i2c.org/kpis/cloud/"
MAU_CSV_FILENAME = "hub-activity-mau.csv"
MAU_CSV_LINK_RE = re.compile(
    r'href=["\']([^"\']*hub-activity-mau\.csv[^"\']*)["\']', re.IGNORECASE
)


def fetch_html(url: str = KPI_CLOUD_URL, timeout: int = 30) -> str:
    """Fetch HTML content from the KPI page."""
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8")


def _resolve_mau_csv_url(html: str, page_url: str) -> str:
    matches = MAU_CSV_LINK_RE.findall(html)
    candidates = [urllib.parse.urljoin(page_url, link) for link in matches]
    if not candidates:
        raise RuntimeError(f"No links to {MAU_CSV_FILENAME} found in KPI page HTML.")
    return candidates[-1]


def _load_mau_csv(csv_url: str) -> pd.DataFrame:
    return pd.read_csv(csv_url)


def build_mau_table(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and return the raw MAU table."""
    required = {"cluster", "date", "users"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"MAU CSV missing required columns: {sorted(missing)}")

    return df


def fetch_mau_table(
    url: str = KPI_CLOUD_URL,
    csv_url: Optional[str] = None,
    timeout: int = 30,
) -> pd.DataFrame:
    """Fetch KPI HTML and return the raw MAU table."""
    if csv_url is None:
        html = fetch_html(url=url, timeout=timeout)
        csv_url = _resolve_mau_csv_url(html, page_url=url)
    df = _load_mau_csv(csv_url)
    return build_mau_table(df)
