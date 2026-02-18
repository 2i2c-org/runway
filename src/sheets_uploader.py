"""Upload DataFrames to Google Sheets."""

import os
from typing import List, Optional

import gspread
import pandas as pd

from src.columns import add_use_date_columns


# HubSpot property names, in the order to upload to Sheets
COLUMN_ORDER = [
    "id",
    "dealname",
    "dealstage",
    "closedate",
    "hs_deal_stage_probability",
    "amount",
    "target_start_date",
    "target_end_date",
    "contract_start_date",
    "contract_end_date",
    "use_start_date",
    "use_end_date",
    "notes_last_updated",
]

DATE_COLUMNS = [
    "closedate",
    "target_start_date",
    "target_end_date",
    "contract_start_date",
    "contract_end_date",
    "use_start_date",
    "use_end_date",
    "notes_last_updated",
]


def _to_iso_date_strings(series: pd.Series) -> pd.Series:
    """Parse mixed ISO date/datetime values and return YYYY-MM-DD strings."""
    parsed = pd.to_datetime(series, errors="coerce", format="ISO8601", utc=True)
    return parsed.dt.strftime("%Y-%m-%d")


def get_sheets_client():
    """Return authenticated Google Sheets client."""
    service_account_file = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
    if not service_account_file:
        raise RuntimeError(
            "Missing GOOGLE_SERVICE_ACCOUNT_FILE environment variable. "
            "Add it to your .env file."
        )
    return gspread.service_account(filename=service_account_file)


def format_for_sheets(df: pd.DataFrame) -> List[List[str]]:
    """Format DataFrame for Google Sheets upload.

    - Formats all date columns to YYYY-MM-DD
    - Sorts by close date (most recent first)
    - Preserves HubSpot property names as headers
    - Returns list of rows including header
    """
    df = add_use_date_columns(df)

    # Ensure all expected columns exist
    for col in COLUMN_ORDER:
        if col not in df.columns:
            df[col] = ""

    # Format date columns to YYYY-MM-DD
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = _to_iso_date_strings(df[col])

    # Sort by close date descending
    if "closedate" in df.columns:
        df["_sort_date"] = pd.to_datetime(
            df["closedate"], errors="coerce", format="ISO8601", utc=True
        )
        df = df.sort_values("_sort_date", ascending=False, na_position="last")
        df = df.drop(columns=["_sort_date"])

    # Select columns in expected order, preserving HubSpot property names as headers
    df = df[[col for col in COLUMN_ORDER if col in df.columns]]

    # Convert to list of lists
    return [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()


def dataframe_to_rows(df: pd.DataFrame) -> List[List[object]]:
    """Convert a DataFrame to rows for Sheets upload."""
    df = df.copy()
    df = df.where(pd.notnull(df), "")
    return [df.columns.tolist()] + df.values.tolist()


def _get_worksheet(spreadsheet, tab_name: Optional[str], worksheet_gid: Optional[int]):
    if worksheet_gid is not None:
        try:
            worksheet = spreadsheet.get_worksheet_by_id(int(worksheet_gid))
        except AttributeError:
            worksheet = None
        if worksheet is not None:
            return worksheet
    if tab_name:
        return spreadsheet.worksheet(tab_name)
    raise RuntimeError("Worksheet not found: provide worksheet gid or tab name.")


def upload_to_sheet(client, sheet_id: str, tab_name: str, df: pd.DataFrame) -> None:
    """Upload DataFrame to a Google Sheet tab."""
    rows = format_for_sheets(df)

    worksheet = client.open_by_key(sheet_id).worksheet(tab_name)
    worksheet.clear()
    worksheet.update(rows, value_input_option="USER_ENTERED")


def upload_dataframe(
    client,
    sheet_id: str,
    df: pd.DataFrame,
    *,
    worksheet_gid: Optional[int] = None,
    tab_name: Optional[str] = None,
) -> None:
    """Upload DataFrame to a Google Sheet tab by gid (preferred) or name."""
    rows = dataframe_to_rows(df)
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = _get_worksheet(
        spreadsheet, tab_name=tab_name, worksheet_gid=worksheet_gid
    )
    worksheet.clear()
    worksheet.update(rows, value_input_option="USER_ENTERED")
