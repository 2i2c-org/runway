"""Upload data to Google Sheets."""

import os
from typing import Any, Dict, List

import gspread
import pandas as pd


def get_sheets_client():
    """Return authenticated Google Sheets client."""
    service_account_file = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
    if not service_account_file:
        raise RuntimeError(
            "Missing GOOGLE_SERVICE_ACCOUNT_FILE environment variable. "
            "Add it to your .env file."
        )

    return gspread.service_account(filename=service_account_file)


def deals_to_rows(deals_data: Dict[str, Any]) -> List[List[str]]:
    """Convert HubSpot deals data to rows for Google Sheets."""
    results = deals_data.get("results", [])

    if not results:
        return [["No data available"]]

    # Convert to pandas DataFrame
    df = pd.json_normalize(results)
    df.columns = df.columns.str.replace("properties.", "", regex=False)

    # Get the column order from the original properties (preserve API order)
    # Start with 'id', then add property columns in the order they appear in first result
    if results:
        first_props = results[0].get("properties", {})
        desired_order = ["id"] + list(first_props.keys())
        # Only include columns that exist in the dataframe
        column_order = [col for col in desired_order if col in df.columns]
        # Add any remaining columns that weren't in the first result
        remaining_cols = [col for col in df.columns if col not in column_order]
        column_order.extend(remaining_cols)
        df = df[column_order]

    # Convert target_start_date to datetime for sorting
    if "target_start_date" in df.columns:
        df["target_start_date"] = pd.to_datetime(
            df["target_start_date"], errors="coerce"
        )
        # Sort by target start date ascending (earliest first)
        df = df.sort_values("target_start_date", ascending=True)

    # Convert to rows (header + data)
    rows = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()

    return rows


def upload_deals_to_sheet(
    client, sheet_id: str, tab_name: str, deals_data: Dict[str, Any]
) -> None:
    """Upload deals data to a Google Sheet."""
    # Open the sheet
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet(tab_name)

    # Convert deals to rows
    rows = deals_to_rows(deals_data)

    # Clear existing data and upload new data
    worksheet.clear()
    # Use USER_ENTERED so Google Sheets parses dates, numbers, etc. automatically
    worksheet.update(rows, value_input_option="USER_ENTERED")
