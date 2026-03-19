"""Upload DataFrames to Google Sheets."""

import os
import gspread


def get_sheets_client():
    path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
    if not path:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_FILE env var.")
    return gspread.service_account(filename=path)


def upload_dataframe(client, sheet_id, df, *, tab_name):
    """Clear a Sheet tab and upload a DataFrame."""
    rows = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
    worksheet = client.open_by_key(sheet_id).worksheet(tab_name)
    worksheet.clear()
    worksheet.update(rows, value_input_option="USER_ENTERED")
