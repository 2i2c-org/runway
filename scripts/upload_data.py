#!/usr/bin/env python3
"""Upload HubSpot deals + KPI MAU data to Google Sheets."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from dotenv import load_dotenv

from src.kpi_mau_fetcher import fetch_mau_table
from src.sheets_uploader import get_sheets_client, upload_dataframe, upload_to_sheet

load_dotenv()

DATA_FILE = Path(__file__).parent.parent / "data" / "deals.csv"

if not DATA_FILE.exists():
    print(f"Error: Data file not found: {DATA_FILE}")
    print("Run 'nox -s download' first.")
    sys.exit(1)

SHEET_ID = "1IMIG2zrvMe-lSPngSLItCqZbP5Iw_6fNOPM5gZJSob8"
HUBSPOT_TAB = "Data: HubSpot"
MAU_TAB = "Data: MAUs"

df = pd.read_csv(DATA_FILE)
client = get_sheets_client()
mau_df = fetch_mau_table()

print(f"Uploading {len(df)} deals to sheet {SHEET_ID} tab '{HUBSPOT_TAB}'...")
upload_to_sheet(client, SHEET_ID, HUBSPOT_TAB, df)

print(f"Uploading KPI MAU table ({len(mau_df)} rows) to sheet {SHEET_ID}...")
upload_dataframe(client, SHEET_ID, mau_df, tab_name=MAU_TAB)

print(f"Done! Uploaded {len(df)} deals and {len(mau_df)} MAU rows.")
