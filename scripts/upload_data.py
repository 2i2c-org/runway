#!/usr/bin/env python3
"""Upload downloaded deals to Google Sheets."""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from dotenv import load_dotenv

from src.sheets_uploader import get_sheets_client, upload_to_sheet

load_dotenv()

DATA_FILE = Path(__file__).parent.parent / "data" / "deals.csv"

if not DATA_FILE.exists():
    print(f"Error: Data file not found: {DATA_FILE}")
    print("Run 'nox -s download' first.")
    sys.exit(1)

sheet_id = os.environ.get("GOOGLE_SHEET_ID")
tab_name = os.environ.get("GOOGLE_SHEET_TAB", "HubSpot Deals")

if not sheet_id:
    print("Error: Missing GOOGLE_SHEET_ID environment variable")
    sys.exit(1)

df = pd.read_csv(DATA_FILE)
print(f"Uploading {len(df)} deals to sheet {sheet_id} tab '{tab_name}'...")

client = get_sheets_client()
upload_to_sheet(client, sheet_id, tab_name, df)

print(f"Done! Uploaded {len(df)} deals.")
