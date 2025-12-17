#!/usr/bin/env python3
"""
Sync HubSpot deals data to Google Sheets.

This script fetches the latest HubSpot deals data and uploads it to a
Google Sheet. Configuration is via environment variables (see .env.example).

Usage:
    python sync_hubspot_to_sheets.py
"""
import os

from dotenv import load_dotenv

from src.hubspot_fetcher import fetch_deals, load_hubspot_token
from src.sheets_uploader import get_sheets_client, upload_deals_to_sheet


def main():
    """Main execution function."""
    # Load environment variables
    load_dotenv()

    # Get configuration
    hubspot_token = load_hubspot_token()
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    tab_name = os.environ.get("GOOGLE_SHEET_TAB", "HubSpot Deals")

    if not sheet_id:
        raise RuntimeError("Missing GOOGLE_SHEET_ID environment variable")

    # Fetch HubSpot deals
    print("Fetching deals from HubSpot...")
    view_id = os.environ.get("HUBSPOT_VIEW_ID")
    deals_data = fetch_deals(hubspot_token, view_id=view_id)
    total = deals_data["meta"]["total"]
    source = deals_data["meta"].get("source", "unknown")
    if view_id:
        print(f"Fetched {total} deals from view {view_id}")
    else:
        print(f"Fetched {total} deals using {source}")

    # Get Google Sheets client
    print("Connecting to Google Sheets...")
    client = get_sheets_client()

    # Upload to Google Sheets
    print(f"Uploading to Google Sheet (ID: {sheet_id}, Tab: {tab_name})...")
    upload_deals_to_sheet(
        client=client, sheet_id=sheet_id, tab_name=tab_name, deals_data=deals_data
    )

    print(f"✓ Successfully uploaded {total} deals to Google Sheets")


if __name__ == "__main__":
    main()
