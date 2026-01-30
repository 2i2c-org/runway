# HubSpot to Google Sheets Sync

Syncs HubSpot deals data to a Google Sheet.

## What it does

1. Downloads all deals from HubSpot (excludes "closedlost")
2. Validates data matches expected schema
3. Uploads to a Google Sheet tab

## Setup

Set these environment variables (or add to `.env` file):

- `HUBSPOT_ACCESS_TOKEN`: HubSpot private app access token
- `GOOGLE_SERVICE_ACCOUNT_FILE`: Path to Google service account JSON file
- `GOOGLE_SHEET_ID`: Target Google Sheet ID (from the URL)
- `GOOGLE_SHEET_TAB`: Sheet tab name (default: "HubSpot Deals")

## Usage

```bash
# Download data from HubSpot
nox -s download

# Run tests (validates schema)
nox -s test

# Upload to Google Sheets
nox -s update

# Do all three in sequence
nox -s download-and-update
```

## Schema Validation

The test suite validates that downloaded data matches `data/schema.json`:
- Expected columns are present
- Deal stage values match expected set

If HubSpot's schema changes, update `data/schema.json` accordingly.

## GitHub Actions

The workflow runs weekly (Monday 9am UTC) or manually. It downloads, tests, then uploads.

**Required secrets:**
- `HUBSPOT_ACCESS_TOKEN`
- `GOOGLE_SERVICE_ACCOUNT_JSON` (full JSON content)
- `GOOGLE_SHEET_ID`

**Optional variables:**
- `GOOGLE_SHEET_TAB` (default: "HubSpot Deals")

## Files

- `src/hubspot_fetcher.py` - Fetches deals from HubSpot
- `src/sheets_uploader.py` - Formats and uploads to Google Sheets
- `scripts/download_data.py` - Downloads deals to `data/deals.csv`
- `scripts/upload_data.py` - Uploads from `data/deals.csv`
- `data/schema.json` - Expected columns and deal stages
