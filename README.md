# HubSpot to Google Sheets Sync

This repository provides a script that syncs HubSpot deals data to a Google Sheet.

- Fetches latest HubSpot deals data using the HubSpot API
- Supports both HubSpot API client and REST API fallback
- Uploads data to a specified Google Sheet tab

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment variables

This expects a few environment variables to be defined in your global environment or in an `.env` file:
- `HUBSPOT_ACCESS_TOKEN`: Your HubSpot private app access token
- `GOOGLE_SERVICE_ACCOUNT_FILE`: Path to your Google service account JSON file
- `GOOGLE_SHEET_ID`: The ID of your target Google Sheet (from the URL)
- `GOOGLE_SHEET_TAB`: The name of the sheet tab (default: "HubSpot Deals")

## Usage

### Using nox (recommended)

Run the sync with nox, which automatically handles the environment:

```bash
nox -s update
```

The script will:
1. Fetch deals from HubSpot (using view filters if `HUBSPOT_VIEW_ID` is set)
2. Connect to your Google Sheet
3. Clear the specified tab
4. Upload the latest data
5. Deals are sorted by `target_start_date` (latest first)

### Using HubSpot Views

You can configure the script to use a specific HubSpot view's filters by setting `HUBSPOT_VIEW_ID` in your `.env` file:

```bash
# Get the view ID from your HubSpot view URL
# https://app-na2.hubspot.com/contacts/{PORTAL_ID}/objects/0-3/views/{VIEW_ID}/list
HUBSPOT_VIEW_ID=341811851
```

When set, the script will:
1. Fetch the view's filter configuration from HubSpot
2. Apply those filters server-side using the Search API
3. Only fetch deals that match your view's criteria

This is more efficient than fetching all deals and filtering client-side.

## Architecture

- `src/hubspot_fetcher.py` - HubSpot API integration
- `src/sheets_uploader.py` - Google Sheets upload
- `sync_hubspot_to_sheets.py` - Main orchestration script
