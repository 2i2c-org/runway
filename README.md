# HubSpot + KPI MAU to Google Sheets Sync

Syncs HubSpot deals data and KPI MAU summaries to our [budget projections google worksheet](https://docs.google.com/spreadsheets/d/1IMIG2zrvMe-lSPngSLItCqZbP5Iw_6fNOPM5gZJSob8/edit?gid=1551246221#gid=1551246221).
We use this to make revenue projections based on this data.

## What it does

**HubSpot Deals Data**:
1. Downloads all deals from HubSpot (excludes "closedlost")
2. Validates data matches expected schema
3. Uploads deals to a Google Sheet tab

**Hub MAUs Data**:
1. Downloads the KPI MAU CSV linked from [our KPIs page](https://2i2c.org/kpis/cloud/)
2. Uploads the raw MAU table to a Google Sheet tab

## Setup

Set these environment variables (or add to `.env` file):

- `HUBSPOT_ACCESS_TOKEN`: HubSpot private app access token
- `GOOGLE_SERVICE_ACCOUNT_FILE`: Path to Google service account JSON file

All other information about where to put data (tab names etc) is hard-coded in the scripts.

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

## Testing

There is lightweight testing to ensure that the underlying data structure hasn't changed in HubSpot etc.

If we change the columns/dropdown values in HubSpot this will start failing, and we need to update `data/schema.json` accordingly.

## GitHub Actions

The workflow runs weekly. It downloads, tests, then uploads.
