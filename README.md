# HubSpot + KPI MAU to Google Sheets Sync

Syncs HubSpot deals and KPI MAU data to our [budget sheet](https://docs.google.com/spreadsheets/d/1IMIG2zrvMe-lSPngSLItCqZbP5Iw_6fNOPM5gZJSob8/edit?gid=1551246221#gid=1551246221)

## Setup

Set these environment variables (or use `.env`):

- `HUBSPOT_ACCESS_TOKEN`
- `GOOGLE_SERVICE_ACCOUNT_FILE`

## Commands

```bash
# Fetch HubSpot deals into data/deals.csv
nox -s download

# Run tests
nox -s test

# Run tests, then upload HubSpot + MAU tables
nox -s update

# Download, then update
nox -s download-and-update
```

## Architecture (short)

- `scripts/download_data.py`: fetch HubSpot deals, write `data/deals.csv`.
- `scripts/upload_data.py`: load deals, fetch MAU table, upload both tabs.
- `src/hubspot_fetcher.py`: HubSpot API fetch + deal transforms.
- `src/kpi_mau_fetcher.py`: KPI HTML/CSV fetch + MAU transform.
- `src/sheets_uploader.py`: sheet upload + HubSpot row formatting.
- `src/hardcoded_assumptions.py`: explicit MAU exclusions/business assumptions.

## Maintainer notes

- If HubSpot columns or stages change intentionally, update `data/schema.json` and tests.
- If MAU exclusions change, edit `src/hardcoded_assumptions.py` and `tests/test_kpi_mau_fetcher.py` together.
- If sheet destination/tab names change, edit constants in `scripts/upload_data.py`.

## CI

GitHub Actions runs weekly (Monday 09:00 UTC):

1. `nox -s download`
2. `nox -s update`
