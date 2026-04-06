# Budget Updates Pipeline

Projects 2i2c's financial runway by combining HubSpot deal data with usage-based MAU revenue, modeling uncertainty via Monte Carlo simulation, and uploading results to the [budget spreadsheet](https://docs.google.com/spreadsheets/d/1IMIG2zrvMe-lSPngSLItCqZbP5Iw_6fNOPM5gZJSob8).

## Quick start

You'll need two things for authentication, both should be environment variables or in a `.env` file:

- `GH_DATA_PRIVATE_TOKEN` — a GitHub token with read access to `2i2c-org/data-private`. Alternatively, authenticate via `gh auth login`.
- `GOOGLE_SERVICE_ACCOUNT_FILE` — allows us to push to Google Sheets. See [this guide](https://docs.gspread.org/en/latest/oauth2.html) for context. Use [this service account](https://console.cloud.google.com/iam-admin/serviceaccounts/details/113674037014124702779;edit=true?previousPage=%2Fapis%2Fcredentials%3Fproject%3Dtwo-eye-two-see&project=two-eye-two-see).

```bash
nox -s sync   # Run the full pipeline
nox -s test   # Run unit tests
```

## How it works

The pipeline orchestrator is [`scripts/sync.py`](scripts/sync.py) — **start there**. It reads as a top-to-bottom narrative where each step calls a helper, runs checks, and uploads results.

At a high level, the pipeline:

1. Downloads raw data from [`2i2c-org/data-private`](https://github.com/2i2c-org/data-private)
2. Cleans, derives revenue columns, and categorizes deals
3. Runs revenue projections (Monte Carlo simulation) and commitment projections (full contract obligations)
4. Uploads everything to the budget spreadsheet

## Repository structure

- `scripts/sync.py` — pipeline orchestrator (the narrative)
- `src/hubspot.py` — deal data: derived columns, categorization
- `src/revenue.py` — Monte Carlo simulation, monthly breakdowns, revenue-by-type grouping
- `src/checks.py` — inline data integrity checks
- `src/mau.py` — MAU-based revenue calculation
- `src/assumptions.py` — tunable constants (simulation runs, scenario percentiles, etc.)
- `tests/test_core.py` — unit and integration tests

## Changing things

- **Tunable assumptions** live in `src/assumptions.py`.
- **Google Sheet tab names** and the worksheet ID are constants at the top of `scripts/sync.py`. If you rename tabs or move important cells in the sheet, update those constants.
