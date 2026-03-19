# Budget Updates Pipeline

Pulls deal data and MAU data from pre-built CSVs in [`2i2c-org/data-private`](https://github.com/2i2c-org/data-private), runs revenue projections, and uploads everything to the [budget spreadsheet](https://docs.google.com/spreadsheets/d/1IMIG2zrvMe-lSPngSLItCqZbP5Iw_6fNOPM5gZJSob8).
## Usage

You'll need two things for authentication, both should be environment variables or in a `.env` file:

- `GH_DATA_PRIVATE_TOKEN` - a GitHub token with read access to `2i2c-org/data-private`. Alternatively, authenticate via `gh auth login`.
- `GOOGLE_SERVICE_ACCOUNT_FILE` allows us to push to google sheets. See [this guide](https://docs.gspread.org/en/latest/oauth2.html) for some context. Use [this service account](https://console.cloud.google.com/iam-admin/serviceaccounts/details/113674037014124702779;edit=true?previousPage=%2Fapis%2Fcredentials%3Fproject%3Dtwo-eye-two-see&project=two-eye-two-see).

Then, to download the latest data and push to our Google Sheet:

```bash
nox -s sync
```

## What it does

The pipeline has a few phases (controlled by `scripts/sync.py`):

1. **Download** - download pre-built CSVs from `2i2c-org/data-private` via `gh release download`.
2. **Clean** - add a few extra columns we use to subset data etc.
3. **Split** - we split deals into three groups for inspection in the google sheet:
    - **Active** - Closed Won contracts that haven't expired + pipeline deals with complete data. These feed the revenue model.
    - **Removed** - pipeline deals missing dates or amount. These need fixing in HubSpot.
    - **Inactive** - everything else (expired contracts, etc).
4. **Validate** - we run validations throughout this process and print their status to the terminal.
5. **Model** - we run a little model of *expected* revenue based on deal amounts and their probability of success. We take the 10th, 50th, and 90th percentile of these results (these are called "pessimistic", "estimated", and "optimistic").
6. **Upload** - we upload the model results and several intermediate data representations for inspection.

The model results are then used by [our budget model](https://docs.google.com/spreadsheets/d/1IMIG2zrvMe-lSPngSLItCqZbP5Iw_6fNOPM5gZJSob8/edit?pli=1&gid=1812316711#gid=1812316711) to create runway projections.

## Tunable assumptions

There are a few hard-coded assumptions in `src/assumptions.py` just to keep things explicit, though honestly there are probably other assumptions we're not quite reasoning with explicitly. The budget sheet should list some of these as well.

## Changing things

There are some assumptions about **where to put data in the google sheet**, so if you need to move things around in the sheet (e.g. renaming tabs, moving important cells), then you'll probably need to update scripts here accordingly.
