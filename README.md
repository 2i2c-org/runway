# Budget Updates Pipeline

Projects 2i2c's financial runway by combining HubSpot deal data with usage-based MAU revenue, project revenue into the future, and upload results to the [budget spreadsheet](https://docs.google.com/spreadsheets/d/1IMIG2zrvMe-lSPngSLItCqZbP5Iw_6fNOPM5gZJSob8).

## Quick start

You'll need two things for authentication, both should be environment variables or in a `.env` file:

- `GH_DATA_PRIVATE_TOKEN` - a GitHub token with read access to `2i2c-org/data-private`. Alternatively, authenticate via `gh auth login`.
- `GOOGLE_SERVICE_ACCOUNT_FILE` - allows us to push to Google Sheets. See [this guide](https://docs.gspread.org/en/latest/oauth2.html) for context. Use [this service account](https://console.cloud.google.com/iam-admin/serviceaccounts/details/113674037014124702779;edit=true?previousPage=%2Fapis%2Fcredentials%3Fproject%3Dtwo-eye-two-see&project=two-eye-two-see).

```bash
nox -s sync   # Run the full pipeline (both downloads and uploads)
nox -s test   # Run unit tests
```

## How it works

The pipeline orchestrator is [`scripts/sync.py`](scripts/sync.py). It reads as a top-to-bottom narrative where each step calls a helper, runs checks, and uploads results.

At a high level, we have these data sources:

- **`2i2c-org/data-private`**: A private repository that publishes the following two datasets each day in a release:
  - **Prometheus**: Unique MAUs per cluster. Used to calculate MAUs.
  - **HubSpot**: Deals and revenue collected for prospective and committed contracts.
- **Google Sheets**: We load in some data from our budget google sheet (like our cash on hand).

The pipeline does the following:

1. Downloads raw data from [`2i2c-org/data-private`](https://github.com/2i2c-org/data-private)
2. Cleans, derives revenue columns, and categorizes deals
3. Runs revenue projections (Monte Carlo simulation) and commitment projections (full contract obligations)
4. Flags deals worth reviewing more closely.
5. Calculates a few indicators for financial health, with red/yellow/green status.
6. Uploads everything to the budget spreadsheet

## Indicators for financial health

We calculate a few indicators, with status indicators for each.
The goal is to understand our financial health and trigger actions based on these metrics.
These should help us know how to "feel" about the numbers here.
These are all in `src/indicators.py`.

**Note**: We assume a **6-9 month sales cycle**, which is relevant to things like "how many months of runway is OK?"

**TODO**: These metrics currently define "good" as "covering costs." In the future we should incorporate explicit revenue targets so that the baseline is "hitting our growth goals," not just "not losing money."

**Committed revenue runway** - How long does our money last? Projects forward from net assets using committed revenue minus total costs.

- 🔴 **< 6 months**: <1 sales cycle remaining. Freeze spending, evaluate team size.
- 🟡 **6–12 months**: Only 1-2 sales cycles of runway. Accelerate sales pipeline and begin contingency planning.
- 🟢 **12–24 months**: Sweet spot - focus on strategic growth as needed.
- 🟢❗ **> 24 months**: We may be under-staffed. Consider hiring or contracting out work.

**Committed monthly surplus/deficit** - Are committed contracts covering costs? We use a 6-month average of committed revenue as a % of total costs. Note that as a ~10 person organization, not covering 10% of costs is equivalent to not covering 1 FTE a year!

- 🔴 **< 85%**: Losing money quickly. Escalate sales pipeline urgency.
- 🟡 **85–100%**: Losing money but likely have time to figure it out. Push to close pipeline deals.
- 🟢 **100–130%**: Business as usual, we are building reserves.
- 🟢❗ **> 130%**: May be under-staffed for commitments. Consider hiring or contracting out work.

**Pipeline coverage** - Can pipeline fill the gap between committed revenue and costs over the next 6 months? Uses "estimated" revenue scenario (probability-weighted pipeline deals) from our model. We try to be conservative by assuming more pipeline deals will slip anyway.

- 🔴 **< 100%**: Pipeline can't fill the gap. Pursue new leads, consider cost cuts.
- 🟡 **100–150%**: Gap is covered but no margin for slippage. Generate more opportunities.
- 🟢 **150–250%**: Healthy pipeline. Maintain current sales efforts.
- 🟢❗ **> 250%**: Expect significant new revenue — start planning for capacity and hiring.


## Changing things

- **Tunable assumptions** live in `src/assumptions.py`.
- **Health metric thresholds** are in `src/indicators.py` with rationale documented above.
- **Google Sheet tab names** and the worksheet ID are constants at the top of `scripts/sync.py`. If you rename tabs or move important cells in the sheet, update those constants.
