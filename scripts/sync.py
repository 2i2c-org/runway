#!/usr/bin/env python3
"""Pipeline for projecting 2i2c's financial runway.

We combine HubSpot deal data with usage-based MAU revenue, model
uncertainty with a few "scenario" projections, and upload results to a
Google Sheet for team visibility and charts.

Here's a brief description of the pipeline:
  1. Download latest data from `2i2c-org/data-private` repo
  2. Clean and derive revenue columns from raw HubSpot data
  3. Categorize deals into active/removed/inactive
  4. Revenue projections
     a. Monte Carlo simulation across deal probabilities
     b. Per-deal monthly revenue breakdown
  5. Commitment projections
     a. Full contract obligations by month
     b. Breakdown by revenue type
"""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# This lets us load our helper modules in src/
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).parent.parent
# A bunch of intermediate data files will be written / over-written here
DATA_DIR = ROOT / "data"

# This is the google sheet where we have our actual budget projections and charts
SHEET_ID = "1IMIG2zrvMe-lSPngSLItCqZbP5Iw_6fNOPM5gZJSob8"
SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit"

# Google Sheet tab names - if you rename a tab, update here (the fmt: is so Black doesn't screw it up)
# fmt: off
ALL_DATA_TAB            = "HubSpot: All data w/ columns"
ACTIVE_TAB              = "HubSpot: Active"
REMOVED_TAB             = "HubSpot: Removed"
INACTIVE_TAB            = "HubSpot: Inactive"
PROJECTIONS_TAB         = "Projections: Revenue"
MONTHLY_REVENUE_TAB     = "Projections: Deal contribution by month"
COMMITMENT_TAB          = "Projections: Full Committed Revenue by month"
COMMITMENT_BY_TYPE_TAB  = "Projections: Full Committed Revenue by type"
MAU_TAB                 = "Hubs: MAUs"
FLAGGED_TAB             = "HubSpot: Flag for review"
VARIABLES_WS_ID         = 1523602458  # "Variables and info" tab
# fmt: on


def download():
    """Download pre-built CSVs from the 2i2c-org/data-private repo."""
    DATA_DIR.mkdir(exist_ok=True)

    # (release tag, remote asset name, local filename)
    releases = [
        ("hubspot-latest", "hubspot-deals.csv", "deals_raw.csv"),
        ("maus-latest", "maus-unique-by-cluster.csv", "mau_raw.csv"),
    ]
    # gh CLI uses GH_TOKEN env var for auth. In CI this is set directly;
    # locally it may be stored as GH_DATA_PRIVATE_TOKEN in .env.
    env = os.environ.copy()
    token = os.environ.get("GH_DATA_PRIVATE_TOKEN")
    if token:
        env["GH_TOKEN"] = token

    for tag, asset, local_name in releases:
        print(f"  Downloading {tag}/{asset}...", flush=True)
        # fmt: off
        subprocess.run(
            [
                "gh", "release", "download", tag,
                "--repo", "2i2c-org/data-private",
                "--dir", str(DATA_DIR),
                "--pattern", asset,
                "--clobber",
            ],
            check=True, env=env,
        )
        # fmt: on
        downloaded = DATA_DIR / asset
        if not downloaded.exists():
            raise FileNotFoundError(f"Expected {downloaded} after downloading {tag}.")
        downloaded.rename(DATA_DIR / local_name)
        # Print when this release was last published
        # fmt: off
        result = subprocess.run(
            [
                "gh", "release", "view", tag,
                "--repo", "2i2c-org/data-private",
                "--json", "publishedAt",
                "--jq", ".publishedAt",
            ],
            capture_output=True, text=True, env=env,
        )
        # fmt: on
        published = result.stdout.strip()
        print(f"  ✅ {asset} → {local_name} (published {published})")


def get_projection_start(client):
    """Read the projection start date from the Google Sheet.

    Projections start the month after the latest budget close date,
    which is maintained manually in the 'Variables and info' tab.

    Parameters
    ----------
    client : gspread.Client
        Authenticated Google Sheets client.

    Returns
    -------
    pd.Timestamp
        First day of the projection start month.
    """
    spreadsheet = client.open_by_key(SHEET_ID)
    ws = spreadsheet.get_worksheet_by_id(VARIABLES_WS_ID)
    close_cell = ws.find("Latest budget close")
    close_date = pd.to_datetime(ws.cell(close_cell.row, close_cell.col + 1).value)
    ps = close_date + pd.DateOffset(months=1)
    projection_start = pd.Timestamp(ps.year, ps.month, 1)
    print(
        f"  Last budget close: {close_date.strftime('%Y-%m')},"
        f" projections start: {projection_start.strftime('%Y-%m')}"
    )
    return projection_start


def build_deals(raw_df, projection_start):
    """Add derived columns and categorize deals.

    Parameters
    ----------
    raw_df : pd.DataFrame
        Raw deals from HubSpot CSV.
    projection_start : pd.Timestamp
        First day of the projection start month.

    Returns
    -------
    df : pd.DataFrame
        All deals with derived columns.
    active_df : pd.DataFrame
        Deals used for projections (Closed Won + pipeline with data).
    removed_df : pd.DataFrame
        Pipeline deals missing required fields.
    inactive_df : pd.DataFrame
        Expired or otherwise unused deals.
    """
    from src.hubspot import add_columns, categorize_deals

    df = add_columns(raw_df, projection_start=projection_start)

    views = categorize_deals(df, projection_start=projection_start)
    return df, views["active"], views["removed"], views["inactive"]


def build_mau_revenue(raw_mau_df):
    """Build MAU table and calculate revenue from historical data.

    Parameters
    ----------
    raw_mau_df : pd.DataFrame
        Raw MAU data from CSV.

    Returns
    -------
    mau_df : pd.DataFrame
        MAU counts by cluster and month.
    mau_revenue : float
        Average monthly MAU revenue (last 12 months).
    """
    from src.mau import build_mau_table, calculate_revenue as calculate_mau_revenue

    mau_df = build_mau_table(raw_mau_df)
    mau_df, mau_revenue = calculate_mau_revenue(mau_df)
    return mau_df, mau_revenue


if __name__ == "__main__":
    from src.assumptions import SIMULATION_RUNS
    from src.checks import (
        test_all_deals_accounted_for,
        test_monte_carlo_matches_weighted,
        test_monthly_revenue_sums,
        test_no_duplicate_deals,
        test_no_pipeline_deals_lost,
        test_scenario_revenue_projection_ordering,
        flag_deals_for_review,
    )
    from src.revenue import (
        build_monthly_revenue,
        simulate_revenue_projections,
        build_revenue_by_type,
    )
    from src.sheets_uploader import get_sheets_client, upload_dataframe

    # =========================================================================
    # Download latest data
    # =========================================================================
    # Fetch deal and MAU CSVs published by our data-private repo.
    print("Download raw data assets")
    # This downloads a few things to disk rather than returning stuff here.
    # It uses the `gh` CLI so we need to read it in from disk later
    download()

    # This is how we'll speak to our Google Sheets for reading/writing
    client = get_sheets_client()

    # =========================================================================
    # Clean and categorize deals
    # =========================================================================
    # Derive effective dates and revenue columns from raw HubSpot data,
    # then split into active (used for projections), removed (missing data),
    # and inactive (expired).
    print("\nClean & categorize")
    projection_start = get_projection_start(client)
    # This is what we downloaded with download()
    raw_df = pd.read_csv(DATA_DIR / "deals_raw.csv")
    # Categorize into various types of deals for projections
    df, active_df, removed_df, inactive_df = build_deals(raw_df, projection_start)

    test_no_pipeline_deals_lost(df, active_df, removed_df)
    test_all_deals_accounted_for(df, active_df, removed_df, inactive_df)
    test_no_duplicate_deals(active_df, removed_df, inactive_df)

    # Flag deals that may have data quality issues in HubSpot
    flagged_df = flag_deals_for_review(df, projection_start)
    if len(flagged_df) > 0:
        print(f"  ⚠️  {len(flagged_df)} deal(s) flagged for review:")
        for _, row in flagged_df.iterrows():
            print(f"      - {row['dealname']}: {row['reason']}")

    # If our tests have passed, we save to disk and also upload for inspection!
    df.to_csv(DATA_DIR / "deals.csv", index=False)
    active_df.to_csv(DATA_DIR / "deals_active.csv", index=False)
    removed_df.to_csv(DATA_DIR / "deals_removed.csv", index=False)
    inactive_df.to_csv(DATA_DIR / "deals_inactive.csv", index=False)

    upload_dataframe(client, SHEET_ID, df, tab_name=ALL_DATA_TAB)
    print(f"  ✅ {ALL_DATA_TAB}: {len(df)} rows")
    upload_dataframe(client, SHEET_ID, active_df, tab_name=ACTIVE_TAB)
    print(f"  ✅ {ACTIVE_TAB}: {len(active_df)} rows")
    upload_dataframe(client, SHEET_ID, removed_df, tab_name=REMOVED_TAB)
    print(f"  ✅ {REMOVED_TAB}: {len(removed_df)} rows")
    upload_dataframe(client, SHEET_ID, inactive_df, tab_name=INACTIVE_TAB)
    print(f"  ✅ {INACTIVE_TAB}: {len(inactive_df)} rows")
    upload_dataframe(client, SHEET_ID, flagged_df, tab_name=FLAGGED_TAB)
    print(f"  ✅ {FLAGGED_TAB}: {len(flagged_df)} deals")

    # =========================================================================
    # MAU revenue
    # =========================================================================
    # Calculate usage-based revenue from historical MAU data.
    print("\nMAU revenue")
    # This was also downloaded by the `gh` CLI in download()
    raw_mau_df = pd.read_csv(DATA_DIR / "mau_raw.csv")
    # Calculate average monthly MAU revenue from historical data
    # TODO: We're just assuming an average MAUs that is consistent, that's probably wrong!
    # TODO: We should check/test this in the future if MAU calculation becomes more complex
    mau_df, mau_revenue = build_mau_revenue(raw_mau_df)

    upload_dataframe(client, SHEET_ID, mau_df, tab_name=MAU_TAB)
    print(f"  ✅ {MAU_TAB}: {len(mau_df)} rows, ${mau_revenue:,.0f}/month avg")

    # =========================================================================
    # Split active deals into committed vs pipeline
    # =========================================================================
    # Committed deals (prob=1, i.e. Closed Won) are on the books.
    # Pipeline deals (prob<1) are uncertain and modeled probabilistically.
    prob = pd.to_numeric(
        active_df["hs_deal_stage_probability"], errors="coerce"
    ).fillna(0)
    commitment_df = active_df[prob >= 1.0].copy()
    pipeline_df = active_df[prob < 1.0].copy()

    # =========================================================================
    # Revenue - Monte Carlo simulation
    # =========================================================================
    # Produce pessimistic/estimated/optimistic scenarios plus committed (on-the-books) revenue.
    #
    # For each simularion, for each deal, flip a weighted coin to model whether it closes.
    #  If it does, project its remaining revenue into the remaining months of that deal.
    #  If it does not, then don't include it in projections.
    #
    # Committed deals (prob=1) always contribute their remaining amount spread over remaining contract months.
    # Pipeline deals (prob<1) only contribute if their coin flip wins for that simulation.
    print(f"\nRevenue - Monte Carlo simulation ({SIMULATION_RUNS} runs)")
    projections_df = simulate_revenue_projections(
        active_df, projection_start, mau_revenue=mau_revenue
    )

    test_scenario_revenue_projection_ordering(projections_df)

    # Quick sanity check: print per-scenario averages
    for scenario in projections_df.index:
        nonzero = projections_df.loc[scenario]
        nonzero = nonzero[nonzero > 0]
        if len(nonzero) > 0:
            print(f"    {scenario}: ${nonzero.mean():,.0f}/month avg")

    upload_dataframe(
        client, SHEET_ID, projections_df.reset_index(), tab_name=PROJECTIONS_TAB
    )
    print(f"  ✅ {PROJECTIONS_TAB}: {len(projections_df)} scenarios")

    # =========================================================================
    # Revenue - Per-deal monthly breakdown
    # =========================================================================
    # Spread each deal's projected monthly revenue across future months.
    # Shows which individual deals drive our revenue projections.
    print("\nRevenue - Per-deal monthly breakdown")
    monthly_revenue_df = build_monthly_revenue(active_df, projection_start)

    test_monthly_revenue_sums(monthly_revenue_df)
    test_monte_carlo_matches_weighted(projections_df, monthly_revenue_df)

    upload_dataframe(client, SHEET_ID, monthly_revenue_df, tab_name=MONTHLY_REVENUE_TAB)
    print(f"  ✅ {MONTHLY_REVENUE_TAB}: {len(monthly_revenue_df)} deals")

    # =========================================================================
    # Commitments - Full contract obligations by month
    # =========================================================================
    # Same per-deal breakdown but using full contract values (not adjusted for collections).
    # Shows our total contractual obligations over time (only using committed deals).
    print("\nCommitments - Full contract obligations by month")
    commitment_monthly = build_monthly_revenue(
        commitment_df, projection_start, revenue_column="monthly_revenue"
    )

    upload_dataframe(client, SHEET_ID, commitment_monthly, tab_name=COMMITMENT_TAB)
    print(f"  ✅ {COMMITMENT_TAB}: {len(commitment_monthly)} deals")

    # =========================================================================
    # Commitments - Breakdown by revenue type
    # =========================================================================
    # Group commitments and pipeline by revenue type for charting.
    #   Committed deals show total contract obligations.
    #   Pipeline deals show probability-weighted projections.
    # TODO: Pipeline deals are hard to reason about here!
    #   We want to model uncertainty, but if they land the "commitment" will be higher than what we show.
    #   Need to deal w/ this better.
    print("\nCommitments - Breakdown by revenue type")
    pipeline_monthly = build_monthly_revenue(pipeline_df, projection_start)

    # Sanity: make sure build_monthly_revenue didn't silently drop deals
    assert len(commitment_monthly[commitment_monthly["dealname"] != "TOTAL"]) == len(
        commitment_df
    )
    assert len(pipeline_monthly[pipeline_monthly["dealname"] != "TOTAL"]) == len(
        pipeline_df
    )

    by_type = build_revenue_by_type(
        commitment_monthly, pipeline_monthly, projection_start
    )

    upload_dataframe(
        client, SHEET_ID, by_type.reset_index(), tab_name=COMMITMENT_BY_TYPE_TAB
    )
    print(f"  ✅ {COMMITMENT_BY_TYPE_TAB}: {len(by_type)} rows")

    # =========================================================================
    # Update timestamp
    # =========================================================================
    # This just ensures that the Google Sheet reflects when we've run this.
    spreadsheet = client.open_by_key(SHEET_ID)
    ws = spreadsheet.get_worksheet_by_id(VARIABLES_WS_ID)
    cell = ws.find("Latest hubspot upload")
    if cell:
        ws.update_cell(
            cell.row, cell.col + 1, datetime.now().strftime("%Y-%m-%d %H:%M")
        )

    print(f"\n  Done! \n\n📊 URL: {SHEET_URL}")
