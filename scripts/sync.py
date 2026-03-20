#!/usr/bin/env python3
"""Runs our budget update pipeline. This includes these steps roughly broken down below:

- Download pre-built CSVs from the data-private repo
- Clean it up and categorize data into revenue buckets
- Model different scenarios based on p(success)
- Upload it to the Google Sheet and run projections

It uploads a bunch of intermediata data representations to the Google Sheet as well
so that we can sanity-check them. As a general rule, we should look at those and see
if anything feels "off" so that we can trust these numbers.
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
ALL_DATA_TAB     = "HubSpot: All data w/ columns"
ACTIVE_TAB       = "HubSpot: Active"
REMOVED_TAB      = "HubSpot: Removed"
INACTIVE_TAB     = "HubSpot: Inactive"
PROJECTIONS_TAB  = "Projections: Revenue"
MONTHLY_REVENUE_TAB  = "Projections: Deal contribution by month"
MAU_TAB          = "Hubs: MAUs"
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
        subprocess.run(
            [
                "gh",
                "release",
                "download",
                tag,
                "--repo",
                "2i2c-org/data-private",
                "--dir",
                str(DATA_DIR),
                "--pattern",
                asset,
                "--clobber",
            ],
            check=True,
            env=env,
        )
        downloaded = DATA_DIR / asset
        if not downloaded.exists():
            raise FileNotFoundError(f"Expected {downloaded} after downloading {tag}.")
        downloaded.rename(DATA_DIR / local_name)
        # Print when this release was last published
        result = subprocess.run(
            [
                "gh", "release", "view", tag,
                "--repo", "2i2c-org/data-private",
                "--json", "publishedAt",
                "--jq", ".publishedAt",
            ],
            capture_output=True, text=True, env=env,
        )
        published = result.stdout.strip()
        print(f"  ✅ {asset} → {local_name} (published {published})")


def clean():
    """Add derived columns, categorize deals, calculate MAU revenue."""
    from src.mau import build_mau_table, calculate_revenue as calculate_mau_revenue
    from src.hubspot import add_columns, categorize_deals
    from src.sheets_uploader import get_sheets_client, upload_dataframe

    raw_path = DATA_DIR / "deals_raw.csv"
    client = get_sheets_client()

    # Figure out when projections start (month after latest budget close)
    spreadsheet = client.open_by_key(SHEET_ID)
    ws = spreadsheet.get_worksheet_by_id(1523602458)  # "Variables and info"
    close_cell = ws.find("Latest budget close")
    close_date = pd.to_datetime(ws.cell(close_cell.row, close_cell.col + 1).value)
    ps = close_date + pd.DateOffset(months=1)
    projection_start = pd.Timestamp(ps.year, ps.month, 1)  # normalize to 1st of month
    print(
        f"  Last budget close: {close_date.strftime('%Y-%m')}, projections start: {projection_start.strftime('%Y-%m')}"
    )

    # Add use_start/end_date and monthly_revenue columns
    raw_df = pd.read_csv(raw_path)
    df = add_columns(raw_df, projection_start=projection_start)
    df.to_csv(DATA_DIR / "deals.csv", index=False)
    upload_dataframe(client, SHEET_ID, df, tab_name=ALL_DATA_TAB)
    print(f"  ✅ {ALL_DATA_TAB}: {len(df)} rows → deals.csv")

    # Split into active / removed / inactive
    views = categorize_deals(df, projection_start=projection_start)
    active_df = views["active"]
    removed_df = views["removed"]
    inactive_df = views["inactive"]

    active_df.to_csv(DATA_DIR / "deals_active.csv", index=False)
    upload_dataframe(client, SHEET_ID, active_df, tab_name=ACTIVE_TAB)
    print(f"  ✅ {ACTIVE_TAB}: {len(active_df)} rows → deals_active.csv")

    removed_df.to_csv(DATA_DIR / "deals_removed.csv", index=False)
    upload_dataframe(client, SHEET_ID, removed_df, tab_name=REMOVED_TAB)
    print(f"  ✅ {REMOVED_TAB}: {len(removed_df)} rows → deals_removed.csv")

    inactive_df.to_csv(DATA_DIR / "deals_inactive.csv", index=False)
    upload_dataframe(client, SHEET_ID, inactive_df, tab_name=INACTIVE_TAB)
    print(f"  ✅ {INACTIVE_TAB}: {len(inactive_df)} rows → deals_inactive.csv")

    # MAU revenue - this is just based on historical data, no projected growth etc
    mau_path = DATA_DIR / "mau_raw.csv"
    mau_df = build_mau_table(pd.read_csv(mau_path))
    mau_df, mau_revenue = calculate_mau_revenue(mau_df)
    upload_dataframe(client, SHEET_ID, mau_df, tab_name=MAU_TAB)
    print(f"  ✅ {MAU_TAB}: {len(mau_df)} rows, mean ${mau_revenue:,.0f}/month")

    # Integrity checks
    from src.checks import (
        test_all_deals_accounted_for,
        test_no_duplicate_deals,
        test_no_pipeline_deals_lost,
    )

    test_no_pipeline_deals_lost(df, active_df, removed_df)
    test_all_deals_accounted_for(df, active_df, removed_df, inactive_df)
    test_no_duplicate_deals(active_df, removed_df, inactive_df)

    return active_df, mau_revenue, projection_start


def model(active_df, mau_revenue, projection_start):
    """Run Monte Carlo simulations and upload projections."""
    from src.assumptions import SIMULATION_RUNS
    from src.revenue import build_monthly_revenue, build_projections
    from src.sheets_uploader import get_sheets_client, upload_dataframe

    client = get_sheets_client()

    print(f"  Running {SIMULATION_RUNS} Monte Carlo simulations...")
    projections_df = build_projections(
        active_df, projection_start, mau_revenue=mau_revenue
    )

    # Print some model results for a quick sanity check
    for scenario in projections_df.index:
        nonzero = projections_df.loc[scenario]
        nonzero = nonzero[nonzero > 0]
        if len(nonzero) > 0:
            print(f"    {scenario}: ${nonzero.mean():,.0f}/month avg")

    monthly_revenue_df = build_monthly_revenue(
        active_df, projection_start=projection_start
    )

    # Checks before uploading
    from src.checks import (
        test_monthly_revenue_sums,
        test_monte_carlo_matches_weighted,
        test_scenario_ordering,
    )

    test_scenario_ordering(projections_df)
    test_monthly_revenue_sums(monthly_revenue_df)
    test_monte_carlo_matches_weighted(projections_df, monthly_revenue_df)

    upload_dataframe(
        client, SHEET_ID, projections_df.reset_index(), tab_name=PROJECTIONS_TAB
    )
    print(f"  ✅ {PROJECTIONS_TAB}: {len(projections_df)} scenarios")
    upload_dataframe(client, SHEET_ID, monthly_revenue_df, tab_name=MONTHLY_REVENUE_TAB)
    print(f"  ✅ {MONTHLY_REVENUE_TAB}: {len(monthly_revenue_df)} deals")

    # Update timestamp in "Variables and info" tab
    spreadsheet = client.open_by_key(SHEET_ID)
    ws = spreadsheet.get_worksheet_by_id(1523602458)  # "Variables and info"
    cell = ws.find("Latest hubspot upload")
    if cell:
        ws.update_cell(
            cell.row, cell.col + 1, datetime.now().strftime("%Y-%m-%d %H:%M")
        )

    print(f"\n  Done! \n\n📊 URL: {SHEET_URL}")


if __name__ == "__main__":
    print("download")
    download()
    print("\nclean")
    active_df, mau_revenue, projection_start = clean()
    print("\nmodel")
    model(active_df, mau_revenue, projection_start)
