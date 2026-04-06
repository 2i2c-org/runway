"""Core unit tests for budget pipeline logic."""

from pathlib import Path

import pandas as pd

from src.hubspot import add_columns
from src.mau import cluster_revenue


# --- MAU tiered pricing tests (moved from src/mau.py) ---


def test_cluster_revenue_tiers():
    """Sanity check on the tier boundaries formula."""
    assert cluster_revenue(0) == 0
    assert cluster_revenue(10) == 100  # 10 x $10
    assert cluster_revenue(11) == 105  # $100 + 1 x $5
    assert cluster_revenue(100) == 550  # $100 + 90 x $5
    assert cluster_revenue(101) == 552.50  # $550 + 1 x $2.50
    assert cluster_revenue(1000) == 2800  # $550 + 900 x $2.50
    assert cluster_revenue(10000) == 14050  # $2800 + 9000 x $1.25
    assert cluster_revenue(10001) == 14050  # free above 10k
    assert cluster_revenue(-5) == 0


# --- HubSpot amortization tests ---


def test_monthly_revenue_simple():
    """A deal with no amount_collected splits evenly over the contract."""
    df = pd.DataFrame(
        {
            "id": ["1"],
            "dealname": ["Test Deal"],
            "dealstage": ["Closed Won"],
            "amount": ["12000"],
            "amount_collected": [None],
            "contract_start_date": ["2025-01-01"],
            "contract_end_date": ["2025-12-31"],
            "target_start_date": [None],
            "target_end_date": [None],
            "hs_deal_stage_probability": ["1"],
        }
    )
    projection_start = pd.Timestamp("2025-06-01")
    result = add_columns(df, projection_start)

    # 12 months at ~30.44 days each -> 12 months -> $1000/month
    assert result["monthly_revenue"].iloc[0] == 1000.0

    # Derived date columns should be normalized to YYYY-MM-DD
    assert result["effective_start_date"].iloc[0] == "2025-01-01"
    assert result["effective_end_date"].iloc[0] == "2025-12-31"

    # Projection rate: $12000 over 7 remaining months (Jun-Dec)
    assert result["projection_monthly_revenue"].iloc[0] == 1714.0


def test_monthly_revenue_with_collected():
    """Partially-collected deal spreads remaining revenue from projection_start."""
    df = pd.DataFrame(
        {
            "id": ["1"],
            "dealname": ["Partial Deal"],
            "dealstage": ["Closed Won"],
            "amount": ["12000"],
            "amount_collected": ["8000"],
            "contract_start_date": ["2025-01-01"],
            "contract_end_date": ["2025-12-31"],
            "target_start_date": [None],
            "target_end_date": [None],
            "hs_deal_stage_probability": ["1"],
        }
    )
    projection_start = pd.Timestamp("2025-07-01")
    result = add_columns(df, projection_start)

    # Full contract rate is unchanged: $12000 / 12 = $1000/month
    assert result["monthly_revenue"].iloc[0] == 1000.0

    # effective_start_date stays at contract start (NOT shifted)
    assert result["effective_start_date"].iloc[0] == "2025-01-01"

    # Projection rate differs because we collected ahead of schedule:
    # remaining $4000 / 6 months = $667/month
    assert result["projection_monthly_revenue"].iloc[0] == 667.0


def test_commitment_revenue_uses_full_contract():
    """Commitment path should use full contract amount, not remaining."""
    from src.revenue import build_monthly_revenue

    df = pd.DataFrame(
        {
            "id": ["1"],
            "dealname": ["Partial Deal"],
            "dealstage": ["Closed Won"],
            "revenue_type": ["membership-general"],
            "amount": ["12000"],
            "amount_collected": ["3000"],
            "contract_start_date": ["2025-01-01"],
            "contract_end_date": ["2025-12-31"],
            "target_start_date": [None],
            "target_end_date": [None],
            "hs_deal_stage_probability": ["1"],
        }
    )
    projection_start = pd.Timestamp("2025-07-01")
    active = add_columns(df, projection_start)

    # Commitment path: use full contract rate (monthly_revenue column)
    # Full contract: $12000 / 12 months = $1000/month
    # 6 months from projection_start (Jul-Dec 2025) = $6000 total
    # Note: amount_collected should NOT affect this - commitment
    # uses the full contract rate, not remaining amount.
    result = build_monthly_revenue(
        active, projection_start, revenue_column="monthly_revenue"
    )
    month_cols = [c for c in result.columns if len(c) == 7 and c[4] == "-"]
    total_row = result[result["dealname"] == "TOTAL"]
    total = total_row[month_cols].sum(axis=1).iloc[0]
    assert total == 6000.0, f"Expected $6000 commitment revenue, got ${total}"


# --- Model checks on sample data ---

# Sample deals are loaded from a CSV of 8 real deals copy/pasted from
# deals_raw.csv on 2026-04-01. If the upstream HubSpot export schema
# changes, update the CSV and the expected columns below.
SAMPLE_DEALS_CSV = Path(__file__).parent / "sample_deals.csv"

RAW_DEAL_COLUMNS = {
    "id",
    "dealname",
    "revenue_type",
    "amount",
    "closedate",
    "pipeline",
    "dealstage",
    "contract_start_date",
    "contract_end_date",
    "hs_mrr",
    "hs_arr",
    "target_start_date",
    "target_end_date",
    "hs_deal_stage_probability",
    "hs_projected_amount",
    "amount_collected",
}


def test_sample_data_matches_raw_schema():
    """Fixture columns match real HubSpot CSV schema. Update both if schema changes."""
    raw = pd.read_csv(SAMPLE_DEALS_CSV)
    assert set(raw.columns) == RAW_DEAL_COLUMNS, (
        f"Fixture columns don't match expected schema.\n"
        f"  Missing: {RAW_DEAL_COLUMNS - set(raw.columns)}\n"
        f"  Extra: {set(raw.columns) - RAW_DEAL_COLUMNS}"
    )


def test_model_checks_on_sample_data():
    """Run the model checks against real sample deals to catch regressions."""
    from src.checks import (
        test_monte_carlo_matches_weighted,
        test_monthly_revenue_sums,
        test_scenario_revenue_projection_ordering,
    )
    from src.revenue import build_monthly_revenue, simulate_revenue_projections

    projection_start = pd.Timestamp("2026-03-01")
    active = add_columns(pd.read_csv(SAMPLE_DEALS_CSV), projection_start)

    projections_df = simulate_revenue_projections(active, projection_start)
    monthly_revenue_df = build_monthly_revenue(active, projection_start)

    test_scenario_revenue_projection_ordering(projections_df)
    test_monthly_revenue_sums(monthly_revenue_df)
    test_monte_carlo_matches_weighted(projections_df, monthly_revenue_df)
