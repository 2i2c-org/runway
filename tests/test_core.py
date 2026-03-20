"""Core unit tests for budget pipeline logic."""

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
    assert result["use_start_date"].iloc[0] == "2025-01-01"
    assert result["use_end_date"].iloc[0] == "2025-12-31"


def test_monthly_revenue_with_collected():
    """Partially-collected deal spreads remaining revenue from projection_start."""
    df = pd.DataFrame(
        {
            "id": ["1"],
            "dealname": ["Partial Deal"],
            "dealstage": ["Closed Won"],
            "amount": ["12000"],
            "amount_collected": ["6000"],
            "contract_start_date": ["2025-01-01"],
            "contract_end_date": ["2025-12-31"],
            "target_start_date": [None],
            "target_end_date": [None],
            "hs_deal_stage_probability": ["1"],
        }
    )
    projection_start = pd.Timestamp("2025-07-01")
    result = add_columns(df, projection_start)

    # Remaining = 12000 - 6000 = 6000
    # Months from 2025-07-01 to 2025-12-31 ~ 6 months
    assert result["monthly_revenue"].iloc[0] == 1000.0

    # use_start_date should be shifted to projection_start
    assert result["use_start_date"].iloc[0] == "2025-07-01"


# --- Monthly revenue projection tests ---


def _make_active_df(start, end, amount, collected=None, prob="1"):
    """Helper to build a minimal active deal DataFrame (post-add_columns)."""
    df = pd.DataFrame(
        {
            "id": ["1"],
            "dealname": ["Test Deal"],
            "dealstage": ["Closed Won"],
            "amount": [amount],
            "amount_collected": [collected],
            "contract_start_date": [start],
            "contract_end_date": [end],
            "target_start_date": [None],
            "target_end_date": [None],
            "hs_deal_stage_probability": [prob],
        }
    )
    return add_columns(df, pd.Timestamp(start))


def _deal_month_sum(detail):
    """Sum the month columns for the first deal row (excludes TOTAL)."""
    deal_row = detail.iloc[0]
    month_cols = [c for c in detail.columns if c[:2] == "20"]
    return pd.to_numeric(deal_row[month_cols], errors="coerce").fillna(0).sum()


def test_monthly_revenue_sums_to_amount_month_aligned():
    """Month-aligned deal: monthly columns should sum to the full amount."""
    from src.revenue import build_monthly_revenue

    active = _make_active_df("2025-01-01", "2025-12-31", "12000")
    detail = build_monthly_revenue(active, pd.Timestamp("2025-01-01"))
    assert _deal_month_sum(detail) == 12000


def test_monthly_revenue_sums_to_amount_mid_month():
    """Mid-month start/end: monthly columns should still sum to amount."""
    from src.revenue import build_monthly_revenue

    active = _make_active_df("2025-01-15", "2025-12-15", "12000")
    detail = build_monthly_revenue(active, pd.Timestamp("2025-01-01"))
    assert _deal_month_sum(detail) == 12000


def test_monthly_revenue_sums_to_amount_with_collected():
    """Partially-collected deal: months should sum to remaining amount."""
    from src.revenue import build_monthly_revenue

    active = _make_active_df(
        "2025-01-01", "2025-12-31", "12000", collected="6000"
    )
    projection_start = pd.Timestamp("2025-07-01")
    # Re-run add_columns with the real projection_start for collected logic
    df = pd.DataFrame(
        {
            "id": ["1"],
            "dealname": ["Test Deal"],
            "dealstage": ["Closed Won"],
            "amount": ["12000"],
            "amount_collected": ["6000"],
            "contract_start_date": ["2025-01-01"],
            "contract_end_date": ["2025-12-31"],
            "target_start_date": [None],
            "target_end_date": [None],
            "hs_deal_stage_probability": ["1"],
        }
    )
    active = add_columns(df, projection_start)
    detail = build_monthly_revenue(active, projection_start)
    assert _deal_month_sum(detail) == 6000
