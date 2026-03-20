"""Inline data integrity checks. The @check decorator prints the docstring on success."""

import pandas as pd

from src.assumptions import PIPELINE_STAGES


# This is a little wrapper that lets us print the docstring of a check as the success message
def check(fn):
    def wrapper(*args, **kwargs):
        fn(*args, **kwargs)
        print(f"  ✅ Check: {fn.__doc__}")

    return wrapper


@check
def test_all_deals_accounted_for(df, active_df, removed_df, inactive_df):
    """All deals accounted for (active + removed + inactive = total)."""
    total = len(active_df) + len(removed_df) + len(inactive_df)
    assert total == len(
        df
    ), f"Deal count mismatch: {total} categorized but {len(df)} total"


@check
def test_no_pipeline_deals_lost(df, active_df, removed_df):
    """All pipeline-stage deals in active or removed."""
    all_pipeline = set(df[df["dealstage"].isin(PIPELINE_STAGES)]["id"].astype(str))
    accounted = set(active_df["id"].astype(str)) | set(removed_df["id"].astype(str))
    missing = all_pipeline - accounted
    assert not missing, f"Pipeline deals unaccounted for: {missing}"


@check
def test_no_duplicate_deals(active_df, removed_df, inactive_df):
    """No duplicate deals within views."""
    for name, view in [
        ("active", active_df),
        ("removed", removed_df),
        ("inactive", inactive_df),
    ]:
        assert view["id"].is_unique, f"Duplicate deal IDs in {name}"


@check
def test_scenario_ordering(projections_df):
    """Scenarios ordered: Committed ≤ Pessimistic ≤ Estimated ≤ Optimistic."""
    order = ["Committed", "Pessimistic", "Estimated", "Optimistic"]
    present = [s for s in order if s in projections_df.index]
    for col in projections_df.columns:
        for i in range(len(present) - 1):
            lo, hi = present[i], present[i + 1]
            assert (
                projections_df.loc[lo, col] <= projections_df.loc[hi, col]
            ), f"{lo} (${projections_df.loc[lo, col]:,.0f}) > {hi} (${projections_df.loc[hi, col]:,.0f}) in {col}"


@check
def test_deal_detail_sums_match_amounts(deal_detail_df, active_df):
    """Deal detail sums are consistent with deal amounts (>$10k deals only)."""
    # Short/small contracts can span more calendar months than their actual duration
    # due to month-boundary rounding, so we only check deals large enough that
    # this rounding error is proportionally small.
    deals = deal_detail_df[deal_detail_df["dealname"] != "TOTAL"]
    month_cols = [c for c in deals.columns if c[:4].isdigit()]
    failures = []
    for _, row in deals.iterrows():
        amount = pd.to_numeric(row.get("amount", 0), errors="coerce") or 0
        # Compare against remaining amount (amount - collected)
        collected = pd.to_numeric(row.get("amount_collected", 0), errors="coerce") or 0
        expected = max(amount - collected, 0)
        # Remove deals with very little remaining because we start to hit weird month boundary errors when its v short
        if expected < 10000:
            continue
        monthly_sum = (
            pd.to_numeric(pd.Series([row[c] for c in month_cols]), errors="coerce")
            .fillna(0)
            .sum()
        )
        if monthly_sum > expected * 1.15 or monthly_sum < expected * 0.85:
            failures.append(
                f"  {row['dealname']}: projections sum ${monthly_sum:,.0f} vs expected ${expected:,.0f}"
            )
    assert not failures, "Deal detail sums exceed deal amounts:\n" + "\n".join(failures)


@check
def test_monte_carlo_matches_weighted(projections_df, deal_detail_df):
    """Monte Carlo 'estimated' scenario ≈ probability-weighted sums (within 20%)."""
    total_row = deal_detail_df[deal_detail_df["dealname"] == "TOTAL"]
    # Only compare months that exist in both (they start from different dates)
    detail_cols = set(deal_detail_df.columns)
    month_cols = [
        c for c in projections_df.columns if c[:4].isdigit() and c in detail_cols
    ]
    for col in month_cols:
        mc_val = projections_df.loc["Estimated", col]
        weighted_val = pd.to_numeric(total_row[col].iloc[0], errors="coerce") or 0
        if weighted_val == 0 and mc_val == 0:
            continue
        # Check whether the two are roughly equal - we'd expect them to be very close
        if weighted_val > 0:
            ratio = mc_val / weighted_val
            assert (
                0.8 < ratio < 1.2
            ), f"MC mean (${mc_val:,.0f}) differs >20% from weighted sum (${weighted_val:,.0f}) in {col}"
