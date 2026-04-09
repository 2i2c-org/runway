"""Inline data integrity checks. The @check decorator prints the docstring on success."""

import pandas as pd

from src.assumptions import PIPELINE_STAGES
from src.revenue import find_month_columns


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
def test_scenario_revenue_projection_ordering(projections_df):
    """Scenarios ordered: Committed ≤ Pessimistic ≤ Estimated ≤ Optimistic."""
    order = ["Committed", "Pessimistic", "Estimated", "Optimistic"]
    present = [s for s in order if s in projections_df.index]
    for col in projections_df.columns:
        for i in range(len(present) - 1):
            lo, hi = present[i], present[i + 1]
            lo_val = projections_df.loc[lo, col]
            hi_val = projections_df.loc[hi, col]
            # Skip months with negligible revenue
            # This is for months in the far future with *only* low-probability deals, where the predictions become unstable and often show $0)
            if hi_val == 0 and lo_val < 1000:
                continue
            # Allow a small tolerance so we just stay within sampling noise
            margin = hi_val * 0.05
            assert (
                lo_val <= hi_val + margin
            ), f"{lo} (${lo_val:,.0f}) > {hi} (${hi_val:,.0f}) in {col}"


@check
def test_monthly_revenue_sums(monthly_revenue_df):
    """Monthly revenue sums match expected_monthly_revenue * active months."""
    deals = monthly_revenue_df[monthly_revenue_df["dealname"] != "TOTAL"]
    month_cols = find_month_columns(deals)
    failures = []
    for _, row in deals.iterrows():
        monthly_rate = (
            pd.to_numeric(row.get("expected_monthly_revenue", 0), errors="coerce") or 0
        )
        monthly_vals = pd.to_numeric(
            pd.Series([row[c] for c in month_cols]), errors="coerce"
        ).fillna(0)
        active_months = (monthly_vals > 0).sum()
        monthly_sum = monthly_vals.sum()
        expected = monthly_rate * active_months
        # Skip small totals where rounding can cause weird errors since the N is small
        if expected < 10000:
            continue
        if abs(monthly_sum - expected) > expected * 0.05:
            failures.append(
                f"  {row['dealname']}: sum ${monthly_sum:,.0f} "
                f"vs {active_months} months * ${monthly_rate:,.0f} "
                f"= ${expected:,.0f}"
            )
    assert (
        not failures
    ), "Monthly revenue sums don't match monthly rates:\n" + "\n".join(failures)


def flag_deals_for_review(df, projection_start):
    """Flag deals that may have data quality issues in HubSpot.

    Parameters
    ----------
    df : pd.DataFrame
        All deals with derived columns (from ``add_columns``).
    projection_start : pd.Timestamp
        First day of the projection start month.

    Returns
    -------
    pd.DataFrame
        Flagged deals with columns for inspection and a ``reason`` column.
    """
    start = pd.to_datetime(df["effective_start_date"], errors="coerce")
    end = pd.to_datetime(df["effective_end_date"], errors="coerce")
    amount = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    raw_collected = pd.to_numeric(df.get("amount_collected", 0), errors="coerce")
    collected_is_missing = raw_collected.isna()
    collected = raw_collected.fillna(0)
    # These flags only apply to Closed Won deals because these are delivery/invoicing checks
    is_closed_won = df["dealstage"] == "Closed Won"
    contract_start = pd.to_datetime(df["contract_start_date"], errors="coerce")
    contract_end = pd.to_datetime(df["contract_end_date"], errors="coerce")

    flags = {}

    # Stale collection: started well before projection_start,
    # and amount_collected is missing (null, not explicit $0)
    stale_months_threshold = 6
    threshold = projection_start - pd.DateOffset(months=stale_months_threshold)
    flags[
        f"Stale: started {stale_months_threshold}+ months ago with no collection data"
    ] = (is_closed_won & (start < threshold) & collected_is_missing)

    # Over-collected: collected more than 10% above the deal amount
    flags["Over-collected: amount_collected > 110% of amount"] = (
        is_closed_won & (collected > amount * 1.10) & (amount > 0)
    )

    # Missing contract dates (falling back to target dates)
    flags["Missing contract dates"] = is_closed_won & (
        contract_start.isna() | contract_end.isna()
    )

    # Contract ended but not fully collected
    flags["Contract ended but not fully collected"] = (
        is_closed_won & (end < projection_start) & (collected < amount) & (amount > 0)
    )

    # Contract ending soon with significant budget remaining
    ending_soon_months = 3
    min_remaining_fraction = 0.5
    ending_soon = projection_start + pd.DateOffset(months=ending_soon_months)
    remaining_fraction = ((amount - collected) / amount).where(amount > 0, 0)
    flags[
        f"Ending within {ending_soon_months} months"
        f" with >{min_remaining_fraction:.0%} of budget remaining"
    ] = (
        is_closed_won
        & (end <= ending_soon)
        & (end >= projection_start)
        & (remaining_fraction > min_remaining_fraction)
    )

    # Estimated collection: amount_collected was missing so we estimated it
    if "amount_collected_is_estimated" in df.columns:
        flags["amount_collected was estimated from elapsed contract time"] = (
            is_closed_won & df["amount_collected_is_estimated"].fillna(False)
        )

    display_cols = [
        "dealname",
        "reason",
        "dealstage",
        "amount",
        "amount_collected",
        "amount_remaining",
        "effective_start_date",
        "effective_end_date",
        "monthly_revenue",
        "projection_monthly_revenue",
    ]

    # Combine: one row per deal, with reasons joined
    flagged_ids = set()
    reasons = {}
    for reason, mask in flags.items():
        for idx in df.index[mask]:
            flagged_ids.add(idx)
            reasons.setdefault(idx, []).append(reason)

    if not flagged_ids:
        return pd.DataFrame(columns=display_cols)

    flagged = df.loc[list(flagged_ids)].copy()
    flagged["reason"] = flagged.index.map(lambda idx: "; ".join(reasons[idx]))
    flagged["amount_remaining"] = (
        amount.loc[flagged.index] - collected.loc[flagged.index]
    ).clip(lower=0)

    result = flagged[[c for c in display_cols if c in flagged.columns]]
    return result.sort_values("projection_monthly_revenue", ascending=False)


@check
def test_monte_carlo_matches_weighted(projections_df, monthly_revenue_df):
    """Monte Carlo 'estimated' scenario ≈ probability-weighted sums."""
    total_row = monthly_revenue_df[monthly_revenue_df["dealname"] == "TOTAL"]
    # Only compare months that exist in both (they start from different dates)
    detail_cols = set(find_month_columns(monthly_revenue_df))
    month_cols = [c for c in find_month_columns(projections_df) if c in detail_cols]
    for col in month_cols:
        mc_val = projections_df.loc["Estimated", col]
        weighted_val = pd.to_numeric(total_row[col].iloc[0], errors="coerce") or 0
        if weighted_val == 0 and mc_val == 0:
            continue
        # Check whether the two are roughly equal - we'd expect them to be very close
        if weighted_val > 0:
            ratio = mc_val / weighted_val
            assert (
                0.9 < ratio < 1.1
            ), f"MC mean (${mc_val:,.0f}) differs >10% from weighted sum (${weighted_val:,.0f}) in {col}"
