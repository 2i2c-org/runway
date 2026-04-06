"""Revenue projections: Monte Carlo simulation and monthly revenue breakdowns.

This generates the monthly projections for each deal and the simulations for various scenarios.
This is what ultimately goes into the runway calculation!
"""

import numpy as np
import pandas as pd

from src.assumptions import (
    PROJECTION_ORIGIN,
    SCENARIO_PERCENTILES,
    SIMULATION_RUNS,
    SIMULATION_SEED,
)


def find_month_columns(df):
    """Return column names that represent months (formatted as YYYY-MM).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with month columns like `"2025-06"`.

    Returns
    -------
    list of str
        Sorted month column names.
    """
    return [c for c in df.columns if len(c) == 7 and c[4] == "-" and c[:4].isdigit()]


def _month_start(dt):
    return pd.Timestamp(dt.year, dt.month, 1)


def _month_range(start_month, end_month):
    return pd.date_range(start_month, end_month, freq="MS").tolist()


def _remaining(deal):
    """Amount minus amount_collected, handling NaN."""
    amt = pd.to_numeric(deal.get("amount", 0), errors="coerce")
    col = pd.to_numeric(deal.get("amount_collected", 0), errors="coerce")
    return max(0, (0 if pd.isna(amt) else amt) - (0 if pd.isna(col) else col))


def build_monthly_revenue(
    active_df, projection_start, revenue_column="projection_monthly_revenue"
):
    """Spread each deal's monthly revenue across future months.

    Parameters
    ----------
    active_df : pd.DataFrame
        Active deals with derived columns.
    projection_start : pd.Timestamp
        First month to project into.
    revenue_column : str
        Column to use for monthly rate. `"projection_monthly_revenue"`
        for revenue projections, `"monthly_revenue"` for commitments.

    Returns
    -------
    pd.DataFrame
        One row per deal plus a TOTAL row, with month columns.
    """

    end_dates = pd.to_datetime(active_df["effective_end_date"])
    months = _month_range(projection_start, _month_start(end_dates.max()))
    # We're basically adding an extra column for each month of our projections.
    # We'll end up concatenating all this into one big dataframe that will fill in
    # $0 for holes in months w/o projections for a deal.
    month_labels = [m.strftime("%Y-%m") for m in months]
    deal_rows = []

    start = pd.to_datetime(active_df["effective_start_date"])
    prob = pd.to_numeric(
        active_df.get("hs_deal_stage_probability", 0), errors="coerce"
    ).fillna(0)

    # Looping through deals to project out monthly revenue for each
    for idx in active_df.index:
        deal = active_df.loc[idx]
        deal_prob = prob.loc[idx]
        d_start, d_end = start.loc[idx], end_dates.loc[idx]

        # Figure out which calendar months this deal is active in.
        # This lets us know how many months to use for our monthly projections,
        # and ensures that we don't accidentally over or under-count.
        active_months = []
        for month, ml in zip(months, month_labels):
            month_end = month + pd.DateOffset(months=1) - pd.DateOffset(days=1)
            active = d_start <= month_end and d_end >= month
            active_months.append((ml, active))

        monthly_rev = deal[revenue_column]
        expected = round(monthly_rev * deal_prob, 0)

        row = {
            "dealname": deal.get("dealname", ""),
            "revenue_type": deal.get("revenue_type", ""),
            "dealstage": deal.get("dealstage", ""),
            "probability": deal_prob,
            "monthly_revenue": monthly_rev,
            "expected_monthly_revenue": expected,
            "amount": deal.get("amount", ""),
            # This is just for visualizing in the google sheet, not used in logic below
            # Note that:
            #   for committed deals, deal_prob is 1 but remaining might be < deal total,
            #   for pipeline deals, deal_prob will be < 1 but remaining will always be the deal total.
            "amount_future_revenue": round(_remaining(deal) * deal_prob, 0),
        }
        for ml, active in active_months:
            row[ml] = expected if active else 0
        deal_rows.append(row)

    result = pd.DataFrame(deal_rows).sort_values(
        "expected_monthly_revenue", ascending=False
    )

    # Add a "total" row to make it easy for us to sanity-check in google sheets
    # This isn't used for any actual runway calculations
    totals = {"dealname": "TOTAL"}
    for ml in month_labels:
        totals[ml] = pd.to_numeric(result[ml], errors="coerce").fillna(0).sum()
    return pd.concat([result, pd.DataFrame([totals])], ignore_index=True)


def simulate_revenue_projections(active_df, projection_start, mau_revenue=None):
    """Monte Carlo revenue projections.

    For each simulation, for each deal:
    - Flip a coin weighted by the deal's probability of success.
    - If it "wins," include its projected monthly revenue for remaining months after projection_start.
    - If not, exclude it entirely.
    - Sum across all deals for each month.

    Collect percentiles and the mean across simulations to produce
    pessimistic/estimated/optimistic scenarios.
    Committed deals (prob=1) always contribute.

    Parameters
    ----------
    active_df : pd.DataFrame
        Active deals with derived columns.
    projection_start : pd.Timestamp
        First month to project into.
    mau_revenue : float, optional
        Monthly MAU revenue to include as a flat row.

    Returns
    -------
    pd.DataFrame
        Rows are scenarios (Committed, Pessimistic, Estimated, Optimistic,
        etc.), columns are months.
    """
    start_month = pd.Timestamp(PROJECTION_ORIGIN)
    end_dates = pd.to_datetime(active_df["effective_end_date"])
    end_month = _month_start(
        end_dates.max()
    )  # End at the latest contract month across all deals

    months = _month_range(start_month, end_month)
    month_labels = [m.strftime("%Y-%m") for m in months]
    n_months = len(months)

    d_start = pd.to_datetime(active_df["effective_start_date"])
    d_end = end_dates
    d_prob = (
        pd.to_numeric(active_df["hs_deal_stage_probability"], errors="coerce")
        .fillna(0)
        .values
    )
    n_deals = len(active_df)

    # Build per-deal, per-month active mask
    active_mask = np.zeros((n_deals, n_months), dtype=bool)
    for i, month in enumerate(months):
        if month < projection_start:
            # Our projections start in the past (since the google sheet starts in 2025)
            continue
        month_end = month + pd.DateOffset(months=1) - pd.DateOffset(days=1)
        # Mark a deal as active for that month if the month falls w/in period of performance
        active_mask[:, i] = ((d_start <= month_end) & (d_end >= month)).values

    d_rev = active_df["projection_monthly_revenue"].fillna(0).values

    # Full-contract rate for the "total contract obligations" row.
    d_rev_full = active_df["monthly_revenue"].fillna(0).values

    # Commitment mask uses full contract dates (including past months).
    # This is intentional — it's used for committed_full_totals which shows
    # total obligations, not just future revenue.
    commitment_mask = np.zeros((n_deals, n_months), dtype=bool)
    for i, month in enumerate(months):
        month_end = month + pd.DateOffset(months=1) - pd.DateOffset(days=1)
        commitment_mask[:, i] = ((d_start <= month_end) & (d_end >= month)).values

    committed_mask = d_prob >= 1.0
    # Gifts are one-time contributions, not ongoing service commitments
    not_gift = active_df.get("revenue_type", pd.Series()).fillna("") != "gift"

    committed_totals = np.zeros(n_months)
    committed_full_totals = np.zeros(n_months)
    for i in range(n_months):
        # committed_totals uses active_mask (stargin w/ projection_start) and projection rate
        # This is what we expect to *receive* going forward
        committed_totals[i] = d_rev[committed_mask & active_mask[:, i]].sum()
        # committed_full_totals uses commitment_mask (full contract period) and full contract rate.
        # This shows *total* obligations
        committed_full_totals[i] = d_rev_full[
            committed_mask & not_gift.values & commitment_mask[:, i]
        ].sum()

    # Each simulation run flips a coin per deal: does it close or not?
    # A deal either contributes its full monthly revenue or nothing.
    # We seed it to have consistent results across runs, though this will change
    # whenever the data changes...
    rng = np.random.default_rng(SIMULATION_SEED)
    # This is where the deals "close" or not in each simulation
    closes = rng.random((SIMULATION_RUNS, n_deals)) < d_prob[np.newaxis, :]

    # Now sum the closed deals for this simulation so we can project
    sim = np.zeros((SIMULATION_RUNS, n_months))
    for d in range(n_deals):
        sim += closes[:, d, np.newaxis] * d_rev[d] * active_mask[d, :]

    # Grab the percentiles for each simulation
    rows = {
        # Committed (on the books) revenue
        "Committed": np.round(committed_totals).tolist(),
        # These are all model results
        "Pessimistic": np.round(
            np.percentile(sim, SCENARIO_PERCENTILES["Pessimistic"], axis=0)
        ).tolist(),
        "Estimated": np.round(np.mean(sim, axis=0)).tolist(),
        "Optimistic": np.round(
            np.percentile(sim, SCENARIO_PERCENTILES["Optimistic"], axis=0)
        ).tolist(),
    }

    # Now add a row for MAU revenue, which is not a simulation just taking the
    # mean of the last 12 months of historical data. This is a bit hacky but we
    # wanna keep it simple for now.
    # Note that we only include MAU revenue for future-facing months.
    rows["Estimated MAU revenue"] = [
        mau_revenue if m >= projection_start else 0.0 for m in months
    ]

    # Full contract commitment (not subtracting already-invoiced amounts).
    # Useful for understanding our total workload obligations.
    rows["Committed (total contract less gifts)"] = np.round(
        committed_full_totals
    ).tolist()

    result = pd.DataFrame(rows, index=month_labels).T
    result.index.name = "scenario"
    return result


def build_revenue_by_type(commitment_monthly, pipeline_monthly, projection_start, n_months=24):
    """Group commitment and pipeline revenue by type for charting.

    Groups per-deal monthly breakdowns by revenue_type, with committed
    and pipeline sections plus totals.

    Parameters
    ----------
    commitment_monthly : pd.DataFrame
        Per-deal commitment breakdown (from `build_monthly_revenue`).
    pipeline_monthly : pd.DataFrame
        Per-deal pipeline breakdown (from `build_monthly_revenue`).
    projection_start : pd.Timestamp
        First month of the projection window.
    n_months : int
        Number of months to include from projection_start (default 24).

    Returns
    -------
    pd.DataFrame
        Rows are revenue types plus TOTAL rows, columns are months.
    """
    from src.assumptions import REVENUE_TYPE_ORDER

    # This helps us keep the plotting consistent in Google Sheets
    order = {t: i for i, t in enumerate(REVENUE_TYPE_ORDER)}

    def _group_by_type(monthly_df, prefix=None):
        deals = monthly_df[monthly_df["dealname"] != "TOTAL"].copy()
        deals["revenue_type"] = deals["revenue_type"].fillna("unknown")
        months = find_month_columns(deals)
        grouped = deals.groupby("revenue_type")[months].sum()
        # Sort by preferred order, then alphabetically for unlisted types
        sort_key = [order.get(t, len(order)) for t in grouped.index]
        grouped = grouped.iloc[
            sorted(range(len(sort_key)), key=lambda i: (sort_key[i], grouped.index[i]))
        ]
        if prefix:
            grouped.index = prefix + grouped.index
        return grouped

    committed_by_type = _group_by_type(commitment_monthly)
    committed_by_type.loc["TOTAL (committed)"] = committed_by_type.sum()

    pipeline_by_type = _group_by_type(pipeline_monthly, prefix="pipeline: ")
    pipeline_by_type.loc["TOTAL (pipeline)"] = pipeline_by_type.sum()

    combined_total = (
        committed_by_type.loc["TOTAL (committed)"]
        + pipeline_by_type.loc["TOTAL (pipeline)"]
    )
    by_type = pd.concat([committed_by_type, pipeline_by_type]).fillna(0)
    by_type.loc["TOTAL (committed + pipeline)"] = combined_total.fillna(0)

    # Limit to n_months from projection_start
    all_months = find_month_columns(by_type)
    cutoff = (projection_start + pd.DateOffset(months=n_months)).strftime("%Y-%m")
    keep = [m for m in all_months if m < cutoff]
    by_type = by_type[keep]

    return by_type
