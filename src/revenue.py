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


def _month_start(dt):
    return pd.Timestamp(dt.year, dt.month, 1)


def _month_range(start_month, end_month):
    return pd.date_range(start_month, end_month, freq="MS").tolist()


def _remaining(deal):
    """Amount minus amount_collected, handling NaN."""
    amt = pd.to_numeric(deal.get("amount", 0), errors="coerce")
    col = pd.to_numeric(deal.get("amount_collected", 0), errors="coerce")
    return max(0, (0 if pd.isna(amt) else amt) - (0 if pd.isna(col) else col))


def build_monthly_revenue(active_df, projection_start):
    """Spread monthly expected revenue across future months based on start date and expected monthly amount."""

    end_dates = pd.to_datetime(active_df["use_end_date"])
    months = _month_range(projection_start, _month_start(end_dates.max()))
    # We're basically adding an extra column for each month of our projections.
    # We'll end up concatenating all this into one big dataframe that will fill in
    # $0 for holes in months w/o projections for a deal.
    month_labels = [m.strftime("%Y-%m") for m in months]
    deal_rows = []

    start = pd.to_datetime(active_df["use_start_date"])
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

        monthly_rev = deal["monthly_revenue"]
        expected = round(monthly_rev * deal_prob, 0)

        row = {
            "dealname": deal.get("dealname", ""),
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
            row[ml] = expected if active else ""
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


def build_projections(active_df, projection_start, mau_revenue=None):
    """Monte Carlo revenue projections. It basically does the following 1000 times:

    - For each deal
        - Flip a coin, weighted by the probability of success of that deal
        - If yes, then it is "won" so we include full revenue in revenue projections
        - We then project the monthly revenue over each month of the period of performance.
        - If no, then it is "lost" and we drop it.
    - We then sum across all deals for each month to get the projected monthly revenue for that simulation.

    After 1000 simulations, we collect the 10th/50th/90th percentiles of the results.

    Note that any "closed won" deals always contribute because their probability is 1.
    """
    start_month = pd.Timestamp(PROJECTION_ORIGIN)
    end_dates = pd.to_datetime(active_df["use_end_date"])
    end_month = _month_start(
        end_dates.max()
    )  # End at the latest contract month across all deals

    months = _month_range(start_month, end_month)
    month_labels = [m.strftime("%Y-%m") for m in months]
    n_months = len(months)

    d_start = pd.to_datetime(active_df["use_start_date"])
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

    d_rev = active_df["monthly_revenue"].fillna(0).values

    # "Committed" deals have probability = 1 and always contribute
    committed_mask = d_prob >= 1.0
    committed_totals = np.zeros(n_months)
    for i in range(n_months):
        committed_totals[i] = d_rev[committed_mask & active_mask[:, i]].sum()

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
        "Committed": np.round(committed_totals).tolist(),
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

    result = pd.DataFrame(rows, index=month_labels).T
    result.index.name = "scenario"
    return result
