"""Compute a few "metrics" from our revenue and cost data. This is used to give
risk indicators in our google sheets dashboard.

It uses average monthly cost data from the google sheet as a proxy for costs in a few
places, this could be more accurate if we pulled in cost data directly from Sage.

It calculates metrics and when we upload we add a little "traffic light" emoji to it.

See README.md for threshold rationale and metric definitions.

"""

import pandas as pd

from src.revenue import find_month_columns


def _total_costs(monthly_costs, revenue, fsp_fee):
    """Monthly costs including the FSP fee on revenue. Just so it's use is more semantic."""
    return monthly_costs + revenue * fsp_fee


def _classify(value, thresholds):
    """Find status and next-step-up threshold from a descending thresholds list.

    Returns (status, next_val, next_status) where next_val/next_status are
    the nearest improvement target (None if already green or over).

    Thresholds must be sorted descending by value (e.g. [(24, "over"), (12, "green"), ...]).
    """
    vals = [v for v, _ in thresholds]
    assert vals == sorted(vals, reverse=True), f"Thresholds must be descending: {vals}"

    status = "red"
    for min_val, s in thresholds:
        if value >= min_val:
            status = s
            break

    next_val = next_status = None
    if status in ("red", "yellow"):
        # Walk up from bottom, take the first that isn't current status or "over"
        # This is for displaying the "next target" in the dashboard
        for v, s in reversed(thresholds):
            if s not in ("over", status):
                next_val, next_status = v, s
                break

    return status, next_val, next_status


def build_scorecard(
    projections_df,
    monthly_costs,
    fsp_fee,
    cash_on_hand=0,
    projection_start=None,
):
    """Calculate financial health metrics. Returns a DataFrame with
    columns: metric, value, status, target, detail. See README.md for definitions."""
    months = find_month_columns(projections_df)
    if projection_start is not None:
        cutoff = projection_start.strftime("%Y-%m")
        months = [m for m in months if m >= cutoff]
    committed = projections_df.loc["Committed"]

    results = [
        _runway(committed, months, monthly_costs, fsp_fee, cash_on_hand),
        _surplus_deficit(committed, months, monthly_costs, fsp_fee),
        _pipeline_coverage(projections_df, months, monthly_costs, fsp_fee),
    ]
    df = pd.DataFrame(results)
    icons = {"green": "🟢", "yellow": "🟡", "red": "🔴", "over": "🟢❗"}
    df["status"] = df["status"].map(icons)
    return df[["metric", "value", "status", "target", "detail", "question"]]



def _runway(revenue, months, monthly_costs, fsp_fee, cash_on_hand=0):
    """Months until cash runs out, starting from cash on hand.

    Note: costs vary per month (FSP fee scales with that month's revenue),
    unlike _surplus_deficit which uses a 6-month average.
    """
    runway_months = 0
    balance = cash_on_hand
    ran_out = False
    for m in months:
        rev = revenue[m]
        costs = _total_costs(monthly_costs, rev, fsp_fee)
        balance += rev - costs
        if balance >= 0:
            runway_months += 1
        else:
            ran_out = True
            break

    # Thresholds anchored to 6-9 month sales cycle — see README.md
    thresholds = [(24, "over"), (12, "green"), (6, "yellow")]
    status, next_val, next_status = _classify(runway_months, thresholds)

    target = ""
    if next_val is not None:
        more = next_val - runway_months
        target = f"Need {more} more months to reach {next_status} ({next_val}mo)"

    if ran_out:
        detail = (
            f"Starting from ${cash_on_hand:,.0f} net assets,"
            f" cash runs out in month {runway_months + 1}"
        )
    else:
        detail = (
            f"Starting from ${cash_on_hand:,.0f} net assets,"
            f" still solvent after all {runway_months} projected months"
        )

    return {
        "metric": "Committed revenue runway",
        "question": "How long does our money last?",
        "value": f"{runway_months} months",
        "status": status,
        "target": target,
        "detail": detail,
    }


def _surplus_deficit(committed, months, monthly_costs, fsp_fee):
    """6-month forward avg of committed revenue vs total costs (incl FSP fee)."""
    window = months[:6]
    avg_revenue = committed[window].mean()
    costs = _total_costs(monthly_costs, avg_revenue, fsp_fee)
    ratio = avg_revenue / costs

    thresholds = [(1.30, "over"), (1.0, "green"), (0.85, "yellow")]
    status, next_val, next_status = _classify(ratio, thresholds)

    gap = avg_revenue - costs
    direction = "surplus" if gap >= 0 else "shortfall"

    # Solving ratio = revenue / (monthly_costs + revenue * fsp_fee) for revenue
    target = ""
    if next_val is not None:
        needed = next_val * monthly_costs / (1 - next_val * fsp_fee)
        more = needed - avg_revenue
        target = f"Need ${more:,.0f}/mo more revenue to reach {next_status} ({next_val:.0%})"

    return {
        "metric": "Committed monthly surplus/deficit",
        "question": "Are we losing money month to month?",
        "value": f"{ratio:.0%}",
        "status": status,
        "target": target,
        "detail": (
            f"6-month avg: ${avg_revenue:,.0f}/mo revenue vs"
            f" ${costs:,.0f}/mo costs = ${abs(gap):,.0f}/mo {direction}"
        ),
    }


def _pipeline_coverage(projections_df, months, monthly_costs, fsp_fee):
    """Pipeline coverage ratio over next 6 months."""
    window = months[:6]
    committed = projections_df.loc["Committed"]
    estimated = projections_df.loc["Estimated"]

    total_gap = 0.0
    total_pipeline = 0.0
    for m in window:
        committed_rev = committed[m]
        costs = _total_costs(monthly_costs, committed_rev, fsp_fee)
        gap = max(0, costs - committed_rev)
        total_gap += gap
        # Pipeline contribution = estimated - committed (the uncertain part)
        pipeline_rev = max(0, estimated[m] - committed[m])
        total_pipeline += pipeline_rev

    if total_gap == 0:
        return {
            "metric": "Pipeline coverage",
            "question": "Does our pipeline cover the gap?",
            "value": "No gap",
            "status": "green",
            "target": "",
            "detail": "Committed revenue covers costs for next 6 months",
        }

    ratio = total_pipeline / total_gap

    thresholds = [(2.5, "over"), (1.5, "green"), (1.0, "yellow")]
    status, next_val, next_status = _classify(ratio, thresholds)

    target = ""
    if next_val is not None:
        more = next_val * total_gap - total_pipeline
        target = f"Need ${more:,.0f} more estimated pipeline to reach {next_status} ({next_val:.0%})"

    return {
        "metric": "Pipeline coverage",
        "question": "Does our pipeline cover the gap?",
        "value": f"{ratio:.0%}",
        "status": status,
        "target": target,
        "detail": (
            f"${total_gap:,.0f} gap over {len(window)} months,"
            f" ${total_pipeline:,.0f} in estimated pipeline ({ratio:.0%} coverage)"
        ),
    }
