"""Business assumptions. All tunable rules in one place."""

MAU_EXCLUDED_CLUSTER_SUBSTRINGS = ("prometheus", "hhmi")

PIPELINE_STAGES = ("Discovery", "Enrichment", "Outreach", "Proposal", "Renewal")

PROJECTION_ORIGIN = "2025-06-01"

SIMULATION_RUNS = 1000
SIMULATION_SEED = 42

SCENARIO_PERCENTILES = {
    "Pessimistic": 10,
    "Optimistic": 90,
}

# Preferred display order for revenue types in the "by type" tab.
# Types not in this list will appear alphabetically after these.
REVENUE_TYPE_ORDER = [
    "gift",
    "grant",
    "project",
    "membership-premier",
    "membership-general",
    "mrr_membership",
    "unknown",
]
