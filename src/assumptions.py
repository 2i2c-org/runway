"""Business assumptions. All tunable rules in one place."""

MAU_EXCLUDED_CLUSTER_SUBSTRINGS = ("prometheus", "hhmi")

PIPELINE_STAGES = ("Discovery", "Enrichment", "Outreach", "Proposal", "Renewal")

AVG_DAYS_PER_MONTH = 30.44

PROJECTION_ORIGIN = "2025-06-01"

SIMULATION_RUNS = 1000
SIMULATION_SEED = 42

SCENARIO_PERCENTILES = {
    "Pessimistic": 10,
    "Optimistic": 90,
}
