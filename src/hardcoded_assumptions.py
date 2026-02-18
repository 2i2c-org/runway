"""Centralized hard-coded business assumptions used in data transforms."""

# MAU table exclusions copied from KPI dashboard behavior and known data issues.
# Keep these in one place so it is clear what we intentionally drop.
MAU_EXCLUDED_HUB_SUBSTRINGS = ("staging",)
MAU_EXCLUDED_CLUSTER_SUBSTRINGS = ("prometheus",)
MAU_EXCLUDED_CLUSTER_HUB_PAIRS = (
    # Upstream data issue: this pair is currently known-bad in KPI source data.
    ("utoronto", "highmem"),
)
