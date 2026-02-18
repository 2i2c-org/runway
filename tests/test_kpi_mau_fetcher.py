"""Tests for KPI monthly users fetcher transforms."""

import pandas as pd
import pytest

from src.kpi_mau_fetcher import build_mau_table


def test_build_mau_table_requires_expected_columns():
    """Missing required fields should fail with a clear error."""
    df = pd.DataFrame({"cluster": ["a"], "date": ["2026-01-01"], "users": [1]})

    with pytest.raises(RuntimeError, match="missing required columns"):
        build_mau_table(df)


def test_build_mau_table_uses_monthly_fields_and_dashboard_filters():
    """Monthly users should come from hub-activity monthly timescale rows."""
    df = pd.DataFrame(
        [
            # ucmerced monthly values should aggregate to month max
            {
                "cluster": "ucmerced",
                "hub": "prod",
                "date": "2026-02-09",
                "users": 1508,
                "timescale": "monthly",
            },
            {
                "cluster": "ucmerced",
                "hub": "prod",
                "date": "2026-02-10",
                "users": 1510,
                "timescale": "monthly",
            },
            # Daily rows should not be used
            {
                "cluster": "ucmerced",
                "hub": "prod",
                "date": "2026-02-10",
                "users": 180,
                "timescale": "daily",
            },
            # Staging hubs should be dropped
            {
                "cluster": "ucmerced",
                "hub": "staging",
                "date": "2026-02-10",
                "users": 1,
                "timescale": "monthly",
            },
            # Prometheus clusters should be dropped
            {
                "cluster": "federated-prometheus",
                "hub": "r-prod",
                "date": "2026-02-10",
                "users": 4113,
                "timescale": "monthly",
            },
            # uToronto/highmem should be dropped due known bug
            {
                "cluster": "utoronto",
                "hub": "highmem",
                "date": "2026-02-10",
                "users": 9999,
                "timescale": "monthly",
            },
            # Keep valid uToronto r-prod row
            {
                "cluster": "utoronto",
                "hub": "r-prod",
                "date": "2026-02-10",
                "users": 4113,
                "timescale": "monthly",
            },
        ]
    )

    out = build_mau_table(df)

    assert list(out.columns) == ["cluster", "hub", "date", "users"]
    assert len(out) == 2

    ucmerced = out[(out["cluster"] == "ucmerced") & (out["hub"] == "prod")].iloc[0]
    assert ucmerced["date"] == "2026-02-01"
    assert ucmerced["users"] == 1510

    utoronto = out[(out["cluster"] == "utoronto") & (out["hub"] == "r-prod")].iloc[0]
    assert utoronto["date"] == "2026-02-01"
    assert utoronto["users"] == 4113
