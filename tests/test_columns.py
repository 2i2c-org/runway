"""Tests for derived column helpers."""

import pandas as pd

from src.columns import add_use_date_columns


def test_add_use_date_columns_prefers_contract_then_target():
    """Derived columns should prefer contract dates, then target dates."""
    df = pd.DataFrame(
        {
            "contract_start_date": ["2026-01-01", None, "", None],
            "target_start_date": ["2026-01-02", "2026-02-01", "2026-03-01", None],
            "contract_end_date": ["2026-12-31", "", None, None],
            "target_end_date": ["2027-01-01", "2027-02-01", "2027-03-01", None],
        }
    )

    out = add_use_date_columns(df)

    assert out["use_start_date"].fillna("").tolist() == [
        "2026-01-01",
        "2026-02-01",
        "2026-03-01",
        "",
    ]
    assert out["use_end_date"].fillna("").tolist() == [
        "2026-12-31",
        "2027-02-01",
        "2027-03-01",
        "",
    ]
