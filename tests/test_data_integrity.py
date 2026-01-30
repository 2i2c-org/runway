"""Tests to verify downloaded data matches expected schema."""

import json
from pathlib import Path

import pandas as pd
import pytest

DATA_DIR = Path(__file__).parent.parent / "data"
DEALS_FILE = DATA_DIR / "deals.csv"
SCHEMA_FILE = DATA_DIR / "schema.json"


@pytest.fixture
def schema():
    """Load expected schema."""
    with open(SCHEMA_FILE) as f:
        return json.load(f)


@pytest.fixture
def deals_df():
    """Load downloaded deals data."""
    if not DEALS_FILE.exists():
        pytest.fail(
            f"Data file not found: {DEALS_FILE}\n"
            "Run 'nox -s download' first to download data from HubSpot."
        )
    return pd.read_csv(DEALS_FILE)


def test_columns_match(deals_df, schema):
    """Verify downloaded data has expected columns."""
    expected = set(schema["columns"])
    actual = set(deals_df.columns)

    missing = expected - actual
    if missing:
        pytest.fail(f"Missing expected columns: {missing}")

    extra = actual - expected
    if extra:
        pytest.fail(f"Unexpected new columns: {extra}")


def test_dealstages_match(deals_df, schema):
    """Verify deal stages match expected values."""
    expected = set(schema["dealstages"])
    actual = set(deals_df["dealstage"].dropna().unique())

    missing = expected - actual
    new = actual - expected

    errors = []
    if missing:
        errors.append(f"Missing deal stages: {missing}")
    if new:
        errors.append(f"New deal stages found: {new}")

    if errors:
        pytest.fail(
            "\n".join(errors) + "\n\n"
            "If these changes are expected, update data/schema.json"
        )
