"""Tests for Sheets upload formatting."""

import pandas as pd

from src.columns import add_use_date_columns
from src.sheets_uploader import COLUMN_ORDER, DATE_COLUMNS, format_for_sheets


def test_format_for_sheets_uses_hubspot_property_names():
    """Header row should use HubSpot property names, not display labels."""
    df = pd.DataFrame(
        {
            "id": ["123"],
            "dealname": ["Deal A"],
            "closedate": ["2026-02-18T00:00:00.000Z"],
            "contract_start_date": ["2026-03-01"],
            "contract_end_date": ["2027-02-28"],
        }
    )

    rows = format_for_sheets(df)
    header = rows[0]

    assert header[0] == "id"
    assert header[1] == "dealname"
    assert "contract_start_date" in header
    assert "contract_end_date" in header
    assert "use_start_date" in header
    assert "use_end_date" in header
    assert "Record ID" not in header
    assert "Deal Name" not in header
    assert "CONTRACT_START_DATE" not in header


def test_format_for_sheets_parses_mixed_iso_closedate_formats():
    """Date-only and timestamp strings should both produce close dates."""
    df = pd.DataFrame(
        {
            "id": ["1", "2"],
            "dealname": ["Date only", "Timestamp"],
            "closedate": ["2026-06-30", "2026-02-11T20:15:15.040Z"],
        }
    )

    rows = format_for_sheets(df)
    header = rows[0]
    idx_name = header.index("dealname")
    idx_close = header.index("closedate")
    close_by_name = {row[idx_name]: row[idx_close] for row in rows[1:]}

    assert close_by_name["Date only"] == "2026-06-30"
    assert close_by_name["Timestamp"] == "2026-02-11"


def test_format_for_sheets_preserves_metadata_and_parseable_dates_by_id():
    """Parseable date inputs should not become blank after formatting."""
    df = pd.DataFrame(
        {
            "id": ["101", "102", "103"],
            "dealname": ["Timestamp close", "Date-only close", "No close date"],
            "dealstage": ["Closed Won", "Renewal", "Proposal"],
            "closedate": ["2026-02-11T20:15:15.040Z", "2026-06-30", ""],
            "hs_deal_stage_probability": [1.0, 0.4, 0.2],
            "amount": [152000, 15000, 5000],
            "target_start_date": ["2025-11-24", "2026-06-01", ""],
            "target_end_date": ["2026-11-30", "2027-05-31", ""],
            "contract_start_date": ["2026-01-01", "", ""],
            "contract_end_date": ["2026-12-31", "", ""],
            "notes_last_updated": ["2026-02-12", "2025-09-03", ""],
        }
    )

    rows = format_for_sheets(df)
    header = rows[0]
    assert header == COLUMN_ORDER

    idx_id = header.index("id")
    output_by_id = {
        row[idx_id]: {header[i]: row[i] for i in range(len(header))} for row in rows[1:]
    }

    expected_df = add_use_date_columns(df.copy())
    for _, expected_row in expected_df.iterrows():
        deal_id = str(expected_row["id"])
        actual_row = output_by_id[deal_id]

        # Metadata columns should survive formatting unchanged (stringified for Sheets)
        for col in ["dealname", "dealstage", "hs_deal_stage_probability", "amount"]:
            assert actual_row[col] == str(expected_row[col])

        # Any parseable date should remain present as YYYY-MM-DD
        for col in DATE_COLUMNS:
            parsed_value = (
                pd.to_datetime(
                    pd.Series([expected_row[col]]),
                    errors="coerce",
                    format="ISO8601",
                    utc=True,
                )
                .dt.strftime("%Y-%m-%d")
                .iloc[0]
            )
            if pd.notna(parsed_value):
                assert actual_row[col] == parsed_value
