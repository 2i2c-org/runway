"""Helpers for deriving additional columns from HubSpot deal data."""

import pandas as pd


def _blank_to_na(series: pd.Series) -> pd.Series:
    """Treat empty strings as missing values."""
    return series.replace(r"^\s*$", pd.NA, regex=True)


def add_use_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived date columns using contract values, then target fallback.

    - use_start_date: contract_start_date -> target_start_date -> empty
    - use_end_date: contract_end_date -> target_end_date -> empty
    """
    df = df.copy()

    contract_start = _blank_to_na(
        df.get("contract_start_date", pd.Series(pd.NA, index=df.index))
    )
    target_start = _blank_to_na(
        df.get("target_start_date", pd.Series(pd.NA, index=df.index))
    )
    contract_end = _blank_to_na(
        df.get("contract_end_date", pd.Series(pd.NA, index=df.index))
    )
    target_end = _blank_to_na(
        df.get("target_end_date", pd.Series(pd.NA, index=df.index))
    )

    df["use_start_date"] = contract_start.combine_first(target_start)
    df["use_end_date"] = contract_end.combine_first(target_end)

    return df
