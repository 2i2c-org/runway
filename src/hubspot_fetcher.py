"""Fetch HubSpot deals data as a pandas DataFrame."""

import os
from typing import Dict, Tuple

import pandas as pd
from hubspot import HubSpot
from hubspot.crm.deals import ApiException as DealsApiException
from hubspot.crm.pipelines import ApiException as PipelinesApiException


EXCLUDED_DEALSTAGES = {"closedlost"}
DEAL_PROPERTIES = [
    "dealname",
    "dealstage",
    "closedate",
    "hs_deal_stage_probability",
    "amount",
    "target_start_date",
    "target_end_date",
    "notes_last_updated",
]


def load_hubspot_token() -> str:
    """Return the HubSpot token from the environment."""
    token = os.environ.get("HUBSPOT_ACCESS_TOKEN") or os.environ.get("HUBSPOT_TOKEN")
    if not token:
        raise RuntimeError(
            "Missing HUBSPOT_ACCESS_TOKEN (or HUBSPOT_TOKEN). "
            "Add it to your environment or .env file."
        )
    return token


def _get_dealstage_labels(client: HubSpot) -> Dict[str, str]:
    """Return mapping of deal stage id -> stage label."""
    pipelines = client.crm.pipelines.pipelines_api.get_all("deals")
    labels: Dict[str, str] = {}
    for pipeline in pipelines.results:
        for stage in pipeline.stages:
            labels[str(stage.id)] = stage.label
    return labels


def fetch_deals(token: str) -> Tuple[pd.DataFrame, dict]:
    """Fetch all deals as a DataFrame, filtering out closed-lost deals.

    Returns:
        Tuple of (DataFrame with deals, metadata dict with counts and any errors)
    """
    client = HubSpot(access_token=token)

    try:
        deals = client.crm.deals.get_all(properties=DEAL_PROPERTIES)
    except DealsApiException as err:
        raise RuntimeError(f"HubSpot API error: {err}") from err

    # Convert to DataFrame and keep only the columns we need
    df = pd.json_normalize([deal.to_dict() for deal in deals])
    df.columns = df.columns.str.replace("properties.", "", regex=False)

    # Keep only id + requested properties
    keep_cols = ["id"] + DEAL_PROPERTIES
    df = df[[c for c in keep_cols if c in df.columns]]

    # Filter out closed-lost deals
    original_count = len(df)
    if "dealstage" in df.columns:
        df = df[~df["dealstage"].isin(EXCLUDED_DEALSTAGES)]
    filtered_out = original_count - len(df)

    # Resolve deal stage IDs to labels
    stage_error = None
    try:
        stages = _get_dealstage_labels(client)
        if "dealstage" in df.columns:
            df["dealstage"] = df["dealstage"].map(stages).fillna(df["dealstage"])
    except (PipelinesApiException, Exception) as err:
        stage_error = str(err)

    meta = {
        "total": len(df),
        "filtered_out": filtered_out,
        "stage_error": stage_error,
    }
    return df, meta
