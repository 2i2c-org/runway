"""Fetch HubSpot deals data."""

import json
import os
from datetime import datetime
from typing import Dict, List

try:
    from hubspot import HubSpot
    from hubspot.crm.deals import ApiException
except ImportError:
    HubSpot = None

PROPERTIES: List[str] = [
    "dealname",
    "amount",
    "closedate",
    "dealstage",
    "contract_start_date",
    "contract_end_date",
    "hs_mrr",
    "hs_arr",
    "target_start_date",
    "target_end_date",
    "hs_deal_stage_probability",  # Deal probability (auto-set by deal stage)
    "hs_forecast_probability",
    "hs_projected_amount",
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


def fetch_deals(token: str, view_id: str = None) -> Dict:
    """Fetch deals using HubSpot Search API with view filters or get all deals.

    Args:
        token: HubSpot access token
        view_id: Optional HubSpot view ID to fetch filters from
    """
    if view_id:
        # Fetch view configuration and apply its filters
        print(f"Fetching filters from view {view_id}...")
        filter_groups = get_view_filters(token, view_id)
        return fetch_deals_search(token, filter_groups=filter_groups)
    elif HubSpot is not None:
        return fetch_deals_client(token)
    else:
        return fetch_deals_rest(token)


def fetch_deals_client(token: str) -> Dict:
    """Fetch all deals via hubspot-api-client (handles pagination)."""
    client = HubSpot(access_token=token)
    try:
        deals = client.crm.deals.get_all(properties=PROPERTIES)
    except ApiException as err:
        raise RuntimeError(f"HubSpot client error: {err}") from err

    # Convert datetimes to ISO strings so JSON serialization succeeds
    raw_results = [deal.to_dict() for deal in deals]
    results = json.loads(json.dumps(raw_results, default=str))
    return {
        "results": results,
        "meta": {
            "total": len(results),
            "fetched_at": datetime.utcnow().isoformat(),
            "properties": PROPERTIES,
            "source": "hubspot-api-client",
        },
    }


def get_view_filters(token: str, view_id: str) -> list:
    """Fetch filter configuration from a HubSpot view.

    Args:
        token: HubSpot access token
        view_id: The view ID from the HubSpot URL

    Returns:
        List of filter groups compatible with Search API
    """
    import requests

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Try to fetch view configuration
    # Note: This might be an internal API endpoint
    url = f"https://api.hubapi.com/crm/v3/objects/deals/views/{view_id}"

    try:
        response = requests.get(url, headers=headers, timeout=30)
        print(f"View API response status: {response.status_code}")
        if response.status_code == 200:
            view_config = response.json()
            print(f"View configuration received")
            # Extract filters from view configuration
            # The exact structure depends on HubSpot's API response
            if "filters" in view_config:
                print("Using 'filters' from view config")
                return view_config["filters"]
            elif "filterGroups" in view_config:
                print("Using 'filterGroups' from view config")
                return view_config["filterGroups"]
            else:
                print(f"View config keys: {list(view_config.keys())}")
        else:
            print(f"View API error: {response.text[:200]}")
    except Exception as e:
        print(f"Could not fetch view config: {e}")

    # Fallback: return default filter (exclude closedlost)
    return [
        {
            "filters": [
                {
                    "propertyName": "dealstage",
                    "operator": "NEQ",
                    "value": "closedlost",
                }
            ]
        }
    ]


def fetch_deals_search(token: str, filter_groups: list = None) -> Dict:
    """Fetch deals using HubSpot Search API with filters."""
    import json as json_module
    import requests

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    url = "https://api.hubapi.com/crm/v3/objects/deals/search"

    # Default filter: exclude closedlost
    if filter_groups is None:
        filter_groups = [
            {
                "filters": [
                    {
                        "propertyName": "dealstage",
                        "operator": "NEQ",
                        "value": "closedlost",
                    }
                ]
            }
        ]

    # Print the search parameters
    print("Search API filter groups:")
    print(json_module.dumps(filter_groups, indent=2))

    payload = {
        "filterGroups": filter_groups,
        "properties": PROPERTIES,
        "limit": 100,
    }

    results = []
    after = None

    while True:
        if after:
            payload["after"] = after

        response = requests.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(
                f"HubSpot Search API error {response.status_code}: {response.text}"
            )

        data = response.json()
        results.extend(data.get("results", []))

        paging = data.get("paging", {})
        after = paging.get("next", {}).get("after")
        if not after:
            break

    return {
        "results": results,
        "meta": {
            "total": len(results),
            "fetched_at": datetime.utcnow().isoformat(),
            "properties": PROPERTIES,
            "source": "hubspot-search-api",
        },
    }


def fetch_deals_rest(token: str) -> Dict:
    """Fetch all deals from HubSpot REST API with pagination."""
    import requests

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    params = {
        "limit": 100,
        "properties": ",".join(PROPERTIES),
    }
    url = "https://api.hubapi.com/crm/v3/objects/deals"

    results = []
    after = None
    page = 1

    while True:
        if after:
            params["after"] = after
        elif "after" in params:
            params.pop("after")

        response = requests.get(url, headers=headers, params=params, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(
                f"HubSpot API error {response.status_code}: {response.text}"
            )

        data = response.json()
        results.extend(data.get("results", []))

        paging = data.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after:
            break
        page += 1

    return {
        "results": results,
        "meta": {
            "total": len(results),
            "fetched_at": datetime.utcnow().isoformat(),
            "properties": PROPERTIES,
            "pages": page,
        },
    }
