"""Tests for HubSpot deal fetch behavior."""

import pytest

import src.hubspot_fetcher as hubspot_fetcher


class _DummyDeal:
    def __init__(self, deal_id, properties):
        self._deal_id = deal_id
        self._properties = properties

    def to_dict(self):
        return {"id": self._deal_id, "properties": self._properties}


class _DummyDealsApi:
    def get_all(self, properties):
        return [
            _DummyDeal(
                "1",
                {
                    "dealname": "Deal 1",
                    "dealstage": "stage-1",
                    "closedate": "2026-01-01T00:00:00Z",
                },
            )
        ]


class _DummyCRM:
    deals = _DummyDealsApi()


class _DummyHubSpotClient:
    crm = _DummyCRM()


def test_fetch_deals_fails_fast_when_stage_mapping_fails(monkeypatch):
    """Stage label resolution failures should stop the sync."""
    monkeypatch.setattr(
        hubspot_fetcher, "HubSpot", lambda access_token: _DummyHubSpotClient()
    )

    def _raise_stage_error(client):
        raise hubspot_fetcher.PipelinesApiException(status=500, reason="boom")

    monkeypatch.setattr(hubspot_fetcher, "_get_dealstage_labels", _raise_stage_error)

    with pytest.raises(RuntimeError, match="HubSpot pipelines API error"):
        hubspot_fetcher.fetch_deals("token")
