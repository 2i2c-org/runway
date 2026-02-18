#!/usr/bin/env python3
"""Download latest deals from HubSpot and save locally."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

from src.hubspot_fetcher import fetch_deals, load_hubspot_token

load_dotenv()

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

token = load_hubspot_token()
df, meta = fetch_deals(token)

# Save as CSV for simplicity
output_path = DATA_DIR / "deals.csv"
df.to_csv(output_path, index=False)

print(f"Downloaded {len(df)} deals to {output_path}")
print(f"Filtered out closed-lost deals: {meta['filtered_out']}")
print(f"Columns: {list(df.columns)}")
print(f"Deal stages: {sorted(df['dealstage'].dropna().unique().tolist())}")
