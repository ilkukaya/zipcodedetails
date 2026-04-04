"""
00_bootstrap_data.py

Creates minimal placeholder data files so the Astro site can build
even when the full data pipeline hasn't run yet.

This is only used as a fallback. In the normal GitHub Actions pipeline,
scripts 01-06 generate the full dataset before the Astro build.
"""

import json
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
ZIPS_DIR = DATA_DIR / "zips"
PUBLIC_DIR = PROJECT_ROOT / "public"


def ensure_file(path: Path, default_content: str) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(default_content, encoding="utf-8")
    print(f"  Created placeholder: {path}")


def main() -> None:
    print("Checking for required data files …")

    ZIPS_DIR.mkdir(parents=True, exist_ok=True)

    # Check if real data exists
    existing_zips = list(ZIPS_DIR.glob("*.json"))
    if len(existing_zips) > 100:
        print(f"  Found {len(existing_zips)} ZIP files — data looks complete, skipping bootstrap.")
        return

    print("  No full data found. Creating minimal placeholders for Astro build …")

    ensure_file(DATA_DIR / "state_index.json", "{}")
    ensure_file(DATA_DIR / "city_index.json", "{}")
    ensure_file(DATA_DIR / "search_index.json", "[]")
    ensure_file(DATA_DIR / "popular_zips.json", "[]")
    ensure_file(PUBLIC_DIR / "search_index.json", "[]")

    print("  Bootstrap complete. Run the full pipeline for real data.")


if __name__ == "__main__":
    main()
