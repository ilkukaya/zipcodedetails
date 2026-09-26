#!/usr/bin/env python3
"""
Notify IndexNow (Bing, Yandex, Seznam, Naver, …) about site URLs after a deploy.

Usage: python3 scripts/indexnow.py [--all]
  default  → core pages, states, rankings (a few hundred URLs)
  --all    → every URL in the built sitemaps (use after big content changes)

Reads SITE_URL and INDEXNOW_KEY from the environment (same defaults as
src/config/site.ts) and the generated sitemaps from dist/.
"""
import json
import os
import re
import sys
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
DIST = ROOT / "dist"
SITE = (os.environ.get("SITE_URL") or "https://zipcodedetails.netlify.app").rstrip("/")
KEY = os.environ.get("INDEXNOW_KEY") or "6f1c2a9e4b7d4e0f9a3c5b8d2e1f7a64"
CORE = ["sitemap-pages.xml", "sitemap-states.xml"]
BATCH = 10000


def urls_from(name: str) -> list[str]:
    p = DIST / name
    if not p.exists():
        return []
    return re.findall(r"<loc>([^<]+)</loc>", p.read_text(encoding="utf-8"))


def main() -> None:
    everything = "--all" in sys.argv
    files = sorted(f.name for f in DIST.glob("sitemap-*.xml") if f.name != "sitemap-index.xml") if everything else CORE
    urls: list[str] = []
    for f in files:
        urls += urls_from(f)
    if not urls:
        print("No URLs found — did the build run?")
        return
    host = urlparse(SITE).hostname
    print(f"Submitting {len(urls):,} URLs for {host} to IndexNow …")
    for i in range(0, len(urls), BATCH):
        payload = json.dumps({
            "host": host,
            "key": KEY,
            "keyLocation": f"{SITE}/{KEY}.txt",
            "urlList": urls[i:i + BATCH],
        }).encode()
        req = urllib.request.Request(
            "https://api.indexnow.org/indexnow",
            data=payload,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                print(f"  batch {i // BATCH + 1}: HTTP {r.status}")
        except Exception as e:  # never fail a deploy because of IndexNow
            print(f"  batch {i // BATCH + 1}: {e}")


if __name__ == "__main__":
    main()
