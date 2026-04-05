"""
scraper_appstore.py — Apple App Store review collector.

Fetches reviews from Apple's public iTunes customer-reviews RSS feed using
httpx (no third-party scraper package required). Normalizes them to the shared
schema, then runs the full pipeline via shared.py.

Apple's RSS feed returns up to 10 pages × 50 reviews = 500 reviews per
country. With COUNTRIES = ['us', 'gb', 'ca', 'au'] the maximum raw pool is
~2,000 reviews.

Usage:
    pipenv run python scraper_appstore.py
"""

import time
from datetime import datetime, timezone

import httpx

import shared

# iTunes customer reviews RSS feed — returns JSON
# Page range: 1–10, ~50 reviews per page
_RSS_URL = (
    "https://itunes.apple.com/{country}/rss/customerreviews"
    "/page={page}/id={app_id}/sortby=mostrecent/json"
)

MAX_PAGES = 10  # Apple caps the feed at 10 pages per country


# ─────────────────────────────────────────────────────────────────────────────
# COLLECTION
# ─────────────────────────────────────────────────────────────────────────────

def collect(app_id, countries, rate_limit):
    """
    Collect reviews from the Apple App Store via the iTunes RSS feed.

    Iterates through every country in `countries`, fetching up to MAX_PAGES
    pages per country. Pauses `rate_limit * 2` seconds between country
    requests to reduce the chance of Apple returning 403 responses. Each raw
    review dict gets a 'country' key added before being appended to the result.

    Returns a flat list of raw review dicts parsed from the RSS JSON feed.
    """
    all_reviews = []

    with httpx.Client(timeout=30, follow_redirects=True) as client:
        for country in countries:
            print(f"  [App Store] Country: {country.upper()}")
            country_count = 0

            for page in range(1, MAX_PAGES + 1):
                url = _RSS_URL.format(country=country, page=page, app_id=app_id)
                try:
                    resp = client.get(url, headers={"User-Agent": "Mozilla/5.0"})
                    if resp.status_code == 403:
                        print(f"    [WARN] 403 on page {page} — skipping remaining pages for {country}.")
                        break
                    resp.raise_for_status()

                    feed = resp.json().get("feed", {})
                    entries = feed.get("entry", [])

                    # The first entry is app metadata, not a review
                    if page == 1 and entries:
                        entries = entries[1:]

                    if not entries:
                        print(f"    No entries on page {page}; done with {country}.")
                        break

                    for entry in entries:
                        entry["country"] = country
                    all_reviews.extend(entries)
                    country_count += len(entries)
                    print(f"    Page {page}/{MAX_PAGES} — {len(entries)} fetched "
                          f"(country total: {country_count})")

                except Exception as exc:
                    print(f"    [WARN] Page {page} for {country} failed: {exc}")
                    break

                time.sleep(rate_limit)

            print(f"  {country.upper()} done — {country_count} reviews.\n")
            time.sleep(rate_limit * 2)

    print(f"  [App Store] Done — {len(all_reviews)} raw reviews.\n")
    return all_reviews


# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZATION
# ─────────────────────────────────────────────────────────────────────────────

def _parse_date(raw):
    """Parse an ISO-8601 date string from the iTunes feed into a datetime."""
    if not raw:
        return None
    try:
        # Feed format: '2024-03-15T08:22:00-07:00'
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def normalize(raw_reviews):
    """
    Map raw iTunes RSS feed entries to the shared normalized schema.

    iTunes RSS JSON keys are nested dicts with a 'label' leaf for values,
    e.g. entry['im:rating']['label']. Reviews without text content are
    dropped. The 'country' key was added during collection.
    """
    normalized = []
    for r in raw_reviews:
        text = (r.get("content", {}).get("label") or "").strip()
        if not text:
            continue

        raw_date = r.get("updated", {}).get("label", "")

        try:
            rating = int(r.get("im:rating", {}).get("label", 0))
        except (ValueError, TypeError):
            rating = None

        normalized.append({
            "review_id":   r.get("id", {}).get("label", ""),
            "platform":    "app_store",
            "country":     r.get("country", ""),
            "star_rating": rating,
            "date":        _parse_date(raw_date),
            "text":        text,
            "author":      r.get("author", {}).get("name", {}).get("label", ""),
            "sort_order":  "",
        })

    print(f"  Normalization: {len(raw_reviews)} raw → {len(normalized)} valid.\n")
    return normalized


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"  App Store Scraper — {shared.APP_NAME}")
    print("=" * 60 + "\n")

    print("── COLLECTION ──────────────────────────────────────────────\n")
    raw = collect(shared.IOS_APP_ID, shared.COUNTRIES, shared.RATE_LIMIT_SECONDS)
    total_collected = len(raw)

    print("── NORMALIZATION ───────────────────────────────────────────\n")
    reviews = normalize(raw)

    shared.run_pipeline(
        reviews,
        platform_label="App Store",
        app_name=shared.APP_NAME,
    )

    print(f"  Total collected from App Store: {total_collected}")


if __name__ == "__main__":
    main()
