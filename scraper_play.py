"""
scraper_play.py — Google Play review collector.

Collects reviews from Google Play, normalizes them to the shared schema, then
runs the full pipeline (dedup → date filter → keyword filter → sample →
anonymize → export) via shared.py.

Usage:
    pipenv run python scraper_play.py
"""

import time

from google_play_scraper import reviews as gp_reviews, Sort

import shared

# ─────────────────────────────────────────────────────────────────────────────
# COLLECTION
# ─────────────────────────────────────────────────────────────────────────────

def collect(package_id, pages, rate_limit):
    """
    Collect reviews from Google Play for the given package ID.

    Runs two passes — NEWEST and MOST_RELEVANT — so the dataset captures both
    recent feedback and historically upvoted reviews. Each pass fetches `pages`
    pages (~200 reviews per page), pausing `rate_limit` seconds between
    requests to avoid throttling. Returns a flat list of raw review dicts, each
    with an added 'sort_order' key.
    """
    all_reviews = []

    for sort_order, label in [(Sort.NEWEST, "newest"),
                               (Sort.MOST_RELEVANT, "most_relevant")]:
        print(f"  [Play] Collecting {pages} pages — sort: {label}")
        token = None

        for page in range(1, pages + 1):
            try:
                result, token = gp_reviews(
                    package_id,
                    lang="en",
                    country="us",
                    sort=sort_order,
                    count=200,
                    continuation_token=token,
                )
                for r in result:
                    r["sort_order"] = label
                all_reviews.extend(result)
                print(f"    Page {page}/{pages} — {len(result)} fetched "
                      f"(total: {len(all_reviews)})")
            except Exception as exc:
                print(f"    [WARN] Page {page} failed: {exc}")

            if token is None:
                print(f"    No more pages after {page}.")
                break

            time.sleep(rate_limit)

    print(f"\n  [Play] Done — {len(all_reviews)} raw reviews.\n")
    return all_reviews


# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZATION
# ─────────────────────────────────────────────────────────────────────────────

def normalize(raw_reviews):
    """
    Map raw google-play-scraper dicts to the shared normalized schema.

    google-play-scraper keys: reviewId, score, at (datetime), content,
    userName, sort_order. Country is hard-coded to 'us' because Google Play
    returns a global feed regardless of the country parameter. Reviews with
    missing or empty text are dropped.
    """
    normalized = []
    for r in raw_reviews:
        text = (r.get("content") or "").strip()
        if not text:
            continue
        normalized.append({
            "review_id":   r.get("reviewId", ""),
            "platform":    "google_play",
            "country":     "us",
            "star_rating": r.get("score"),
            "date":        r.get("at"),
            "text":        text,
            "author":      r.get("userName", ""),
            "sort_order":  r.get("sort_order", ""),
        })
    print(f"  Normalization: {len(raw_reviews)} raw → {len(normalized)} valid.\n")
    return normalized


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"  Google Play Scraper — {shared.APP_NAME}")
    print("=" * 60 + "\n")

    print("── COLLECTION ──────────────────────────────────────────────\n")
    raw = collect(shared.PLAY_PACKAGE_ID, shared.PLAY_PAGES, shared.RATE_LIMIT_SECONDS)
    total_collected = len(raw)

    print("── NORMALIZATION ───────────────────────────────────────────\n")
    reviews = normalize(raw)

    shared.run_pipeline(
        reviews,
        platform_label="Google Play",
        app_name=shared.APP_NAME,
    )

    print(f"  Total collected from Google Play: {total_collected}")


if __name__ == "__main__":
    main()
