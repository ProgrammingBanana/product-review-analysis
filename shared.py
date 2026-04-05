"""
shared.py — Shared CONFIG, schema definitions, and pipeline functions.

Imported by scraper_play.py and scraper_appstore.py. Contains everything
that is platform-agnostic: filtering, deduplication, sampling, anonymization,
and export.
"""

import math
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG — edit these values before running either scraper
# ─────────────────────────────────────────────────────────────────────────────

APP_NAME        = "Instagram"                  # Human-readable name; used in filenames
PLAY_PACKAGE_ID = "com.instagram.android"        # e.g. 'com.zhiliaoapp.musically'
IOS_APP_ID      = "389801252"        # e.g. '835599320'
IOS_APP_NAME    = "instagram"                      # URL slug used in the App Store

COUNTRIES       = ["us", "gb", "ca", "au"] # App Store country codes to query

KEYWORDS = [
    "scroll", "scrolling", "addiction", "addicted", "addict",
    "mental health", "anxiety", "depression", "dopamine", "algorithm",
    "privacy", "data", "tracking", "manipulate", "manipulation",
    "can't stop", "cannot stop", "hours", "waste", "screen time",
    "compulsive", "compulsion", "mindless", "endless",
]

MONTHS_BACK        = 24    # How many months back to collect reviews
TARGET_SAMPLE_SIZE = 300   # Target number of reviews in the final sample
PLAY_PAGES         = 30    # Pages per sort order for Google Play (~200 reviews/page)
RATE_LIMIT_SECONDS = 1.5   # Pause between paginated requests (seconds)

# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZED REVIEW SCHEMA
# ─────────────────────────────────────────────────────────────────────────────
# Both scrapers produce dicts with these keys:
#   review_id   str       platform-assigned ID
#   platform    str       'google_play' or 'app_store'
#   country     str       two-letter country code
#   star_rating int       1–5
#   date        datetime  timezone-aware
#   text        str       review body
#   author      str       username (anonymized at export stage)
#   sort_order  str       'newest'/'most_relevant' (Play); '' (App Store)

# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def deduplicate(reviews):
    """
    Remove duplicate reviews within each platform.

    Uses a (platform, review_id) composite key so identical numeric IDs that
    happen to appear on both platforms are kept. Duplicates arise when the
    same review surfaces across multiple collection passes or sort orders.
    """
    seen = set()
    unique = []
    for r in reviews:
        key = (r["platform"], r["review_id"])
        if key not in seen:
            seen.add(key)
            unique.append(r)

    removed = len(reviews) - len(unique)
    print(f"  Deduplication: {len(reviews)} → {len(unique)} ({removed} removed).\n")
    return unique


def filter_by_date(reviews, months_back):
    """
    Retain only reviews posted within the past `months_back` months.

    Uses relativedelta for calendar-accurate month arithmetic. Reviews with a
    missing or non-datetime date are dropped to avoid including reviews of
    unknown age in the dataset.
    """
    cutoff = datetime.now(timezone.utc) - relativedelta(months=months_back)
    kept, dropped = [], 0

    for r in reviews:
        d = r.get("date")
        if not isinstance(d, datetime):
            dropped += 1
            continue
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        if d >= cutoff:
            kept.append(r)
        else:
            dropped += 1

    print(f"  Date filter ({months_back} months): kept {len(kept)}, "
          f"dropped {dropped}.\n")
    return kept


def filter_by_keywords(reviews, keywords):
    """
    Retain only reviews that contain at least one keyword (case-insensitive).

    Prints per-keyword match counts after filtering so the team can evaluate
    which topics appear most in the corpus and refine the keyword list.
    """
    lower_kws = [kw.lower() for kw in keywords]
    matched = []
    counts = {kw: 0 for kw in keywords}

    for r in reviews:
        text_lower = r["text"].lower()
        hits = [kw for kw, lkw in zip(keywords, lower_kws) if lkw in text_lower]
        if hits:
            matched.append(r)
            for kw in hits:
                counts[kw] += 1

    print(f"  Keyword filter: {len(matched)} matched of {len(reviews)}.")
    print("  Per-keyword counts (non-zero only):")
    for kw, n in sorted(counts.items(), key=lambda x: -x[1]):
        if n:
            print(f"    {kw!r}: {n}")
    print()
    return matched


def stratified_sample(reviews, target_size):
    """
    Draw a proportionally stratified sample of up to `target_size` reviews.

    Stratifies across star_rating (1–5) × platform (google_play, app_store).
    Each stratum's allocation is proportional to its share of the pool. When
    a stratum is smaller than its allocation, all its reviews are included and
    the shortfall is redistributed proportionally to remaining strata.
    """
    if len(reviews) <= target_size:
        print(f"  Sample: pool ({len(reviews)}) ≤ target ({target_size}); "
              "returning full pool.\n")
        return reviews

    strata = {}
    for r in reviews:
        key = (r.get("star_rating"), r.get("platform"))
        strata.setdefault(key, []).append(r)

    # Phase 1: iteratively fix strata that are smaller than their proportional
    # share, redistributing their unused quota to the remaining strata.
    # Each iteration removes at least one stratum from `free`, so the loop
    # always terminates.
    quota = target_size
    fixed = {}          # key -> final count (stratum exhausted)
    free  = dict(strata)

    while free:
        free_total = sum(len(v) for v in free.values())
        newly_fixed = {
            k: len(v)
            for k, v in free.items()
            if math.floor(len(v) / free_total * quota) >= len(v)
        }
        if not newly_fixed:
            break
        for k, n in newly_fixed.items():
            fixed[k] = n
            quota -= n
            del free[k]

    # Phase 2: distribute remaining quota across free strata using floor
    # allocation + largest-remainder method to avoid rounding drift.
    if free:
        free_total = sum(len(v) for v in free.values())
        raw    = {k: len(v) / free_total * quota for k, v in free.items()}
        floors = {k: math.floor(v) for k, v in raw.items()}
        remainder = quota - sum(floors.values())
        for k in sorted(free, key=lambda k: raw[k] - floors[k], reverse=True):
            if remainder <= 0:
                break
            floors[k] += 1
            remainder -= 1
        fixed.update(floors)

    sampled = {k: strata[k][: fixed.get(k, 0)] for k in strata}
    result = [r for items in sampled.values() for r in items]

    print("  Sample composition:")
    for (rating, platform), items in sorted(sampled.items()):
        print(f"    {platform} ★{rating}: {len(items)}")
    print(f"  Final sample size: {len(result)}\n")
    return result


def anonymize(reviews):
    """
    Replace each reviewer's author field with Reviewer_001, Reviewer_002, …

    Sequential IDs are assigned in the order reviews appear in the sampled
    list. This satisfies basic IRB de-identification requirements.
    """
    for i, r in enumerate(reviews, start=1):
        r["author"] = f"Reviewer_{i:03d}"
    return reviews


def export(reviews, app_name):
    """
    Export the final anonymized sample to a date-stamped CSV and Excel file.

    CSV uses UTF-8-BOM encoding so it opens without garbled characters in
    Excel on Windows. Both files are written to the current directory.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    base  = f"{app_name.replace(' ', '_')}_reviews_sample_{today}"

    columns = ["review_id", "platform", "country", "star_rating",
               "date", "text", "author", "sort_order"]

    df = pd.DataFrame(reviews, columns=columns)

    csv_path = f"{base}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    print(f"  Exported {len(df)} rows:")
    print(f"    CSV: {csv_path}\n")
    return csv_path


def run_pipeline(reviews, platform_label, app_name,
                 months_back=MONTHS_BACK,
                 keywords=KEYWORDS,
                 target_size=TARGET_SAMPLE_SIZE):
    """
    Run the shared post-collection pipeline on a list of normalized reviews.

    Stages: deduplication → date filter → keyword filter → stratified sample
    → anonymize → export. Returns the final sampled list.

    `platform_label` is used only for the section header printed to the
    terminal so the user knows which scraper is running.
    """
    print(f"── DEDUPLICATION ({platform_label}) " + "─" * 30 + "\n")
    reviews = deduplicate(reviews)
    after_dedup = len(reviews)

    print(f"── DATE FILTER ({platform_label}) " + "─" * 33 + "\n")
    reviews = filter_by_date(reviews, months_back)
    after_date = len(reviews)

    print(f"── KEYWORD FILTER ({platform_label}) " + "─" * 30 + "\n")
    reviews = filter_by_keywords(reviews, keywords)
    after_keywords = len(reviews)

    print(f"── STRATIFIED SAMPLE ({platform_label}) " + "─" * 27 + "\n")
    reviews = stratified_sample(reviews, target_size)
    final_size = len(reviews)

    print(f"── ANONYMIZE ({platform_label}) " + "─" * 35 + "\n")
    reviews = anonymize(reviews)
    print(f"  {len(reviews)} identities anonymized.\n")

    print(f"── EXPORT ({platform_label}) " + "─" * 38 + "\n")
    export(reviews, app_name)

    print("=" * 60)
    print(f"  SUMMARY — {platform_label}")
    print("=" * 60)
    print(f"  After deduplication:  {after_dedup}")
    print(f"  After date filter:    {after_date}")
    print(f"  After keyword filter: {after_keywords}")
    print(f"  Final sample size:    {final_size}")
    print("=" * 60 + "\n")

    return reviews
