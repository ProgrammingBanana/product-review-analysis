# product-review-analysis

HCI needfinding project — app store review collector.

## File structure

```
shared.py            # CONFIG + shared pipeline functions (imported by both scrapers)
scraper_play.py      # Google Play collector — run independently
scraper_appstore.py  # Apple App Store collector — run independently
```

## Setup

Requires [Homebrew](https://brew.sh) and `pipenv` (already installed if you followed setup).

Install dependencies:

```bash
pipenv install
```

## Configuration

Open `shared.py` and fill in the `CONFIG` section at the top:

| Variable | Description | Example |
|---|---|---|
| `APP_NAME` | Human-readable app name (used in output filenames) | `'TikTok'` |
| `PLAY_PACKAGE_ID` | Google Play package ID | `'com.zhiliaoapp.musically'` |
| `IOS_APP_ID` | Apple App Store numeric app ID | `'835599320'` |
| `IOS_APP_NAME` | App name slug as it appears in the App Store URL | `'tiktok'` |

The other variables (`COUNTRIES`, `KEYWORDS`, `MONTHS_BACK`, etc.) are pre-configured with sensible defaults but can be adjusted.

**Where to find the IDs:**
- **Google Play package ID**: open the app's Play Store page in a browser; the ID is the `id=` parameter in the URL (e.g. `https://play.google.com/store/apps/details?id=com.zhiliaoapp.musically`).
- **Apple App Store numeric ID**: open the app's App Store page in a browser; the numeric ID appears in the URL after `/id` (e.g. `https://apps.apple.com/us/app/tiktok/id835599320` → ID is `835599320`).

## Running the scrapers

Run each scraper independently. Both read CONFIG from `shared.py`.

**Google Play:**
```bash
pipenv run python scraper_play.py
```

**Apple App Store:**
```bash
pipenv run python scraper_appstore.py
```

Each scraper prints progress through every stage: collection → normalization → deduplication → date filter → keyword filter → stratified sample → anonymize → export.

## Output

Each scraper writes two files to the current directory:

```
{APP_NAME}_reviews_sample_{YYYY-MM-DD}.csv
{APP_NAME}_reviews_sample_{YYYY-MM-DD}.xlsx
```

The CSV uses UTF-8 with BOM encoding so it opens correctly in Excel on Windows without garbled characters. Both files share the same columns:

| Column | Description |
|---|---|
| `review_id` | Platform-assigned review identifier |
| `platform` | `google_play` or `app_store` |
| `country` | Two-letter country code |
| `star_rating` | 1–5 star rating |
| `date` | Review date |
| `text` | Review body text |
| `author` | Anonymized as `Reviewer_001`, `Reviewer_002`, … |
| `sort_order` | Google Play sort pass (`newest` / `most_relevant`); empty for App Store |

## Pipeline (same for both scrapers)

```
Collect
  → Normalize to common schema (defined in shared.py)
  → Deduplicate by (platform, review_id)
  → Filter: reviews from the past 24 months
  → Filter: reviews containing at least one keyword
  → Stratified sample (by star rating × platform, up to 150 reviews)
  → Anonymize reviewer identities
  → Export CSV + Excel
```
