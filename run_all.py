"""
run_all.py — Orchestrator: collects from both platforms, merges, then runs
the shared pipeline once on the combined dataset.

Produces a single CSV and Excel file containing a stratified sample drawn
across Google Play AND App Store reviews together, so star-rating × platform
balance is handled in one pass rather than sampling each platform separately.

If one platform fails entirely, the script continues with whatever was
collected and notes the failure in the summary.

Usage:
    pipenv run python run_all.py

To collect from only one platform, run the individual scrapers instead:
    pipenv run python scraper_play.py
    pipenv run python scraper_appstore.py
"""

import scraper_play
import scraper_appstore
import shared


def main():
    print("=" * 60)
    print(f"  Full Orchestrator — {shared.APP_NAME}")
    print("  Google Play  +  Apple App Store")
    print("=" * 60 + "\n")

    # ── COLLECTION ───────────────────────────────────────────────────────────

    print("── COLLECTION: GOOGLE PLAY ─────────────────────────────────\n")
    play_raw, play_norm = [], []
    try:
        play_raw  = scraper_play.collect(
            shared.PLAY_PACKAGE_ID, shared.PLAY_PAGES, shared.RATE_LIMIT_SECONDS
        )
        play_norm = scraper_play.normalize(play_raw)
    except Exception as exc:
        print(f"  [ERROR] Google Play collection failed: {exc}\n")

    print("── COLLECTION: APP STORE ───────────────────────────────────\n")
    ios_raw, ios_norm = [], []
    try:
        ios_raw  = scraper_appstore.collect(
            shared.IOS_APP_ID, shared.COUNTRIES, shared.RATE_LIMIT_SECONDS
        )
        ios_norm = scraper_appstore.normalize(ios_raw)
    except Exception as exc:
        print(f"  [ERROR] App Store collection failed: {exc}\n")

    # ── MERGE ────────────────────────────────────────────────────────────────

    print("── MERGE ───────────────────────────────────────────────────\n")
    combined = play_norm + ios_norm
    print(f"  Google Play normalized: {len(play_norm)}")
    print(f"  App Store normalized:   {len(ios_norm)}")
    print(f"  Combined total:         {len(combined)}\n")

    if not combined:
        print("  No reviews collected from either platform. Exiting.")
        return

    # ── SHARED PIPELINE ──────────────────────────────────────────────────────

    shared.run_pipeline(
        combined,
        platform_label="Combined",
        app_name=shared.APP_NAME,
    )

    # ── COLLECTION SUMMARY ───────────────────────────────────────────────────

    print("  Raw collection totals:")
    print(f"    Google Play: {len(play_raw)}")
    print(f"    App Store:   {len(ios_raw)}")
    print(f"    Combined:    {len(play_raw) + len(ios_raw)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
