"""Hirist.tech scraper — uses the gladiator REST API via Playwright.

Hirist is a React SPA backed by the gladiator.hirist.tech REST API.
Job data loads client-side, so the SSR HTML always has 0 items.  We use
Playwright to navigate to hirist.tech (for CORS), then call the API
directly from the page context.

API endpoint:
    GET https://gladiator.hirist.tech/job/keyword/
        ?query={keyword}&page={0-indexed}&size={count}
        &loc={id}  (comma-separated location IDs)

Known location IDs:
    3=Bangalore, 4=Hyderabad, 7=Chennai, 6=Pune,
    37=Gurgaon, 2=Delhi/NCR, 1=Mumbai, 84=Coimbatore
"""

import re
import time

import pandas as pd
from playwright.sync_api import sync_playwright, Page

from config import HIRIST_DELAY
from scrapers.utils import CHROME_UA
from scrapers.gladiator_utils import (
    resolve_location_id,
    build_keyword_tokens,
    is_relevant,
    parse_api_job,
    call_api,
)

# Max API pages to scan (safety cap — each page = 100 jobs)
MAX_API_PAGES = 15

HIRIST_BASE_URL = "https://www.hirist.tech"
HIRIST_API_BASE = "https://gladiator.hirist.tech"


def search_hirist(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search Hirist.tech for jobs via the gladiator REST API.

    Args:
        keywords: Job title search terms.
        locations: Locations to search.
        max_results: Max results per keyword+location combo.

    Returns:
        DataFrame with job listings from Hirist.
    """
    all_jobs = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent=CHROME_UA,
        )
        page = context.new_page()

        # Navigate once to hirist.tech so API calls pass CORS
        try:
            page.goto("https://www.hirist.tech", wait_until="domcontentloaded", timeout=20000)
            page.wait_for_timeout(2000)
        except Exception as e:
            print(f"  [Hirist] Failed to load hirist.tech: {e}")
            browser.close()
            return pd.DataFrame()

        try:
            for keyword in keywords:
                for location in locations:
                    print(f"  [Hirist] Searching '{keyword}' in '{location}'...")
                    jobs = _fetch_via_api(page, keyword, location, max_results)
                    all_jobs.extend(jobs)
                    print(f"    Found {len(jobs)} results")
        finally:
            browser.close()

    if not all_jobs:
        return pd.DataFrame()

    df = pd.DataFrame(all_jobs)
    df["platform"] = "hirist"

    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"], keep="first")

    print(f"\n  [Hirist] Total unique jobs: {len(df)}")
    return df


def _fetch_via_api(
    page: Page,
    keyword: str,
    location: str,
    max_results: int,
) -> list[dict]:
    """Fetch jobs from gladiator API, keeping only keyword-relevant results.

    The gladiator API ignores the `query` parameter and returns the
    full job feed.  We compensate by scanning more pages and filtering
    client-side.
    """
    jobs = []
    loc_id = resolve_location_id(location)
    keyword_tokens = build_keyword_tokens(keyword)
    page_size = 100  # max the API accepts
    skipped = 0

    if not keyword_tokens:
        print(f"    No meaningful tokens in '{keyword}', skipping")
        return jobs

    for page_num in range(MAX_API_PAGES):
        if len(jobs) >= max_results:
            break

        try:
            api_url = (
                f"{HIRIST_API_BASE}/job/keyword/"
                f"?query={keyword}&page={page_num}&size={page_size}"
            )
            if loc_id:
                api_url += f"&loc={loc_id}"

            result = call_api(page, api_url)
            if not result:
                break

            items = result.get("data", [])
            if not items:
                break

            for item in items:
                job = parse_api_job(item, keyword, location, HIRIST_BASE_URL)
                if not job:
                    continue
                if is_relevant(job["title"], keyword_tokens):
                    jobs.append(job)
                else:
                    skipped += 1

            if not result.get("hasMore", False):
                break

            time.sleep(HIRIST_DELAY)

        except Exception as e:
            print(f"    API page {page_num} error: {e}")
            break

    if skipped:
        print(f"    Filtered out {skipped} irrelevant jobs")

    return jobs[:max_results]
