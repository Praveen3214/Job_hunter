"""IIMJobs.com scraper — uses the gladiator.iimjobs.com REST API.

IIMJobs uses a tag-based system: each predefined keyword (like
"Marketing", "Brand Management") maps to a numeric tag ID.  The API
returns jobs only for recognized tags; arbitrary text queries don't
work.

Strategy:
    1.  For each user keyword, navigate to /k/{slug}-jobs and intercept
        the gladiator API response.  If the tag exists (keywordId != -1),
        we get targeted results.
    2.  If the tag doesn't exist, fall back to the Sales & Marketing
        *category* endpoint (categoryId=14, ~10 000 jobs) and filter
        client-side by keyword relevance.

API endpoints (on gladiator.iimjobs.com):
    Tag:      /job/keyword/?query={tagId}&page={n}&keywordId={tagId}&size=50
    Category: /job/category/?page={n}&categoryId=14&size=100
"""

import re
import time

import pandas as pd
from playwright.sync_api import sync_playwright, Page

from config import IIMJOBS_DELAY
from scrapers.utils import to_slug, CHROME_UA
from scrapers.gladiator_utils import (
    resolve_location_id,
    build_keyword_tokens,
    is_relevant,
    parse_api_job,
    call_api,
)

# Safety caps
MAX_TAG_PAGES = 10       # per keyword tag (50 jobs/page → 500 max)
MAX_CATEGORY_PAGES = 8   # category fallback (100 jobs/page → 800 scanned)

# Sales & Marketing category ID on IIMJobs
MARKETING_CATEGORY_ID = 14

IIMJOBS_BASE_URL = "https://www.iimjobs.com"
IIMJOBS_API_BASE = "https://gladiator.iimjobs.com"


def search_iimjobs(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search IIMJobs.com for premium/MBA-level jobs.

    Uses a two-tier strategy:
        1. Try to match each keyword to an IIMJobs tag (via /k/ page).
        2. Fall back to the Sales & Marketing category if no tag match.

    Args:
        keywords: Job title search terms.
        locations: Locations to search (used for API loc filter).
        max_results: Max results per keyword+location combo.

    Returns:
        DataFrame with job listings from IIMJobs.
    """
    all_jobs = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent=CHROME_UA,
        )
        page = context.new_page()

        try:
            for keyword in keywords:
                for location in locations:
                    print(f"  [IIMJobs] Searching '{keyword}' in '{location}'...")
                    jobs = _search_keyword(page, keyword, location, max_results)
                    all_jobs.extend(jobs)
                    print(f"    Found {len(jobs)} results")
        finally:
            browser.close()

    if not all_jobs:
        return pd.DataFrame()

    df = pd.DataFrame(all_jobs)
    df["platform"] = "iimjobs"

    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"], keep="first")

    print(f"\n  [IIMJobs] Total unique jobs: {len(df)}")
    return df


# ── Keyword → tag discovery ─────────────────────────────────────


def _slugify(keyword: str) -> str:
    """Convert a keyword to an IIMJobs URL slug."""
    return to_slug(keyword)


def _search_keyword(
    page: Page,
    keyword: str,
    location: str,
    max_results: int,
) -> list[dict]:
    """Try tag-based search first, then fall back to category search."""

    # -- Tier 1: navigate to /k/{slug}-jobs and intercept API response --
    tag_id = _discover_tag_id(page, keyword)

    if tag_id and tag_id != -1:
        print(f"    Tag matched: keywordId={tag_id}")
        jobs = _fetch_tag_jobs(page, tag_id, keyword, location, max_results)
        if jobs:
            return jobs

    # -- Tier 2: category fallback with client-side keyword filter --
    print(f"    No tag match — falling back to category search")
    return _fetch_category_jobs(page, keyword, location, max_results)


def _discover_tag_id(page: Page, keyword: str) -> int | None:
    """Navigate to /k/{slug}-jobs and extract the keywordId from the API call."""
    slug = _slugify(keyword)
    url = f"https://www.iimjobs.com/k/{slug}-jobs"

    captured_id = {"value": None}

    def on_response(response):
        if "gladiator.iimjobs.com/job/keyword" in response.url:
            try:
                data = response.json()
                # The API echoes back the keywordId it resolved
                kid = data.get("keywordId")
                if kid is None:
                    # Parse from URL
                    m = re.search(r"keywordId=(-?\d+)", response.url)
                    if m:
                        kid = int(m.group(1))
                captured_id["value"] = kid
            except Exception:
                pass

    page.on("response", on_response)

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=20000)
        page.wait_for_timeout(4000)
    except Exception:
        pass

    page.remove_listener("response", on_response)
    return captured_id["value"]


# ── Tier 1: Tag-based API fetch ──────────────────────────────────


def _fetch_tag_jobs(
    page: Page,
    tag_id: int,
    keyword: str,
    location: str,
    max_results: int,
) -> list[dict]:
    """Fetch jobs from gladiator.iimjobs.com keyword endpoint."""
    jobs = []
    loc_id = resolve_location_id(location)
    keyword_tokens = build_keyword_tokens(keyword)

    for page_num in range(MAX_TAG_PAGES):
        if len(jobs) >= max_results:
            break

        api_url = (
            f"https://gladiator.iimjobs.com/job/keyword/"
            f"?query={tag_id}&page={page_num}&keywordId={tag_id}&size=50"
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
            job = parse_api_job(item, keyword, location, IIMJOBS_BASE_URL)
            if job:
                # Tag results are already relevant, but apply light filter
                # to avoid completely unrelated items
                if keyword_tokens and not is_relevant(job["title"], keyword_tokens):
                    continue
                jobs.append(job)

        if not result.get("hasMore", False):
            break

        time.sleep(IIMJOBS_DELAY)

    return jobs[:max_results]


# ── Tier 2: Category fallback ────────────────────────────────────


def _fetch_category_jobs(
    page: Page,
    keyword: str,
    location: str,
    max_results: int,
) -> list[dict]:
    """Fetch from the Sales & Marketing category, filter by keyword."""
    jobs = []
    loc_id = resolve_location_id(location)
    keyword_tokens = build_keyword_tokens(keyword)
    skipped = 0

    if not keyword_tokens:
        print(f"    No meaningful tokens in '{keyword}', skipping")
        return jobs

    for page_num in range(MAX_CATEGORY_PAGES):
        if len(jobs) >= max_results:
            break

        api_url = (
            f"https://gladiator.iimjobs.com/job/category/"
            f"?page={page_num}&categoryId={MARKETING_CATEGORY_ID}&size=100"
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
            job = parse_api_job(item, keyword, location, IIMJOBS_BASE_URL)
            if not job:
                continue
            if is_relevant(job["title"], keyword_tokens):
                jobs.append(job)
            else:
                skipped += 1

        if not result.get("hasMore", False):
            break

        time.sleep(IIMJOBS_DELAY)

    if skipped:
        print(f"    Filtered out {skipped} irrelevant jobs from category")

    return jobs[:max_results]
