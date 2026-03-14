"""Cutshort.io scraper — extracts jobs from Next.js SSR __NEXT_DATA__.

Cutshort serves full job data as server-side rendered HTML with embedded
JSON.  No login or browser automation required — plain HTTP + JSON parsing.

Note: Cutshort was acquired by Instahyre; the site redirects in-browser
but the SSR data is still fully available via HTTP requests.
"""

import json
import re
import time

import pandas as pd
import httpx
from bs4 import BeautifulSoup

from config import CUTSHORT_DELAY
from scrapers.utils import to_slug


def search_cutshort(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search Cutshort.io for jobs by keyword and location.

    Args:
        keywords: Job title search terms (e.g. ["Marketing Director", "CMO"]).
        locations: Locations to filter (e.g. ["Bangalore", "Mumbai"]).
        max_results: Maximum results per keyword+location combo.

    Returns:
        DataFrame with job listings from Cutshort.
    """
    all_jobs = []

    print("    [!] Cutshort was acquired by Instahyre -- SSR job data is no longer "
          "available.  Results will likely be empty.  Use Instahyre instead.")

    client = httpx.Client(
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        },
        follow_redirects=False,   # Don't follow JS redirects to Instahyre
        timeout=20.0,
    )

    for keyword in keywords:
        slug = to_slug(keyword)
        for location in locations:
            loc_slug = to_slug(location)
            print(f"  [Cutshort] Searching '{keyword}' in '{location}'...")

            jobs = _fetch_jobs(client, slug, loc_slug, keyword, location, max_results)
            all_jobs.extend(jobs)
            print(f"    Found {len(jobs)} results")
            time.sleep(CUTSHORT_DELAY)

    client.close()

    if not all_jobs:
        return pd.DataFrame()

    df = pd.DataFrame(all_jobs)
    df["platform"] = "cutshort"

    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"], keep="first")

    print(f"\n  [Cutshort] Total unique jobs: {len(df)}")
    return df



def _fetch_jobs(
    client: httpx.Client,
    keyword_slug: str,
    location_slug: str,
    keyword_raw: str,
    location_raw: str,
    max_results: int,
) -> list[dict]:
    """Fetch jobs from Cutshort SSR pages."""
    jobs = []

    # Try multiple URL patterns — Cutshort uses various slug formats
    urls_to_try = [
        f"https://cutshort.io/jobs/{keyword_slug}-jobs-in-{location_slug}",
        f"https://cutshort.io/jobs/{keyword_slug}-jobs",
    ]

    for url in urls_to_try:
        if len(jobs) >= max_results:
            break

        try:
            resp = client.get(url)
            if resp.status_code != 200:
                continue

            page_jobs = _extract_next_data(resp.text, keyword_raw, location_raw)
            if page_jobs:
                jobs.extend(page_jobs)
                break  # Got results, no need to try other URL patterns
        except Exception as e:
            print(f"    Error fetching {url}: {e}")

    return jobs[:max_results]


def _extract_next_data(
    html: str,
    keyword: str,
    location: str,
) -> list[dict]:
    """Extract job listings from __NEXT_DATA__ embedded JSON.

    NOTE (Mar 2026): Cutshort was acquired by Instahyre and has gutted its
    server-side rendered job data.  ``dehydratedState`` is now ``None`` on
    every page, so this function will return an empty list.  The defensive
    ``or {}`` guards below prevent the ``NoneType.get()`` crash that occurs
    when a key exists with an explicit ``None`` value.
    """
    jobs = []

    try:
        soup = BeautifulSoup(html, "lxml")
        script_tag = soup.find("script", id="__NEXT_DATA__")
        if not script_tag or not script_tag.string:
            return jobs

        data = json.loads(script_tag.string)

        # Navigate the dehydrated state to find job data
        page_props = (data.get("props") or {}).get("pageProps") or {}

        # Try dehydrated React Query state
        # NOTE: `or {}` is essential — the key may exist with value None
        dehydrated = page_props.get("dehydratedState") or {}
        queries = dehydrated.get("queries") or []

        for query in queries:
            state_data = (query.get("state") or {}).get("data") or {}
            if isinstance(state_data, dict):
                # Look for jobs in nested pageData or direct data
                inner_data = state_data.get("data") or {}
                page_data = inner_data.get("pageData") or {}
                job_list = page_data.get("jobs") or []
                if not job_list:
                    job_list = inner_data.get("jobs") or []
                if not job_list:
                    job_list = state_data.get("jobs") or []

                for item in job_list:
                    job = _parse_job(item, keyword, location)
                    if job:
                        jobs.append(job)

        # Fallback: try direct pageProps jobs
        if not jobs:
            direct_jobs = page_props.get("jobs") or []
            for item in direct_jobs:
                job = _parse_job(item, keyword, location)
                if job:
                    jobs.append(job)

    except (json.JSONDecodeError, KeyError, TypeError) as e:
        print(f"    Error parsing __NEXT_DATA__: {e}")

    return jobs


def _parse_job(item: dict, keyword: str, location: str) -> dict | None:
    """Parse a single job item from Cutshort JSON data."""
    if not isinstance(item, dict):
        return None

    title = item.get("headline") or item.get("title") or ""
    if not title:
        return None

    # Company details
    company_data = item.get("companyDetails", {}) or {}
    company = company_data.get("name") or item.get("company", "")

    # Location
    locations = item.get("locations", [])
    loc_text = item.get("locationsText", "")
    if isinstance(locations, list) and locations:
        loc_text = ", ".join(locations)

    # Salary
    salary_data = item.get("salaryRange", {}) or {}
    salary_text = item.get("salaryRangeText", "")
    if not salary_text and salary_data:
        s_min = salary_data.get("vanityMin") or salary_data.get("min", "")
        s_max = salary_data.get("vanityMax") or salary_data.get("max", "")
        currency = salary_data.get("currency", "INR")
        if s_min and s_max:
            salary_text = f"{currency} {s_min} - {s_max}"

    # Experience
    exp_data = item.get("expRange", {}) or {}
    exp_min = exp_data.get("min", "")
    exp_max = exp_data.get("max", "")
    experience = ""
    if exp_min or exp_max:
        experience = f"{exp_min}-{exp_max} years" if exp_max else f"{exp_min}+ years"

    # URL
    public_url = item.get("publicUrl") or item.get("url", "")
    if public_url and not public_url.startswith("http"):
        public_url = f"https://cutshort.io{public_url}"

    # Description snippet
    description = item.get("sanitizedComment") or item.get("description", "")
    if description:
        # Strip HTML tags, truncate
        description = re.sub(r"<[^>]+>", " ", description)
        description = re.sub(r"\s+", " ", description).strip()[:300]

    # Skills
    skills = item.get("allSkills", [])
    if isinstance(skills, list):
        skills = ", ".join(skills[:8])
    else:
        skills = ""

    # Remote type
    remote_type = item.get("remoteType", "")

    return {
        "title": title,
        "company": company,
        "location": loc_text or location,
        "salary": salary_text,
        "experience": experience,
        "job_url": public_url,
        "description": description,
        "skills": skills,
        "remote_type": remote_type,
        "date_posted": "",
        "search_keyword": keyword,
        "search_location": location,
    }
