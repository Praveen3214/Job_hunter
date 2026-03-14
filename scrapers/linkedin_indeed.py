"""LinkedIn + Indeed job scraper using python-jobspy."""

import pandas as pd
from jobspy import scrape_jobs

from config import RESULTS_PER_PLATFORM, HOURS_OLD


def search_linkedin_indeed(
    keywords: list[str],
    locations: list[str],
    platforms: list[str] | None = None,
    max_results: int = RESULTS_PER_PLATFORM,
    hours_old: int = HOURS_OLD,
) -> pd.DataFrame:
    """
    Search LinkedIn and/or Indeed for jobs using python-jobspy.

    Args:
        keywords: List of job title search terms.
        locations: List of locations to search.
        platforms: Which sites to use ("linkedin", "indeed"). Defaults to both.
        max_results: Max results per keyword+location combo.
        hours_old: Only return jobs posted within this many hours.

    Returns:
        DataFrame with columns: platform, title, company, location, job_url, etc.
    """
    if platforms is None:
        platforms = ["linkedin", "indeed"]

    all_jobs = []

    for keyword in keywords:
        for location in locations:
            print(f"  [JobSpy] Searching '{keyword}' in '{location}' on {platforms}...")
            try:
                jobs = scrape_jobs(
                    site_name=platforms,
                    search_term=keyword,
                    location=location,
                    results_wanted=max_results,
                    hours_old=hours_old,
                    country_indeed="India" if _is_india_location(location) else "USA",
                )
                if not jobs.empty:
                    jobs["search_keyword"] = keyword
                    jobs["search_location"] = location
                    all_jobs.append(jobs)
                    print(f"    Found {len(jobs)} results")
                else:
                    print(f"    No results found")
            except Exception as e:
                print(f"    Error: {e}")

    if not all_jobs:
        return pd.DataFrame()

    combined = pd.concat(all_jobs, ignore_index=True)

    # Deduplicate by job URL (same job found via different keywords)
    if "job_url" in combined.columns:
        combined = combined.drop_duplicates(subset=["job_url"], keep="first")

    # Standardize column names for downstream use
    combined = _normalize_columns(combined)

    print(f"\n  [JobSpy] Total unique jobs found: {len(combined)}")
    return combined


def _is_india_location(location: str) -> bool:
    india_terms = [
        "india", "bangalore", "bengaluru", "mumbai", "delhi",
        "hyderabad", "chennai", "pune", "kolkata", "noida",
        "gurgaon", "gurugram", "ahmedabad", "jaipur",
    ]
    return location.strip().lower() in india_terms


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure consistent column naming."""
    rename_map = {
        "site": "platform",
        "job_url_direct": "apply_url",
    }
    for old, new in rename_map.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})
    return df
