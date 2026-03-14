"""Shared utilities for gladiator API scrapers (IIMJobs + Hirist).

Both IIMJobs and Hirist are powered by the same gladiator REST API and
share identical data structures, location IDs, relevance filtering, and
job-parsing logic.  Only the base URL differs.

Usage:
    from scrapers.gladiator_utils import (
        LOCATION_IDS, resolve_location_id,
        build_keyword_tokens, is_relevant,
        parse_api_job, call_api,
    )
"""

import datetime
import re

from playwright.sync_api import Page


# ── Stop words for keyword tokenisation ──────────────────────────
STOP_WORDS = frozenset(
    {"of", "the", "and", "a", "an", "in", "at", "for", "to", "or", "with"}
)

# ── Location name → gladiator API location IDs ──────────────────
LOCATION_IDS = {
    "bangalore": "3", "bengaluru": "3",
    "hyderabad": "4",
    "chennai": "7",
    "pune": "6",
    "gurgaon": "37", "gurugram": "37",
    "delhi": "2", "new delhi": "2", "ncr": "2", "noida": "2",
    "mumbai": "1",
    "coimbatore": "84",
    "kolkata": "5",
    "ahmedabad": "8",
}


def resolve_location_id(location: str) -> str:
    """Convert a location name to a gladiator API location ID."""
    return LOCATION_IDS.get(location.lower().strip(), "")


def build_keyword_tokens(keyword: str) -> list[str]:
    """Split a keyword into meaningful lowercase tokens (drop stop words)."""
    tokens = re.split(r"[\s/,&+\-]+", keyword.lower())
    return [t for t in tokens if t and t not in STOP_WORDS and len(t) > 1]


def is_relevant(title: str, keyword_tokens: list[str]) -> bool:
    """Check if a job title contains at least one keyword token.

    For multi-word keywords like 'Head of Marketing', matching
    'marketing' alone is enough.
    """
    title_lower = title.lower()
    return any(token in title_lower for token in keyword_tokens)


def call_api(page: Page, api_url: str) -> dict | None:
    """Call a gladiator API endpoint from within the Playwright page context.

    Returns the parsed JSON response dict, or None on error.
    """
    try:
        result = page.evaluate(
            """async (url) => {
                try {
                    const resp = await fetch(url);
                    if (!resp.ok) return { error: resp.status };
                    return await resp.json();
                } catch(e) {
                    return { error: e.message };
                }
            }""",
            api_url,
        )

        if not result or result.get("error"):
            err = result.get("error", "unknown") if result else "null response"
            print(f"    API error: {err}")
            return None

        return result

    except Exception as e:
        print(f"    API call failed: {e}")
        return None


def parse_api_job(
    item: dict,
    keyword: str,
    location: str,
    base_url: str,
) -> dict | None:
    """Parse a single job from the gladiator API response.

    Args:
        item:     Raw job dict from the API ``data`` array.
        keyword:  Search keyword used (stored in output for traceability).
        location: Search location used.
        base_url: Site base URL for building job links
                  (e.g. ``https://www.iimjobs.com`` or ``https://www.hirist.tech``).
    """
    if not isinstance(item, dict):
        return None

    title = item.get("title") or item.get("jobdesignation", "")
    if not title:
        return None

    # Company
    company_data = item.get("companyData") or {}
    company = company_data.get("companyName", "")

    # Locations — array of {id, name}
    locations = item.get("locations") or []
    if isinstance(locations, list) and locations:
        loc_names = [loc.get("name", "") for loc in locations if isinstance(loc, dict)]
        loc_str = ", ".join(n for n in loc_names if n)
    else:
        loc_str = location

    # Salary
    min_sal = item.get("minSal")
    max_sal = item.get("maxSal")
    sal_show = item.get("salShow", True)
    salary = ""
    if sal_show and min_sal and max_sal:
        salary = f"{min_sal}-{max_sal} LPA"
    elif sal_show and max_sal:
        salary = f"Up to {max_sal} LPA"

    # Experience
    min_exp = item.get("min")
    max_exp = item.get("max")
    experience = ""
    if min_exp is not None and max_exp is not None:
        experience = f"{min_exp}-{max_exp} years"
    elif min_exp is not None:
        experience = f"{min_exp}+ years"

    # Skills / tags
    tags = item.get("tags") or []
    skills = ", ".join(
        t.get("name", "") for t in tags
        if isinstance(t, dict) and t.get("name")
    )[:200]

    # Job URL
    job_detail_url = item.get("jobDetailUrl", "")
    job_id = item.get("id", "")
    if job_detail_url:
        job_url = (
            f"{base_url}{job_detail_url}"
            if not job_detail_url.startswith("http")
            else job_detail_url
        )
    elif job_id:
        job_url = f"{base_url}/j/{job_id}"
    else:
        job_url = ""

    # Date posted
    created_ms = item.get("createdTimeMs")
    date_posted = ""
    if created_ms:
        try:
            date_posted = datetime.datetime.fromtimestamp(
                created_ms / 1000
            ).strftime("%Y-%m-%d")
        except (ValueError, OSError):
            pass

    return {
        "title": str(title).strip(),
        "company": str(company).strip(),
        "location": loc_str,
        "salary": salary,
        "experience": experience,
        "skills": skills,
        "job_url": job_url,
        "date_posted": date_posted,
        "description": "",
        "search_keyword": keyword,
        "search_location": location,
    }
