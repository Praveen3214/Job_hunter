"""Antal International scraper — extracts jobs from antal.com.

Antal (antal.com) is a server-rendered site with job listings filterable
by country.  We filter for India-based roles and match by keyword relevance.
"""

import re
import time
from urllib.parse import quote_plus

import pandas as pd
import httpx
from bs4 import BeautifulSoup

from config import ANTAL_BASE_URL, ANTAL_DELAY
from scrapers.utils import CHROME_UA, is_non_india_location


def search_antal(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search antal.com for jobs by keyword, filtered to India.

    Args:
        keywords: Job title search terms.
        locations: Locations (used for relevance matching).
        max_results: Max results to return.

    Returns:
        DataFrame with job listings from Antal.
    """
    all_jobs: list[dict] = []

    client = httpx.Client(
        headers={
            "User-Agent": CHROME_UA,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        },
        follow_redirects=True,
        timeout=25.0,
    )

    try:
        for keyword in keywords:
            print(f"  [Antal] Searching '{keyword}'...")
            jobs = _fetch_jobs(client, keyword, locations, max_results)
            all_jobs.extend(jobs)
            print(f"    Found {len(jobs)} results")
            time.sleep(ANTAL_DELAY)
    finally:
        client.close()

    if not all_jobs:
        return pd.DataFrame()

    df = pd.DataFrame(all_jobs)
    df["platform"] = "antal"

    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"], keep="first")

    print(f"\n  [Antal] Total unique jobs: {len(df)}")
    return df


def _fetch_jobs(
    client: httpx.Client,
    keyword: str,
    locations: list[str],
    max_results: int,
) -> list[dict]:
    """Fetch jobs from Antal, filtering for India."""
    jobs: list[dict] = []

    # Try the India-specific vacancies page and the general jobs page
    urls_to_try = [
        f"{ANTAL_BASE_URL}/jobs?country=India&keyword={quote_plus(keyword)}",
        f"{ANTAL_BASE_URL}/jobs?keyword={quote_plus(keyword)}",
        f"{ANTAL_BASE_URL}/location/India.htm",
    ]

    for url in urls_to_try:
        if len(jobs) >= max_results:
            break

        try:
            resp = client.get(url)
            if resp.status_code != 200:
                continue

            page_jobs = _parse_listings(resp.text, keyword, locations)
            if page_jobs:
                jobs.extend(page_jobs)
                break  # Got results
        except Exception as e:
            print(f"    Error fetching {url}: {e}")

    return jobs[:max_results]


def _parse_listings(html: str, keyword: str, locations: list[str]) -> list[dict]:
    """Parse job listing cards from Antal HTML."""
    jobs: list[dict] = []
    keyword_lower = keyword.lower()
    location_lowers = [loc.lower() for loc in locations]

    try:
        soup = BeautifulSoup(html, "lxml")

        # Strategy 1: Look for job links with /job/ path
        job_links = soup.find_all("a", href=re.compile(r"/job/"))
        seen_urls = set()

        for link in job_links:
            href = link.get("href", "")
            if not href or href in seen_urls:
                continue

            full_url = href if href.startswith("http") else f"{ANTAL_BASE_URL}{href}"
            seen_urls.add(href)

            title = link.get_text(strip=True)
            if not title or len(title) < 3:
                continue

            # Relevance check — title should contain keyword terms
            title_lower = title.lower()
            keyword_words = keyword_lower.split()
            if not any(w in title_lower for w in keyword_words):
                # Check parent card for keyword relevance
                card = link.find_parent(["div", "article", "li", "tr"])
                if card:
                    card_text = card.get_text(" ", strip=True).lower()
                    if not any(w in card_text for w in keyword_words):
                        continue

            # Extract details from parent card
            card = link.find_parent(["div", "article", "li", "tr"])
            loc_text = ""
            salary_text = ""
            experience = ""
            company = "Antal International"
            description = ""

            if card:
                card_text = card.get_text(" ", strip=True)

                # Extract location
                loc_match = re.search(
                    r"(Mumbai|Delhi|Bangalore|Bengaluru|Hyderabad|Chennai|Pune|"
                    r"Kolkata|Noida|Gurugram|Gurgaon|Ahmedabad|Jaipur|Kochi|"
                    r"Lucknow|Chandigarh|Indore|Nashik|India)",
                    card_text, re.IGNORECASE,
                )
                if loc_match:
                    loc_text = loc_match.group(0)

                # Extract salary
                sal_match = re.search(
                    r"(?:INR|₹|Rs\.?|GBP|£|\$|EUR|€)\s*[\d,]+(?:\s*[-–]\s*[\d,]+)?",
                    card_text, re.IGNORECASE,
                )
                if sal_match:
                    salary_text = sal_match.group(0)

                # Extract experience
                exp_match = re.search(r"(\d+)\+?\s*(?:[-–]\s*(\d+)\s*)?(?:years?|yrs?)", card_text, re.IGNORECASE)
                if exp_match:
                    exp_min = exp_match.group(1)
                    exp_max = exp_match.group(2)
                    experience = f"{exp_min}-{exp_max} years" if exp_max else f"{exp_min}+ years"

                # Snippet
                paragraphs = card.find_all(["p", "span"])
                for p in paragraphs:
                    p_text = p.get_text(strip=True)
                    if len(p_text) > 40 and p_text != title:
                        description = p_text[:300]
                        break

            # India-only filter: Antal is global, skip clearly non-India
            if is_non_india_location(loc_text) or is_non_india_location(card_text[:200] if card else ""):
                continue

            jobs.append({
                "title": title,
                "company": company,
                "location": loc_text or "",
                "salary": salary_text,
                "experience": experience,
                "job_url": full_url,
                "description": description,
                "date_posted": "",
                "search_keyword": keyword,
                "search_location": ", ".join(locations),
            })

        # Strategy 2: Fallback — look for structured data (JSON-LD)
        if not jobs:
            scripts = soup.find_all("script", type="application/ld+json")
            for script in scripts:
                try:
                    import json
                    data = json.loads(script.string)
                    if isinstance(data, list):
                        items = data
                    elif isinstance(data, dict) and data.get("@type") == "JobPosting":
                        items = [data]
                    elif isinstance(data, dict) and "itemListElement" in data:
                        items = [
                            e.get("item", e) for e in data["itemListElement"]
                        ]
                    else:
                        continue

                    for item in items:
                        if item.get("@type") != "JobPosting":
                            continue
                        title = item.get("title", "")
                        if not title:
                            continue

                        loc_data = item.get("jobLocation", {})
                        if isinstance(loc_data, dict):
                            address = loc_data.get("address", {})
                            loc_text = address.get("addressLocality", "")
                        elif isinstance(loc_data, list) and loc_data:
                            loc_text = loc_data[0].get("address", {}).get("addressLocality", "")
                        else:
                            loc_text = ""

                        # India-only filter for JSON-LD results
                        if is_non_india_location(loc_text):
                            continue

                        jobs.append({
                            "title": title,
                            "company": item.get("hiringOrganization", {}).get("name", "Antal International"),
                            "location": loc_text,
                            "salary": "",
                            "experience": "",
                            "job_url": item.get("url", ""),
                            "description": (item.get("description", "") or "")[:300],
                            "date_posted": item.get("datePosted", ""),
                            "search_keyword": keyword,
                            "search_location": ", ".join(locations),
                        })
                except Exception:
                    continue

    except Exception as e:
        print(f"    Error parsing Antal HTML: {e}")

    return jobs
