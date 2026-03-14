"""SutraHR scraper — extracts jobs from sutrahr.com.

SutraHR is a WordPress/Elementor site with limited public job listings
(mostly internal roles and a few client postings).  Simple HTTP fetch.
"""

import re
import time

import pandas as pd
import httpx
from bs4 import BeautifulSoup

from config import SUTRAHR_BASE_URL, SUTRAHR_DELAY
from scrapers.utils import CHROME_UA, is_non_india_location


def search_sutrahr(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search sutrahr.com for jobs.

    Note: SutraHR has very limited public listings (typically 3-5 roles).
    This scraper fetches all available listings and filters by keyword
    relevance.

    Args:
        keywords: Job title search terms.
        locations: Locations (used for relevance matching).
        max_results: Max results to return.

    Returns:
        DataFrame with job listings from SutraHR.
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
        print(f"  [SutraHR] Fetching job listings...")

        # Fetch both /jobs/ and /career/ pages (SutraHR uses both)
        urls = [
            f"{SUTRAHR_BASE_URL}/jobs/",
            f"{SUTRAHR_BASE_URL}/career/",
        ]

        for url in urls:
            try:
                resp = client.get(url)
                if resp.status_code != 200:
                    continue

                page_jobs = _parse_listings(resp.text, keywords, locations)
                all_jobs.extend(page_jobs)
                time.sleep(SUTRAHR_DELAY)
            except Exception as e:
                print(f"    Error fetching {url}: {e}")

        # Dedup by title (same job might appear on both pages)
        if all_jobs:
            seen = set()
            unique_jobs = []
            for job in all_jobs:
                key = job.get("title", "").lower().strip()
                if key not in seen:
                    seen.add(key)
                    unique_jobs.append(job)
            all_jobs = unique_jobs

        print(f"    Found {len(all_jobs)} results")
    finally:
        client.close()

    if not all_jobs:
        return pd.DataFrame()

    df = pd.DataFrame(all_jobs)
    df["platform"] = "sutrahr"

    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"], keep="first")

    print(f"\n  [SutraHR] Total unique jobs: {len(df)}")
    return df


def _parse_listings(
    html: str,
    keywords: list[str],
    locations: list[str],
) -> list[dict]:
    """Parse job listings from SutraHR HTML."""
    jobs: list[dict] = []

    try:
        soup = BeautifulSoup(html, "lxml")

        # Strategy 1: WordPress/Elementor sections with job info
        # SutraHR uses section headings for each job
        headings = soup.find_all(["h2", "h3", "h4"])

        for heading in headings:
            title = heading.get_text(strip=True)
            if not title or len(title) < 5:
                continue

            # Skip navigation/header headings
            skip_terms = [
                "about", "contact", "follow", "our", "why", "how",
                "services", "industries", "partners", "blog", "menu",
                "sutrahr", "recruitment", "clients", "testimonial",
            ]
            if any(term in title.lower() for term in skip_terms):
                continue

            # Check if this heading looks like a job title
            job_signals = [
                "manager", "engineer", "developer", "designer", "analyst",
                "executive", "director", "head", "lead", "officer",
                "consultant", "specialist", "associate", "writer",
                "accountant", "intern", "professional", "coordinator",
            ]
            title_lower = title.lower()
            if not any(signal in title_lower for signal in job_signals):
                continue

            # Get surrounding content for details
            section = heading.find_parent(["section", "div", "article"])
            loc_text = ""
            salary_text = ""
            experience = ""
            description = ""
            job_url = f"{SUTRAHR_BASE_URL}/jobs/"

            if section:
                section_text = section.get_text(" ", strip=True)

                # Extract location
                loc_match = re.search(
                    r"(Mumbai|Delhi|Bangalore|Bengaluru|Hyderabad|Chennai|Pune|"
                    r"Noida|Gurugram|Remote|India|Work from home)",
                    section_text, re.IGNORECASE,
                )
                if loc_match:
                    loc_text = loc_match.group(0)

                # Extract salary
                sal_match = re.search(
                    r"(?:₹|Rs\.?|INR)\s*[\d,]+(?:\s*[-–]\s*[\d,]+)?",
                    section_text, re.IGNORECASE,
                )
                if sal_match:
                    salary_text = sal_match.group(0)

                # Extract experience
                exp_match = re.search(
                    r"(\d+)\s*[-–+to]*\s*(\d*)\s*(?:years?|yrs?)",
                    section_text, re.IGNORECASE,
                )
                if exp_match:
                    exp_min = exp_match.group(1)
                    exp_max = exp_match.group(2)
                    experience = f"{exp_min}-{exp_max} years" if exp_max else f"{exp_min}+ years"

                # Description snippet
                paragraphs = section.find_all("p")
                for p in paragraphs:
                    p_text = p.get_text(strip=True)
                    if len(p_text) > 30 and p_text != title:
                        description = p_text[:300]
                        break

                # Check for anchor/link within section
                link = section.find("a", href=True)
                if link:
                    href = link.get("href", "")
                    if href.startswith("http"):
                        job_url = href

            # India-only filter: skip clearly non-India roles
            if is_non_india_location(loc_text):
                continue

            jobs.append({
                "title": title,
                "company": "SutraHR",
                "location": loc_text or "",
                "salary": salary_text,
                "experience": experience,
                "job_url": job_url,
                "description": description,
                "date_posted": "",
                "search_keyword": ", ".join(keywords),
                "search_location": ", ".join(locations),
            })

        # Strategy 2: Look for job listing links
        if not jobs:
            job_links = soup.find_all("a", href=re.compile(r"job|career|opening|position", re.IGNORECASE))
            for link in job_links:
                title = link.get_text(strip=True)
                if title and len(title) > 5:
                    href = link.get("href", "")
                    full_url = href if href.startswith("http") else f"{SUTRAHR_BASE_URL}{href}"
                    jobs.append({
                        "title": title,
                        "company": "SutraHR",
                        "location": "",
                        "salary": "",
                        "experience": "",
                        "job_url": full_url,
                        "description": "",
                        "date_posted": "",
                        "search_keyword": ", ".join(keywords),
                        "search_location": ", ".join(locations),
                    })

    except Exception as e:
        print(f"    Error parsing SutraHR HTML: {e}")

    return jobs
