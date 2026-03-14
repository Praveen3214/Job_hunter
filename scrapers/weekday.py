"""Weekday.works scraper — extracts jobs from SSR pages.

Weekday is a hybrid SPA with server-side rendering.  Job listings are
publicly accessible without login.  We use HTTP requests with
BeautifulSoup, falling back to Playwright if needed.

URL patterns:
    /jobs/marketing-jobs-in-india-remote
    /jobs/in/marketing/bengaluru
    /jobs?designation=marketing+director&location=bangalore
"""

import json
import re
import time

import httpx
import pandas as pd
from bs4 import BeautifulSoup

from config import WEEKDAY_DELAY
from scrapers.utils import to_slug


def search_weekday(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search Weekday.works for jobs.

    Args:
        keywords: Job title search terms.
        locations: Locations to search.
        max_results: Max results per keyword+location combo.

    Returns:
        DataFrame with job listings from Weekday.
    """
    all_jobs = []

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
        follow_redirects=True,
        timeout=20.0,
    )

    for keyword in keywords:
        for location in locations:
            print(f"  [Weekday] Searching '{keyword}' in '{location}'...")
            jobs = _fetch_jobs(client, keyword, location, max_results)
            all_jobs.extend(jobs)
            print(f"    Found {len(jobs)} results")
            time.sleep(WEEKDAY_DELAY)

    client.close()

    if not all_jobs:
        # Fallback: try Playwright if HTTP returned nothing
        print("  [Weekday] HTTP returned no results, trying browser...")
        all_jobs = _fallback_playwright(keywords, locations, max_results)

    if not all_jobs:
        return pd.DataFrame()

    df = pd.DataFrame(all_jobs)
    df["platform"] = "weekday"

    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"], keep="first")

    print(f"\n  [Weekday] Total unique jobs: {len(df)}")
    return df



def _fetch_jobs(
    client: httpx.Client,
    keyword: str,
    location: str,
    max_results: int,
) -> list[dict]:
    """Fetch jobs from Weekday via HTTP."""
    jobs = []
    kw_slug = to_slug(keyword)
    loc_slug = to_slug(location)

    # Try multiple URL patterns
    urls = [
        f"https://weekday.works/jobs/{kw_slug}-jobs-in-{loc_slug}",
        f"https://weekday.works/jobs/{kw_slug}-jobs-in-india-remote",
        f"https://weekday.works/jobs/in/{kw_slug}/{loc_slug}",
    ]

    for url in urls:
        if len(jobs) >= max_results:
            break

        try:
            resp = client.get(url)
            if resp.status_code != 200:
                continue

            page_jobs = _parse_html(resp.text, keyword, location)
            if page_jobs:
                jobs.extend(page_jobs)
                break  # Got results
        except Exception as e:
            print(f"    Error: {e}")

    return jobs[:max_results]


def _parse_html(html: str, keyword: str, location: str) -> list[dict]:
    """Parse job listings from Weekday HTML."""
    jobs = []
    soup = BeautifulSoup(html, "lxml")

    # Strategy 1: Look for __NEXT_DATA__ (Next.js SSR)
    next_data = soup.find("script", id="__NEXT_DATA__")
    if next_data and next_data.string:
        try:
            data = json.loads(next_data.string)
            page_props = data.get("props", {}).get("pageProps", {})

            # Look for jobs in various keys
            for key in ["jobs", "listings", "results", "data", "initialJobs"]:
                items = page_props.get(key, [])
                if isinstance(items, list):
                    for item in items:
                        job = _parse_json_job(item, keyword, location)
                        if job:
                            jobs.append(job)

            # Try dehydrated state
            dehydrated = page_props.get("dehydratedState", {})
            for query in dehydrated.get("queries", []):
                state_data = query.get("state", {}).get("data", {})
                if isinstance(state_data, dict):
                    for key in ["jobs", "listings", "results", "data", "pages"]:
                        items = state_data.get(key, [])
                        if isinstance(items, list):
                            for item in items:
                                if isinstance(item, dict):
                                    job = _parse_json_job(item, keyword, location)
                                    if job:
                                        jobs.append(job)

        except (json.JSONDecodeError, KeyError, TypeError):
            pass

    if jobs:
        return jobs

    # Strategy 2: Parse from rendered HTML
    # Weekday job cards: look for card-like containers with job info
    cards = soup.select('[class*="card"], [class*="job"], [class*="listing"], article')
    seen = set()

    for card in cards:
        # Find the job link
        link = card.find("a", href=re.compile(r"/jobs/|/job/|weekday\.works"))
        if not link:
            continue

        href = link.get("href", "")
        if href in seen:
            continue
        seen.add(href)

        # Title
        title_el = card.find(["h2", "h3", "h4"]) or link
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            continue

        card_text = card.get_text(" ", strip=True)

        # Company — look for a separate element near title
        company = ""
        company_el = card.find(class_=re.compile(r"company|org|employer"))
        if company_el:
            company = company_el.get_text(strip=True)

        # Location
        loc_pattern = r"(Bangalore|Bengaluru|Mumbai|Delhi|Hyderabad|Chennai|Pune|Kolkata|Noida|Gurgaon|Gurugram|Remote|Ahmedabad|New Delhi|Dehradun|Coimbatore)"
        loc_match = re.search(loc_pattern, card_text, re.IGNORECASE)
        loc_text = loc_match.group(0) if loc_match else ""

        # Salary
        salary_match = re.search(
            r"((?:₹|Rs\.?|INR)?\s*\d+[\d.,]*\s*[-–to]+\s*(?:₹|Rs\.?|INR)?\s*\d+[\d.,]*\s*(?:Lacs?|Lakhs?|LPA|Cr|L|P\.A\.))",
            card_text, re.IGNORECASE
        )
        salary = salary_match.group(0).strip() if salary_match else ""

        # Experience
        exp_match = re.search(r"(\d+[-+]?\s*(?:to|-)?\s*\d*\s*(?:Yrs?|years?|yr))", card_text, re.IGNORECASE)
        experience = exp_match.group(0).strip() if exp_match else ""

        full_url = href if href.startswith("http") else f"https://weekday.works{href}"

        jobs.append({
            "title": title,
            "company": company,
            "location": loc_text or location,
            "salary": salary,
            "experience": experience,
            "job_url": full_url,
            "date_posted": "",
            "description": "",
            "search_keyword": keyword,
            "search_location": location,
        })

    # Strategy 3: Look for JSON-LD structured data
    if not jobs:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld = json.loads(script.string)
                items = []
                if isinstance(ld, list):
                    items = ld
                elif isinstance(ld, dict):
                    if ld.get("@type") == "ItemList":
                        items = ld.get("itemListElement", [])
                    elif ld.get("@type") == "JobPosting":
                        items = [ld]

                for item in items:
                    inner = item.get("item", item)
                    if inner.get("@type") == "JobPosting" or inner.get("title"):
                        jobs.append({
                            "title": inner.get("title", ""),
                            "company": (inner.get("hiringOrganization") or {}).get("name", ""),
                            "location": _extract_ld_location(inner),
                            "salary": "",
                            "experience": "",
                            "job_url": inner.get("url", ""),
                            "date_posted": inner.get("datePosted", ""),
                            "description": (inner.get("description") or "")[:300],
                            "search_keyword": keyword,
                            "search_location": location,
                        })
            except (json.JSONDecodeError, TypeError):
                pass

    return jobs


def _parse_json_job(item: dict, keyword: str, location: str) -> dict | None:
    """Parse a single job from JSON data."""
    if not isinstance(item, dict):
        return None

    title = item.get("title") or item.get("jobTitle") or item.get("designation", "")
    if not title:
        return None

    company = item.get("company") or item.get("companyName") or item.get("employer", "")
    if isinstance(company, dict):
        company = company.get("name", "")

    loc = item.get("location") or item.get("city", "")
    if isinstance(loc, list):
        loc = ", ".join(str(l) for l in loc)

    job_url = item.get("url") or item.get("jobUrl") or item.get("applyUrl", "")
    if job_url and not job_url.startswith("http"):
        job_url = f"https://weekday.works{job_url}"

    return {
        "title": str(title).strip(),
        "company": str(company).strip() if company else "",
        "location": str(loc).strip() if loc else location,
        "salary": str(item.get("salary") or item.get("salaryRange", "")).strip(),
        "experience": str(item.get("experience") or "").strip(),
        "job_url": job_url,
        "date_posted": item.get("postedDate") or item.get("createdDate", ""),
        "description": (str(item.get("description") or ""))[:300],
        "search_keyword": keyword,
        "search_location": location,
    }


def _extract_ld_location(item: dict) -> str:
    """Extract location from JSON-LD."""
    loc = item.get("jobLocation")
    if isinstance(loc, dict):
        addr = loc.get("address", {})
        if isinstance(addr, dict):
            return addr.get("addressLocality", "")
    return ""


def _fallback_playwright(
    keywords: list[str],
    locations: list[str],
    max_results: int,
) -> list[dict]:
    """Fallback: use Playwright if HTTP doesn't return jobs."""
    all_jobs = []

    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=False)
            context = browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()

            for keyword in keywords:
                kw_slug = to_slug(keyword)
                for location in locations:
                    loc_slug = to_slug(location)
                    url = f"https://weekday.works/jobs/{kw_slug}-jobs-in-{loc_slug}"

                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=25000)
                        page.wait_for_timeout(5000)

                        html = page.content()
                        jobs = _parse_html(html, keyword, location)
                        all_jobs.extend(jobs[:max_results])
                        print(f"    [Playwright fallback] Found {len(jobs)} results")
                    except Exception as e:
                        print(f"    [Playwright fallback] Error: {e}")

                    time.sleep(WEEKDAY_DELAY)

            browser.close()

    except ImportError:
        print("    Playwright not available for fallback")

    return all_jobs
