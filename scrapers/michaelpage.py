"""Michael Page India scraper — extracts jobs from Drupal SSR pages.

Michael Page (michaelpage.co.in) is a Drupal-based SSR site with AJAX Views
for dynamic filtering.  Job cards are server-rendered HTML — no browser needed.
"""

import re
import time

import pandas as pd
import httpx
from bs4 import BeautifulSoup

from config import MICHAELPAGE_BASE_URL, MICHAELPAGE_DELAY
from scrapers.utils import to_slug, CHROME_UA


def search_michaelpage(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search michaelpage.co.in for jobs by keyword and location.

    Args:
        keywords: Job title search terms.
        locations: Locations to filter (Indian cities).
        max_results: Max results per keyword+location combo.

    Returns:
        DataFrame with job listings from Michael Page.
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

    for keyword in keywords:
        for location in locations:
            loc_lower = location.lower()
            if loc_lower in ("remote", "usa", "uk", "europe", "india"):
                # Michael Page India — skip non-city locations
                if loc_lower != "india":
                    continue
                loc_slug = ""
            else:
                loc_slug = to_slug(location)

            print(f"  [MichaelPage] Searching '{keyword}' in '{location}'...")
            jobs = _fetch_jobs(client, keyword, loc_slug, location, max_results)
            all_jobs.extend(jobs)
            print(f"    Found {len(jobs)} results")
            time.sleep(MICHAELPAGE_DELAY)

    client.close()

    if not all_jobs:
        return pd.DataFrame()

    df = pd.DataFrame(all_jobs)
    df["platform"] = "michaelpage"

    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"], keep="first")

    print(f"\n  [MichaelPage] Total unique jobs: {len(df)}")
    return df


def _fetch_jobs(
    client: httpx.Client,
    keyword: str,
    location_slug: str,
    location_raw: str,
    max_results: int,
) -> list[dict]:
    """Fetch jobs from Michael Page search pages."""
    jobs: list[dict] = []

    # Michael Page uses keyword URL param
    kw_slug = to_slug(keyword)

    # Try location-specific and general URLs
    urls = []
    if location_slug:
        urls.append(f"{MICHAELPAGE_BASE_URL}/jobs/{location_slug}?keyword={kw_slug}")
    urls.append(f"{MICHAELPAGE_BASE_URL}/jobs?keyword={kw_slug}")

    for base_url in urls:
        if len(jobs) >= max_results:
            break

        for page_num in range(1, 6):  # Max 5 pages
            if len(jobs) >= max_results:
                break

            url = base_url if page_num == 1 else f"{base_url}&page={page_num}"
            try:
                resp = client.get(url)
                if resp.status_code != 200:
                    break

                page_jobs = _parse_listings(resp.text, keyword, location_raw)
                if not page_jobs:
                    break  # No more results

                jobs.extend(page_jobs)
                time.sleep(MICHAELPAGE_DELAY * 0.5)
            except Exception as e:
                print(f"    Error fetching {url}: {e}")
                break

        if jobs:
            break  # Got results from first URL pattern

    return jobs[:max_results]


def _parse_listings(html: str, keyword: str, location: str) -> list[dict]:
    """Parse job listing cards from Michael Page HTML."""
    jobs: list[dict] = []

    try:
        soup = BeautifulSoup(html, "lxml")

        # Michael Page uses article or div cards for job listings
        # Strategy 1: Look for job listing links with /job-detail/ pattern
        job_links = soup.find_all("a", href=re.compile(r"/job-detail/"))
        seen_urls = set()

        for link in job_links:
            href = link.get("href", "")
            if not href or href in seen_urls:
                continue

            full_url = href if href.startswith("http") else f"{MICHAELPAGE_BASE_URL}{href}"
            seen_urls.add(href)

            title = link.get_text(strip=True)
            if not title or len(title) < 3:
                continue

            # Look for parent container with more details
            card = link.find_parent(["article", "div", "li"])
            loc_text = ""
            salary_text = ""
            description = ""

            if card:
                card_text = card.get_text(" ", strip=True)

                # Extract location (common Indian cities)
                loc_match = re.search(
                    r"(Mumbai|Delhi|Bangalore|Bengaluru|Hyderabad|Chennai|Pune|"
                    r"Kolkata|Noida|Gurugram|Gurgaon|Ahmedabad|Jaipur|Kochi|"
                    r"Lucknow|Chandigarh|Indore|India)",
                    card_text, re.IGNORECASE,
                )
                if loc_match:
                    loc_text = loc_match.group(0)

                # Extract salary (INR format)
                sal_match = re.search(
                    r"(?:INR|₹|Rs\.?)\s*[\d,]+(?:\s*[-–]\s*(?:INR|₹|Rs\.?)\s*[\d,]+)?",
                    card_text, re.IGNORECASE,
                )
                if sal_match:
                    salary_text = sal_match.group(0)

                # Extract description snippet
                paragraphs = card.find_all("p")
                for p in paragraphs:
                    p_text = p.get_text(strip=True)
                    if len(p_text) > 40 and p_text != title:
                        description = p_text[:300]
                        break

            jobs.append({
                "title": title,
                "company": "Michael Page",  # Recruiter — client name may be in description
                "location": loc_text or location,
                "salary": salary_text,
                "experience": "",
                "job_url": full_url,
                "description": description,
                "date_posted": "",
                "search_keyword": keyword,
                "search_location": location,
            })

        # Strategy 2: Fallback — look for structured listing containers
        if not jobs:
            listings = soup.select(".search-results .listing, .views-row, .job-item")
            for listing in listings:
                title_el = listing.find(["h2", "h3", "h4"])
                link_el = listing.find("a", href=True)

                if not title_el:
                    continue

                title = title_el.get_text(strip=True)
                url = ""
                if link_el:
                    href = link_el.get("href", "")
                    url = href if href.startswith("http") else f"{MICHAELPAGE_BASE_URL}{href}"

                card_text = listing.get_text(" ", strip=True)
                loc_match = re.search(
                    r"(Mumbai|Delhi|Bangalore|Bengaluru|Hyderabad|Chennai|Pune|"
                    r"Kolkata|Noida|Gurugram|India)",
                    card_text, re.IGNORECASE,
                )

                jobs.append({
                    "title": title,
                    "company": "Michael Page",
                    "location": loc_match.group(0) if loc_match else location,
                    "salary": "",
                    "experience": "",
                    "job_url": url,
                    "description": card_text[:300] if card_text else "",
                    "date_posted": "",
                    "search_keyword": keyword,
                    "search_location": location,
                })

    except Exception as e:
        print(f"    Error parsing Michael Page HTML: {e}")

    return jobs
