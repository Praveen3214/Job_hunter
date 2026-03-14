"""CIEL HR scraper — extracts jobs from cielhr.com.

CIEL HR is a WordPress/Elementor site with job listings organized by city.
City-specific pages (e.g., /jobs/bangalore/) have structured job tables.
Uses Playwright for JavaScript-rendered content.
"""

import re
import time
from urllib.parse import quote_plus

import pandas as pd
from playwright.sync_api import sync_playwright, Page

from config import CIELHR_BASE_URL, CIELHR_DELAY
from scrapers.utils import to_slug


# Map common location names to CIEL HR URL slugs
CIELHR_CITY_SLUGS = {
    "bangalore": "bangalore",
    "bengaluru": "bangalore",
    "mumbai": "mumbai",
    "delhi": "delhi",
    "new delhi": "delhi",
    "hyderabad": "hyderabad",
    "chennai": "chennai",
    "pune": "pune",
    "kolkata": "kolkata",
    "noida": "noida",
    "gurugram": "gurgaon",
    "gurgaon": "gurgaon",
    "ahmedabad": "ahmedabad",
    "jaipur": "jaipur",
    "lucknow": "lucknow",
    "chandigarh": "chandigarh",
    "indore": "indore",
    "kochi": "kochi",
    "coimbatore": "coimbatore",
}


def search_cielhr(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search cielhr.com for jobs using browser automation.

    Args:
        keywords: Job title search terms.
        locations: Locations to search (Indian cities).
        max_results: Max results to return.

    Returns:
        DataFrame with job listings from CIEL HR.
    """
    all_jobs: list[dict] = []

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

        try:
            # First try city-specific pages for targeted results
            city_slugs_to_try = set()
            for location in locations:
                slug = CIELHR_CITY_SLUGS.get(location.lower())
                if slug:
                    city_slugs_to_try.add(slug)

            for city_slug in city_slugs_to_try:
                if len(all_jobs) >= max_results:
                    break

                url = f"{CIELHR_BASE_URL}/jobs/{city_slug}/"
                print(f"  [CIELHR] Searching jobs in '{city_slug}'...")

                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=25000)
                    page.wait_for_timeout(4000)

                    jobs = _extract_jobs(page, keywords, city_slug)
                    all_jobs.extend(jobs)
                    print(f"    Found {len(jobs)} results")
                    time.sleep(CIELHR_DELAY)
                except Exception as e:
                    print(f"    Error loading {url}: {e}")

            # Also try the main jobs page
            if len(all_jobs) < max_results:
                url = f"{CIELHR_BASE_URL}/jobs/"
                print(f"  [CIELHR] Searching main jobs page...")

                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=25000)
                    page.wait_for_timeout(4000)

                    jobs = _extract_jobs(page, keywords, "")
                    all_jobs.extend(jobs)
                    print(f"    Found {len(jobs)} results from main page")
                except Exception as e:
                    print(f"    Error loading {url}: {e}")

            # Try the search page
            if len(all_jobs) < max_results:
                url = f"{CIELHR_BASE_URL}/search-jobs/"
                print(f"  [CIELHR] Trying search page...")
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=25000)
                    page.wait_for_timeout(3000)

                    # Try to fill search form
                    search_input = page.query_selector(
                        'input[type="search"], input[name*="search" i], '
                        'input[placeholder*="search" i], input[placeholder*="keyword" i]'
                    )
                    if search_input:
                        for keyword in keywords[:3]:  # Limit to 3 keyword searches
                            search_input.fill(keyword)
                            search_input.press("Enter")
                            page.wait_for_timeout(3000)

                            jobs = _extract_jobs(page, [keyword], "")
                            all_jobs.extend(jobs)
                            print(f"    Search '{keyword}': {len(jobs)} results")
                            time.sleep(CIELHR_DELAY)
                except Exception as e:
                    print(f"    Error with search: {e}")

        except Exception as e:
            print(f"  [CIELHR] Error: {e}")
        finally:
            browser.close()

    if not all_jobs:
        return pd.DataFrame()

    df = pd.DataFrame(all_jobs)
    df["platform"] = "cielhr"

    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"], keep="first")

    print(f"\n  [CIELHR] Total unique jobs: {len(df)}")
    return df


def _extract_jobs(
    page: Page,
    keywords: list[str],
    city: str,
) -> list[dict]:
    """Extract job data from CIEL HR page via DOM evaluation."""
    keyword_lower_set = {w.lower() for kw in keywords for w in kw.split()}

    try:
        jobs_data = page.evaluate("""() => {
            const jobs = [];
            const seen = new Set();

            // Strategy 1: Table rows (CIEL uses tables for job listings)
            const rows = document.querySelectorAll('table tr, .vjbuilds-table tr');
            for (const row of rows) {
                const cells = row.querySelectorAll('td');
                if (cells.length < 2) continue;

                const link = row.querySelector('a[href]');
                const href = link ? (link.href || link.getAttribute('href') || '') : '';
                if (href && seen.has(href)) continue;
                if (href) seen.add(href);

                const text = row.innerText || row.textContent || '';
                const title = cells[0] ? (cells[0].innerText || '').trim() : '';
                if (!title || title.length < 3) continue;

                jobs.push({
                    title: title,
                    url: href.startsWith('http') ? href : (href ? window.location.origin + href : ''),
                    text: text.trim().substring(0, 500),
                });
            }

            // Strategy 2: Job cards/posts
            if (jobs.length === 0) {
                const cards = document.querySelectorAll(
                    '.nooz-post, .job-card, .job-item, article, ' +
                    '[class*="job"], [class*="listing"], [class*="post-item"]'
                );
                for (const card of cards) {
                    const heading = card.querySelector('h2, h3, h4, h5, strong, .title');
                    const link = card.querySelector('a[href]');
                    if (!heading) continue;

                    const title = (heading.innerText || heading.textContent || '').trim();
                    if (!title || title.length < 3) continue;

                    const href = link ? (link.href || link.getAttribute('href') || '') : '';
                    if (href && seen.has(href)) continue;
                    if (href) seen.add(href);

                    const text = card.innerText || card.textContent || '';

                    jobs.push({
                        title: title.split('\\n')[0],
                        url: href.startsWith('http') ? href : (href ? window.location.origin + href : ''),
                        text: text.trim().substring(0, 500),
                    });
                }
            }

            // Strategy 3: Any links to job-detail-style pages
            if (jobs.length === 0) {
                const links = document.querySelectorAll('a[href*="job"], a[href*="career"], a[href*="opening"]');
                for (const link of links) {
                    const href = link.href || link.getAttribute('href') || '';
                    if (!href || seen.has(href)) continue;
                    seen.add(href);

                    const title = (link.innerText || link.textContent || '').trim();
                    if (!title || title.length < 5) continue;
                    // Skip nav links
                    if (title.toLowerCase().includes('search') || title.toLowerCase().includes('home')) continue;

                    jobs.push({
                        title: title.split('\\n')[0],
                        url: href.startsWith('http') ? href : window.location.origin + href,
                        text: title,
                    });
                }
            }

            return jobs;
        }""")

        parsed = []
        for item in jobs_data:
            title = item.get("title", "")
            text = item.get("text", "")

            # Extract fields from card text
            loc_text = city.title() if city else ""
            if not loc_text:
                loc_match = re.search(
                    r"(Mumbai|Delhi|Bangalore|Bengaluru|Hyderabad|Chennai|Pune|"
                    r"Kolkata|Noida|Gurugram|Gurgaon|India)",
                    text, re.IGNORECASE,
                )
                if loc_match:
                    loc_text = loc_match.group(0)

            exp_match = re.search(
                r"(\d+)\+?\s*[-–to]*\s*(\d*)\s*(?:years?|yrs?)",
                text, re.IGNORECASE,
            )
            experience = ""
            if exp_match:
                exp_min = exp_match.group(1)
                exp_max = exp_match.group(2)
                experience = f"{exp_min}-{exp_max} years" if exp_max else f"{exp_min}+ years"

            sal_match = re.search(
                r"(?:INR|₹|Rs\.?|LPA|Lacs?)\s*[\d,.]+(?:\s*[-–]\s*[\d,.]+)?",
                text, re.IGNORECASE,
            )
            salary_text = sal_match.group(0) if sal_match else ""

            description = text.replace(title, "").strip()[:300]

            parsed.append({
                "title": title,
                "company": "CIEL HR",
                "location": loc_text,
                "salary": salary_text,
                "experience": experience,
                "job_url": item.get("url", ""),
                "description": description,
                "date_posted": "",
                "search_keyword": ", ".join(keywords),
                "search_location": city or "",
            })

        return parsed

    except Exception as e:
        print(f"    Error extracting jobs: {e}")
        return []
