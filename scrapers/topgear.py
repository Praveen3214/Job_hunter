"""TopGear Consultants scraper — extracts jobs from jobs.topgearconsultants.com.

TopGear has a separate subdomain job portal.  The site may be intermittently
unavailable; this scraper handles connection failures gracefully.
Uses Playwright for JavaScript-rendered content.
"""

import re
import time
from urllib.parse import quote_plus

import pandas as pd
from playwright.sync_api import sync_playwright, Page

from config import TOPGEAR_BASE_URL, TOPGEAR_DELAY
from scrapers.utils import is_non_india_location


def search_topgear(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search jobs.topgearconsultants.com for jobs using browser automation.

    Note: The TopGear jobs portal may be intermittently unavailable.
    This scraper returns an empty DataFrame gracefully if the site is down.

    Args:
        keywords: Job title search terms.
        locations: Locations (used for relevance filtering).
        max_results: Max results to return.

    Returns:
        DataFrame with job listings from TopGear.
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
            # Try loading the jobs portal
            print(f"  [TopGear] Loading jobs portal...")
            page.goto(
                f"{TOPGEAR_BASE_URL}/jobs/",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            page.wait_for_timeout(5000)

            # Extract all jobs from the main listing
            jobs = _extract_jobs(page, keywords, locations, max_results)
            all_jobs.extend(jobs)
            print(f"    Found {len(jobs)} results from main page")

            # Try keyword search if the portal supports it
            if len(all_jobs) < max_results:
                for keyword in keywords[:3]:
                    try:
                        # Try URL-based search
                        search_url = f"{TOPGEAR_BASE_URL}/jobs/?keyword={quote_plus(keyword)}"
                        page.goto(search_url, wait_until="domcontentloaded", timeout=20000)
                        page.wait_for_timeout(3000)

                        search_jobs = _extract_jobs(page, [keyword], locations, max_results - len(all_jobs))
                        all_jobs.extend(search_jobs)
                        print(f"    Search '{keyword}': {len(search_jobs)} results")
                        time.sleep(TOPGEAR_DELAY)
                    except Exception as e:
                        print(f"    Search '{keyword}' failed: {e}")

        except Exception as e:
            print(f"  [TopGear] Error (site may be down): {e}")
        finally:
            browser.close()

    if not all_jobs:
        return pd.DataFrame()

    df = pd.DataFrame(all_jobs)
    df["platform"] = "topgear"

    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"], keep="first")

    print(f"\n  [TopGear] Total unique jobs: {len(df)}")
    return df


def _extract_jobs(
    page: Page,
    keywords: list[str],
    locations: list[str],
    max_results: int,
) -> list[dict]:
    """Extract job data from TopGear page via DOM evaluation."""
    try:
        jobs_data = page.evaluate("""() => {
            const jobs = [];
            const seen = new Set();

            // Strategy 1: Job listing links
            const links = document.querySelectorAll('a[href*="job"], a[href*="opening"], a[href*="position"]');
            for (const link of links) {
                const href = link.href || link.getAttribute('href') || '';
                if (!href || seen.has(href)) continue;
                seen.add(href);

                const card = link.closest('div, article, li, tr') || link;
                const title = (link.innerText || link.textContent || '').trim();
                const text = (card.innerText || card.textContent || '').trim();

                if (!title || title.length < 5) continue;
                // Skip nav links
                if (title.toLowerCase() === 'jobs' || title.toLowerCase() === 'search') continue;

                jobs.push({
                    title: title.split('\\n')[0],
                    url: href.startsWith('http') ? href : window.location.origin + href,
                    text: text.substring(0, 500),
                });
            }

            // Strategy 2: Card/container elements
            if (jobs.length === 0) {
                const cards = document.querySelectorAll(
                    '[class*="job"], [class*="card"], [class*="listing"], ' +
                    '[class*="vacancy"], article'
                );
                for (const card of cards) {
                    const heading = card.querySelector('h2, h3, h4, h5, strong');
                    const link = card.querySelector('a[href]');
                    if (!heading) continue;

                    const title = (heading.innerText || heading.textContent || '').trim();
                    if (!title || title.length < 5) continue;

                    const href = link ? (link.href || link.getAttribute('href') || '') : '';
                    if (href && seen.has(href)) continue;
                    if (href) seen.add(href);

                    const text = (card.innerText || card.textContent || '').trim();

                    jobs.push({
                        title: title,
                        url: href.startsWith('http') ? href : (href ? window.location.origin + href : ''),
                        text: text.substring(0, 500),
                    });
                }
            }

            return jobs;
        }""")

        parsed = []
        for item in jobs_data[:max_results]:
            title = item.get("title", "")
            text = item.get("text", "")

            # Extract fields
            loc_match = re.search(
                r"(Mumbai|Delhi|Bangalore|Bengaluru|Hyderabad|Chennai|Pune|"
                r"Kolkata|Noida|Gurugram|Gurgaon|India|Remote)",
                text, re.IGNORECASE,
            )
            loc_text = loc_match.group(0) if loc_match else ""

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

            # India-only filter: skip clearly non-India roles
            if is_non_india_location(loc_text):
                continue

            description = text.replace(title, "").strip()[:300]

            parsed.append({
                "title": title,
                "company": "TopGear Consultants",
                "location": loc_text,
                "salary": salary_text,
                "experience": experience,
                "job_url": item.get("url", ""),
                "description": description,
                "date_posted": "",
                "search_keyword": ", ".join(keywords),
                "search_location": ", ".join(locations),
            })

        return parsed

    except Exception as e:
        print(f"    Error extracting jobs: {e}")
        return []
