"""Crescendo Global scraper — extracts jobs from crescendo-global.com.

Crescendo Global is a modern SPA (React/Next.js style) with client-side
rendering.  Job cards only appear after JavaScript execution, so we use
Playwright to render the page and extract job data from the DOM.
"""

import re
import time

import pandas as pd
from playwright.sync_api import sync_playwright, Page

from config import CRESCENDO_BASE_URL, CRESCENDO_DELAY
from scrapers.utils import is_non_india_location


def search_crescendo(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search crescendo-global.com for jobs using browser automation.

    Args:
        keywords: Job title search terms.
        locations: Locations to filter (Indian cities).
        max_results: Max results to return.

    Returns:
        DataFrame with job listings from Crescendo Global.
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
            # Load the jobs page first
            print(f"  [Crescendo] Loading jobs page...")
            page.goto(f"{CRESCENDO_BASE_URL}/jobs", wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(5000)  # Wait for SPA render

            # Try to extract all visible jobs
            jobs = _extract_jobs(page, keywords, locations, max_results)
            all_jobs.extend(jobs)
            print(f"    Found {len(jobs)} results from main page")

            # Try keyword-specific search if the page has a search input
            if len(all_jobs) < max_results:
                for keyword in keywords:
                    print(f"  [Crescendo] Searching '{keyword}'...")
                    search_jobs = _search_and_extract(page, keyword, locations, max_results - len(all_jobs))
                    all_jobs.extend(search_jobs)
                    print(f"    Found {len(search_jobs)} results")
                    time.sleep(CRESCENDO_DELAY)

        except Exception as e:
            print(f"  [Crescendo] Error: {e}")
        finally:
            browser.close()

    if not all_jobs:
        return pd.DataFrame()

    df = pd.DataFrame(all_jobs)
    df["platform"] = "crescendo"

    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"], keep="first")

    print(f"\n  [Crescendo] Total unique jobs: {len(df)}")
    return df


def _extract_jobs(
    page: Page,
    keywords: list[str],
    locations: list[str],
    max_results: int,
) -> list[dict]:
    """Extract job cards from the currently loaded page via DOM evaluation."""
    keyword_lower_set = {w.lower() for kw in keywords for w in kw.split()}
    location_lower_set = {loc.lower() for loc in locations}

    try:
        # Use page.evaluate() to extract job data from the DOM
        jobs_data = page.evaluate("""() => {
            const jobs = [];

            // Strategy 1: Look for job card elements (links to individual job pages)
            const links = document.querySelectorAll('a[href*="/job"], a[href*="/jobs/"], a[href*="/career"]');
            const seen = new Set();

            for (const link of links) {
                const href = link.href || link.getAttribute('href') || '';
                if (!href || seen.has(href)) continue;
                if (href.includes('/jobs') && !href.includes('/jobs/')) continue;  // Skip main jobs page link
                seen.add(href);

                const card = link.closest('div, article, li') || link;
                const text = card.innerText || card.textContent || '';
                const title = link.innerText || link.textContent || '';

                if (title.length < 5) continue;

                jobs.push({
                    title: title.trim().split('\\n')[0],  // First line is usually the title
                    url: href.startsWith('http') ? href : window.location.origin + href,
                    text: text.trim().substring(0, 500),
                });
            }

            // Strategy 2: Look for card-like containers with job info
            if (jobs.length === 0) {
                const cards = document.querySelectorAll(
                    '[class*="card"], [class*="job"], [class*="listing"], [class*="post"]'
                );
                for (const card of cards) {
                    const heading = card.querySelector('h2, h3, h4, strong');
                    const link = card.querySelector('a[href]');
                    if (!heading) continue;

                    const title = heading.innerText || heading.textContent || '';
                    if (title.length < 5) continue;

                    const href = link ? (link.href || link.getAttribute('href') || '') : '';
                    const text = card.innerText || card.textContent || '';

                    jobs.push({
                        title: title.trim(),
                        url: href.startsWith('http') ? href : (href ? window.location.origin + href : ''),
                        text: text.trim().substring(0, 500),
                    });
                }
            }

            return jobs;
        }""")

        parsed = []
        for item in jobs_data[:max_results]:
            title = item.get("title", "")
            text = item.get("text", "")

            # Extract structured fields from card text
            loc_text = ""
            salary_text = ""
            experience = ""

            # Location
            loc_match = re.search(
                r"(Mumbai|Delhi|Bangalore|Bengaluru|Hyderabad|Chennai|Pune|"
                r"Kolkata|Noida|Gurugram|Gurgaon|Ahmedabad|Jaipur|Chandigarh|"
                r"Lucknow|Indore|India|Remote)",
                text, re.IGNORECASE,
            )
            if loc_match:
                loc_text = loc_match.group(0)

            # Experience
            exp_match = re.search(
                r"(\d+)\+?\s*[-–to]*\s*(\d*)\s*(?:years?|yrs?)",
                text, re.IGNORECASE,
            )
            if exp_match:
                exp_min = exp_match.group(1)
                exp_max = exp_match.group(2)
                experience = f"{exp_min}-{exp_max} years" if exp_max else f"{exp_min}+ years"

            # Salary
            sal_match = re.search(
                r"(?:INR|₹|Rs\.?|LPA|Lacs?)\s*[\d,.]+(?:\s*[-–]\s*[\d,.]+)?",
                text, re.IGNORECASE,
            )
            if sal_match:
                salary_text = sal_match.group(0)

            # India-only filter: skip clearly non-India roles
            if is_non_india_location(loc_text):
                continue

            # Description (card text minus title)
            description = text.replace(title, "").strip()[:300]

            parsed.append({
                "title": title,
                "company": "Crescendo Global",
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
        print(f"    Error extracting jobs from DOM: {e}")
        return []


def _search_and_extract(
    page: Page,
    keyword: str,
    locations: list[str],
    max_results: int,
) -> list[dict]:
    """Try to use the page's search/filter functionality."""
    try:
        # Look for search input
        search_input = page.query_selector(
            'input[type="search"], input[placeholder*="search" i], '
            'input[placeholder*="keyword" i], input[name*="search" i]'
        )

        if search_input:
            search_input.fill(keyword)
            search_input.press("Enter")
            page.wait_for_timeout(3000)  # Wait for results to update
            return _extract_jobs(page, [keyword], locations, max_results)

        # Try URL-based search
        page.goto(
            f"{CRESCENDO_BASE_URL}/jobs?keyword={keyword.replace(' ', '+')}",
            wait_until="domcontentloaded",
            timeout=20000,
        )
        page.wait_for_timeout(4000)
        return _extract_jobs(page, [keyword], locations, max_results)

    except Exception as e:
        print(f"    Search failed: {e}")
        return []
