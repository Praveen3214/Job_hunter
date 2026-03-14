"""ABC Consultants scraper — extracts jobs from abcconsultants.in.

ABC Consultants is one of India's oldest executive search firms (since 1969).
Their career portal is a WordPress site that serves results via a POST form
at /executive-careers/.  Results render as `.appl-card` elements.

Requires Playwright (the form is JS-driven).
"""

import re
import time

import pandas as pd
from playwright.sync_api import sync_playwright

from config import ABC_BASE_URL, ABC_DELAY
from scrapers.utils import CHROME_UA, is_india_location, is_non_india_location


def search_abcconsultants(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search abcconsultants.in for executive jobs.

    Args:
        keywords: Job title search terms.
        locations: Locations (matched against result cards).
        max_results: Max results to return.

    Returns:
        DataFrame with job listings from ABC Consultants.
    """
    all_jobs: list[dict] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=CHROME_UA,
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        try:
            page.goto(
                f"{ABC_BASE_URL}/executive-careers/",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            time.sleep(2)

            for keyword in keywords:
                if len(all_jobs) >= max_results:
                    break

                print(f"  [ABC] Searching '{keyword}'...")
                jobs = _search_and_extract(page, keyword, locations)
                all_jobs.extend(jobs)
                print(f"    Found {len(jobs)} results")
                time.sleep(ABC_DELAY)

        except Exception as e:
            print(f"    [ABC] Error: {e}")
        finally:
            context.close()
            browser.close()

    if not all_jobs:
        return pd.DataFrame()

    df = pd.DataFrame(all_jobs)
    df["platform"] = "abcconsultants"

    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"], keep="first")

    print(f"\n  [ABC] Total unique jobs: {len(df)}")
    return df


def _search_and_extract(
    page,
    keyword: str,
    locations: list[str],
) -> list[dict]:
    """Fill the search form, click search, and extract result cards."""
    jobs: list[dict] = []

    try:
        # Fill keyword
        title_input = page.locator('input[name="jobTitle"]')
        if title_input.count() > 0:
            title_input.fill(keyword)

        # Fill location if there's a text input for it
        loc_input = page.locator('input[name="location"]')
        if loc_input.count() > 0 and locations:
            # Use first location as seed (the site does partial matching)
            loc_input.fill(locations[0] if locations[0].lower() != "india" else "")

        # Click search
        search_btn = page.locator("#searchButton")
        if search_btn.count() > 0:
            search_btn.click()
            time.sleep(4)
        else:
            return jobs

        # Extract result cards
        cards = page.evaluate("""() => {
            const cards = document.querySelectorAll('.appl-card');
            return Array.from(cards).map(card => {
                const title = card.querySelector('.result-title');
                const lis = card.querySelectorAll('li');
                const viewLink = card.querySelector('a[href*="job-detail"], a[href*="job_id"]');
                const allLinks = card.querySelectorAll('a');
                let link = '';
                if (viewLink) {
                    link = viewLink.href;
                } else if (allLinks.length > 0) {
                    // Last link is usually "View"
                    link = allLinks[allLinks.length - 1].href;
                }
                return {
                    title: title ? title.textContent.trim() : '',
                    details: Array.from(lis).map(li => li.textContent.trim()),
                    link: link,
                    fullText: card.textContent.trim(),
                };
            });
        }""")

        for card in cards:
            title = card.get("title", "")
            if not title:
                continue

            # Location from list items
            details = card.get("details", [])
            loc_text = details[0] if details else ""

            # Job URL
            job_url = card.get("link", "")
            if job_url and not job_url.startswith("http"):
                job_url = f"{ABC_BASE_URL}{job_url}"

            # India-only filter: ABC is India-focused but skip clearly non-India
            if is_non_india_location(loc_text):
                continue

            # Extract salary/experience from full card text
            full_text = card.get("fullText", "")
            salary_text = ""
            experience = ""

            sal_match = re.search(
                r"(?:INR|Rs\.?|CTC|LPA)\s*[\d.,]+(?:\s*[-\u2013]\s*[\d.,]+)?(?:\s*(?:LPA|Cr|L))?",
                full_text, re.IGNORECASE,
            )
            if sal_match:
                salary_text = sal_match.group(0)

            exp_match = re.search(
                r"(\d+)\+?\s*(?:[-\u2013]\s*(\d+)\s*)?(?:years?|yrs?)",
                full_text, re.IGNORECASE,
            )
            if exp_match:
                exp_min = exp_match.group(1)
                exp_max = exp_match.group(2)
                experience = (
                    f"{exp_min}-{exp_max} years" if exp_max else f"{exp_min}+ years"
                )

            jobs.append({
                "title": title,
                "company": "ABC Consultants",  # Placement firm
                "location": loc_text,
                "salary": salary_text,
                "experience": experience,
                "job_url": job_url,
                "description": "",
                "date_posted": "",
                "search_keyword": keyword,
                "search_location": ", ".join(locations),
            })

    except Exception as e:
        print(f"    [ABC] Extraction error: {e}")

    return jobs
