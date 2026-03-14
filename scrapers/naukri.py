"""Naukri.com scraper using Playwright (browser automation).

Naukri is a Next.js SPA with captcha-protected APIs, so we use a real
browser to render the page and extract job data from the DOM.
"""

import json
import re
import time

import pandas as pd
from playwright.sync_api import sync_playwright, Page

from config import NAUKRI_BASE_URL, NAUKRI_DELAY, NAUKRI_MIN_EXPERIENCE


def search_naukri(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
    min_experience: int = NAUKRI_MIN_EXPERIENCE,
) -> pd.DataFrame:
    """
    Search Naukri.com for senior marketing jobs using browser automation.

    Args:
        keywords: Job title search terms.
        locations: Locations to search (Indian cities).
        max_results: Max results per keyword+location combo.
        min_experience: Minimum years of experience filter.

    Returns:
        DataFrame with job listings from Naukri.
    """
    all_jobs = []

    with sync_playwright() as pw:
        # Naukri's SPA detection may block headless; use headed mode
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
            for keyword in keywords:
                for location in locations:
                    print(f"  [Naukri] Searching '{keyword}' in '{location}'...")
                    jobs = _scrape_search(page, keyword, location, max_results, min_experience)
                    all_jobs.extend(jobs)
                    print(f"    Found {len(jobs)} results")
        finally:
            browser.close()

    if not all_jobs:
        return pd.DataFrame()

    df = pd.DataFrame(all_jobs)
    df["platform"] = "naukri"

    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"], keep="first")

    print(f"\n  [Naukri] Total unique jobs: {len(df)}")
    return df


def _scrape_search(
    page: Page,
    keyword: str,
    location: str,
    max_results: int,
    min_experience: int,
) -> list[dict]:
    """Scrape paginated search results from Naukri."""
    jobs = []
    slug = keyword.lower().replace(" ", "-")
    loc_slug = location.lower().replace(" ", "-")
    pages_needed = (max_results // 20) + 1

    for page_num in range(1, pages_needed + 1):
        if len(jobs) >= max_results:
            break

        # Build URL
        if page_num == 1:
            url = f"{NAUKRI_BASE_URL}/{slug}-jobs-in-{loc_slug}?experience={min_experience}"
        else:
            url = f"{NAUKRI_BASE_URL}/{slug}-jobs-in-{loc_slug}-{page_num}?experience={min_experience}"

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=20000)
            # Naukri is a SPA — need extra time for JS to render job cards
            page.wait_for_timeout(5000)

            # Try to extract from API intercepts or DOM
            page_jobs = _extract_jobs_from_dom(page)

            if not page_jobs:
                # Try extracting from __NEXT_DATA__ or inline scripts
                page_jobs = _extract_from_scripts(page)

            if not page_jobs:
                print(f"    Page {page_num}: No jobs found in DOM")
                break

            jobs.extend(page_jobs)
            time.sleep(NAUKRI_DELAY)

        except Exception as e:
            print(f"    Page {page_num} error: {e}")
            break

    return jobs[:max_results]


def _extract_jobs_from_dom(page: Page) -> list[dict]:
    """Extract job listings from the rendered DOM."""
    jobs = []

    # Naukri renders job cards — try multiple selector strategies
    # The class names are hashed but the structure is consistent
    job_data = page.evaluate("""() => {
        const jobs = [];

        // Strategy 1: Look for job card links with structured content
        // Naukri job cards typically have an anchor with the job title
        // and contain company, location, experience, salary info
        const allLinks = document.querySelectorAll('a[href*="/job-listings-"]');
        const seen = new Set();

        for (const link of allLinks) {
            const href = link.getAttribute('href') || '';
            if (seen.has(href) || !href.includes('job-listings')) continue;
            seen.add(href);

            // Find the parent card container (usually 2-3 levels up)
            let card = link.closest('article') || link.closest('[class*="job"]') || link.parentElement?.parentElement?.parentElement;
            if (!card) continue;

            const title = link.textContent?.trim() || '';
            if (!title || title.length < 3) continue;

            // Extract company name — usually in a separate anchor or span near the title
            const companyEl = card.querySelector('a[href*="company-jobs"], a[href*="/company/"]')
                || card.querySelector('a:not([href*="job-listings"])');
            const company = companyEl?.textContent?.trim() || '';

            // Extract other fields from the card text
            const cardText = card.textContent || '';

            // Location: Extract only city names, stop at non-location text
            const cities = 'Bangalore|Bengaluru|Mumbai|Delhi|Hyderabad|Chennai|Pune|Kolkata|Noida|Gurgaon|Gurugram|Ahmedabad|Jaipur|Remote|India|NCR|Kochi|Chandigarh|Lucknow|Indore|Bhopal|New Delhi';
            const locRegex = new RegExp('(' + cities + ')(?:[,\\s]*(?:' + cities + '))*', 'gi');
            const locMatches = cardText.match(locRegex);
            const location = locMatches ? locMatches[0].trim() : '';

            // Experience: pattern like "10-15 Yrs" or "10+ Yrs"
            const expMatch = cardText.match(/(\\d+[-+]?\\s*(?:to|-)?\\s*\\d*\\s*(?:Yrs?|years?))/i);
            const experience = expMatch ? expMatch[0].trim() : '';

            // Salary: pattern like "30-50 Lacs" or "₹30L - ₹50L"
            const salaryMatch = cardText.match(/((?:₹|Rs\\.?|INR)?\\s*\\d+[\\d.,]*\\s*[-–to]+\\s*(?:₹|Rs\\.?|INR)?\\s*\\d+[\\d.,]*\\s*(?:Lacs?|Lakhs?|LPA|Cr|L|P\\.A\\.))/i);
            const salary = salaryMatch ? salaryMatch[0].trim() : '';

            const fullUrl = href.startsWith('http') ? href : 'https://www.naukri.com' + href;

            jobs.push({ title, company, location, experience, salary, job_url: fullUrl });
        }

        // Strategy 2: If Strategy 1 found nothing, try looking for structured data
        if (jobs.length === 0) {
            // Look for any div/article that contains both a job title link and company info
            const cards = document.querySelectorAll('[class*="tuple"], [class*="jobTuple"], [class*="srp-jobtuple"]');
            for (const card of cards) {
                const titleEl = card.querySelector('a[title]') || card.querySelector('a');
                if (!titleEl) continue;

                const title = titleEl.getAttribute('title') || titleEl.textContent?.trim() || '';
                const href = titleEl.getAttribute('href') || '';
                if (!title || title.length < 3) continue;

                jobs.push({
                    title,
                    company: '',
                    location: '',
                    experience: '',
                    salary: '',
                    job_url: href.startsWith('http') ? href : 'https://www.naukri.com' + href,
                });
            }
        }

        return jobs;
    }""")

    if job_data:
        for job in job_data:
            if job.get("title"):
                job["date_posted"] = ""
                job["description"] = ""
                jobs.append(job)

    return jobs


def _extract_from_scripts(page: Page) -> list[dict]:
    """Try to extract job data from __NEXT_DATA__ or inline scripts."""
    jobs = []

    script_data = page.evaluate("""() => {
        // Check for __NEXT_DATA__
        const nextData = document.getElementById('__NEXT_DATA__');
        if (nextData) {
            try {
                const data = JSON.parse(nextData.textContent);
                const props = data?.props?.pageProps;
                if (props) return { type: 'next', data: props };
            } catch(e) {}
        }

        // Check for window.__INITIAL_STATE__ or similar
        if (window.__INITIAL_STATE__) {
            return { type: 'initial', data: window.__INITIAL_STATE__ };
        }

        return null;
    }""")

    if not script_data:
        return jobs

    data = script_data.get("data", {})

    # Try to find job listings in the data structure
    if isinstance(data, dict):
        # Common Naukri patterns: searchResult, jobDetails, jobList
        for key in ["searchResult", "jobDetails", "jobList", "jobs", "results"]:
            items = data.get(key, [])
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict) and (item.get("title") or item.get("jobTitle")):
                        jobs.append({
                            "title": item.get("title") or item.get("jobTitle", ""),
                            "company": item.get("companyName") or item.get("company", ""),
                            "location": item.get("location") or item.get("city", ""),
                            "job_url": item.get("jdURL") or item.get("url", ""),
                            "salary": item.get("salary") or item.get("salaryLabel", ""),
                            "experience": item.get("experience") or item.get("experienceLabel", ""),
                            "date_posted": item.get("createdDate") or item.get("postedDate", ""),
                            "description": (item.get("description") or "")[:200],
                        })

    return jobs
