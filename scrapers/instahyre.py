"""Instahyre.com scraper using Playwright (browser automation).

Instahyre is an Angular SPA backed by a Django REST API.  Job listing
pages are publicly accessible but require JavaScript execution to render.
We use Playwright to load pages and extract job data from the DOM.
"""

import json
import re
import time

import pandas as pd
from playwright.sync_api import sync_playwright, Page

from config import INSTAHYRE_DELAY
from scrapers.utils import to_slug


def search_instahyre(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search Instahyre.com for jobs using browser automation.

    Args:
        keywords: Job title search terms.
        locations: Locations to search (Indian cities or "Remote").
        max_results: Max results per keyword+location combo.

    Returns:
        DataFrame with job listings from Instahyre.
    """
    all_jobs = []

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
            for keyword in keywords:
                for location in locations:
                    print(f"  [Instahyre] Searching '{keyword}' in '{location}'...")
                    jobs = _scrape_search(page, keyword, location, max_results)
                    all_jobs.extend(jobs)
                    print(f"    Found {len(jobs)} results")
        finally:
            browser.close()

    if not all_jobs:
        return pd.DataFrame()

    df = pd.DataFrame(all_jobs)
    df["platform"] = "instahyre"

    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"], keep="first")

    print(f"\n  [Instahyre] Total unique jobs: {len(df)}")
    return df



def _scrape_search(
    page: Page,
    keyword: str,
    location: str,
    max_results: int,
) -> list[dict]:
    """Scrape job search results from Instahyre."""
    jobs = []
    slug = to_slug(keyword)

    # Instahyre URL patterns:
    #   /keyword-jobs/           (all locations)
    #   /keyword-jobs-in-city/   (specific city)
    loc_slug = to_slug(location)
    if location.lower() in ("india", "remote", "all"):
        url = f"https://www.instahyre.com/{slug}-jobs/"
    else:
        url = f"https://www.instahyre.com/{slug}-jobs-in-{loc_slug}/"

    pages_to_fetch = max(1, (max_results // 15) + 1)

    for page_num in range(1, pages_to_fetch + 1):
        if len(jobs) >= max_results:
            break

        page_url = url if page_num == 1 else f"{url}?page={page_num}"

        try:
            page.goto(page_url, wait_until="domcontentloaded", timeout=25000)
            # Wait for Angular to render job cards
            page.wait_for_timeout(5000)

            # Try to extract from intercepted API data or DOM
            page_jobs = _extract_from_dom(page)

            if not page_jobs:
                # Try extracting from inline script data
                page_jobs = _extract_from_scripts(page)

            if not page_jobs:
                print(f"    Page {page_num}: No jobs found")
                break

            for job in page_jobs:
                job["search_keyword"] = keyword
                job["search_location"] = location
            jobs.extend(page_jobs)
            time.sleep(INSTAHYRE_DELAY)

        except Exception as e:
            print(f"    Page {page_num} error: {e}")
            break

    return jobs[:max_results]


def _extract_from_dom(page: Page) -> list[dict]:
    """Extract job listings from the rendered Instahyre DOM."""
    job_data = page.evaluate("""() => {
        const jobs = [];
        const seen = new Set();

        // Strategy 1: Job listing cards with links to /job-* detail pages
        const jobLinks = document.querySelectorAll('a[href*="/job-"]');

        for (const link of jobLinks) {
            const href = link.getAttribute('href') || '';
            if (!href.includes('/job-') || seen.has(href)) continue;
            seen.add(href);

            // Find parent card container
            let card = link.closest('div[class*="job"]')
                    || link.closest('article')
                    || link.parentElement?.parentElement?.parentElement;
            if (!card) card = link.parentElement;

            const title = link.textContent?.trim() || '';
            if (!title || title.length < 3) continue;

            const cardText = card?.textContent || '';

            // Company: look for company name links or spans
            const companyEl = card?.querySelector('a[href*="/company/"]')
                           || card?.querySelector('a:not([href*="/job-"])');
            let company = companyEl?.textContent?.trim() || '';
            // Fallback: if no company link, look for any emphasized text near title
            if (!company) {
                const spans = card?.querySelectorAll('span, p, div');
                for (const s of (spans || [])) {
                    const t = s.textContent?.trim() || '';
                    if (t.length > 2 && t.length < 60 && t !== title && !t.includes('Apply')) {
                        company = t;
                        break;
                    }
                }
            }

            // Location
            const cities = 'Bangalore|Bengaluru|Mumbai|Delhi|Hyderabad|Chennai|Pune|Kolkata|Noida|Gurgaon|Gurugram|Remote|India|NCR';
            const locRegex = new RegExp('(' + cities + ')(?:[,\\\\s]*(?:' + cities + '))*', 'gi');
            const locMatches = cardText.match(locRegex);
            const location = locMatches ? locMatches[0].trim() : '';

            // Experience
            const expMatch = cardText.match(/(\\d+[-+]?\\s*(?:to|-)?\\s*\\d*\\s*(?:Yrs?|years?|yr))/i);
            const experience = expMatch ? expMatch[0].trim() : '';

            // Salary
            const salaryMatch = cardText.match(/((?:₹|Rs\\.?|INR)?\\s*\\d+[\\d.,]*\\s*[-–to]+\\s*(?:₹|Rs\\.?|INR)?\\s*\\d+[\\d.,]*\\s*(?:Lacs?|Lakhs?|LPA|Cr|L|P\\.A\\.))/i);
            const salary = salaryMatch ? salaryMatch[0].trim() : '';

            const fullUrl = href.startsWith('http') ? href : 'https://www.instahyre.com' + href;

            jobs.push({ title, company, location, experience, salary, job_url: fullUrl });
        }

        // Strategy 2: Broader search — any card-like elements with job info
        if (jobs.length === 0) {
            const cards = document.querySelectorAll('[class*="card"], [class*="listing"], [class*="result"]');
            for (const card of cards) {
                const titleEl = card.querySelector('a[href*="job"], h2, h3, h4');
                if (!titleEl) continue;

                const title = titleEl.textContent?.trim() || '';
                const href = titleEl.getAttribute('href') || '';
                if (!title || title.length < 3) continue;

                const fullUrl = href.startsWith('http') ? href
                              : href ? 'https://www.instahyre.com' + href
                              : '';

                jobs.push({
                    title,
                    company: '',
                    location: '',
                    experience: '',
                    salary: '',
                    job_url: fullUrl,
                });
            }
        }

        return jobs;
    }""")

    jobs = []
    if job_data:
        for job in job_data:
            if job.get("title"):
                job.setdefault("date_posted", "")
                job.setdefault("description", "")
                jobs.append(job)

    return jobs


def _extract_from_scripts(page: Page) -> list[dict]:
    """Try to extract job data from inline scripts or Angular scope."""
    jobs = []

    script_data = page.evaluate("""() => {
        // Check for window-level job data (Angular services often attach here)
        const candidates = [
            window.__INITIAL_DATA__,
            window.__JOBS_DATA__,
            window.jobsData,
            window.pageData,
        ];

        for (const data of candidates) {
            if (data && typeof data === 'object') {
                return { type: 'window', data };
            }
        }

        // Check for JSON-LD structured data
        const ldScripts = document.querySelectorAll('script[type="application/ld+json"]');
        for (const script of ldScripts) {
            try {
                const ld = JSON.parse(script.textContent);
                if (ld['@type'] === 'JobPosting' || (Array.isArray(ld) && ld[0]?.['@type'] === 'JobPosting')) {
                    return { type: 'jsonld', data: Array.isArray(ld) ? ld : [ld] };
                }
            } catch(e) {}
        }

        // Check for __NEXT_DATA__ (Cutshort-originated pages redirected here)
        const nextData = document.getElementById('__NEXT_DATA__');
        if (nextData) {
            try {
                return { type: 'next', data: JSON.parse(nextData.textContent) };
            } catch(e) {}
        }

        return null;
    }""")

    if not script_data:
        return jobs

    data = script_data.get("data")
    data_type = script_data.get("type")

    if data_type == "jsonld":
        # JSON-LD structured data
        items = data if isinstance(data, list) else [data]
        for item in items:
            if item.get("@type") != "JobPosting":
                continue
            jobs.append({
                "title": item.get("title", ""),
                "company": (item.get("hiringOrganization") or {}).get("name", ""),
                "location": _extract_ld_location(item),
                "salary": _extract_ld_salary(item),
                "job_url": item.get("url", ""),
                "date_posted": item.get("datePosted", ""),
                "description": (item.get("description") or "")[:300],
                "experience": "",
            })
    elif data_type == "window" and isinstance(data, dict):
        # Window-level data — look for job arrays
        for key in ["jobs", "results", "listings", "jobList", "data"]:
            items = data.get(key, [])
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict) and (item.get("title") or item.get("jobTitle")):
                        jobs.append({
                            "title": item.get("title") or item.get("jobTitle", ""),
                            "company": item.get("company") or item.get("companyName", ""),
                            "location": item.get("location") or item.get("city", ""),
                            "salary": item.get("salary", ""),
                            "experience": item.get("experience", ""),
                            "job_url": item.get("url", ""),
                            "date_posted": item.get("datePosted", ""),
                            "description": (item.get("description") or "")[:300],
                        })

    return jobs


def _extract_ld_location(item: dict) -> str:
    """Extract location from JSON-LD jobLocation field."""
    loc = item.get("jobLocation")
    if isinstance(loc, dict):
        address = loc.get("address", {})
        if isinstance(address, dict):
            parts = [
                address.get("addressLocality", ""),
                address.get("addressRegion", ""),
            ]
            return ", ".join(p for p in parts if p)
    if isinstance(loc, list):
        locs = []
        for l in loc:
            if isinstance(l, dict):
                addr = l.get("address", {})
                if isinstance(addr, dict):
                    locs.append(addr.get("addressLocality", ""))
        return ", ".join(l for l in locs if l)
    return ""


def _extract_ld_salary(item: dict) -> str:
    """Extract salary from JSON-LD baseSalary field."""
    salary = item.get("baseSalary")
    if isinstance(salary, dict):
        value = salary.get("value", {})
        if isinstance(value, dict):
            lo = value.get("minValue", "")
            hi = value.get("maxValue", "")
            currency = salary.get("currency", "INR")
            if lo and hi:
                return f"{currency} {lo} - {hi}"
    return ""
