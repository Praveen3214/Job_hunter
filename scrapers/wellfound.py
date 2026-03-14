"""Wellfound.com (formerly AngelList Talent) scraper using Playwright.

Wellfound is a React SPA with Apollo GraphQL backend, protected by
DataDome + Cloudflare.  Requires an authenticated browser session to
access job data — we log in via Playwright, then query the GraphQL
endpoint from within the browser context.

Setup:
    1. Create a free account at https://wellfound.com (sign up with Google or email)
    2. Set WELLFOUND_EMAIL and WELLFOUND_PASSWORD in your .env file
    3. First run may need manual CAPTCHA solve in the browser window
"""

import json
import os
import re
import time

import pandas as pd
from playwright.sync_api import sync_playwright, Page, BrowserContext

from config import BASE_DIR, WELLFOUND_DELAY
from scrapers.utils import to_slug, is_non_india_location

WELLFOUND_PAGE_DELAY = 5.0

# Session cookie file
WELLFOUND_SESSION_FILE = BASE_DIR / ".session" / "wellfound_cookies.json"


def search_wellfound(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search Wellfound.com for startup jobs.

    Requires WELLFOUND_EMAIL and WELLFOUND_PASSWORD in .env,
    OR a valid session cookie file from a previous login.

    Args:
        keywords: Job title search terms.
        locations: Locations to search.
        max_results: Max results per keyword+location combo.

    Returns:
        DataFrame with job listings from Wellfound.
    """
    email = os.getenv("WELLFOUND_EMAIL", "")
    password = os.getenv("WELLFOUND_PASSWORD", "")

    if not email and not WELLFOUND_SESSION_FILE.exists():
        print("  [Wellfound] Skipped — no credentials configured.")
        print("    Set WELLFOUND_EMAIL and WELLFOUND_PASSWORD in .env")
        print("    Or sign up free at https://wellfound.com")
        return pd.DataFrame()

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

        # Restore session cookies if available
        if WELLFOUND_SESSION_FILE.exists():
            try:
                cookies = json.loads(WELLFOUND_SESSION_FILE.read_text())
                context.add_cookies(cookies)
                print("  [Wellfound] Restored session cookies")
            except Exception:
                pass

        page = context.new_page()

        # Check if we need to log in
        if not _is_logged_in(page):
            if email and password:
                if not _login(page, email, password):
                    print("  [Wellfound] Login failed — skipping")
                    browser.close()
                    return pd.DataFrame()
                # Save session cookies for next time
                _save_cookies(context)
            else:
                print("  [Wellfound] Not logged in and no credentials — skipping")
                browser.close()
                return pd.DataFrame()

        # Search for jobs
        for keyword in keywords:
            for location in locations:
                print(f"  [Wellfound] Searching '{keyword}' in '{location}'...")
                jobs = _search_jobs(page, keyword, location, max_results)
                all_jobs.extend(jobs)
                print(f"    Found {len(jobs)} results")

        browser.close()

    if not all_jobs:
        return pd.DataFrame()

    df = pd.DataFrame(all_jobs)
    df["platform"] = "wellfound"

    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"], keep="first")

    print(f"\n  [Wellfound] Total unique jobs: {len(df)}")
    return df


def _is_logged_in(page: Page) -> bool:
    """Check if the current session is authenticated."""
    try:
        page.goto("https://wellfound.com/jobs", wait_until="domcontentloaded", timeout=20000)
        page.wait_for_timeout(5000)

        # If we land on a page with job results (not login/captcha), we're logged in
        url = page.url
        if "login" in url or "signup" in url:
            return False

        # Check for auth indicators in the page
        has_avatar = page.evaluate("""() => {
            return !!(
                document.querySelector('[data-test="UserAvatar"]')
                || document.querySelector('[class*="avatar"]')
                || document.querySelector('[aria-label*="profile"]')
                || document.querySelector('nav a[href*="/profile"]')
            );
        }""")
        return bool(has_avatar)

    except Exception:
        return False


def _login(page: Page, email: str, password: str) -> bool:
    """Attempt to log in to Wellfound."""
    try:
        page.goto("https://wellfound.com/login", wait_until="domcontentloaded", timeout=20000)
        page.wait_for_timeout(3000)

        # Fill login form
        email_input = page.locator('input[type="email"], input[name="email"], input[placeholder*="email" i]')
        if email_input.count() > 0:
            email_input.first.fill(email)
        else:
            print("    Could not find email input field")
            return False

        password_input = page.locator('input[type="password"]')
        if password_input.count() > 0:
            password_input.first.fill(password)
        else:
            print("    Could not find password input field")
            return False

        # Click login button
        login_btn = page.locator('button[type="submit"], button:has-text("Log in"), button:has-text("Sign in")')
        if login_btn.count() > 0:
            login_btn.first.click()
        else:
            print("    Could not find login button")
            return False

        # Wait for login to complete (CAPTCHA may appear here — user solves manually)
        print("    Logging in... (solve CAPTCHA if prompted)")
        page.wait_for_timeout(8000)

        # Check for CAPTCHA or challenge page
        page_text = page.evaluate("() => document.body?.innerText || ''")
        if "captcha" in page_text.lower() or "verify" in page_text.lower():
            print("    CAPTCHA detected — please solve it in the browser window")
            print("    Waiting up to 60 seconds...")
            # Wait for user to solve CAPTCHA
            for _ in range(12):
                page.wait_for_timeout(5000)
                if "login" not in page.url.lower():
                    break

        # Verify login succeeded
        if "login" in page.url.lower() or "signup" in page.url.lower():
            print("    Login failed — still on login page")
            return False

        print("    Login successful!")
        return True

    except Exception as e:
        print(f"    Login error: {e}")
        return False


def _save_cookies(context: BrowserContext):
    """Save session cookies for future use."""
    try:
        cookies = context.cookies()
        WELLFOUND_SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
        WELLFOUND_SESSION_FILE.write_text(json.dumps(cookies, indent=2))
        print("    Session cookies saved for next time")
    except Exception as e:
        print(f"    Could not save cookies: {e}")


def _search_jobs(
    page: Page,
    keyword: str,
    location: str,
    max_results: int,
) -> list[dict]:
    """Search for jobs on Wellfound using the web UI."""
    jobs = []
    slug = to_slug(keyword)
    loc_slug = to_slug(location)

    # Navigate to role/location search page
    if location.lower() in ("india", "remote", "all"):
        url = f"https://wellfound.com/role/r/{slug}"
    else:
        url = f"https://wellfound.com/role/l/{slug}/{loc_slug}"

    pages_to_fetch = max(1, (max_results // 10) + 1)

    for page_num in range(1, pages_to_fetch + 1):
        if len(jobs) >= max_results:
            break

        try:
            if page_num == 1:
                page.goto(url, wait_until="domcontentloaded", timeout=25000)
            else:
                # Wellfound uses "Load more" or scroll-based pagination
                # Try scrolling to trigger lazy load
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(3000)

            page.wait_for_timeout(WELLFOUND_PAGE_DELAY * 1000)

            # Extract jobs from the rendered page
            page_jobs = _extract_jobs_from_dom(page, keyword, location)

            if not page_jobs:
                break

            # Only add new jobs we haven't seen
            existing_urls = {j["job_url"] for j in jobs}
            new_jobs = [j for j in page_jobs if j.get("job_url") not in existing_urls]

            if not new_jobs:
                break  # No new jobs from scrolling

            jobs.extend(new_jobs)
            time.sleep(WELLFOUND_DELAY)

        except Exception as e:
            print(f"    Page {page_num} error: {e}")
            break

    return jobs[:max_results]


def _extract_jobs_from_dom(
    page: Page,
    keyword: str,
    location: str,
) -> list[dict]:
    """Extract job data from the Wellfound DOM."""
    job_data = page.evaluate("""() => {
        const jobs = [];
        const seen = new Set();

        // Wellfound groups jobs by company (startup cards)
        // Each startup card has company info + multiple job listings

        // Strategy 1: Look for job listing links within startup cards
        const jobLinks = document.querySelectorAll(
            'a[href*="/company/"][href*="/jobs/"],'
            + 'a[href*="/jobs/"][class*="job"],'
            + 'a[class*="listing"]'
        );

        for (const link of jobLinks) {
            const href = link.getAttribute('href') || '';
            if (seen.has(href)) continue;
            seen.add(href);

            const title = link.textContent?.trim() || '';
            if (!title || title.length < 3 || title.length > 200) continue;

            // Walk up to find the startup card container
            let card = link.closest('[class*="startup"]')
                    || link.closest('[class*="company"]')
                    || link.closest('[class*="card"]')
                    || link.parentElement?.parentElement?.parentElement;

            const cardText = card?.textContent || '';

            // Company name — usually in a prominent link/heading in the card
            let company = '';
            const companyLink = card?.querySelector('a[href*="/company/"]:not([href*="/jobs/"])');
            if (companyLink) {
                company = companyLink.textContent?.trim() || '';
            }

            // Compensation
            const compMatch = cardText.match(/(\\$[\\d,]+k?\\s*[-–]\\s*\\$[\\d,]+k?)/i)
                           || cardText.match(/(₹[\\d,.]+\\s*[-–]\\s*₹[\\d,.]+)/i);
            const salary = compMatch ? compMatch[0].trim() : '';

            // Location
            const locMatch = cardText.match(/(?:Remote|San Francisco|New York|Bangalore|Bengaluru|Mumbai|Delhi|London|Berlin|India|USA|US|UK)/gi);
            const loc = locMatch ? [...new Set(locMatch)].join(', ') : '';

            // Job type
            const typeMatch = cardText.match(/\\b(full.?time|part.?time|contract|intern)/i);
            const jobType = typeMatch ? typeMatch[0].trim() : '';

            const fullUrl = href.startsWith('http') ? href : 'https://wellfound.com' + href;

            jobs.push({
                title,
                company,
                location: loc,
                salary,
                job_type: jobType,
                job_url: fullUrl,
            });
        }

        // Strategy 2: Broader fallback — look for any job-related card content
        if (jobs.length === 0) {
            const allLinks = document.querySelectorAll('a[href*="/jobs/"]');
            for (const link of allLinks) {
                const href = link.getAttribute('href') || '';
                const title = link.textContent?.trim() || '';
                if (!title || title.length < 3 || seen.has(href)) continue;
                seen.add(href);

                const fullUrl = href.startsWith('http') ? href : 'https://wellfound.com' + href;
                jobs.push({
                    title,
                    company: '',
                    location: '',
                    salary: '',
                    job_type: '',
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
                # India-only filter: Wellfound is global, skip non-India roles
                job_loc = job.get("location", "")
                if is_non_india_location(job_loc):
                    continue
                job["search_keyword"] = keyword
                job["search_location"] = location
                job.setdefault("experience", "")
                job.setdefault("date_posted", "")
                job.setdefault("description", "")
                jobs.append(job)

    return jobs


