"""Find HR/Recruiter contacts at companies via LinkedIn browser automation."""

import json
import random
import time
import urllib.parse
from pathlib import Path

import httpx
import pandas as pd
from playwright.sync_api import sync_playwright, Page, BrowserContext

from config import (
    SESSION_DIR,
    HR_SEARCH_QUERY,
    MAX_HR_LOOKUPS_PER_SESSION,
    HR_DELAY_MIN,
    HR_DELAY_MAX,
)

COOKIES_FILE = SESSION_DIR / "linkedin_cookies.json"


def find_hr_contacts(
    companies: list[str],
    max_lookups: int = MAX_HR_LOOKUPS_PER_SESSION,
) -> pd.DataFrame:
    """
    Search LinkedIn for HR/recruiter contacts at each company.

    First run: opens a browser for manual LinkedIn login, saves cookies.
    Subsequent runs: reuses saved cookies.

    Args:
        companies: List of unique company names from job scraping.
        max_lookups: Max companies to search (to avoid LinkedIn blocks).

    Returns:
        DataFrame with columns: company, hr_name, hr_title, linkedin_url.
    """
    companies = list(dict.fromkeys(companies))[:max_lookups]  # dedupe + cap
    all_contacts = []

    print(f"\n[HR Finder] Searching HR contacts for {len(companies)} companies...")

    with sync_playwright() as pw:
        # Use a persistent Chrome profile so login survives across runs
        user_data_dir = str(SESSION_DIR / "chrome_profile")
        context = pw.chromium.launch_persistent_context(
            user_data_dir,
            headless=False,
            channel="chrome",
            viewport={"width": 1280, "height": 800},
            args=["--start-maximized"],
        )
        page = context.pages[0] if context.pages else context.new_page()

        if not ensure_logged_in(page, context, timeout=300):
            context.close()
            return pd.DataFrame()

        for i, company in enumerate(companies, 1):
            print(f"  [{i}/{len(companies)}] Looking up HR at: {company}")
            try:
                contacts = _search_company_hr(page, company)
                for c in contacts:
                    c["company"] = company
                all_contacts.extend(contacts)
                print(f"    Found {len(contacts)} HR contact(s)")
            except Exception as e:
                print(f"    Error: {e}")

            # Human-like delay
            delay = random.uniform(HR_DELAY_MIN, HR_DELAY_MAX)
            time.sleep(delay)

        context.close()

    if not all_contacts:
        return pd.DataFrame()

    df = pd.DataFrame(all_contacts)
    print(f"\n[HR Finder] Total HR contacts found: {len(df)}")
    return df


def _get_or_create_session(browser) -> BrowserContext:
    """Load saved cookies or create a fresh context."""
    context = browser.new_context(
        viewport={"width": 1280, "height": 800},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    )

    if COOKIES_FILE.exists():
        try:
            cookies = json.loads(COOKIES_FILE.read_text())
            context.add_cookies(cookies)
            print("  [Session] Loaded saved LinkedIn cookies")
        except Exception:
            print("  [Session] Failed to load cookies, starting fresh")

    return context


def _save_cookies(context: BrowserContext):
    """Save current session cookies to disk."""
    cookies = context.cookies()
    COOKIES_FILE.write_text(json.dumps(cookies, indent=2))


def _verify_login(page: Page) -> bool:
    """Check if we're logged into LinkedIn."""
    try:
        page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=15000)
        # If redirected to login, we're not authenticated
        return "feed" in page.url and "login" not in page.url
    except Exception:
        return False


def _wait_for_login(page: Page, timeout: int = 120) -> bool:
    """Poll the page URL until the user completes LinkedIn login.

    Checks every 3 seconds whether the browser has navigated away from
    the login/checkpoint pages.  Works in both terminal and subprocess
    (dashboard) mode — no stdin needed.
    """
    import math

    # Pages that mean "not yet logged in"
    _login_pages = ("/login", "/checkpoint", "/uas/", "/authwall")

    checks = math.ceil(timeout / 3)
    for i in range(checks):
        time.sleep(3)
        try:
            current_url = page.url.lower()

            # Log every URL change so we can debug
            if (i + 1) % 5 == 0:
                remaining = timeout - (i + 1) * 3
                print(f"  Still waiting for login... ({remaining}s remaining)  [url: {current_url[:80]}]")

            # If we're on a known login/challenge page, keep waiting
            if any(p in current_url for p in _login_pages):
                continue

            # If we've landed on any linkedin.com page that isn't login,
            # the user is in — feed, home, mynetwork, messaging, profile, etc.
            if "linkedin.com" in current_url:
                print(f"  Login detected! URL: {current_url[:80]}")
                return True

        except Exception:
            pass
    return False


def ensure_logged_in(page: Page, context: BrowserContext, timeout: int = 120) -> bool:
    """Verify LinkedIn login; prompt for manual login if needed.

    Args:
        page: Playwright page instance.
        context: Browser context (for saving cookies).
        timeout: Seconds to wait for manual login (default 120).

    Returns True if logged in, False if login timed out.
    Saves cookies on success.
    """
    if _verify_login(page):
        return True

    print("\n  *** A Chrome window will open — log into LinkedIn THERE ***")
    print("  (It's a separate window with no extensions/bookmarks)")
    page.goto("https://www.linkedin.com/login")
    # Make it obvious which window to use
    page.evaluate("document.title = '>>> LOG IN HERE — Job Hunter <<<'")
    print(f"  Waiting for login... ({timeout}s window)")
    if not _wait_for_login(page, timeout=timeout):
        print(f"  Login timed out after {timeout}s. Aborting.")
        return False
    _save_cookies(context)
    print("  Login successful! Cookies saved for future runs.\n")
    return True


def _search_company_hr(page: Page, company_name: str) -> list[dict]:
    """Search for HR contacts at a specific company on LinkedIn."""
    contacts = []

    # Step 1: Resolve company name to LinkedIn company ID
    company_id = _resolve_company_id(company_name)

    # Step 2: Build people search URL
    if company_id:
        search_url = (
            f"https://www.linkedin.com/search/results/people/"
            f"?keywords={urllib.parse.quote(HR_SEARCH_QUERY)}"
            f"&currentCompany=%5B%22{company_id}%22%5D"
        )
    else:
        # Fallback: search by company name in keywords
        query = f"{HR_SEARCH_QUERY} {company_name}"
        search_url = (
            f"https://www.linkedin.com/search/results/people/"
            f"?keywords={urllib.parse.quote(query)}"
        )

    page.goto(search_url, wait_until="domcontentloaded", timeout=20000)
    time.sleep(3)  # Let results render

    # Step 3: Extract people from search results (updated selectors — Mar 2026)
    cards = page.query_selector_all("div[data-view-name='people-search-result']")

    for card in cards[:3]:  # Top 3 contacts per company
        try:
            contact = _parse_person_result(card)
            if contact:
                contacts.append(contact)
        except Exception:
            continue

    return contacts


def _resolve_company_id(company_name: str) -> str | None:
    """Resolve company name to LinkedIn company ID via typeahead API."""
    try:
        url = (
            "https://www.linkedin.com/jobs-guest/api/typeaheadHits"
            f"?typeaheadType=COMPANY&query={urllib.parse.quote(company_name)}"
        )
        resp = httpx.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data and len(data) > 0:
                # Return the first match's ID
                return str(data[0].get("id", ""))
    except Exception:
        pass
    return None


def _parse_person_result(card) -> dict | None:
    """Parse a single person result card from LinkedIn search.

    Updated Mar 2026 — LinkedIn uses hashed CSS classes now.
    Structure under div[data-view-name='people-search-result']:
      a > div[role=listitem] > div > div
        ├─ P          → name
        ├─ DIV (2nd)  → headline / title
        └─ DIV (3rd)  → location
      a[href*='/in/'] → profile URL
    """
    # Profile link
    link_el = card.query_selector("a[href*='/in/']")
    linkedin_url = ""
    if link_el:
        href = link_el.get_attribute("href") or ""
        if "/in/" in href:
            linkedin_url = href.split("?")[0]
            if not linkedin_url.startswith("http"):
                linkedin_url = "https://www.linkedin.com" + linkedin_url

    # Extract name + title from <p> elements inside the card
    # p[0] = name, p[1] = title/headline, p[2] = location
    person_data = card.evaluate(
        """el => {
            const link = el.querySelector('a[href*="/in/"]');
            const ps = el.querySelectorAll('p');
            const texts = Array.from(ps).map(p => p.textContent.trim()).filter(t => t.length > 0);
            return {
                name: texts[0] || '',
                title: texts[1] || '',
                href: link ? link.href : '',
            };
        }"""
    )

    if person_data and person_data.get("href"):
        href = person_data["href"]
        if "/in/" in href:
            linkedin_url = href.split("?")[0]
            if not linkedin_url.startswith("http"):
                linkedin_url = "https://www.linkedin.com" + linkedin_url

    name = str(person_data.get("name", "")).strip() if person_data else ""
    title = str(person_data.get("title", "")).strip() if person_data else ""

    if not name or name.lower() == "linkedin member":
        return None

    return {
        "hr_name": name,
        "hr_title": title,
        "linkedin_url": linkedin_url,
    }
