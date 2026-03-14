"""Korn Ferry scraper — extracts executive search mandates from kornferry.com.

Korn Ferry is a global exec search / org consulting firm.  Their candidate
portal lives at jobs.candidate.kornferry.com and uses jQuery + AJAX to render
a table of open positions.  Initial load returns 25 jobs; searches filter
client-side + server-side via POST to /source/GetSearchJobData2.

We use Playwright because the AJAX endpoints require a valid session cookie
that can only be obtained by loading the page first.
"""

import re
import time

import pandas as pd
from playwright.sync_api import sync_playwright

from config import KORNFERRY_BASE_URL, KORNFERRY_DELAY
from scrapers.utils import CHROME_UA, is_india_location


def search_kornferry(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search Korn Ferry's candidate portal for executive roles.

    Args:
        keywords: Job title search terms.
        locations: Locations (used in search + relevance filtering).
        max_results: Max results to return.

    Returns:
        DataFrame with job listings from Korn Ferry.
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
                f"{KORNFERRY_BASE_URL}/jobs",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            time.sleep(4)  # Wait for initial AJAX load

            for keyword in keywords:
                if len(all_jobs) >= max_results:
                    break

                print(f"  [KornFerry] Searching '{keyword}'...")
                jobs = _search_and_extract(page, keyword, locations)
                all_jobs.extend(jobs)
                print(f"    Found {len(jobs)} results")
                time.sleep(KORNFERRY_DELAY)

        except Exception as e:
            print(f"    [KornFerry] Error: {e}")
        finally:
            context.close()
            browser.close()

    if not all_jobs:
        return pd.DataFrame()

    df = pd.DataFrame(all_jobs)
    df["platform"] = "kornferry"

    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"], keep="first")

    print(f"\n  [KornFerry] Total unique jobs: {len(df)}")
    return df


def _search_and_extract(
    page,
    keyword: str,
    locations: list[str],
) -> list[dict]:
    """Fill the search form and extract job rows from the results table."""
    jobs: list[dict] = []

    try:
        # Clear previous search
        page.evaluate(
            "() => {"
            "  const kw = document.querySelector('input[name=\"keyword\"]');"
            "  const loc = document.getElementById('location');"
            "  if (kw) kw.value = '';"
            "  if (loc) loc.value = '';"
            "}"
        )

        # Fill keyword — the input has name="keyword", no id
        kw_input = page.locator('input[name="keyword"]')
        if kw_input.count() > 0:
            kw_input.fill(keyword)

        # NOTE: Do NOT fill the location field.
        # Korn Ferry's portal is US/global focused; filling "India" yields 0
        # results.  We search by keyword only and keep all results — the user
        # benefits from seeing global exec mandates from a top-tier firm.

        # Click search — input[type="submit"] (no id)
        search_btn = page.locator('input[type="submit"]')
        if search_btn.count() > 0:
            search_btn.first.click()
            time.sleep(5)
        else:
            return jobs

        # Extract job rows from the results table
        rows = page.evaluate(
            "() => {"
            "  const links = document.querySelectorAll('.joblink');"
            "  return Array.from(links).map(a => {"
            "    const row = a.closest('tr');"
            "    const cells = row"
            "      ? Array.from(row.querySelectorAll('td')).map(td => td.textContent.trim())"
            "      : [];"
            "    const dateEl = row ? row.querySelector('.smallDate') : null;"
            "    const locEl = row ? row.querySelector('.smallLocation') : null;"
            "    return {"
            "      title: a.textContent.trim().split('\\n')[0].trim(),"
            "      href: a.href,"
            "      location: locEl ? locEl.textContent.trim()"
            "               : (cells.length > 1 ? cells[1] : ''),"
            "      date: dateEl ? dateEl.textContent.trim()"
            "            : (cells.length > 2 ? cells[2] : ''),"
            "    };"
            "  });"
            "}"
        )

        for row in rows:
            title = row.get("title", "").strip()
            # Clean multiline titles (KF embeds location in the same text)
            title = " ".join(title.split())
            if not title:
                continue

            job_url = row.get("href", "")
            loc_text = row.get("location", "").strip()
            date_posted = row.get("date", "").strip()

            # Remove location suffix from title if it leaked in
            if loc_text and title.endswith(loc_text):
                title = title[: -len(loc_text)].strip()

            # Relevance filter: keyword match in title
            kw_lower = keyword.lower()
            kw_words = kw_lower.split()
            title_lower = title.lower()
            if not any(w in title_lower for w in kw_words):
                continue

            # India-only filter: KF is a global portal — keep only India roles
            if loc_text and not is_india_location(loc_text):
                continue
            # Also skip if location is empty (KF is global; unknown = likely non-India)
            if not loc_text:
                continue

            jobs.append({
                "title": title,
                "company": "Korn Ferry",  # Exec search firm
                "location": loc_text,
                "salary": "",
                "experience": "",
                "job_url": job_url,
                "description": "",
                "date_posted": date_posted,
                "search_keyword": keyword,
                "search_location": ", ".join(locations),
            })

    except Exception as e:
        print(f"    [KornFerry] Extraction error: {e}")

    return jobs
