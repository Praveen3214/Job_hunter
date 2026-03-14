"""Find people on LinkedIn who are actively hiring.

Three search modes:
  1. **People search** — finds profiles with "#Hiring" in their headline
     (e.g., "#Hiring Marketing Director Bangalore").
  2. **Post search** — scans LinkedIn posts for hiring hashtags
     (#hiring, #wearehiring, #jobalert, #vacancy, #openposition, #openrole)
     and extracts the poster's info + what role they're hiring for.
  3. **Firm post search** — scans posts specifically from employees of
     named recruiting firms (Crescendo Global, Michael Page, etc.)
     using LinkedIn's company filter (f_C parameter).

After collecting results from any mode, enriches each profile with
headline, about section, company details, and contact info.

Reuses the LinkedIn session management from linkedin_people.py — same
saved cookies, login flow, and Playwright patterns.
"""

import random
import re
import time
import urllib.parse

import pandas as pd
from playwright.sync_api import sync_playwright, Page, BrowserContext

from config import SESSION_DIR
from hr_finder.linkedin_people import (
    _get_or_create_session,
    _save_cookies,
    ensure_logged_in,
)

# ---------- constants ----------
HIRING_DELAY_MIN = 3.5
HIRING_DELAY_MAX = 6.0
PROFILE_DELAY_MIN = 2.5
PROFILE_DELAY_MAX = 4.5
COMPANY_DELAY_MIN = 2.0
COMPANY_DELAY_MAX = 3.5
POST_DELAY_MIN = 3.0
POST_DELAY_MAX = 5.5
CONSULTANT_DELAY_MIN = 3.5
CONSULTANT_DELAY_MAX = 6.5
FIRM_POST_DELAY_MIN = 3.5
FIRM_POST_DELAY_MAX = 6.0

MAX_PAGES_PER_SEARCH = 2
MAX_RESULTS_PER_KEYWORD = 15
MAX_PROFILE_ENRICHMENTS = 40   # cap to avoid LinkedIn blocks
MAX_POSTS_PER_SEARCH = 10
MAX_FIRM_POSTS_PER_SEARCH = 10
MAX_CONSULTANT_ENRICHMENTS = 50

# Hashtags that signal active hiring intent
HIRING_HASHTAGS = [
    "#hiring", "#wearehiring", "#jobalert", "#jobopening",
    "#vacancy", "#openposition", "#openrole", "#nowhiring",
    "#hiringnow", "#jobopportunity", "#joinourteam",
    "#urgenthiring", "#immediatehiring", "#lookingfor",
]

# Role classification: maps category → keywords found in title/post text
ROLE_CATEGORIES = {
    "Marketing": [
        "marketing", "brand", "growth", "digital marketing",
        "content", "seo", "social media", "performance marketing",
        "demand generation", "product marketing", "cmo",
    ],
    "Sales": [
        "sales", "business development", "account executive",
        "account manager", "revenue", "partnerships",
    ],
    "Engineering": [
        "engineer", "developer", "software", "frontend", "backend",
        "full stack", "fullstack", "devops", "sre", "cto", "tech lead",
        "data engineer", "ml engineer", "platform",
    ],
    "Product": [
        "product manager", "product owner", "product lead",
        "product director", "head of product", "vp product",
    ],
    "Design": [
        "designer", "ux", "ui", "creative director",
        "design lead", "visual design", "graphic design",
    ],
    "Data": [
        "data scientist", "data analyst", "analytics",
        "business analyst", "business intelligence", "bi ",
    ],
    "HR": [
        "hr ", "human resources", "talent acquisition",
        "recruiter", "people operations", "people partner",
    ],
    "Finance": [
        "finance", "accounting", "cfo", "controller",
        "financial analyst", "fp&a",
    ],
    "Operations": [
        "operations", "supply chain", "logistics", "coo",
        "project manager", "program manager",
    ],
}

# Recruiter/consultant title keywords — used to build search queries
CONSULTANT_TITLE_KEYWORDS = [
    "Recruitment Consultant",
    "Staffing Manager",
    "Headhunter",
    "Executive Search",
    "Placement Consultant",
    "Recruitment Manager",
    "Talent Acquisition Consultant",
    "Recruitment Specialist",
    "Staffing Consultant",
    "Search Consultant",
    "Recruitment Partner",
    "Recruiting",
]

# Signals that a person works at a recruitment/staffing firm (vs in-house)
# Note: uses multi-word phrases to reduce false positives
# (e.g. "talent solutions" instead of just "talent")
AGENCY_COMPANY_SIGNALS = [
    "staffing", "recruitment", "recruiting", "manpower",
    "placement", "hr solutions", "people solutions",
    "workforce solutions", "headhunt",
    "human capital", "personnel services", "job placement",
    "executive search", "talent solutions", "talent consulting",
    "talent corner", "talent partner",
    # Known agency brands (India + global)
    "randstad", "adecco", "robert half", "michael page",
    "hays ", "teamlease", "abc consultants", "quess",
    "kelly services", "ciel hr", "hunt partners", "korn ferry",
    "egon zehnder", "spencer stuart", "heidrick", "boyden",
    "genius consultants", "mancer consulting",
    "xpheno", "careernet", "taggd",
    "naukri hiring", "foundit hiring",
    "persolkelly", "gi group", "manpowergroup",
    "hudson", "page group", "pagegroup",
    "antal", "collabera", "nri consulting",
]

# Consultant type classification
CONSULTANT_TYPES = {
    "Executive Search": [
        "executive search", "c-suite", "c-level", "board",
        "korn ferry", "egon zehnder", "spencer stuart",
        "heidrick", "boyden", "hunt partners",
    ],
    "Agency Recruiter": [
        "recruitment consultant", "recruiter", "recruiting",
        "talent acquisition", "staffing", "placement",
        "headhunter", "headhunt", "search consultant",
    ],
    "Staffing Firm": [
        "staffing", "manpower", "workforce", "contract staffing",
        "temp staffing", "temporary", "outsourcing",
        "randstad", "adecco", "teamlease", "quess", "kelly",
    ],
    "HR Consultancy": [
        "hr consulting", "hr solutions", "hr consultancy",
        "people solutions", "human capital", "hr advisory",
    ],
    "Recruitment Partner": [
        "recruitment partner", "talent partner", "rpo",
        "recruitment process outsourcing", "embedded recruiter",
    ],
}


# ================================================================
#  MAIN ENTRY POINT
# ================================================================

def find_hiring_people(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search LinkedIn for people who are actively hiring, then enrich
    each result with full profile + company details.

    Returns DataFrame with columns:
        name, title, headline, about, location,
        linkedin_url, email, phone,
        company, company_linkedin_url, company_website,
        company_industry, company_size, search_keyword.
    """
    all_results = []

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

        # ---- Login ----
        if not ensure_logged_in(page, context, timeout=300):
            context.close()
            return pd.DataFrame()

        # ---- Phase 1: Search ----
        print("\n  -- Phase 1: Searching for #Hiring profiles --")
        search_count = 0
        for keyword in keywords:
            if len(all_results) >= max_results:
                break
            search_locations = locations if locations else [""]
            for location in search_locations:
                if len(all_results) >= max_results:
                    break
                query = f"#Hiring {keyword}"
                if location and location.lower() != "remote":
                    query += f" {location}"
                print(f"  [Search] '{query}'...")
                results = _search_and_collect(page, query, keyword, max_results - len(all_results))
                all_results.extend(results)
                print(f"    -> {len(results)} results")
                search_count += 1
                if search_count < len(keywords) * len(search_locations):
                    time.sleep(random.uniform(HIRING_DELAY_MIN, HIRING_DELAY_MAX))

        if not all_results:
            print("\n  [Hiring] No hiring signals found.")
            context.close()
            return pd.DataFrame()

        # Deduplicate before enrichment (save time)
        seen_urls = set()
        unique_results = []
        for r in all_results:
            url = r.get("linkedin_url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_results.append(r)
            elif not url:
                unique_results.append(r)
        all_results = unique_results[:max_results]
        print(f"\n  Unique profiles to enrich: {len(all_results)}")

        # ---- Phase 2: Profile enrichment ----
        print("\n  -- Phase 2: Enriching profiles --")
        enrich_count = min(len(all_results), MAX_PROFILE_ENRICHMENTS)
        if len(all_results) > MAX_PROFILE_ENRICHMENTS:
            print(f"  [Note] Capping profile enrichment at {MAX_PROFILE_ENRICHMENTS} (got {len(all_results)})")
        enrich_failures = 0
        for i, person in enumerate(all_results[:enrich_count], 1):
            print(f"  [{i}/{enrich_count}] {person.get('name', '?')}...", end="")
            _enrich_from_profile(page, person)
            status_parts = []
            if person.get("email"):
                status_parts.append(f"email={person['email']}")
            if person.get("company_linkedin_url"):
                status_parts.append("co-page OK")
            if person.get("about"):
                status_parts.append("about OK")
            if not status_parts:
                enrich_failures += 1
            print(f" {', '.join(status_parts) or 'basic only'}")
            time.sleep(random.uniform(PROFILE_DELAY_MIN, PROFILE_DELAY_MAX))
        if enrich_failures > enrich_count * 0.5 and enrich_count > 5:
            print(f"\n  WARNING: {enrich_failures}/{enrich_count} profiles returned no enrichment data.")
            print(f"    LinkedIn may be blocking your session. Try again later or re-login.")

        # ---- Phase 3: Company enrichment ----
        print("\n  -- Phase 3: Enriching company details --")
        _enrich_companies(page, all_results)

        context.close()

    df = pd.DataFrame(all_results)

    # ---- Role classification ----
    if not df.empty:
        df["hiring_for_role"] = df.apply(
            lambda row: _classify_hiring_role(
                str(row.get("title", "")) + " " +
                str(row.get("headline", "")) + " " +
                str(row.get("about", ""))
            ),
            axis=1,
        )
        df["signal_source"] = "people_search"

    print(f"\n  [Hiring] Total enriched profiles: {len(df)}")
    return df


# ================================================================
#  POST SEARCH — find hiring activity in LinkedIn posts
# ================================================================

def find_hiring_posts(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search LinkedIn posts/content for hiring hashtags and extract
    the poster's info + what role they're hiring for.

    Searches for combinations of hiring hashtags + user keywords, e.g.:
      "#hiring Marketing Director Bangalore"
      "#wearehiring CMO India"

    Returns DataFrame with columns:
        name, title, company, location, linkedin_url,
        post_text, post_url, hiring_for_role, search_keyword,
        signal_source.
    """
    all_results = []
    # Use a subset of the most common hashtags to avoid too many searches
    search_hashtags = ["#hiring", "#wearehiring", "#jobalert", "#openposition"]

    with sync_playwright() as pw:
        user_data_dir = str(SESSION_DIR / "chrome_profile")
        context = pw.chromium.launch_persistent_context(
            user_data_dir,
            headless=False,
            channel="chrome",
            viewport={"width": 1280, "height": 800},
            args=["--start-maximized"],
        )
        page = context.pages[0] if context.pages else context.new_page()

        # ---- Login ----
        if not ensure_logged_in(page, context, timeout=300):
            context.close()
            return pd.DataFrame()

        print("\n  == Scanning LinkedIn Posts for Hiring Signals ==")
        search_count = 0
        total_searches = len(keywords) * len(search_hashtags)

        for keyword in keywords:
            if len(all_results) >= max_results:
                break
            for hashtag in search_hashtags:
                if len(all_results) >= max_results:
                    break

                query = f"{hashtag} {keyword}"
                # Add first location if available (keep queries focused)
                if locations and locations[0].lower() != "remote":
                    query += f" {locations[0]}"

                print(f"  [Posts] '{query}'...")
                posts = _search_posts(page, query, keyword)
                all_results.extend(posts)
                print(f"    -> {len(posts)} posts found")

                search_count += 1
                if search_count < total_searches:
                    time.sleep(random.uniform(POST_DELAY_MIN, POST_DELAY_MAX))

        context.close()

    if not all_results:
        print("\n  [Posts] No hiring posts found.")
        return pd.DataFrame()

    # Deduplicate by poster URL + post URL
    seen = set()
    unique = []
    for r in all_results:
        key = (r.get("linkedin_url", ""), r.get("post_url", ""))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    all_results = unique[:max_results]

    df = pd.DataFrame(all_results)

    # ---- Role classification from post text ----
    if not df.empty:
        df["hiring_for_role"] = df.apply(
            lambda row: _classify_hiring_role(
                str(row.get("title", "")) + " " +
                str(row.get("post_text", ""))
            ),
            axis=1,
        )
        df["signal_source"] = "post_search"

    print(f"\n  [Posts] Total hiring posts: {len(df)}")
    return df


# ================================================================
#  FIRM POST SEARCH — posts from employees of named recruiting firms
# ================================================================

def find_firm_posts(
    firms: list[dict],
    keywords: list[str],
    locations: list[str] | None = None,
    max_results: int = 50,
) -> pd.DataFrame:
    """
    Search LinkedIn posts made by employees of specific recruiting firms,
    filtered by hiring/marketing keywords AND location context.

    Each firm dict must have:  {"name": "...", "slug": "company-slug"}

    Flow:
      1. Resolve each firm's LinkedIn slug -> numeric company ID
      2. Build location-aware queries (e.g. "hiring Marketing Director India")
      3. For each firm, search posts with company filter (f_C=<id>) + queries
      4. Keep only posts that mention hiring keywords
      5. Extract poster name, title, LinkedIn URL, post text, post URL

    Returns DataFrame with columns:
        name, title, company, location, linkedin_url,
        post_text, post_url, hiring_for_role, recruiter_firm,
        search_keyword, signal_source.
    """
    all_results = []

    with sync_playwright() as pw:
        user_data_dir = str(SESSION_DIR / "chrome_profile")
        context = pw.chromium.launch_persistent_context(
            user_data_dir,
            headless=False,
            channel="chrome",
            viewport={"width": 1280, "height": 800},
            args=["--start-maximized"],
        )
        page = context.pages[0] if context.pages else context.new_page()

        # ---- Login ----
        if not ensure_logged_in(page, context, timeout=300):
            context.close()
            return pd.DataFrame()

        print("\n  == Searching Posts from Recruiting Firms ==")

        # ---- Step 1: Resolve firm slugs to numeric IDs ----
        firm_ids = {}
        for firm in firms:
            slug = firm.get("slug", "")
            name = firm.get("name", slug)
            if not slug:
                continue
            print(f"  [Resolve] {name} ({slug})...", end="")
            numeric_id = _resolve_company_id(page, slug)
            if numeric_id:
                firm_ids[name] = numeric_id
                print(f" ID={numeric_id}")
            else:
                print(" skipped (could not resolve)")
            time.sleep(random.uniform(1.5, 2.5))

        if not firm_ids:
            print("\n  [FirmPosts] No firm IDs resolved. Check slugs in config.")
            context.close()
            return pd.DataFrame()

        print(f"\n  Resolved {len(firm_ids)}/{len(firms)} firms")

        # ---- Step 2: Build location-aware search queries ----
        search_count = 0
        # Use a subset of keywords — top 4 to keep query count manageable
        search_keywords = keywords[:4] if len(keywords) > 4 else keywords

        # Build location suffixes for queries.
        # Instead of "hiring Marketing Director" (global noise),
        # use "hiring Marketing Director India" or city-specific queries.
        # Pick up to 3 representative location terms to keep query count sane.
        loc_suffixes = [""]  # fallback: no location (original behaviour)
        if locations:
            # Deduplicate and pick smart representatives
            # Prefer country-level ("India") + top 2 cities
            loc_lower_set = {loc.lower() for loc in locations}
            picked = []
            # Country-level first
            for country in ("India", "Remote"):
                if country.lower() in loc_lower_set:
                    picked.append(country)
            # Then top cities (by typical job volume)
            city_priority = [
                "Mumbai", "Bangalore", "Delhi", "Gurugram", "Pune",
                "Hyderabad", "Chennai", "Kolkata", "Noida",
            ]
            for city in city_priority:
                if len(picked) >= 3:
                    break
                if city.lower() in loc_lower_set:
                    picked.append(city)
            # If nothing matched, use first 2 raw locations
            if not picked:
                picked = locations[:2]
            loc_suffixes = picked

        for firm_name, company_id in firm_ids.items():
            if len(all_results) >= max_results:
                break

            for keyword in search_keywords:
                if len(all_results) >= max_results:
                    break

                for loc_suffix in loc_suffixes:
                    if len(all_results) >= max_results:
                        break

                    query = f"hiring {keyword}"
                    if loc_suffix:
                        query = f"hiring {keyword} {loc_suffix}"

                    print(f"  [FirmPosts] '{firm_name}' + '{query}'...")
                    posts = _search_firm_posts(
                        page, query, company_id, firm_name, keyword
                    )
                    all_results.extend(posts)
                    print(f"    -> {len(posts)} posts")

                    search_count += 1
                    time.sleep(random.uniform(FIRM_POST_DELAY_MIN, FIRM_POST_DELAY_MAX))

        context.close()

    if not all_results:
        print("\n  [FirmPosts] No recruiting firm posts found.")
        return pd.DataFrame()

    # Deduplicate by poster URL + post URL
    seen = set()
    unique = []
    for r in all_results:
        key = (r.get("linkedin_url", ""), r.get("post_url", ""))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    all_results = unique[:max_results]

    df = pd.DataFrame(all_results)

    if not df.empty:
        df["hiring_for_role"] = df.apply(
            lambda row: _classify_hiring_role(
                str(row.get("title", "")) + " " +
                str(row.get("post_text", ""))
            ),
            axis=1,
        )
        df["signal_source"] = "firm_post_search"

    print(f"\n  [FirmPosts] Total firm posts: {len(df)}")
    if not df.empty and "recruiter_firm" in df.columns:
        firm_counts = df["recruiter_firm"].value_counts()
        print(f"  [FirmPosts] By firm: {', '.join(f'{f}({c})' for f, c in firm_counts.items())}")

    return df


def _resolve_company_id(page: Page, slug: str) -> str | None:
    """Resolve a LinkedIn company slug to its numeric company ID.

    Visits the company page and extracts the ID from the page source
    or network response (present in urn:li:fsd_company:XXXXX patterns).
    """
    company_url = f"https://www.linkedin.com/company/{slug}/"
    try:
        page.goto(company_url, wait_until="domcontentloaded", timeout=15000)
        time.sleep(2)

        # Method 1: Look for company ID in page HTML / data attributes
        company_id = page.evaluate(
            """() => {
                // Method A: meta tag or data attribute
                const meta = document.querySelector('meta[content*="company:"]');
                if (meta) {
                    const m = meta.content.match(/company:(\\d+)/);
                    if (m) return m[1];
                }

                // Method B: script tags with fsd_company URN
                const scripts = document.querySelectorAll('script');
                for (const s of scripts) {
                    const text = s.textContent || '';
                    const m = text.match(/fsd_company:(\\d+)/);
                    if (m) return m[1];
                }

                // Method C: look in any data-entity-urn attributes
                const urnEls = document.querySelectorAll('[data-entity-urn*="company"]');
                for (const el of urnEls) {
                    const urn = el.getAttribute('data-entity-urn') || '';
                    const m = urn.match(/(\\d+)/);
                    if (m) return m[1];
                }

                // Method D: URL might have changed to include ID
                const url = window.location.href;
                const m2 = url.match(/company\\/(\\d+)/);
                if (m2) return m2[1];

                // Method E: look in the page body for company ID patterns
                const body = document.body.innerHTML;
                const m3 = body.match(/\\"companyId\\":(\\d+)/);
                if (m3) return m3[1];

                // Method F: look for normalized company URN
                const m4 = body.match(/urn:li:fs_normalized_company:(\\d+)/);
                if (m4) return m4[1];

                // Method G: look in code tags (LinkedIn sometimes wraps data)
                const codes = document.querySelectorAll('code');
                for (const c of codes) {
                    const text = c.textContent || '';
                    const m5 = text.match(/fsd_company:(\\d+)/);
                    if (m5) return m5[1];
                    const m6 = text.match(/fs_normalized_company:(\\d+)/);
                    if (m6) return m6[1];
                    const m7 = text.match(/"objectUrn":"urn:li:company:(\\d+)"/);
                    if (m7) return m7[1];
                }

                // Method H: og:url meta tag sometimes contains company ID
                const ogUrl = document.querySelector('meta[property="og:url"]');
                if (ogUrl) {
                    const m8 = ogUrl.content.match(/company\\/(\\d+)/);
                    if (m8) return m8[1];
                }

                return null;
            }"""
        )
        return company_id

    except Exception as e:
        print(f" [error: {e}]", end="")
        return None


def _search_firm_posts(
    page: Page, query: str, company_id: str, firm_name: str, keyword: str,
) -> list[dict]:
    """Search LinkedIn posts filtered by company ID.

    Uses the f_C parameter to restrict results to posts from employees
    of the specified company.
    """
    results = []
    encoded = urllib.parse.quote(query)
    url = (
        f"https://www.linkedin.com/search/results/content/"
        f"?keywords={encoded}"
        f"&f_C={company_id}"
        f"&origin=FACETED_SEARCH"
        f"&sortBy=%22date_posted%22"
    )

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=20000)
        time.sleep(5)
        # Scroll to load posts
        for step in range(4):
            page.evaluate(f"window.scrollTo(0, {(step + 1) * 800})")
            time.sleep(1.5)
        page.evaluate("window.scrollTo(0, 0)")
        time.sleep(1)
    except Exception as e:
        print(f"    Firm post search error: {e}")
        return results

    # Reuse the same DOM parsing as _search_posts (same LinkedIn DOM)
    post_data_list = page.evaluate(
        """() => {
            const posts = [];
            const cards = document.querySelectorAll('div[data-view-name="feed-full-update"]');

            for (const card of cards) {
                try {
                    let authorName = '';
                    let authorUrl = '';

                    const figure = card.querySelector('a[data-view-name="feed-actor-image"] figure[aria-label]');
                    if (figure) {
                        const label = figure.getAttribute('aria-label') || '';
                        const match = label.match(/^View\\s+(.+?)(?:'s|\\u2019s)\\s+profile/i);
                        if (match) authorName = match[1].trim();
                    }

                    const actorLink = card.querySelector('a[data-view-name="feed-actor-image"]');
                    if (actorLink) {
                        authorUrl = actorLink.href.split('?')[0];
                    }

                    if (!authorName) {
                        const allLinks = card.querySelectorAll('a[href*="/in/"]');
                        for (const link of allLinks) {
                            if (link.getAttribute('data-view-name') === 'feed-actor-image') continue;
                            const txt = link.textContent.trim();
                            if (txt.length > 2 && txt.length < 100) {
                                authorName = txt.replace(/Premium Profile/gi, '')
                                                .replace(/Verified Profile/gi, '')
                                                .replace(/\\s*(1st|2nd|3rd\\+?).*$/i, '')
                                                .replace(/,\\s*Hiring/gi, '')
                                                .trim();
                                if (!authorUrl) authorUrl = link.href.split('?')[0];
                                break;
                            }
                        }
                    }

                    let authorTitle = '';
                    const infoLink = card.querySelector('a[href*="/in/"]:not([data-view-name="feed-actor-image"])');
                    if (infoLink) {
                        const divs = infoLink.querySelectorAll(':scope > div > div');
                        if (divs.length > 1) {
                            authorTitle = divs[1].textContent.trim();
                        }
                    }

                    const postEl = card.querySelector('p[data-view-name="feed-commentary"]');
                    const postText = postEl ? postEl.textContent.trim().substring(0, 500) : '';

                    let postUrl = '';
                    const jobLink = card.querySelector('a[data-view-name="feed-job-card-entity"]');
                    if (jobLink) {
                        postUrl = jobLink.href.split('?')[0];
                    }
                    if (!postUrl) {
                        const reactionLink = card.querySelector('a[data-view-name="feed-reaction-count"]');
                        if (reactionLink) {
                            postUrl = reactionLink.href.split('?')[0];
                        }
                    }

                    if (authorName && postText) {
                        posts.push({
                            name: authorName,
                            title: authorTitle,
                            linkedin_url: authorUrl,
                            post_text: postText,
                            post_url: postUrl,
                        });
                    }
                } catch(e) {}
            }
            return posts;
        }"""
    )

    if not post_data_list:
        print("    [Note] No posts from this firm")

    for post in (post_data_list or [])[:MAX_FIRM_POSTS_PER_SEARCH]:
        name = str(post.get("name", "")).strip()
        if not name or name.lower() == "linkedin member":
            continue

        linkedin_url = str(post.get("linkedin_url", ""))
        if linkedin_url and not linkedin_url.startswith("http"):
            linkedin_url = "https://www.linkedin.com" + linkedin_url

        company = _extract_company_from_title(str(post.get("title", "")))

        results.append({
            "name": name,
            "title": str(post.get("title", "")),
            "company": company or firm_name,
            "recruiter_firm": firm_name,
            "location": "",
            "linkedin_url": linkedin_url,
            "post_text": str(post.get("post_text", ""))[:500],
            "post_url": str(post.get("post_url", "")),
            "search_keyword": keyword,
            "headline": "",
            "about": "",
            "email": "",
            "phone": "",
            "company_linkedin_url": "",
            "company_website": "",
            "company_industry": "",
            "company_size": "",
        })

    return results


def _search_posts(page: Page, query: str, keyword: str) -> list[dict]:
    """Search LinkedIn content/posts and collect hiring signal posts.

    Updated Mar 2026 -- LinkedIn content search uses:
      div[data-view-name="feed-full-update"] for each post card.
    Inside each card:
      a[data-view-name="feed-actor-image"]  -> author profile link
      figure[aria-label]                    -> author name in aria-label
      a[href*="/in/"] (second one)          -> author link with DIV children for name/title
      p[data-view-name="feed-commentary"]   -> post text
    """
    results = []
    encoded = urllib.parse.quote(query)
    url = (
        f"https://www.linkedin.com/search/results/content/"
        f"?keywords={encoded}"
        f"&origin=GLOBAL_SEARCH_HEADER"
        f"&sortBy=%22date_posted%22"  # most recent first
    )

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=20000)
        time.sleep(5)
        # Scroll to trigger lazy loading of post cards
        for step in range(4):
            page.evaluate(f"window.scrollTo(0, {(step + 1) * 800})")
            time.sleep(1.5)
        page.evaluate("window.scrollTo(0, 0)")
        time.sleep(1)
    except Exception as e:
        print(f"    Post search error: {e}")
        return results

    # Extract from feed-full-update cards (Mar 2026 LinkedIn DOM)
    post_data_list = page.evaluate(
        """() => {
            const posts = [];
            const cards = document.querySelectorAll('div[data-view-name="feed-full-update"]');

            for (const card of cards) {
                try {
                    // Author name: from figure aria-label or second a[href*="/in/"]
                    let authorName = '';
                    let authorUrl = '';

                    // Method 1: figure aria-label (e.g. "View Sri Harsha BN's profile")
                    const figure = card.querySelector('a[data-view-name="feed-actor-image"] figure[aria-label]');
                    if (figure) {
                        const label = figure.getAttribute('aria-label') || '';
                        // Parse "View Sri Harsha BN's profile, ..." -> "Sri Harsha BN"
                        const match = label.match(/^View\\s+(.+?)(?:'s|\\u2019s)\\s+profile/i);
                        if (match) authorName = match[1].trim();
                    }

                    // Author URL: from the actor image link
                    const actorLink = card.querySelector('a[data-view-name="feed-actor-image"]');
                    if (actorLink) {
                        authorUrl = actorLink.href.split('?')[0];
                    }

                    // Method 2: if figure didn't give us the name, try the second a[href*="/in/"]
                    if (!authorName) {
                        const allLinks = card.querySelectorAll('a[href*="/in/"]');
                        for (const link of allLinks) {
                            if (link.getAttribute('data-view-name') === 'feed-actor-image') continue;
                            const txt = link.textContent.trim();
                            if (txt.length > 2 && txt.length < 100) {
                                // Clean up badges and connection degree
                                authorName = txt.replace(/Premium Profile/gi, '')
                                                .replace(/Verified Profile/gi, '')
                                                .replace(/\\s*(1st|2nd|3rd\\+?).*$/i, '')
                                                .replace(/,\\s*Hiring/gi, '')
                                                .trim();
                                if (!authorUrl) authorUrl = link.href.split('?')[0];
                                break;
                            }
                        }
                    }

                    // Author title: from the second a's DIV children
                    let authorTitle = '';
                    const infoLink = card.querySelector('a[href*="/in/"]:not([data-view-name="feed-actor-image"])');
                    if (infoLink) {
                        const divs = infoLink.querySelectorAll(':scope > div > div');
                        // div[0] = name, div[1] = title/headline, div[2] = timestamp
                        if (divs.length > 1) {
                            authorTitle = divs[1].textContent.trim();
                        }
                    }

                    // Post text: p[data-view-name="feed-commentary"]
                    const postEl = card.querySelector('p[data-view-name="feed-commentary"]');
                    const postText = postEl ? postEl.textContent.trim().substring(0, 500) : '';

                    // Post URL: look for job card link or the reaction count link
                    let postUrl = '';
                    const jobLink = card.querySelector('a[data-view-name="feed-job-card-entity"]');
                    if (jobLink) {
                        postUrl = jobLink.href.split('?')[0];
                    }
                    if (!postUrl) {
                        const reactionLink = card.querySelector('a[data-view-name="feed-reaction-count"]');
                        if (reactionLink) {
                            // This links to the post's activity page
                            postUrl = reactionLink.href.split('?')[0];
                        }
                    }

                    if (authorName && postText) {
                        posts.push({
                            name: authorName,
                            title: authorTitle,
                            linkedin_url: authorUrl,
                            post_text: postText,
                            post_url: postUrl,
                        });
                    }
                } catch(e) {
                    // skip this card
                }
            }
            return posts;
        }"""
    )

    if not post_data_list:
        print("    [Note] No feed-full-update cards found on this page")

    for post in (post_data_list or [])[:MAX_POSTS_PER_SEARCH]:
        name = str(post.get("name", "")).strip()
        if not name or name.lower() == "linkedin member":
            continue

        linkedin_url = str(post.get("linkedin_url", ""))
        if linkedin_url and not linkedin_url.startswith("http"):
            linkedin_url = "https://www.linkedin.com" + linkedin_url

        company = _extract_company_from_title(str(post.get("title", "")))

        results.append({
            "name": name,
            "title": str(post.get("title", "")),
            "company": company,
            "location": "",  # posts don't always show location
            "linkedin_url": linkedin_url,
            "post_text": str(post.get("post_text", ""))[:500],
            "post_url": str(post.get("post_url", "")),
            "search_keyword": keyword,
            "headline": "",
            "about": "",
            "email": "",
            "phone": "",
            "company_linkedin_url": "",
            "company_website": "",
            "company_industry": "",
            "company_size": "",
        })

    return results


# ================================================================
#  PHASE 1 — SEARCH
# ================================================================

def _build_search_url(query: str) -> str:
    """Build a LinkedIn People search URL for a #Hiring query."""
    encoded = urllib.parse.quote(query)
    return (
        f"https://www.linkedin.com/search/results/people/"
        f"?keywords={encoded}"
        f"&origin=GLOBAL_SEARCH_HEADER"
    )


def _search_and_collect(
    page: Page, query: str, keyword: str, max_results: int,
) -> list[dict]:
    """Navigate to LinkedIn search and collect results across pages."""
    results = []
    url = _build_search_url(query)

    for page_num in range(MAX_PAGES_PER_SEARCH):
        if len(results) >= min(max_results, MAX_RESULTS_PER_KEYWORD):
            break

        page_url = url if page_num == 0 else f"{url}&page={page_num + 1}"
        try:
            page.goto(page_url, wait_until="domcontentloaded", timeout=20000)
            time.sleep(2.5)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
            time.sleep(1)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1)
        except Exception as e:
            print(f"    Page {page_num + 1} error: {e}")
            break

        # Check for "no results" — use page text as fallback since CSS classes are hashed
        try:
            body_text = page.inner_text("body")
            if "No results found" in body_text or "no results" in body_text.lower()[:500]:
                break
        except Exception:
            pass

        # Updated Mar 2026: LinkedIn uses data-view-name attribute (CSS classes are hashed)
        cards = page.query_selector_all("div[data-view-name='people-search-result']")
        if not cards:
            break

        for card in cards:
            if len(results) >= min(max_results, MAX_RESULTS_PER_KEYWORD):
                break
            try:
                person = _parse_search_card(card, keyword)
                if person:
                    results.append(person)
            except Exception:
                continue

        if page_num < MAX_PAGES_PER_SEARCH - 1:
            next_btn = page.query_selector(
                "button[aria-label='Next'], "
                "button[aria-label='Forward']"
            )
            if not next_btn or next_btn.get_attribute("disabled"):
                break
            try:
                next_btn.click()
                time.sleep(2.5)
            except Exception:
                break

    return results


def _parse_search_card(card, keyword: str) -> dict | None:
    """Parse a single LinkedIn search result card.

    Updated Mar 2026 — LinkedIn uses hashed CSS classes now.
    Structure under div[data-view-name='people-search-result']:
      a > div[role=listitem] > div > div
        - P          -> name
        - DIV > P    -> headline / title
        - DIV > P    -> location
      a[href*='/in/'] -> profile URL
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

    # Extract name + title + location from <p> elements inside the card
    person_data = card.evaluate(
        """el => {
            const link = el.querySelector('a[href*="/in/"]');
            const ps = el.querySelectorAll('p');
            const texts = Array.from(ps).map(p => p.textContent.trim()).filter(t => t.length > 0);
            return {
                name: texts[0] || '',
                title: texts[1] || '',
                location: texts[2] || '',
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
    location = str(person_data.get("location", "")).strip() if person_data else ""

    if not name or name.lower() == "linkedin member":
        return None

    # The headline snippet (hiring signal) might be in the title or a separate element
    # LinkedIn search results sometimes show a summary snippet below the card
    headline = ""
    try:
        # Look for any element that contains the hiring signal text
        all_text = card.inner_text() or ""
        # Extract everything after the location as potential headline snippet
        if "#" in all_text.lower() or "hiring" in all_text.lower():
            headline = all_text.strip()
    except Exception:
        pass

    company = _extract_company_from_title(title)

    return {
        "name": name,
        "title": title,
        "headline": headline,
        "about": "",
        "location": location,
        "linkedin_url": linkedin_url,
        "email": "",
        "phone": "",
        "company": company,
        "company_linkedin_url": "",
        "company_website": "",
        "company_industry": "",
        "company_size": "",
        "search_keyword": keyword,
    }


# ================================================================
#  PHASE 2 — PROFILE ENRICHMENT
# ================================================================

def _enrich_from_profile(page: Page, person: dict):
    """Visit a person's LinkedIn profile and fill in richer details."""
    url = person.get("linkedin_url", "")
    if not url or not url.startswith("http"):
        return

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=15000)
        time.sleep(2.5)

        # Scroll to load lazy sections
        page.evaluate("window.scrollTo(0, 600)")
        time.sleep(1)

        # ---- Full headline (below name on profile) ----
        headline_el = page.query_selector(
            "div.text-body-medium.break-words, "
            "h2.text-body-medium"
        )
        if headline_el:
            full_headline = headline_el.inner_text().strip()
            if full_headline:
                person["headline"] = full_headline

        # ---- About section ----
        # Try the "About" section — LinkedIn wraps it in various containers
        about_text = _extract_about(page)
        if about_text:
            person["about"] = about_text[:500]

        # ---- Current experience → company LinkedIn URL ----
        company_link = page.query_selector(
            "section:has(#experience) a[href*='/company/'], "
            "li.artdeco-list__item a[href*='/company/'], "
            "a[data-field='experience_company_logo'][href*='/company/']"
        )
        if company_link:
            href = company_link.get_attribute("href") or ""
            if "/company/" in href:
                co_url = href.split("?")[0]
                if not co_url.startswith("http"):
                    co_url = "https://www.linkedin.com" + co_url
                person["company_linkedin_url"] = co_url

        # If we didn't get company from title, try the experience section
        if not person.get("company"):
            co_name_el = page.query_selector(
                "section:has(#experience) span.t-14.t-normal, "
                "li.artdeco-list__item span.t-14.t-normal"
            )
            if co_name_el:
                person["company"] = co_name_el.inner_text().strip().split("·")[0].strip()

        # ---- Contact info overlay ----
        _try_extract_contact_info(page, person)

    except Exception as e:
        # Log but don't crash — we still have the search card data
        print(f" [skip: {type(e).__name__}]", end="")


def _extract_about(page: Page) -> str:
    """Try multiple selectors to find the About section text."""
    # Method 1: direct about section
    selectors = [
        "section:has(#about) div.display-flex span[aria-hidden='true']",
        "section:has(#about) span.visually-hidden + span",
        "div#about + div span[aria-hidden='true']",
        "section.artdeco-card:has(h2:text-is('About')) span[aria-hidden='true']",
    ]
    for sel in selectors:
        try:
            el = page.query_selector(sel)
            if el:
                text = el.inner_text().strip()
                if len(text) > 20:  # Filter out empty/short fragments
                    return text
        except Exception:
            continue

    # Method 2: look for "see more" in about and click it
    try:
        see_more = page.query_selector("section:has(#about) button.inline-show-more-text__button")
        if see_more:
            see_more.click()
            time.sleep(0.5)
            for sel in selectors:
                try:
                    el = page.query_selector(sel)
                    if el:
                        text = el.inner_text().strip()
                        if len(text) > 20:
                            return text
                except Exception:
                    continue
    except Exception:
        pass

    return ""


def _try_extract_contact_info(page: Page, person: dict):
    """Try to open the contact info overlay and extract email/phone."""
    try:
        contact_btn = page.query_selector(
            "a[href*='overlay/contact-info'], "
            "a#top-card-text-details-contact-info"
        )
        if not contact_btn:
            return

        contact_btn.click()
        time.sleep(1.5)

        # Email
        email_el = page.query_selector(
            "section.ci-email a[href^='mailto:'], "
            "a[href^='mailto:']"
        )
        if email_el:
            href = email_el.get_attribute("href") or ""
            if href.startswith("mailto:"):
                person["email"] = href[7:].split("?")[0]

        # Phone
        phone_el = page.query_selector(
            "section.ci-phone span.t-14.t-black, "
            "section.ci-phone span.t-14, "
            "li.ci-phone span"
        )
        if phone_el:
            phone_text = phone_el.inner_text().strip()
            if phone_text and any(c.isdigit() for c in phone_text):
                person["phone"] = phone_text

        # Website / Twitter / other links (bonus)
        website_el = page.query_selector(
            "section.ci-websites a[href], "
            "section.ci-ims a[href]"
        )
        if website_el and not person.get("email"):
            # Sometimes people list their website in contact info
            pass  # Don't overwrite — just note availability

        # Close the overlay
        close_btn = page.query_selector(
            "button[aria-label='Dismiss'], "
            "button.artdeco-modal__dismiss"
        )
        if close_btn:
            close_btn.click()
            time.sleep(0.5)

    except Exception:
        # Try to dismiss any open overlay
        try:
            page.keyboard.press("Escape")
            time.sleep(0.3)
        except Exception:
            pass


# ================================================================
#  PHASE 3 — COMPANY ENRICHMENT
# ================================================================

def _enrich_companies(page: Page, results: list[dict]):
    """Visit unique company LinkedIn pages to get website, industry, size."""
    # Collect unique company URLs
    company_map: dict[str, dict] = {}  # url → {website, industry, size}
    for r in results:
        co_url = r.get("company_linkedin_url", "")
        if co_url and co_url not in company_map:
            company_map[co_url] = None  # placeholder

    if not company_map:
        print("  No company pages to enrich.")
        return

    print(f"  Enriching {len(company_map)} unique companies...")

    for i, co_url in enumerate(company_map, 1):
        co_name = ""
        for r in results:
            if r.get("company_linkedin_url") == co_url:
                co_name = r.get("company", "")
                break

        print(f"  [{i}/{len(company_map)}] {co_name or co_url}...", end="")
        info = _scrape_company_page(page, co_url)
        company_map[co_url] = info

        parts = []
        if info.get("website"):
            parts.append(info["website"])
        if info.get("industry"):
            parts.append(info["industry"])
        if info.get("size"):
            parts.append(info["size"])
        print(f" {', '.join(parts) or 'no data'}")

        time.sleep(random.uniform(COMPANY_DELAY_MIN, COMPANY_DELAY_MAX))

    # Apply company info back to all results
    for r in results:
        co_url = r.get("company_linkedin_url", "")
        info = company_map.get(co_url)
        if info:
            r["company_website"] = info.get("website", "")
            r["company_industry"] = info.get("industry", "")
            r["company_size"] = info.get("size", "")


def _scrape_company_page(page: Page, company_url: str) -> dict:
    """Visit a LinkedIn company page and extract key details."""
    info = {"website": "", "industry": "", "size": ""}

    # Ensure we're hitting the /about page for full details
    about_url = company_url.rstrip("/") + "/about/"

    try:
        page.goto(about_url, wait_until="domcontentloaded", timeout=15000)
        time.sleep(2)

        # LinkedIn company /about page has a definition-list style layout:
        #   <dt>Website</dt><dd>...</dd>
        #   <dt>Industry</dt><dd>...</dd>
        #   <dt>Company size</dt><dd>...</dd>

        # Method 1: dt/dd pairs
        dts = page.query_selector_all("dt")
        for dt in dts:
            label = dt.inner_text().strip().lower()
            dd = dt.evaluate("el => el.nextElementSibling?.textContent?.trim() || ''")

            if "website" in label and dd:
                info["website"] = dd.strip()
            elif "industry" in label and dd:
                info["industry"] = dd.strip()
            elif "company size" in label and dd:
                # Extract just the number range (e.g., "201-500 employees")
                info["size"] = dd.strip()

        # Method 2: fallback — look for specific elements
        if not info["website"]:
            link_el = page.query_selector(
                "a[href][data-test-id='about-us__website'], "
                "div.org-page-details-module__card-spacing a[href^='http']"
            )
            if link_el:
                info["website"] = (link_el.get_attribute("href") or "").split("?")[0]

        if not info["industry"]:
            industry_el = page.query_selector(
                "div[data-test-id='about-us__industry'], "
                "dd.org-page-details__definition-text"
            )
            if industry_el:
                info["industry"] = industry_el.inner_text().strip()

    except Exception:
        pass

    return info


# ================================================================
#  HELPERS
# ================================================================

def _extract_company_from_title(title: str) -> str:
    """Extract company name from a title like 'VP Marketing at TechCorp'."""
    if " at " in title:
        return title.split(" at ", 1)[1].strip()
    if " @ " in title:
        return title.split(" @ ", 1)[1].strip()
    return ""


def _classify_hiring_role(text: str) -> str:
    """Classify what role/function someone is hiring for.

    Scans the combined title + headline + post text for role category
    keywords and returns the best match (or 'General').

    Returns comma-separated categories if multiple strong matches
    (e.g., "Marketing, Sales").
    """
    if not text:
        return "General"

    text_lower = text.lower()
    scores: dict[str, int] = {}

    for category, keywords in ROLE_CATEGORIES.items():
        score = sum(1 for kw in keywords if kw in text_lower)
        if score > 0:
            scores[category] = score

    if not scores:
        return "General"

    # Return top categories (up to 2) sorted by score
    sorted_cats = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top_score = sorted_cats[0][1]

    # Include categories with score >= half of the top score
    matches = [cat for cat, sc in sorted_cats if sc >= top_score * 0.5][:2]
    return ", ".join(matches)


def merge_hiring_results(
    people_df: pd.DataFrame,
    posts_df: pd.DataFrame,
) -> pd.DataFrame:
    """Merge people-search and post-search results into a single DataFrame.

    Deduplicates by linkedin_url, preferring the record with more data.
    """
    if people_df.empty and posts_df.empty:
        return pd.DataFrame()
    if people_df.empty:
        return posts_df
    if posts_df.empty:
        return people_df

    combined = pd.concat([people_df, posts_df], ignore_index=True)

    # Deduplicate by linkedin_url — keep the row with more non-empty fields
    def _richness(row):
        return sum(1 for v in row if v and str(v).strip() and str(v) != "nan")

    combined["_richness"] = combined.apply(_richness, axis=1)
    combined = combined.sort_values("_richness", ascending=False)
    combined = combined.drop_duplicates(subset="linkedin_url", keep="first")
    combined = combined.drop(columns=["_richness"])

    return combined.reset_index(drop=True)


# ================================================================
#  CONSULTANT SEARCH — find recruitment agencies / staffing firms
# ================================================================

def find_consultants(
    keywords: list[str],
    locations: list[str],
    max_results: int = 50,
    broad: bool = False,
) -> pd.DataFrame:
    """
    Search LinkedIn for recruitment consultants / staffing firms.

    Two modes controlled by the `broad` flag:

    **Broad mode** (broad=True):
      Searches recruiter titles x locations only. No industry keyword coupling.
      e.g., "Recruitment Consultant Bangalore", "Headhunter Mumbai"
      Use this to build a generic list of recruiters in your area.

    **Focused mode** (broad=False, default):
      Combines recruiter titles x user keywords x locations.
      e.g., "Recruitment Consultant Marketing Director Bangalore"
      Use this to find recruiters who specifically work in your domain.

    Returns DataFrame with columns:
        name, title, headline, about, location,
        linkedin_url, email, phone,
        company, company_linkedin_url, company_website,
        company_industry, company_size,
        consultant_type, is_agency, domains_served,
        search_keyword, scraped_at.
    """
    all_results = []

    # In broad mode, use more recruiter titles (no keyword multiplier)
    # In focused mode, use fewer titles (keyword x title x location = many queries)
    if broad:
        recruiter_titles = CONSULTANT_TITLE_KEYWORDS  # all 12
    else:
        recruiter_titles = CONSULTANT_TITLE_KEYWORDS[:5]

    with sync_playwright() as pw:
        user_data_dir = str(SESSION_DIR / "chrome_profile")
        context = pw.chromium.launch_persistent_context(
            user_data_dir,
            headless=False,
            channel="chrome",
            viewport={"width": 1280, "height": 800},
            args=["--start-maximized"],
        )
        page = context.pages[0] if context.pages else context.new_page()

        # ---- Login ----
        if not ensure_logged_in(page, context, timeout=300):
            context.close()
            return pd.DataFrame()

        # ---- Phase 1: Search ----
        if broad:
            print("\n  -- Phase 1: Broad search for Recruitment Consultants --")
        else:
            print("\n  -- Phase 1: Searching for Recruitment Consultants --")
        search_count = 0
        search_locations = locations if locations else [""]

        if broad:
            # Broad: recruiter_title x location (no keywords)
            for recruiter_title in recruiter_titles:
                if len(all_results) >= max_results:
                    break
                for location in search_locations[:3]:  # top 3 locations in broad
                    if len(all_results) >= max_results:
                        break

                    query = recruiter_title
                    if location and location.lower() != "remote":
                        query += f" {location}"

                    print(f"  [Search] '{query}'...")
                    results = _search_and_collect(
                        page, query, recruiter_title,
                        max_results=min(10, max_results - len(all_results)),
                    )
                    all_results.extend(results)
                    print(f"    -> {len(results)} results")
                    search_count += 1
                    time.sleep(random.uniform(CONSULTANT_DELAY_MIN, CONSULTANT_DELAY_MAX))
        else:
            # Focused: recruiter_title x keyword x location
            search_keywords = keywords[:4]
            for keyword in search_keywords:
                if len(all_results) >= max_results:
                    break
                for recruiter_title in recruiter_titles:
                    if len(all_results) >= max_results:
                        break
                    for location in search_locations[:2]:  # top 2 locations
                        if len(all_results) >= max_results:
                            break

                        query = f"{recruiter_title} {keyword}"
                        if location and location.lower() != "remote":
                            query += f" {location}"

                        print(f"  [Search] '{query}'...")
                        results = _search_and_collect(
                            page, query, keyword,
                            max_results=min(10, max_results - len(all_results)),
                        )
                        all_results.extend(results)
                        print(f"    -> {len(results)} results")
                        search_count += 1
                        time.sleep(random.uniform(CONSULTANT_DELAY_MIN, CONSULTANT_DELAY_MAX))

        if not all_results:
            print("\n  [Consultants] No consultants found.")
            context.close()
            return pd.DataFrame()

        # Deduplicate before enrichment
        seen_urls = set()
        unique_results = []
        for r in all_results:
            url = r.get("linkedin_url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_results.append(r)
            elif not url:
                unique_results.append(r)
        all_results = unique_results[:max_results]
        print(f"\n  Unique consultant profiles to enrich: {len(all_results)}")

        # ---- Phase 2: Profile enrichment ----
        print("\n  -- Phase 2: Enriching consultant profiles --")
        enrich_count = min(len(all_results), MAX_CONSULTANT_ENRICHMENTS)
        if len(all_results) > MAX_CONSULTANT_ENRICHMENTS:
            print(f"  [Note] Capping enrichment at {MAX_CONSULTANT_ENRICHMENTS} (got {len(all_results)})")
        enrich_failures = 0
        for i, person in enumerate(all_results[:enrich_count], 1):
            print(f"  [{i}/{enrich_count}] {person.get('name', '?')}...", end="")
            _enrich_from_profile(page, person)
            status_parts = []
            if person.get("email"):
                status_parts.append(f"email={person['email']}")
            if person.get("company_linkedin_url"):
                status_parts.append("co-page OK")
            if person.get("about"):
                status_parts.append("about OK")
            if not status_parts:
                enrich_failures += 1
            print(f" {', '.join(status_parts) or 'basic only'}")
            time.sleep(random.uniform(PROFILE_DELAY_MIN, PROFILE_DELAY_MAX))
        if enrich_failures > enrich_count * 0.5 and enrich_count > 5:
            print(f"\n  WARNING: {enrich_failures}/{enrich_count} profiles had no enrichment data.")
            print(f"    LinkedIn may be blocking your session. Try again later or re-login.")

        # ---- Phase 3: Company enrichment ----
        print("\n  -- Phase 3: Enriching company details --")
        _enrich_companies(page, all_results)

        context.close()

    df = pd.DataFrame(all_results)

    if not df.empty:
        # ---- Classify consultant type ----
        df["consultant_type"] = df.apply(
            lambda row: _classify_consultant_type(
                str(row.get("title", "")) + " " +
                str(row.get("headline", "")) + " " +
                str(row.get("company", "")) + " " +
                str(row.get("about", ""))
            ),
            axis=1,
        )

        # ---- Flag agency vs in-house ----
        df["is_agency"] = df.apply(
            lambda row: _is_agency(
                str(row.get("company", "")),
                str(row.get("company_industry", "")),
                str(row.get("title", "")),
            ),
            axis=1,
        )

        # ---- Classify what domains/roles they recruit for ----
        df["domains_served"] = df.apply(
            lambda row: _classify_hiring_role(
                str(row.get("title", "")) + " " +
                str(row.get("headline", "")) + " " +
                str(row.get("about", ""))
            ),
            axis=1,
        )

        df["signal_source"] = "consultant_search"

    print(f"\n  [Consultants] Total enriched profiles: {len(df)}")
    if not df.empty:
        agency_count = df["is_agency"].sum()
        print(f"  [Consultants] Confirmed agency: {agency_count} / {len(df)}")
        if "consultant_type" in df.columns:
            type_counts = df["consultant_type"].value_counts().head(5)
            if not type_counts.empty:
                print(f"  [Consultants] Types: {', '.join(f'{t}({c})' for t, c in type_counts.items())}")

    return df


def _classify_consultant_type(text: str) -> str:
    """Classify the type of recruitment consultant.

    Returns the best-matching consultant category (e.g., 'Executive Search',
    'Agency Recruiter', 'Staffing Firm', etc.).
    """
    if not text:
        return "Unknown"

    text_lower = text.lower()
    scores: dict[str, int] = {}

    for category, keywords in CONSULTANT_TYPES.items():
        score = sum(1 for kw in keywords if kw in text_lower)
        if score > 0:
            scores[category] = score

    if not scores:
        return "Agency Recruiter"  # default for people found via recruiter queries

    sorted_cats = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return sorted_cats[0][0]


def _is_agency(company: str, industry: str, title: str) -> bool:
    """Check if the person works at a recruitment agency (vs in-house).

    Returns True if any company/industry/title signal matches known
    agency keywords.
    """
    combined = (company + " " + industry + " " + title).lower()
    return any(signal in combined for signal in AGENCY_COMPANY_SIGNALS)
