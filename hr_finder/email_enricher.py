"""Email enrichment via Hunter.io API."""

import re
import time

import httpx
import pandas as pd

from config import HUNTER_API_KEY, HUNTER_FREE_CREDITS


def enrich_with_emails(
    hr_contacts: pd.DataFrame,
    company_domains: dict[str, str] | None = None,
    max_credits: int = HUNTER_FREE_CREDITS,
) -> pd.DataFrame:
    """
    Enrich HR contacts with email addresses using Hunter.io.

    Args:
        hr_contacts: DataFrame with at least 'company' and 'hr_name' columns.
        company_domains: Optional dict mapping company name → domain.
        max_credits: Max Hunter.io credits to use (50 free/month).

    Returns:
        Updated DataFrame with 'email' and 'email_confidence' columns.
    """
    if not HUNTER_API_KEY or HUNTER_API_KEY == "your_hunter_api_key_here":
        print("\n[Hunter.io] No API key configured. Set HUNTER_API_KEY in .env")
        print("  Sign up free at https://hunter.io (50 lookups/month)")
        hr_contacts["email"] = ""
        hr_contacts["email_confidence"] = ""
        return hr_contacts

    try:
        from pyhunter import PyHunter
    except ImportError:
        print("\n[Hunter.io] pyhunter not installed. Run: pip install pyhunter")
        hr_contacts["email"] = ""
        hr_contacts["email_confidence"] = ""
        return hr_contacts

    hunter = PyHunter(HUNTER_API_KEY)
    credits_used = 0

    emails = []
    confidences = []

    if company_domains is None:
        company_domains = {}

    # Check account credits before starting
    try:
        account = hunter.account_information()
        if isinstance(account, dict):
            used = account.get("requests", {}).get("searches", {}).get("used", "?")
            avail = account.get("requests", {}).get("searches", {}).get("available", "?")
            print(f"\n[Hunter.io] Account: {used} credits used / {avail} available this month")
    except Exception:
        pass

    unique_companies = hr_contacts["company"].unique()
    print(f"[Hunter.io] Enriching emails for {len(unique_companies)} companies ({len(hr_contacts)} contacts)...")
    print(f"  Credit budget: {max_credits}")

    # Cache domain search results to save credits
    domain_cache: dict[str, list[dict]] = {}

    for _, row in hr_contacts.iterrows():
        company = row.get("company", "")
        hr_name = row.get("hr_name", "")

        if credits_used >= max_credits:
            print(f"  Credit limit reached ({credits_used}/{max_credits})")
            emails.append("")
            confidences.append("")
            continue

        domain = company_domains.get(company) or _guess_domain(company)
        if not domain:
            emails.append("")
            confidences.append("")
            continue

        # Try domain search first (returns multiple emails per credit)
        if domain not in domain_cache:
            try:
                print(f"  Searching domain: {domain}")
                result = hunter.domain_search(domain, limit=10)
                raw_emails = result.get("emails", []) if isinstance(result, dict) else []
                # Prioritize HR/TA contacts from results
                hr_emails = _filter_hr_emails(raw_emails)
                domain_cache[domain] = hr_emails if hr_emails else raw_emails
                credits_used += 1
                if raw_emails:
                    print(f"    Found {len(raw_emails)} emails ({len(hr_emails)} HR/TA)")
            except Exception as e:
                err_str = str(e)
                if "400" in err_str or "Bad Request" in err_str:
                    print(f"    Domain not found on Hunter: {domain}")
                else:
                    print(f"    Domain search error for {domain}: {e}")
                domain_cache[domain] = []
                credits_used += 1

        # Try to match by name from domain search results
        cached_emails = domain_cache.get(domain, [])
        matched = _match_by_name(cached_emails, hr_name)

        if matched:
            emails.append(matched["value"])
            confidences.append(str(matched.get("confidence", "")))
        elif cached_emails:
            # Use the first HR email from domain search
            emails.append(cached_emails[0].get("value", ""))
            confidences.append(str(cached_emails[0].get("confidence", "")))
        else:
            # Try individual email finder as fallback
            if credits_used < max_credits and hr_name:
                parts = hr_name.split()
                if len(parts) >= 2:
                    try:
                        email_result = hunter.email_finder(
                            domain,
                            first_name=parts[0],
                            last_name=parts[-1],
                        )
                        credits_used += 1
                        if email_result and email_result.get("email"):
                            emails.append(email_result["email"])
                            confidences.append(str(email_result.get("score", "")))
                            continue
                    except Exception:
                        credits_used += 1  # API call was attempted

            emails.append("")
            confidences.append("")

    hr_contacts["email"] = emails
    hr_contacts["email_confidence"] = confidences

    found_count = sum(1 for e in emails if e)
    print(f"  Emails found: {found_count}/{len(emails)}")
    print(f"  Credits used: {credits_used}")

    return hr_contacts


# Known company → domain mappings (saves credits + avoids wrong guesses)
KNOWN_DOMAINS = {
    # Fintech / Payments
    "razorpay": "razorpay.com",
    "cred": "cred.club",
    "groww": "groww.in",
    "phonepe": "phonepe.com",
    "paytm": "paytm.com",
    "bharatpe": "bharatpe.com",
    "cred (india)": "cred.club",
    # NBFC / Lending
    "lendingkart": "lendingkart.com",
    "kreditbee": "kreditbee.in",
    "navi": "navi.com",
    "rupeek": "rupeek.com",
    # E-commerce / D2C
    "meesho": "meesho.com",
    "zepto": "zeptonow.com",
    "blinkit": "blinkit.com",
    "swiggy": "swiggy.com",
    "zomato": "zomato.com",
    "flipkart": "flipkart.com",
    "myntra": "myntra.com",
    "nykaa": "nykaa.com",
    "mamaearth": "mamaearth.in",
    "boat": "boat-lifestyle.com",
    "noise": "gonoise.com",
    "sugar cosmetics": "sugarcosmetics.com",
    "licious": "licious.in",
    "country delight": "countrydelight.in",
    # Tech / SaaS
    "freshworks": "freshworks.com",
    "zoho": "zoho.com",
    "leadsquared": "leadsquared.com",
    "clevertap": "clevertap.com",
    "chargebee": "chargebee.com",
    "postman": "postman.com",
    "browserstack": "browserstack.com",
    "druva": "druva.com",
    "hasura": "hasura.io",
    "unacademy": "unacademy.com",
    "byju's": "byjus.com",
    "upgrad": "upgrad.com",
    "elucidata": "elucidata.io",
    # Media / Entertainment
    "sony music entertainment": "sonymusic.com",
    "sony music": "sonymusic.com",
    "the orchard": "theorchard.com",
    "dow jones": "dowjones.com",
    # Hospitality
    "hyatt centric": "hyatt.com",
    "hyatt regency": "hyatt.com",
    "hyatt": "hyatt.com",
    "andaz": "hyatt.com",
    "marriott": "marriott.com",
    "taj hotels": "tajhotels.com",
    "ihg": "ihg.com",
    # Consulting / Recruiting
    "crescendo global": "crescendo-global.com",
    "michael page": "michaelpage.co.in",
    "abc consultants": "abcconsultants.in",
    "antal international": "antal.com",
    "sutrahr": "sutrahr.com",
    "korn ferry": "kornferry.com",
    "ciel hr": "cielhr.com",
    "topgear consultants": "topgearconsultants.com",
    # Others
    "sw network (sociowash)": "sociowash.com",
    "sociowash": "sociowash.com",
    "wiz": "wiz.io",
    "edhike": "edhike.com",
    "catering collective": "cateringcollective.co.uk",
    "turner & townsend": "turnerandtownsend.com",
}

# Domain verification cache (across calls within same session)
_domain_verify_cache: dict[str, bool] = {}


def _verify_domain(domain: str) -> bool:
    """Quick HTTP HEAD check to see if a domain resolves."""
    if domain in _domain_verify_cache:
        return _domain_verify_cache[domain]

    try:
        resp = httpx.head(
            f"https://{domain}",
            timeout=5.0,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        ok = resp.status_code < 500
        _domain_verify_cache[domain] = ok
        return ok
    except Exception:
        _domain_verify_cache[domain] = False
        return False


def _guess_domain(company_name: str) -> str:
    """Guess company website domain from company name.

    Strategy:
        1. Check known-domains map (instant, no HTTP calls)
        2. Try multiple TLDs with HTTP verification (.com, .in, .co.in, .io)
        3. Return first domain that resolves
    """
    if not company_name:
        return ""

    # Step 1: Check known domains (case-insensitive)
    name_lower = company_name.strip().lower()
    if name_lower in KNOWN_DOMAINS:
        return KNOWN_DOMAINS[name_lower]

    # Also try without common suffixes
    clean = name_lower
    for suffix in [
        " pvt ltd", " pvt. ltd.", " private limited", " limited",
        " ltd", " ltd.", " inc", " inc.", " corp", " corp.",
        " llp", " llc", " co.", " technologies", " tech",
        " solutions", " services", " consulting", " group",
        " india", " global",
    ]:
        clean = clean.replace(suffix, "")
    clean = clean.strip()

    if clean in KNOWN_DOMAINS:
        return KNOWN_DOMAINS[clean]

    # Step 2: Build slug and try multiple TLDs
    slug = re.sub(r"[^a-z0-9\s]", "", clean).strip()
    slug = slug.replace(" ", "")

    if not slug:
        return ""

    # Try TLDs in priority order (most common for Indian companies)
    candidates = [
        f"{slug}.com",
        f"{slug}.in",
        f"{slug}.co.in",
        f"{slug}.io",
    ]

    for domain in candidates:
        if _verify_domain(domain):
            print(f"    Domain verified: {domain}")
            # Cache for future lookups
            KNOWN_DOMAINS[name_lower] = domain
            return domain

    # Fallback: return .com even if unverified (Hunter.io will handle it)
    print(f"    Domain guessed (unverified): {candidates[0]}")
    return candidates[0]


_HR_KEYWORDS = {
    "hr", "human resources", "talent", "recruiting", "recruitment",
    "people", "ta ", "talent acquisition", "hiring", "staffing",
    "head of people", "chro", "hrbp", "people ops",
}


def _filter_hr_emails(emails: list[dict]) -> list[dict]:
    """Filter domain search results to prefer HR/TA department contacts."""
    hr_list = []
    for e in emails:
        position = (e.get("position") or "").lower()
        department = (e.get("department") or "").lower()
        combined = f"{position} {department}"
        if any(kw in combined for kw in _HR_KEYWORDS):
            hr_list.append(e)
    return hr_list


def _match_by_name(emails: list[dict], name: str) -> dict | None:
    """Try to match an email from Hunter results by person name."""
    if not name or not emails:
        return None

    name_lower = name.lower()
    parts = name_lower.split()

    for email_data in emails:
        first = (email_data.get("first_name") or "").lower()
        last = (email_data.get("last_name") or "").lower()
        if first and last:
            if first in name_lower and last in name_lower:
                return email_data
        elif parts:
            if first == parts[0] or last == parts[-1]:
                return email_data

    return None
