"""Email enrichment via Hunter.io API."""

import re

import pandas as pd

from config import HUNTER_API_KEY, HUNTER_FREE_CREDITS, HUNTER_DEPARTMENT


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

    unique_companies = hr_contacts["company"].unique()
    print(f"\n[Hunter.io] Enriching emails for {len(unique_companies)} companies...")
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
                result = hunter.domain_search(
                    domain,
                    department=HUNTER_DEPARTMENT,
                    limit=10,
                )
                domain_cache[domain] = result.get("emails", []) if isinstance(result, dict) else []
                credits_used += 1
            except Exception as e:
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


def _guess_domain(company_name: str) -> str:
    """Guess company website domain from company name."""
    if not company_name:
        return ""

    # Clean up common suffixes
    name = company_name.strip().lower()
    for suffix in [
        " pvt ltd", " pvt. ltd.", " private limited", " limited",
        " ltd", " ltd.", " inc", " inc.", " corp", " corp.",
        " llp", " llc", " co.", " technologies", " tech",
        " solutions", " services", " consulting", " group",
    ]:
        name = name.replace(suffix, "")

    # Remove special chars, create slug
    name = re.sub(r"[^a-z0-9\s]", "", name).strip()
    name = name.replace(" ", "")

    if not name:
        return ""

    return f"{name}.com"


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
