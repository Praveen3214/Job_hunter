"""Shared scraper utilities — DRY helpers used across multiple scrapers."""

import re

import pandas as pd


def to_slug(text: str) -> str:
    """Convert text to a URL-friendly slug.

    Examples:
        "Marketing Director" → "marketing-director"
        "VP Marketing & Growth" → "vp-marketing--growth"
    """
    slug = text.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s]+", "-", slug)
    return slug


# Shared user-agent (Chrome 124 on Windows)
CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# India location filter — used across scrapers to drop non-India results
# ---------------------------------------------------------------------------

# Comprehensive set of Indian city/state/region names (lowercase)
INDIA_LOCATIONS = {
    # Metro cities
    "mumbai", "delhi", "bangalore", "bengaluru", "hyderabad", "chennai",
    "kolkata", "pune", "ahmedabad", "jaipur",
    # Tier-1 / major cities
    "noida", "gurugram", "gurgaon", "ghaziabad", "faridabad",
    "lucknow", "kanpur", "chandigarh", "indore", "bhopal",
    "nagpur", "surat", "vadodara", "coimbatore", "kochi",
    "thiruvananthapuram", "trivandrum", "visakhapatnam", "vizag",
    "patna", "ranchi", "bhubaneswar", "dehradun", "mysore", "mysuru",
    "mangalore", "mangaluru", "hubli", "nashik", "aurangabad",
    "rajkot", "jodhpur", "udaipur", "agra", "varanasi",
    "allahabad", "prayagraj", "meerut", "jalandhar", "amritsar",
    "ludhiana", "shimla", "jammu", "srinagar", "raipur",
    "ranchi", "guwahati", "imphal", "shillong", "gangtok",
    "pondicherry", "puducherry", "madurai", "tiruchirappalli",
    "trichy", "salem", "tiruppur", "erode", "vellore",
    "guntur", "warangal", "nellore", "vijayawada",
    "thane", "navi mumbai", "kalyan", "vasai", "panvel",
    "mohali", "panchkula", "greater noida",
    # NCR variants
    "ncr", "delhi ncr", "delhi-ncr", "new delhi",
    # Country / generic
    "india", "pan india", "pan-india", "across india", "multiple cities india",
    # Remote (keep — could be India-remote)
    "remote", "work from home", "wfh", "hybrid",
    # States (for broader matches)
    "maharashtra", "karnataka", "tamil nadu", "telangana",
    "andhra pradesh", "west bengal", "rajasthan", "gujarat",
    "uttar pradesh", "madhya pradesh", "kerala", "punjab",
    "haryana", "bihar", "odisha", "jharkhand", "chhattisgarh",
    "uttarakhand", "goa", "assam",
}

# Non-India locations that should always be filtered out
NON_INDIA_SIGNALS = {
    # US cities / states
    "new york", "san francisco", "los angeles", "chicago", "boston",
    "seattle", "austin", "denver", "dallas", "houston", "miami",
    "atlanta", "philadelphia", "washington dc", "washington, dc",
    "baltimore", "bethesda", "arlington",
    "san jose", "san diego", "portland", "phoenix", "minneapolis",
    "detroit", "charlotte", "nashville", "raleigh", "st louis",
    "st. louis", "santa monica", "bellevue", "palo alto",
    "mountain view", "cupertino", "redmond", "irvine",
    "salt lake", "pittsburgh", "kansas city", "indianapolis",
    "columbus", "cleveland", "tampa", "orlando", "sacramento",
    "california", "texas", "florida", "illinois", "massachusetts",
    "virginia", "maryland", "colorado", "georgia", "ohio",
    "pennsylvania", "new jersey", "connecticut", "oregon",
    "missouri", "tennessee", "north carolina", "south carolina",
    "michigan", "wisconsin", "minnesota", "arizona", "washington",
    # US generic
    "usa", "united states", "us",
    # UK
    "london", "manchester", "birmingham", "leeds", "glasgow",
    "edinburgh", "bristol", "liverpool", "cambridge", "oxford",
    "uk", "united kingdom", "england", "scotland",
    # Europe
    "berlin", "munich", "hamburg", "frankfurt", "paris", "lyon",
    "amsterdam", "rotterdam", "dublin", "zurich", "geneva",
    "stockholm", "oslo", "copenhagen", "helsinki", "vienna",
    "prague", "warsaw", "barcelona", "madrid", "lisbon", "milan",
    "rome", "brussels", "luxembourg",
    "germany", "france", "netherlands", "switzerland", "spain",
    "italy", "sweden", "norway", "denmark", "finland", "austria",
    "ireland", "belgium", "portugal", "poland", "czech republic",
    "europe",
    # Australia / NZ
    "sydney", "melbourne", "brisbane", "perth", "adelaide",
    "auckland", "wellington",
    "australia", "new zealand",
    # Middle East
    "dubai", "abu dhabi", "riyadh", "jeddah", "doha", "muscat",
    "kuwait city", "bahrain", "manama",
    "uae", "saudi arabia", "qatar", "oman", "kuwait",
    # Asia (non-India)
    "singapore", "hong kong", "tokyo", "shanghai", "beijing",
    "seoul", "taipei", "kuala lumpur", "bangkok", "jakarta",
    "manila", "ho chi minh", "hanoi",
    "japan", "china", "south korea", "taiwan", "malaysia",
    "thailand", "indonesia", "philippines", "vietnam",
    # Canada
    "toronto", "vancouver", "montreal", "ottawa", "calgary",
    "canada",
    # Africa / LatAm
    "johannesburg", "cape town", "nairobi", "lagos",
    "sao paulo", "mexico city", "buenos aires",
    "south africa", "kenya", "nigeria", "brazil", "mexico",
    "argentina",
}


def is_india_location(text: str) -> bool:
    """Check if a location string refers to an Indian city/state/region.

    Returns True if any known Indian location token is found in the text.
    Returns False for empty/blank text (unknown location).
    """
    if not text or not text.strip():
        return False  # Unknown — caller decides how to handle

    text_lower = text.lower().strip()

    # Quick check: any India location token present?
    for loc in INDIA_LOCATIONS:
        if loc in text_lower:
            return True

    return False


def is_non_india_location(text: str) -> bool:
    """Check if a location string is clearly outside India.

    Returns True only if non-India signals found AND no India signals found.
    Returns False for empty/blank text (unknown — not definitively non-India).
    """
    if not text or not text.strip():
        return False  # Unknown — benefit of the doubt

    text_lower = text.lower().strip()

    # If it has India signals, it's not non-India
    for loc in INDIA_LOCATIONS:
        if loc in text_lower:
            return False

    # Check for non-India signals
    for loc in NON_INDIA_SIGNALS:
        if loc in text_lower:
            return True

    return False


def filter_india_jobs(df: pd.DataFrame, strict: bool = False) -> pd.DataFrame:
    """Filter a jobs DataFrame to only India-based roles.

    Args:
        df: DataFrame with a 'location' column.
        strict: If True, drops rows with empty/unknown locations too.
                If False, keeps rows with empty locations (benefit of the doubt).

    Returns:
        Filtered DataFrame with only India-relevant jobs.
    """
    if df.empty or "location" not in df.columns:
        return df

    before = len(df)

    def _keep(loc):
        loc_str = str(loc).strip() if pd.notna(loc) else ""
        if not loc_str:
            return not strict  # Keep empty if not strict
        # Keep if India location found
        if is_india_location(loc_str):
            return True
        # Drop if clearly non-India
        if is_non_india_location(loc_str):
            return False
        # Unknown location that's not empty — keep in lenient mode
        return not strict

    mask = df["location"].apply(_keep)
    filtered = df[mask].copy()
    dropped = before - len(filtered)
    if dropped:
        print(f"    [India filter] Removed {dropped} non-India jobs")
    return filtered
