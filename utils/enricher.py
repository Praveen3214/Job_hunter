"""Job data enrichment — classifies company type, role type, extracts
experience requirements, and generates a crisp role summary.

Works by analyzing text from title, company name, description, and skills
fields already present in the scraped data.
"""

import re
from pathlib import Path

import pandas as pd

from config import B2B_SIGNALS, B2C_SIGNALS, D2C_SIGNALS, OUTPUT_DIR


def enrich_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add enrichment columns to a jobs DataFrame:
      - company_type:  B2B | B2C | D2C | Mixed | Unknown
      - role_type:     B2B | B2C | D2C | Growth | Brand | General
      - experience_required:  e.g. "10-15 years" or ""
      - role_summary:  bullet-point summary of key responsibilities

    Args:
        df: DataFrame with at least title, company, description columns.

    Returns:
        DataFrame with four new columns appended.
    """
    if df.empty:
        return df

    df = df.copy()

    # Ensure columns exist
    for col in ("title", "company", "description", "skills", "experience"):
        if col not in df.columns:
            df[col] = ""

    df["company_type"] = df.apply(_classify_company_type, axis=1)
    df["role_type"] = df.apply(_classify_role_type, axis=1)
    df["experience_required"] = df.apply(_extract_experience, axis=1)

    # Numeric min/max parsed from experience_required (or raw experience field)
    parsed = df.apply(_extract_experience_range, axis=1)
    df["min_experience_years"] = parsed.apply(lambda t: t[0])
    df["max_experience_years"] = parsed.apply(lambda t: t[1])

    df["role_summary"] = df.apply(_generate_summary, axis=1)

    # Salary normalization (before relevance scoring so it can use parsed values)
    salary_parsed = df.apply(_normalize_salary_row, axis=1)
    df["salary_min_lpa"] = salary_parsed.apply(lambda t: t[0])
    df["salary_max_lpa"] = salary_parsed.apply(lambda t: t[1])
    df["salary_normalized"] = salary_parsed.apply(lambda t: t[2])

    # Relevance scoring (0-100)
    df["relevance_score"] = df.apply(_compute_relevance_score, axis=1)

    # Shortlist tag: experience ≤10, salary >27 LPA (or missing), marketing
    # in title, and first-time scrape
    df["shortlist"] = _compute_shortlist(df)

    return df


# ── Company type classification ──────────────────────────────────────

def _classify_company_type(row) -> str:
    """Classify company as B2B, B2C, D2C, Mixed, or Unknown."""
    text = _combine_text(row).lower()

    b2b_score = sum(1 for s in B2B_SIGNALS if s in text)
    b2c_score = sum(1 for s in B2C_SIGNALS if s in text)
    d2c_score = sum(1 for s in D2C_SIGNALS if s in text)

    max_score = max(b2b_score, b2c_score, d2c_score)
    if max_score == 0:
        return "Unknown"

    types = []
    if b2b_score == max_score:
        types.append("B2B")
    if b2c_score == max_score:
        types.append("B2C")
    if d2c_score == max_score:
        types.append("D2C")

    # If only D2C signals but also B2C signals present, it's D2C (a subset of B2C)
    if d2c_score > 0 and d2c_score >= b2c_score:
        return "D2C"
    if len(types) > 1:
        return "Mixed"
    return types[0]


# ── Role type classification ─────────────────────────────────────────

def _classify_role_type(row) -> str:
    """Classify the role focus: B2B, B2C, D2C, Growth, Brand, General."""
    title = str(row.get("title", "")).lower()
    desc = str(row.get("description", "")).lower()
    text = f"{title} {desc}"

    # Check title-level signals first
    if any(k in title for k in ["growth", "performance", "acquisition", "paid"]):
        return "Growth"
    if any(k in title for k in ["brand", "creative", "communications", "pr"]):
        return "Brand"
    if any(k in title for k in ["product marketing", "pmm"]):
        return "B2B"  # Product marketing skews B2B
    if any(k in title for k in ["content", "seo", "social media"]):
        # Content/SEO could be either; check description
        pass

    # Description-level signals
    d2c_hits = sum(1 for s in D2C_SIGNALS if s in text)
    b2b_hits = sum(1 for s in B2B_SIGNALS if s in text)
    b2c_hits = sum(1 for s in B2C_SIGNALS if s in text)

    if d2c_hits > b2b_hits and d2c_hits > b2c_hits:
        return "D2C"
    if b2b_hits > b2c_hits:
        return "B2B"
    if b2c_hits > b2b_hits:
        return "B2C"

    return "General"


# ── Experience extraction ────────────────────────────────────────────

def _extract_experience(row) -> str:
    """Extract years of experience required from the job data."""
    # First check if there's already an experience field
    exp_field = str(row.get("experience", "")).strip()
    if exp_field and exp_field != "nan":
        return exp_field

    # Search in description and title
    text = f"{row.get('title', '')} {row.get('description', '')}"

    patterns = [
        # "10-15 years" / "10+ years" / "10 to 15 years"
        r"(\d+\s*[-–to]+\s*\d+\s*(?:\+\s*)?(?:years?|yrs?))",
        r"(\d+\+?\s*(?:years?|yrs?)\s*(?:of\s+)?(?:experience|exp)?)",
        # "minimum 10 years" / "at least 8 years"
        r"(?:minimum|at\s+least|min\.?)\s*(\d+\s*(?:years?|yrs?))",
        # "experience: 10-15" in structured fields
        r"experience\s*[:=]\s*(\d+\s*[-–to]+\s*\d+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            result = match.group(1).strip()
            # Normalize: "10 Yrs" → "10 years"
            result = re.sub(r"\byrs?\b", "years", result, flags=re.IGNORECASE)
            return result

    return ""


# ── Numeric experience range ─────────────────────────────────────────

# Cap to reject garbage values ("200 years", "115 years")
_MAX_REASONABLE_YEARS = 35


def _parse_experience_range(text: str) -> tuple:
    """Parse a free-text experience string into (min_years, max_years).

    Handles patterns like:
        "10-15 years"  /  "10–15 Yrs"  /  "5 to 10 years"
        "5 – 10 years" /  "8 –15 years" /  "12–18"
        "5 years"      /  "5 Years of experience"
        "5+ years"     /  "minimum 8 years"

    Returns (None, None) if nothing can be parsed or values look bogus.
    """
    if not text or str(text).strip().lower() in ("", "nan"):
        return (None, None)

    text = str(text).strip()

    # ── Range: "X-Y", "X–Y", "X to Y" (with optional spaces) ──
    m = re.search(
        r"(\d+)\s*[-–—]\s*(\d+)",
        text,
    )
    if not m:
        m = re.search(r"(\d+)\s+to\s+(\d+)", text, re.IGNORECASE)

    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        if lo > hi:
            lo, hi = hi, lo
        if hi <= _MAX_REASONABLE_YEARS:
            return (lo, hi)
        return (None, None)  # garbage

    # ── Single: "5+ years" / "5 years" / "minimum 8 years" ──
    m = re.search(r"(\d+)\+?\s*(?:years?|yrs?)", text, re.IGNORECASE)
    if m:
        val = int(m.group(1))
        if val <= _MAX_REASONABLE_YEARS:
            return (val, None) if "+" in text else (val, val)
        return (None, None)

    # ── Bare number (rare) ──
    m = re.search(r"(\d+)", text)
    if m:
        val = int(m.group(1))
        if val <= _MAX_REASONABLE_YEARS:
            return (val, val)

    return (None, None)


def _extract_experience_range(row) -> tuple:
    """Row-level wrapper: try structured 'experience' field first,
    fall back to 'experience_required' enrichment column, then description."""

    # Source 1: raw experience field (cleanest)
    exp = str(row.get("experience", "")).strip()
    if exp and exp != "nan":
        result = _parse_experience_range(exp)
        if result != (None, None):
            return result

    # Source 2: enriched experience_required
    er = str(row.get("experience_required", "")).strip()
    if er and er != "nan":
        result = _parse_experience_range(er)
        if result != (None, None):
            return result

    # Source 3: scan description for first experience mention
    desc = str(row.get("description", ""))
    if desc and desc != "nan":
        # Quick regex scan — grab first match
        m = re.search(
            r"(\d+)\s*[-–—]\s*(\d+)\s*(?:\+?\s*)?(?:years?|yrs?)",
            desc, re.IGNORECASE,
        )
        if m:
            lo, hi = int(m.group(1)), int(m.group(2))
            if lo > hi:
                lo, hi = hi, lo
            if hi <= _MAX_REASONABLE_YEARS:
                return (lo, hi)

        m = re.search(r"(\d+)\+?\s*(?:years?|yrs?)", desc, re.IGNORECASE)
        if m:
            val = int(m.group(1))
            if val <= _MAX_REASONABLE_YEARS:
                return (val, None) if "+" in m.group(0) else (val, val)

    return (None, None)


# ── Salary normalization ────────────────────────────────────────────

def _normalize_salary_row(row) -> tuple:
    """Row-level wrapper: try text 'salary' first, then structured min/max fields."""
    # Try the text salary column
    result = _normalize_salary(row.get("salary"))
    if result[0] is not None:
        return result

    # Fallback: structured min_amount / max_amount (from python-jobspy)
    try:
        min_amt = row.get("min_amount")
        max_amt = row.get("max_amount")
        currency = str(row.get("currency", "")).upper()

        if pd.notna(min_amt) or pd.notna(max_amt):
            lo = float(min_amt) if pd.notna(min_amt) and min_amt else 0
            hi = float(max_amt) if pd.notna(max_amt) and max_amt else lo

            if lo <= 0 and hi <= 0:
                return (None, None, "")

            # Determine if the values are yearly or need conversion
            interval = str(row.get("interval", "")).lower()

            if currency in ("INR", ""):
                # Check if it's LPA already or absolute
                if interval == "monthly" or (lo > 0 and lo < 500 and hi < 500):
                    # Monthly salary or already in lakhs
                    if lo > 999:
                        lo_lpa = _to_lpa(lo * 12 if interval == "monthly" else lo, False)
                        hi_lpa = _to_lpa(hi * 12 if interval == "monthly" else hi, False)
                    else:
                        lo_lpa = lo
                        hi_lpa = hi
                else:
                    lo_lpa = _to_lpa(lo, interval == "monthly")
                    hi_lpa = _to_lpa(hi, interval == "monthly")

                return _format_salary_result(lo_lpa, hi_lpa)

            elif currency == "USD":
                # Convert USD to approximate INR LPA (1 USD ~ 85 INR)
                lo_inr = lo * 85
                hi_inr = hi * 85
                lo_lpa = _to_lpa(lo_inr, interval == "monthly")
                hi_lpa = _to_lpa(hi_inr, interval == "monthly")
                return _format_salary_result(lo_lpa, hi_lpa)
    except (ValueError, TypeError):
        pass

    return (None, None, "")


def _normalize_salary(raw) -> tuple:
    """Parse Indian salary strings into structured (min_lpa, max_lpa, display).

    Handles formats like:
        "₹15-20 LPA"           → (15.0, 20.0, "15-20 LPA")
        "15L"                   → (15.0, 15.0, "15 LPA")
        "15 - 20 Lakhs"        → (15.0, 20.0, "15-20 LPA")
        "Rs 25-35 Lacs"        → (25.0, 35.0, "25-35 LPA")
        "₹ 12,00,000 - 18,00,000"  → (12.0, 18.0, "12-18 LPA")
        "₹ 12,00,000 - 18,00,000 per annum" → (12.0, 18.0, "12-18 LPA")
        "20,00,000"             → (20.0, 20.0, "20 LPA")
        "1500000-2500000"       → (15.0, 25.0, "15-25 LPA")
        "Rs6-8.5 Lacs"          → (6.0, 8.5, "6-8.5 LPA")
        "Not disclosed"         → (None, None, "")
        "Competitive"           → (None, None, "")
        "50000-80000 per month" → (6.0, 9.6, "6-9.6 LPA")
        "80000/month"           → (9.6, 9.6, "9.6 LPA")
        "50K-80K per month"     → (6.0, 9.6, "6-9.6 LPA")

    Returns:
        (min_lpa: float|None, max_lpa: float|None, display: str)
    """
    if not raw or str(raw).strip().lower() in ("", "nan", "none", "not disclosed",
                                                 "competitive", "confidential",
                                                 "as per industry", "best in industry",
                                                 "-"):
        return (None, None, "")

    text = str(raw).strip()
    original = text

    # Clean: remove currency symbols, "Rs", "INR" prefix
    text = re.sub(r"[₹$]", "", text)
    text = re.sub(r"(?i)^(rs\.?|inr)\s*", "", text)
    text = text.strip()

    is_monthly = bool(re.search(r"(?i)(per\s*month|/\s*month|p\.?\s*m\.?|monthly)", text))
    is_annual = bool(re.search(r"(?i)(per\s*annum|p\.?\s*a\.?|annual|yearly|/\s*year)", text))

    # Strip period indicators for number parsing
    text = re.sub(r"(?i)\s*(per\s*month|/\s*month|p\.?\s*m\.?|monthly|per\s*annum|p\.?\s*a\.?|annual|yearly|/\s*year)\s*$", "", text).strip()

    min_lpa = None
    max_lpa = None

    # ── Pattern 1: Indian comma format range "12,00,000 - 18,00,000" ──
    m = re.search(r"([\d,]+(?:,\d{2,3}){1,3})\s*[-–—to]+\s*([\d,]+(?:,\d{2,3}){1,3})", text)
    if m:
        v1 = _indian_number_to_float(m.group(1))
        v2 = _indian_number_to_float(m.group(2))
        if v1 is not None and v2 is not None:
            min_lpa, max_lpa = _to_lpa(v1, is_monthly), _to_lpa(v2, is_monthly)
            return _format_salary_result(min_lpa, max_lpa)

    # ── Pattern 2: Indian comma format single "12,00,000" ──
    m = re.match(r"^([\d,]+(?:,\d{2,3}){1,3})\s*$", text.split('-')[0].strip() if '-' not in text else "")
    if not m:
        m = re.match(r"^([\d,]+(?:,\d{2,3}){1,3})\s*$", text)
    if m:
        v = _indian_number_to_float(m.group(1))
        if v is not None:
            lpa = _to_lpa(v, is_monthly)
            return _format_salary_result(lpa, lpa)

    # ── Pattern 3: "50K-80K" (thousands) ──
    m = re.search(r"([\d.]+)\s*[kK]\s*[-–—to]+\s*([\d.]+)\s*[kK]", text)
    if m:
        try:
            v1 = float(m.group(1)) * 1000
            v2 = float(m.group(2)) * 1000
            min_lpa = _to_lpa(v1, is_monthly)
            max_lpa = _to_lpa(v2, is_monthly)
            return _format_salary_result(min_lpa, max_lpa)
        except ValueError:
            pass

    # ── Pattern 4: Single "80K" ──
    m = re.search(r"([\d.]+)\s*[kK](?:\s|$)", text)
    if m:
        try:
            v = float(m.group(1)) * 1000
            lpa = _to_lpa(v, is_monthly)
            return _format_salary_result(lpa, lpa)
        except ValueError:
            pass

    # ── Pattern 5: Range with LPA/L/Lacs/Lakhs "15-20 LPA" ──
    m = re.search(r"([\d.]+)\s*[-–—to]+\s*([\d.]+)\s*(?:lpa|lakhs?|lacs?|l)\b", text, re.IGNORECASE)
    if m:
        try:
            min_lpa = float(m.group(1))
            max_lpa = float(m.group(2))
            return _format_salary_result(min_lpa, max_lpa)
        except ValueError:
            pass

    # ── Pattern 6: Single "15 LPA" / "15L" / "15 Lakhs" ──
    m = re.search(r"([\d.]+)\s*(?:lpa|lakhs?|lacs?|l)\b", text, re.IGNORECASE)
    if m:
        try:
            lpa = float(m.group(1))
            return _format_salary_result(lpa, lpa)
        except ValueError:
            pass

    # ── Pattern 7: Bare range "1500000-2500000" (absolute numbers) ──
    m = re.search(r"(\d{5,})\s*[-–—to]+\s*(\d{5,})", text)
    if m:
        v1, v2 = float(m.group(1)), float(m.group(2))
        min_lpa = _to_lpa(v1, is_monthly)
        max_lpa = _to_lpa(v2, is_monthly)
        return _format_salary_result(min_lpa, max_lpa)

    # ── Pattern 8: Bare number "1500000" (absolute) ──
    m = re.search(r"(\d{5,})", text)
    if m:
        v = float(m.group(1))
        lpa = _to_lpa(v, is_monthly)
        return _format_salary_result(lpa, lpa)

    # ── Pattern 9: Range without unit "15-20" (assume LPA if reasonable) ──
    m = re.search(r"([\d.]+)\s*[-–—]\s*([\d.]+)", text)
    if m:
        try:
            v1, v2 = float(m.group(1)), float(m.group(2))
            # Only treat as LPA if values look reasonable (1-200)
            if 1 <= v1 <= 200 and 1 <= v2 <= 200:
                min_lpa = min(v1, v2)
                max_lpa = max(v1, v2)
                return _format_salary_result(min_lpa, max_lpa)
        except ValueError:
            pass

    return (None, None, "")


def _indian_number_to_float(s: str) -> float | None:
    """Convert Indian comma-formatted number to float.
    "12,00,000" → 1200000.0
    "18,00,000" → 1800000.0
    """
    try:
        return float(s.replace(",", ""))
    except (ValueError, AttributeError):
        return None


def _to_lpa(amount: float, is_monthly: bool = False) -> float:
    """Convert an absolute amount to LPA (lakhs per annum).

    If is_monthly: multiply by 12 first.
    If amount > 999: it's in absolute terms (e.g. 1500000 = 15 LPA).
    If amount <= 200: it's already in LPA (e.g. 15 = 15 LPA).
    """
    if is_monthly:
        amount = amount * 12

    if amount > 999:
        return round(amount / 100000, 1)  # Convert to lakhs
    return round(amount, 1)


def _format_salary_result(min_lpa, max_lpa):
    """Format the final salary result tuple."""
    if min_lpa is None or max_lpa is None:
        return (None, None, "")

    # Ensure min <= max
    if min_lpa > max_lpa:
        min_lpa, max_lpa = max_lpa, min_lpa

    # Sanity check: reject obviously wrong values
    if max_lpa > 500 or min_lpa < 0:
        return (None, None, "")

    # Format display string
    def _fmt(v):
        return str(int(v)) if v == int(v) else f"{v:.1f}"

    if min_lpa == max_lpa:
        display = f"{_fmt(min_lpa)} LPA"
    else:
        display = f"{_fmt(min_lpa)}-{_fmt(max_lpa)} LPA"

    return (min_lpa, max_lpa, display)


# ── Shortlist tagging ────────────────────────────────────────────────

def _parse_salary_lpa(text: str) -> float | None:
    """Extract the *maximum* salary in LPA from a salary string.

    Handles:
        "25-35 LPA"    → 35.0
        "rs25-40 Lacs" → 40.0
        "rs6-8.5 Lacs" → 8.5
        "30 LPA"       → 30.0

    Returns None if unparseable.
    """
    if not text or str(text).strip().lower() in ("", "nan"):
        return None

    text = str(text).strip().lower()
    # Strip leading "rs" prefix
    text = re.sub(r"^rs\.?\s*", "", text)

    # Try range first: "25-35" or "6-8.5"
    m = re.search(r"([\d.]+)\s*[-–—]\s*([\d.]+)", text)
    if m:
        try:
            return float(m.group(2))
        except ValueError:
            pass

    # Single value: "30 LPA"
    m = re.search(r"([\d.]+)", text)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass

    return None


def _collect_previous_job_urls() -> set:
    """Load known job_url values from master_jobs.csv (centralised).

    Falls back to scanning all jobs_*.csv if master doesn't exist yet.

    Returns:
        Set of job_url strings seen in previous exports.
    """
    urls: set = set()
    output_dir = Path(OUTPUT_DIR)
    if not output_dir.exists():
        return urls

    master = output_dir / "master_jobs.csv"
    if master.exists():
        try:
            chunk = pd.read_csv(master, usecols=["job_url"], dtype=str)
            urls.update(chunk["job_url"].dropna().tolist())
            return urls
        except (ValueError, KeyError):
            pass

    # Fallback: scan timestamped files (only when master doesn't exist yet)
    for csv_path in sorted(output_dir.glob("jobs_*.csv")):
        try:
            chunk = pd.read_csv(csv_path, usecols=["job_url"], dtype=str)
            urls.update(chunk["job_url"].dropna().tolist())
        except (ValueError, KeyError):
            pass
    return urls


# ── Relevance scoring ────────────────────────────────────────────────

# Tier-1 / metro cities (highest relevance for senior marketing in India)
_TIER1_CITIES = {
    "mumbai", "delhi", "bangalore", "bengaluru", "hyderabad",
    "chennai", "pune", "gurugram", "gurgaon", "noida", "new delhi",
}

# Indian location signals (any of these → job is in India)
_INDIA_SIGNALS = _TIER1_CITIES | {
    "india", "kolkata", "ahmedabad", "jaipur", "lucknow", "chandigarh",
    "kochi", "indore", "nagpur", "coimbatore", "vadodara", "thiruvananthapuram",
    "bhubaneswar", "vizag", "visakhapatnam", "mangalore", "mysore", "surat",
    "patna", "ranchi", "guwahati", "dehradun", "faridabad", "greater noida",
    "navi mumbai", "thane", "pan india", "across india",
}

# Non-India signals (explicit foreign markers → heavy penalty)
_NON_INDIA_SIGNALS = {
    "united states", "usa", "u.s.", "new york", "san francisco", "california",
    "london", "uk", "united kingdom", "dubai", "uae", "singapore", "toronto",
    "canada", "australia", "sydney", "melbourne", "germany", "berlin", "paris",
    "france", "japan", "tokyo", "hong kong", "china", "shanghai", "beijing",
    "amsterdam", "netherlands", "zurich", "switzerland", "seattle", "boston",
    "chicago", "los angeles", "texas", "florida", "denver", "austin",
    "philippines", "manila", "vietnam", "thailand", "bangkok", "jakarta",
    "indonesia", "saudi arabia", "riyadh", "qatar", "doha", "bahrain",
    "ireland", "dublin", "spain", "madrid", "italy", "milan", "brazil",
    "mexico", "south africa", "kenya", "nigeria", "new zealand", "auckland",
}

# Strong title signals (senior marketing roles)
_TITLE_STRONG = [
    "cmo", "chief marketing", "vp marketing", "vp of marketing",
    "vice president marketing", "head of marketing", "director of marketing",
    "marketing director", "head of growth", "director of growth",
    "head of digital", "head of brand",
]
_TITLE_GOOD = [
    "marketing", "growth", "brand", "digital marketing",
    "product marketing", "performance marketing",
]

# Seniority signals in title
_SENIORITY_SIGNALS = [
    "head", "director", "vp", "vice president", "chief",
    "lead", "senior manager", "senior director", "avp",
    "general manager", "gm",
]


def _compute_relevance_score(row) -> int:
    """Compute a 0-100 relevance score for a job.

    Scoring breakdown:
        Title match:      0-30 pts  (strong=30, good=20, weak=10, none=0)
        Seniority:        0-15 pts  (C-level/VP=15, Director/Head=12, Lead/Sr=8)
        Experience fit:   0-15 pts  (8-12 yrs=15, 5-15=10, outside=5, unknown=8)
        Salary:           0-15 pts  (>40L=15, 27-40L=12, 15-27L=8, <15L=3, unknown=8)
        Location:         0-10 pts  (Tier-1=10, India=7, Remote=6, other=3)
        India filter:     -40 pts   (penalise jobs clearly outside India)
        Company type:     0-10 pts  (B2C/D2C=10, B2B=8, Mixed=6, Unknown=4)
        Freshness:        0-5 pts   (date present=5, missing=2)
    """
    score = 0
    title = str(row.get("title", "")).lower()
    location = str(row.get("location", "")).lower()
    company_type = str(row.get("company_type", "")).lower()

    # ── Title match (30 pts) ──
    if any(t in title for t in _TITLE_STRONG):
        score += 30
    elif any(t in title for t in _TITLE_GOOD):
        score += 20
    elif "market" in title:
        score += 10
    # else: 0

    # ── Seniority (15 pts) ──
    if any(s in title for s in ("cmo", "chief", "vp", "vice president")):
        score += 15
    elif any(s in title for s in ("director", "head")):
        score += 12
    elif any(s in title for s in ("lead", "senior", "avp", "general manager", "gm")):
        score += 8
    elif any(s in title for s in ("manager",)):
        score += 5

    # ── Experience fit (15 pts) ──
    min_exp = row.get("min_experience_years")
    max_exp = row.get("max_experience_years")
    try:
        min_exp = float(min_exp) if pd.notna(min_exp) else None
    except (ValueError, TypeError):
        min_exp = None
    try:
        max_exp = float(max_exp) if pd.notna(max_exp) else None
    except (ValueError, TypeError):
        max_exp = None

    if min_exp is not None:
        if 8 <= min_exp <= 12:
            score += 15
        elif 5 <= min_exp <= 15:
            score += 10
        elif min_exp <= 20:
            score += 5
    else:
        score += 8  # Unknown = neutral

    # ── Salary (15 pts) ──
    salary_lpa = _parse_salary_lpa(row.get("salary", ""))
    if salary_lpa is not None:
        if salary_lpa > 40:
            score += 15
        elif salary_lpa > 27:
            score += 12
        elif salary_lpa > 15:
            score += 8
        else:
            score += 3
    else:
        score += 8  # Unknown = neutral

    # ── Location (10 pts) ──
    if any(c in location for c in _TIER1_CITIES):
        score += 10
    elif "india" in location:
        score += 7
    elif "remote" in location or "hybrid" in location or "wfh" in location:
        score += 6
    elif location.strip():
        score += 3
    else:
        score += 5  # Unknown

    # ── India filter (-40 pts penalty) ──
    is_india = any(s in location for s in _INDIA_SIGNALS)
    is_remote = any(r in location for r in ("remote", "hybrid", "wfh", "work from home"))
    is_foreign = any(s in location for s in _NON_INDIA_SIGNALS)

    if is_foreign and not is_india:
        # Clearly outside India — heavy penalty
        score -= 40
    elif not is_india and not is_remote and location.strip():
        # Unknown location, not obviously India or remote — mild penalty
        score -= 15

    # ── Company type (10 pts) ──
    if company_type in ("b2c", "d2c"):
        score += 10
    elif company_type == "b2b":
        score += 8
    elif company_type == "mixed":
        score += 6
    else:
        score += 4

    # ── Freshness (5 pts) ──
    date_posted = str(row.get("date_posted", ""))
    if date_posted and date_posted.lower() not in ("", "nan", "none"):
        score += 5
    else:
        score += 2

    return max(0, min(score, 100))


def _compute_shortlist(df: pd.DataFrame) -> pd.Series:
    """Compute the shortlist tag for each row.

    A row is tagged "Yes" when ALL four conditions hold:
      1. min_experience_years <= 10  (or experience data missing)
      2. max salary > 27 LPA        (or salary missing)
      3. title contains "marketing"  (case-insensitive)
      4. job_url not seen in master_jobs.csv (first-time scrape)

    Returns a Series of "Yes" / "No".
    """
    # ── Condition 1: experience ≤ 10 ──
    min_exp = df["min_experience_years"]
    cond_exp = min_exp.isna() | (min_exp <= 10)

    # ── Condition 2: salary > 27 LPA or missing ──
    salary_col = df.get("salary")
    if salary_col is not None:
        max_sal = salary_col.apply(_parse_salary_lpa)
        cond_sal = max_sal.isna() | (max_sal > 27)
    else:
        cond_sal = pd.Series(True, index=df.index)

    # ── Condition 3: title contains "marketing" ──
    title_col = df.get("title")
    if title_col is not None:
        cond_mkt = title_col.str.contains("marketing", case=False, na=False)
    else:
        cond_mkt = pd.Series(False, index=df.index)

    # ── Condition 4: first-time scrape ──
    previous_urls = _collect_previous_job_urls()
    job_url_col = df.get("job_url")
    if job_url_col is not None and previous_urls:
        cond_new = ~job_url_col.isin(previous_urls)
    else:
        # No previous data → everything is first-time
        cond_new = pd.Series(True, index=df.index)

    combined = cond_exp & cond_sal & cond_mkt & cond_new
    return combined.map({True: "Yes", False: "No"})


# ── Role summary generation ──────────────────────────────────────────

def _generate_summary(row) -> str:
    """Generate a crisp bullet-point summary of the role."""
    title = str(row.get("title", ""))
    company = str(row.get("company", ""))
    desc = str(row.get("description", ""))
    skills = str(row.get("skills", ""))
    location = str(row.get("location", ""))
    salary = str(row.get("salary", ""))

    bullets = []

    # Bullet 1: Role at Company (Location)
    role_line = title
    if company and company != "nan":
        role_line += f" at {company}"
    if location and location != "nan":
        role_line += f" ({location})"
    bullets.append(role_line)

    # Bullet 2: Salary if available
    if salary and salary != "nan" and salary.strip():
        bullets.append(f"Comp: {salary.strip()}")

    # Bullet 3-5: Key responsibilities from description
    if desc and desc != "nan":
        key_phrases = _extract_key_phrases(desc)
        bullets.extend(key_phrases[:3])

    # Bullet: Skills if available
    if skills and skills != "nan" and skills.strip():
        bullets.append(f"Skills: {skills.strip()}")

    return " | ".join(bullets) if bullets else title


def _extract_key_phrases(description: str) -> list[str]:
    """Extract key responsibility phrases from a job description."""
    # Clean HTML
    text = re.sub(r"<[^>]+>", " ", description)
    text = re.sub(r"\s+", " ", text).strip()

    if not text:
        return []

    phrases = []

    # Look for action-oriented sentences (verbs that signal responsibilities)
    action_verbs = [
        "lead", "manage", "drive", "develop", "build", "own",
        "create", "scale", "grow", "oversee", "define", "execute",
        "launch", "optimize", "implement", "design", "establish",
        "mentor", "hire", "collaborate", "partner", "transform",
    ]

    sentences = re.split(r"[.;•\n]", text)
    for sentence in sentences:
        sentence = sentence.strip()
        if len(sentence) < 15 or len(sentence) > 120:
            continue
        words = sentence.lower().split()
        if any(v in words[:4] for v in action_verbs):
            # Capitalize and clean
            clean = sentence[0].upper() + sentence[1:]
            phrases.append(clean)
            if len(phrases) >= 3:
                break

    # Fallback: just take first meaningful chunk of text
    if not phrases and len(text) > 30:
        chunk = text[:150].rsplit(" ", 1)[0]
        phrases.append(chunk)

    return phrases


# ── Helpers ──────────────────────────────────────────────────────────

def _combine_text(row) -> str:
    """Combine relevant text fields for classification."""
    parts = [
        str(row.get("title", "")),
        str(row.get("company", "")),
        str(row.get("description", "")),
        str(row.get("skills", "")),
    ]
    return " ".join(p for p in parts if p and p != "nan")
