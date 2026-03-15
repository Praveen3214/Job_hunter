"""Configuration and defaults for the job hunter tool."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(Path(__file__).parent / ".env")

# --- API Keys ---
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY", "")

# --- Directories ---
BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
SESSION_DIR = BASE_DIR / ".session"  # LinkedIn cookies stored here

OUTPUT_DIR.mkdir(exist_ok=True)
SESSION_DIR.mkdir(exist_ok=True)

# --- Default Search Parameters ---
DEFAULT_KEYWORDS = [
    # Tier 1 — Direct next-step roles (7-8 yr exp, Sr. Manager → Director/Head)
    "Marketing Director",
    "Head of Marketing",
    "Director of Marketing",
    "VP Marketing",
    "Head of Growth",
    "Director of Growth",
    # Tier 2 — Specialized / domain roles
    "Head of Digital Marketing",
    "Head of Brand Marketing",
    "Head of Product Marketing",
    "Marketing Lead",
    "Growth Marketing Lead",
    "Senior Marketing Manager",
    # Tier 3 — Manager-level titles
    "Marketing Manager",
    "Growth Manager",
    "Acquisition Manager",
    "Performance Marketing Manager",
    "Paid Marketing Manager",
    "Growth Marketing Manager",
    # Tier 4 — Domain combinations
    "Performance Marketing",
    "Paid Marketing",
    "Growth Marketing",
    "Paid Acquisition",
    "Performance Marketer",
    "Growth Marketer",
    "Digital Marketer",
]

DEFAULT_LOCATIONS = [
    "Delhi",
    "Noida",
    "Ghaziabad",
    "Gurugram",
    "Mumbai",
    "Bangalore",
    "Kolkata",
    "Jaipur",
    "Surat",
    "Kanpur",
    "Lucknow",
    "Chandigarh",
    "Pune",
    "India",
]

# python-jobspy experience levels
# For LinkedIn guest API: 4=Mid-Senior, 5=Director, 6=Executive
EXPERIENCE_LEVELS = ["senior", "director", "executive"]

RESULTS_PER_PLATFORM = 50
HOURS_OLD = 168  # 1 week

# --- HR Search Config ---
HR_SEARCH_TITLES = [
    "HR",
    "Recruiter",
    "Talent Acquisition",
    "People Operations",
    "HR Manager",
    "HR Director",
    "Head of HR",
    "Human Resources",
]

# LinkedIn people search query (Boolean OR)
HR_SEARCH_QUERY = "HR OR Recruiter OR \"Talent Acquisition\" OR \"People Operations\""

# Safety limits for LinkedIn scraping
MAX_HR_LOOKUPS_PER_SESSION = 50
HR_DELAY_MIN = 3  # seconds
HR_DELAY_MAX = 8  # seconds

# --- Naukri Config ---
NAUKRI_BASE_URL = "https://www.naukri.com"
NAUKRI_DELAY = 2.5  # seconds between requests
NAUKRI_MIN_EXPERIENCE = 10  # years for senior roles

# --- Cutshort Config ---
CUTSHORT_DELAY = 2.0  # seconds between requests

# --- Instahyre Config ---
INSTAHYRE_DELAY = 2.5  # seconds between requests

# --- IIMJobs Config ---
IIMJOBS_DELAY = 2.5  # seconds between requests

# --- Hirist Config ---
HIRIST_DELAY = 2.5  # seconds between requests

# --- Weekday Config ---
WEEKDAY_DELAY = 2.0  # seconds between requests

# --- Wellfound Config ---
WELLFOUND_EMAIL = os.getenv("WELLFOUND_EMAIL", "")
WELLFOUND_PASSWORD = os.getenv("WELLFOUND_PASSWORD", "")
WELLFOUND_DELAY = 3.0  # seconds between requests

# --- Crescendo Global Config ---
CRESCENDO_BASE_URL = "https://www.crescendo-global.com"
CRESCENDO_DELAY = 3.0  # seconds between requests

# --- Michael Page Config ---
MICHAELPAGE_BASE_URL = "https://www.michaelpage.co.in"
MICHAELPAGE_DELAY = 2.0  # seconds between requests

# --- SutraHR Config ---
SUTRAHR_BASE_URL = "https://www.sutrahr.com"
SUTRAHR_DELAY = 2.0  # seconds between requests

# --- Antal International Config ---
ANTAL_BASE_URL = "https://www.antal.com"
ANTAL_DELAY = 2.5  # seconds between requests

# --- CIEL HR Config ---
CIELHR_BASE_URL = "https://www.cielhr.com"
CIELHR_DELAY = 2.5  # seconds between requests

# --- TopGear Consultants Config ---
TOPGEAR_BASE_URL = "http://jobs.topgearconsultants.com"
TOPGEAR_DELAY = 2.5  # seconds between requests

# --- ABC Consultants Config ---
ABC_BASE_URL = "https://www.abcconsultants.in"
ABC_DELAY = 3.0  # seconds between requests

# --- Korn Ferry Config ---
KORNFERRY_BASE_URL = "https://jobs.candidate.kornferry.com"
KORNFERRY_DELAY = 3.0  # seconds between requests

# --- Hunter.io Config ---
HUNTER_FREE_CREDITS = 50
HUNTER_DEPARTMENT = "human-resources"

# --- Target Companies (proactive HR search — not tied to scraped jobs) ---
# Add companies you want to track for TA/HR contacts, even if no job posted yet.
# Used by --target-companies flag. Edit this list freely.
TARGET_COMPANIES = [
    # Fintech / Payments
    "Razorpay", "CRED", "Groww", "PhonePe", "Cashfree",
    "Slice", "Fi Money", "Jupiter", "Paytm", "Pine Labs",
    # NBFC / Lending
    "Lendingkart", "KreditBee", "Navi", "Rupeek",
    # E-commerce / D2C
    "Meesho", "Zepto", "Blinkit", "Swiggy", "Zomato",
]

# --- Recruiting Firms (for company-filtered post search) ---
# LinkedIn company slugs → used by --find-firm-posts to search posts
# from employees of these firms. Slug is the last part of the company URL.
RECRUITING_FIRMS = [
    {"name": "Crescendo Global",      "slug": "crescendo-global"},
    {"name": "Michael Page",          "slug": "michael-page"},
    {"name": "ABC Consultants",       "slug": "abc-consultants"},
    {"name": "Antal International",   "slug": "antal-international"},
    {"name": "SutraHR",               "slug": "sutra-services-pvt-ltd-"},
    {"name": "Korn Ferry",            "slug": "kornferry"},
    {"name": "Heidrick & Struggles",  "slug": "heidrick-and-struggles"},
    {"name": "CIEL HR",               "slug": "cielhr"},
    {"name": "Stanton Chase",         "slug": "stanton-chase-international"},
    {"name": "TopGear Consultants",   "slug": "topgear-consultants-ltd"},
]

# Post keywords to filter — posts from firm employees must contain these
FIRM_POST_KEYWORDS = [
    "hiring", "looking for", "mandate", "open role", "open position",
    "requirement", "urgent", "immediate", "walk-in",
]

# --- Company/Role Classification Keywords ---
B2B_SIGNALS = [
    "b2b", "saas", "enterprise", "business solutions", "crm",
    "erp", "cloud platform", "api", "developer tools", "devops",
    "sales enablement", "lead generation", "demand generation",
    "account-based", "abm", "pipeline", "revenue marketing",
]
B2C_SIGNALS = [
    "b2c", "consumer", "e-commerce", "ecommerce", "retail",
    "fashion", "food", "travel", "hospitality", "gaming",
    "entertainment", "media", "social", "app", "marketplace",
    "lifestyle", "beauty", "health", "fitness", "edtech",
    "brand marketing", "consumer marketing", "brand awareness",
]
D2C_SIGNALS = [
    "d2c", "dtc", "direct-to-consumer", "direct to consumer",
    "shopify", "online store", "own brand", "brand building",
    "performance marketing", "facebook ads", "meta ads",
    "google ads", "influencer", "retention marketing",
]
