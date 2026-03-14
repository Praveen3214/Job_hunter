# Job Hunter

A multi-platform job scraping toolkit with a browser-based dashboard. Scrapes **9 platforms** (LinkedIn, Indeed, Naukri, Cutshort, Instahyre, Wellfound, IIMJobs, Hirist, Weekday), finds who's actively hiring (#Hiring profiles + post scanning), discovers recruitment consultants/agencies, finds HR contacts at hiring companies, enriches with emails, and lets you filter/explore everything from a visual dashboard.

## Quick Start

```bash
# 1. Clone or unzip the project
cd job_hunter

# 2. Create a virtual environment (recommended)
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Install Playwright browsers (needed for Naukri, Instahyre, Wellfound, IIMJobs, Hirist & HR finder)
playwright install chromium

# 5. Set up your .env (optional)
copy .env.example .env   # Windows
cp .env.example .env     # macOS/Linux
# Then edit .env:
#   - HUNTER_API_KEY: for email enrichment (free at hunter.io)
#   - WELLFOUND_EMAIL/PASSWORD: for Wellfound scraping (free account)

# 6. Run!
python main.py
```

## Supported Platforms

| Platform | Method | Login? | Notes |
|----------|--------|--------|-------|
| **LinkedIn** | python-jobspy (guest API) | No | Rate-limited; no login needed |
| **Indeed** | python-jobspy | No | Country auto-detected |
| **Naukri** | Playwright browser | No | India-focused, min experience filter |
| **Cutshort** | HTTP + SSR parsing | No | Fast, no browser needed |
| **Instahyre** | Playwright browser | No | India startup ecosystem |
| **Wellfound** | Playwright + login | **Yes** | Free account required (set in .env) |
| **IIMJobs** | Playwright browser | No | Premium/MBA-level roles |
| **Hirist** | Playwright browser | No | Tech & premium roles (by IIMJobs) |
| **Weekday** | HTTP + SSR parsing | No | Fast, Playwright fallback if needed |

## Usage

### Option A: CLI (full pipeline)

```bash
# Basic scrape with defaults (all 9 platforms, 7 keywords, 6 locations)
python main.py

# Custom keywords and locations
python main.py -k "Product Manager,VP Product" -l "Bangalore,Remote"

# Scrape specific platforms only
python main.py -p linkedin,cutshort,iimjobs -n 25 --hours-old 72

# Full pipeline: scrape + find HR contacts + enrich emails
python main.py --find-hr --enrich-emails

# Open dashboard after scraping
python main.py --dashboard
```

### Option B: Browser Dashboard (recommended)

```bash
# Start the dashboard server
python serve_dashboard.py

# Open http://127.0.0.1:8056 in your browser
```

The dashboard gives you:
- **Scrape Builder** — configure and run scrapes from the browser (select platforms, keywords, locations)
- **Query Builder** — filter results by role, location, platform, salary, work type, company type, role type
- **Data Tabs** — browse Jobs, Summary, and HR Contacts tables
- **Enrichment columns** — company type (B2B/B2C/D2C), role type, experience required, role summary
- **Export** — download filtered results as CSV

### Option C: Query Tool (explore saved data)

```bash
# Interactive mode (guided prompts)
python query.py

# CLI filters
python query.py -r "Marketing Director" -l "Mumbai" -p cutshort
```

## CLI Flags

| Flag | Short | Description | Default |
|------|-------|-------------|---------|
| `--keywords` | `-k` | Job title keywords (comma-separated) | 12 senior marketing roles |
| `--locations` | `-l` | Locations (comma-separated) | India + 5 cities + Remote |
| `--platforms` | `-p` | Platforms: linkedin, indeed, naukri, cutshort, instahyre, wellfound, iimjobs, hirist, weekday | All nine |
| `--max-results` | `-n` | Max results per platform per search | 50 |
| `--hours-old` | | Only jobs posted within N hours | 168 (7 days) |
| `--find-hiring` | | Find LinkedIn profiles with #Hiring badge | Off |
| `--find-posts` | | Scan LinkedIn posts for hiring hashtags | Off |
| `--find-consultants` | | Find recruitment consultants (keyword-tied) | Off |
| `--consultants-broad` | | Find ALL recruiters in locations (generic) | Off |
| `--find-hr` | | Find HR contacts at hiring companies | Off |
| `--enrich-emails` | | Enrich HR contacts with emails via Hunter.io | Off |
| `--dashboard` | | Open dashboard after scraping | Off |

## Enrichment Columns

Every scraped job is automatically enriched with:

| Column | Values | How it works |
|--------|--------|-------------|
| `company_type` | B2B, B2C, D2C, Mixed, Unknown | Keyword analysis of title + description + company name |
| `role_type` | B2B, B2C, D2C, Growth, Brand, General | Title + description signal detection |
| `experience_required` | e.g. "10-15 years" | Extracted from experience field or description text |
| `role_summary` | Pipe-separated bullet points | Key responsibilities + comp + skills at a glance |

## Project Structure

```
job_hunter/
├── main.py                 # CLI entry point (scrape pipeline)
├── query.py                # CLI query tool (filter saved data)
├── serve_dashboard.py      # Dashboard web server with scrape API
├── config.py               # Settings & defaults
├── dashboard.html          # Dashboard UI (single-file frontend)
├── guide.html              # User guide (open at /guide.html)
├── requirements.txt        # Python dependencies
├── .env.example            # API key + credential template
├── .gitignore              # Git ignore rules
├── scrapers/
│   ├── linkedin_indeed.py  # LinkedIn + Indeed via python-jobspy
│   ├── naukri.py           # Naukri.com via Playwright
│   ├── cutshort.py         # Cutshort.io via SSR parsing
│   ├── instahyre.py        # Instahyre.com via Playwright
│   ├── wellfound.py        # Wellfound.com via Playwright + auth
│   ├── iimjobs.py          # IIMJobs.com via Playwright
│   ├── hirist.py           # Hirist.tech via Playwright
│   └── weekday.py          # Weekday.works via SSR + Playwright fallback
├── hr_finder/
│   ├── linkedin_people.py  # LinkedIn people search for HR contacts
│   ├── hiring_signals.py   # #Hiring profiles + posts + consultant finder
│   └── email_enricher.py   # Hunter.io email enrichment
├── utils/
│   ├── export.py           # CSV export (jobs, signals, consultants, HR, summary)
│   └── enricher.py         # Job data enrichment (company/role type, summary)
├── output/                 # Scraped CSV files (auto-created)
└── .session/               # Login cookies (auto-created)
```

## Configuration

Edit `config.py` to change defaults:
- **DEFAULT_KEYWORDS** — job title search terms
- **DEFAULT_LOCATIONS** — target locations
- **RESULTS_PER_PLATFORM** — max results per platform per keyword
- **HOURS_OLD** — how far back to look for job posts (in hours)
- **EXPERIENCE_LEVELS** — seniority filter for LinkedIn
- **B2B_SIGNALS / B2C_SIGNALS / D2C_SIGNALS** — keywords for company/role classification

## Requirements

- **Python 3.9+**
- **pip** (comes with Python)
- **Internet connection** (for scraping)
- **Hunter.io API key** (optional, for email enrichment — [free: 50 lookups/month](https://hunter.io))
- **Wellfound account** (optional, for Wellfound scraping — [free signup](https://wellfound.com))

## Notes

- LinkedIn & Indeed use python-jobspy (no login needed)
- Naukri, Instahyre, IIMJobs, Hirist use Playwright in visible browser mode (no login needed)
- Cutshort, Weekday use plain HTTP requests (fastest scrapers, no browser — Weekday falls back to Playwright if needed)
- Wellfound requires a free account — set `WELLFOUND_EMAIL` and `WELLFOUND_PASSWORD` in `.env`
- First Wellfound run may need manual CAPTCHA solve in the browser window
- IIMJobs and Hirist are from the same company — Hirist targets tech roles, IIMJobs targets MBA/premium roles
- Output CSVs are timestamped so you never overwrite previous scrapes
- The dashboard auto-detects all CSV files in `output/`

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `ModuleNotFoundError` | Run `pip install -r requirements.txt` |
| Playwright browser not found | Run `playwright install chromium` |
| Port 8056 already in use | Kill the other process or change port in `serve_dashboard.py` |
| No results from LinkedIn | LinkedIn may rate-limit; wait a few minutes and retry |
| Hunter.io "unauthorized" | Check your API key in `.env` |
| Naukri returns empty | Naukri blocks aggressive scraping; increase `NAUKRI_DELAY` in config |
| Cutshort returns empty | Cutshort may have changed SSR structure; check for `__NEXT_DATA__` |
| Instahyre returns empty | Try with fewer keywords; site may rate-limit |
| Wellfound login fails | Check credentials in `.env`; may need manual CAPTCHA on first run |
| IIMJobs/Hirist returns empty | Sites may block rapid requests; increase delay in config |
| Weekday returns empty | Try fewer keywords; HTTP scraper may fall back to Playwright |

## User Guide

For the full illustrated guide, start the dashboard and visit:
```
http://127.0.0.1:8056/guide.html
```
