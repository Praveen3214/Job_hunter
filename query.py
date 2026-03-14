#!/usr/bin/env python3
"""
Query scraped job data from the command line.

Usage:
    # Interactive mode (prompts for each filter)
    python query.py

    # Direct filters via flags
    python query.py --role "Marketing Director" --location "Bangalore" --salary "20L+" --type remote

    # Combine filters
    python query.py -r "CMO" -l "Mumbai" -s "10L-50L" -t onsite -p naukri

    # Show all jobs, sorted by salary descending
    python query.py --sort salary --desc

    # Export filtered results to a new CSV
    python query.py -r "VP Marketing" -l "India" --export filtered_vp.csv
"""

import argparse
import io
import os
import re
import sys
from pathlib import Path

# Fix Windows console encoding for special characters
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from config import OUTPUT_DIR

# ── Terminal colors ──────────────────────────────────────────────
class C:
    BOLD = "\033[1m"
    DIM = "\033[2m"
    PURPLE = "\033[35m"
    CYAN = "\033[36m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    BLUE = "\033[34m"
    RESET = "\033[0m"
    UNDERLINE = "\033[4m"


# ── Salary parsing ───────────────────────────────────────────────
def parse_salary_value(salary_str, min_amt=None, max_amt=None, currency=None):
    """Parse salary into a numeric value in a common unit (lacs for INR, raw for USD)."""
    if min_amt and not pd.isna(min_amt):
        return float(min_amt), currency or "USD"
    if max_amt and not pd.isna(max_amt):
        return float(max_amt), currency or "USD"
    if not salary_str or str(salary_str) in ("nan", "NaN", ""):
        return None, None

    s = str(salary_str)

    # Indian: "30-50 Lacs", "₹30L", "10 LPA"
    lac_match = re.search(r"([\d,.]+)\s*[-–to]*\s*([\d,.]*)\s*(Lacs?|Lakhs?|LPA|L)\b", s, re.I)
    if lac_match:
        high = lac_match.group(2) or lac_match.group(1)
        return float(high.replace(",", "")), "INR_LAC"

    # Crore
    cr_match = re.search(r"([\d,.]+)\s*[-–to]*\s*([\d,.]*)\s*Cr", s, re.I)
    if cr_match:
        high = cr_match.group(2) or cr_match.group(1)
        return float(high.replace(",", "")) * 100, "INR_LAC"

    # USD: "$100,000"
    usd_match = re.search(r"\$?\s*([\d,]+(?:\.\d+)?)\s*[-–to]*\s*\$?\s*([\d,]*(?:\.\d+)?)", s)
    if usd_match:
        high = usd_match.group(2) or usd_match.group(1)
        val = float(high.replace(",", ""))
        if val > 0:
            return val, "USD"

    return None, None


def parse_salary_filter(filter_str):
    """
    Parse salary filter string into (min_val, max_val, currency).
    Formats: "20L+", "10L-50L", "50K-100K", "$100K+", "1Cr+"
    """
    if not filter_str:
        return None, None, None

    s = filter_str.strip()

    # Lac ranges: "10L-50L", "20L+"
    lac_range = re.match(r"(\d+)\s*L\s*[-–to]+\s*(\d+)\s*L", s, re.I)
    if lac_range:
        return float(lac_range.group(1)), float(lac_range.group(2)), "INR_LAC"

    lac_plus = re.match(r"(\d+)\s*L\+?$", s, re.I)
    if lac_plus:
        return float(lac_plus.group(1)), None, "INR_LAC"

    # Crore
    cr_match = re.match(r"(\d+)\s*Cr\+?$", s, re.I)
    if cr_match:
        return float(cr_match.group(1)) * 100, None, "INR_LAC"

    # USD: "$50K-100K", "100K+", "$200K+"
    usd_range = re.match(r"\$?(\d+)\s*K\s*[-–to]+\s*\$?(\d+)\s*K", s, re.I)
    if usd_range:
        return float(usd_range.group(1)) * 1000, float(usd_range.group(2)) * 1000, "USD"

    usd_plus = re.match(r"\$?(\d+)\s*K\+?$", s, re.I)
    if usd_plus:
        return float(usd_plus.group(1)) * 1000, None, "USD"

    return None, None, None


# ── Experience parsing ───────────────────────────────────────────
def parse_exp_years(exp_str):
    """Parse experience string into (low, high) years."""
    if not exp_str or str(exp_str) in ("nan", "NaN", ""):
        return None, None
    s = str(exp_str)
    m = re.search(r"(\d+)\s*[-–+to]*\s*(\d*)\s*(?:Yrs?|years?)?", s, re.I)
    if m:
        low = int(m.group(1))
        high = int(m.group(2)) if m.group(2) else low
        return low, high
    return None, None


# ── Work type detection ──────────────────────────────────────────
def detect_work_type(row):
    """Detect remote/hybrid/onsite from row data."""
    is_remote = row.get("is_remote")
    if is_remote is True or str(is_remote).lower() == "true":
        return "remote"

    text = " ".join(str(row.get(c, "")) for c in ["title", "location", "description"]).lower()

    if "hybrid" in text:
        return "hybrid"
    if "remote" in text or "work from home" in text or "wfh" in text:
        return "remote"
    return "onsite"


# ── Find latest CSV ──────────────────────────────────────────────
def find_latest_csv(output_dir: Path, prefix: str = "jobs_") -> Path | None:
    csvs = sorted(
        output_dir.glob(f"{prefix}*.csv"),
        key=lambda f: os.path.getmtime(f),
        reverse=True,
    )
    return csvs[0] if csvs else None


# ── Display helpers ──────────────────────────────────────────────
def truncate(s, n):
    s = str(s) if s and str(s) not in ("nan", "NaN") else "-"
    return s[:n-1] + "…" if len(s) > n else s


def format_work_type(wt):
    colors = {"remote": C.GREEN, "hybrid": C.YELLOW, "onsite": C.BLUE}
    return f"{colors.get(wt, '')}{wt.upper()}{C.RESET}"


def platform_color(p):
    p = str(p).lower()
    if "linkedin" in p:
        return C.BLUE
    if "indeed" in p:
        return C.PURPLE
    if "naukri" in p:
        return C.CYAN
    if "cutshort" in p:
        return C.YELLOW
    if "instahyre" in p:
        return C.BLUE
    if "wellfound" in p:
        return C.GREEN
    if "iimjobs" in p:
        return C.YELLOW
    if "hirist" in p:
        return C.PURPLE
    if "weekday" in p:
        return C.GREEN
    return ""


def print_table(df, page=1, per_page=15):
    """Print a formatted table of jobs."""
    total = len(df)
    start = (page - 1) * per_page
    end = min(start + per_page, total)
    page_df = df.iloc[start:end]

    # Header
    print(f"\n{C.DIM}{'─' * 130}{C.RESET}")
    print(
        f"  {C.BOLD}{'#':>3}  {'PLATFORM':<10} {'TITLE':<35} {'COMPANY':<22} "
        f"{'LOCATION':<18} {'SALARY':<14} {'EXP':<10} {'TYPE':<8}{C.RESET}"
    )
    print(f"{C.DIM}{'─' * 130}{C.RESET}")

    # Rows
    for i, (_, row) in enumerate(page_df.iterrows(), start=start + 1):
        plat = str(row.get("platform", "-"))
        pc = platform_color(plat)
        wt = detect_work_type(row)

        print(
            f"  {C.DIM}{i:>3}{C.RESET}  "
            f"{pc}{truncate(plat, 10):<10}{C.RESET} "
            f"{C.BOLD}{truncate(row.get('title', '-'), 35):<35}{C.RESET} "
            f"{truncate(row.get('company', '-'), 22):<22} "
            f"{truncate(row.get('location', '-'), 18):<18} "
            f"{truncate(row.get('salary', '-'), 14):<14} "
            f"{truncate(row.get('experience', '-'), 10):<10} "
            f"{format_work_type(wt)}"
        )

    print(f"{C.DIM}{'─' * 130}{C.RESET}")
    total_pages = max(1, (total + per_page - 1) // per_page)
    print(f"  {C.DIM}Page {page}/{total_pages} | Showing {start+1}-{end} of {total} results{C.RESET}")


def print_summary(df):
    """Print filter summary stats."""
    total = len(df)
    companies = df["company"].nunique() if "company" in df.columns else 0
    platforms = df["platform"].nunique() if "platform" in df.columns else 0
    remote_count = sum(1 for _, r in df.iterrows() if detect_work_type(r) == "remote")
    hybrid_count = sum(1 for _, r in df.iterrows() if detect_work_type(r) == "hybrid")

    print(f"\n  {C.BOLD}{C.PURPLE}Query Results{C.RESET}")
    print(f"  {C.DIM}{'─' * 40}{C.RESET}")
    print(f"  Jobs matched:   {C.BOLD}{total}{C.RESET}")
    print(f"  Companies:      {companies}")
    print(f"  Platforms:      {platforms}")
    print(f"  Remote:         {C.GREEN}{remote_count}{C.RESET}  |  Hybrid: {C.YELLOW}{hybrid_count}{C.RESET}  |  Onsite: {C.BLUE}{total - remote_count - hybrid_count}{C.RESET}")


# ── Filter pipeline ──────────────────────────────────────────────
def apply_filters(df, role=None, location=None, salary=None, work_type=None, company=None, platform=None):
    """Apply all filters and return filtered DataFrame."""
    filtered = df.copy()

    # Platform
    if platform:
        plat_lower = platform.lower()
        filtered = filtered[
            filtered["platform"].fillna("").str.lower().str.contains(plat_lower, regex=False)
        ]

    # Role / job title
    if role:
        pattern = role.lower()
        filtered = filtered[
            filtered["title"].fillna("").str.lower().str.contains(pattern, regex=False)
        ]

    # Company
    if company:
        pattern = company.lower()
        filtered = filtered[
            filtered["company"].fillna("").str.lower().str.contains(pattern, regex=False)
        ]

    # Location
    if location:
        loc_lower = location.lower()
        # Support "india" as a meta-location
        india_cities = [
            "india", "bangalore", "bengaluru", "mumbai", "delhi", "new delhi",
            "hyderabad", "chennai", "pune", "kolkata", "noida", "gurgaon",
            "gurugram", "ahmedabad", "jaipur", "kochi", "lucknow", "indore",
            "chandigarh", "bhopal", "coimbatore", "karnataka", "maharashtra",
            "telangana", "tamil nadu", "haryana", "uttar pradesh", "rajasthan",
            "gujarat", "kerala", "goa", "madhya pradesh", "andhra pradesh",
        ]
        if loc_lower == "india":
            filtered = filtered[
                filtered["location"].fillna("").str.lower().apply(
                    lambda x: any(c in x for c in india_cities)
                )
            ]
        else:
            filtered = filtered[
                filtered["location"].fillna("").str.lower().str.contains(loc_lower, regex=False)
            ]

    # Salary
    if salary:
        s_min, s_max, s_cur = parse_salary_filter(salary)
        if s_min is not None:
            def salary_match(row):
                val, cur = parse_salary_value(
                    row.get("salary"), row.get("min_amount"), row.get("max_amount"), row.get("currency")
                )
                if val is None:
                    return False
                # Currency mismatch — skip
                if s_cur and cur and s_cur != cur:
                    return False
                if s_max is not None:
                    return s_min <= val <= s_max
                return val >= s_min

            filtered = filtered[filtered.apply(salary_match, axis=1)]

    # Work type: remote / hybrid / onsite
    if work_type:
        wt_lower = work_type.lower()
        filtered = filtered[
            filtered.apply(lambda r: detect_work_type(r) == wt_lower, axis=1)
        ]

    return filtered.reset_index(drop=True)


def sort_data(df, sort_key, descending=False):
    """Sort DataFrame by a column key."""
    col_map = {
        "title": "title", "role": "title",
        "company": "company",
        "location": "location",
        "salary": "salary",
        "platform": "platform",
        "experience": "experience", "exp": "experience",
        "date": "date_posted", "posted": "date_posted",
    }
    col = col_map.get(sort_key.lower(), sort_key)
    if col not in df.columns:
        print(f"  {C.RED}Unknown sort column: {sort_key}{C.RESET}")
        print(f"  Available: {', '.join(col_map.keys())}")
        return df

    return df.sort_values(col, ascending=not descending, na_position="last").reset_index(drop=True)


# ── Interactive mode ─────────────────────────────────────────────
def interactive_mode(df):
    """Interactive query prompt."""
    print(f"\n{C.BOLD}{C.PURPLE}  ╔══════════════════════════════════════════╗{C.RESET}")
    print(f"{C.BOLD}{C.PURPLE}  ║       JOB HUNTER — Interactive Query     ║{C.RESET}")
    print(f"{C.BOLD}{C.PURPLE}  ╚══════════════════════════════════════════╝{C.RESET}")
    print(f"  {C.DIM}Loaded {len(df)} jobs. Press Enter to skip a filter.{C.RESET}")
    print(f"  {C.DIM}Type 'q' at any prompt to quit.{C.RESET}\n")

    def ask(prompt, examples=""):
        hint = f" {C.DIM}({examples}){C.RESET}" if examples else ""
        val = input(f"  {C.CYAN}?{C.RESET} {prompt}{hint}: ").strip()
        if val.lower() == "q":
            print(f"\n  {C.DIM}Bye!{C.RESET}\n")
            sys.exit(0)
        return val or None

    platform = ask("Platform", "linkedin / indeed / naukri / cutshort / instahyre / wellfound / iimjobs / hirist / weekday")
    role = ask("Job Role", "e.g. CMO, Marketing Director, VP Marketing")
    location = ask("Location", "e.g. Bangalore, India, Remote, Mumbai")
    salary = ask("Salary range", "e.g. 20L+, 10L-50L, $100K+, 1Cr+")
    work_type_str = ask("Work type", "remote / hybrid / onsite")

    # Validate work type
    work_type = None
    if work_type_str:
        wt = work_type_str.lower()
        if wt in ("remote", "hybrid", "onsite", "on-site", "office"):
            work_type = "onsite" if wt in ("on-site", "office") else wt
        else:
            print(f"  {C.YELLOW}Unknown work type '{work_type_str}', skipping filter{C.RESET}")

    sort_key = ask("Sort by", "title, company, salary, location, platform, date")
    desc = False
    if sort_key:
        order = ask("Order", "asc / desc")
        desc = order and order.lower().startswith("d")

    # Apply
    filtered = apply_filters(df, role=role, location=location, salary=salary, work_type=work_type, platform=platform)

    if sort_key:
        filtered = sort_data(filtered, sort_key, descending=desc)

    if filtered.empty:
        print(f"\n  {C.RED}No jobs matched your filters.{C.RESET}")
        return

    print_summary(filtered)
    page = 1
    per_page = 15

    while True:
        print_table(filtered, page=page, per_page=per_page)
        total_pages = max(1, (len(filtered) + per_page - 1) // per_page)

        print(f"\n  {C.DIM}[n]ext  [p]rev  [e]xport  [o]pen #  [q]uit{C.RESET}")
        cmd = input(f"  {C.CYAN}>{C.RESET} ").strip().lower()

        if cmd in ("n", "next") and page < total_pages:
            page += 1
        elif cmd in ("p", "prev") and page > 1:
            page -= 1
        elif cmd in ("e", "export"):
            export_path = OUTPUT_DIR / "query_results.csv"
            filtered.to_csv(export_path, index=False, encoding="utf-8-sig")
            print(f"  {C.GREEN}Exported to: {export_path}{C.RESET}")
        elif cmd.startswith("o") or cmd.isdigit():
            num = cmd.lstrip("o").strip()
            if num.isdigit():
                idx = int(num) - 1
                if 0 <= idx < len(filtered):
                    url = filtered.iloc[idx].get("job_url", "")
                    if url and str(url).startswith("http"):
                        import webbrowser
                        webbrowser.open(str(url))
                        print(f"  {C.GREEN}Opened in browser{C.RESET}")
                    else:
                        print(f"  {C.YELLOW}No URL for this job{C.RESET}")
                else:
                    print(f"  {C.RED}Invalid number{C.RESET}")
        elif cmd in ("q", "quit", "exit"):
            break
        elif cmd == "":
            if page < total_pages:
                page += 1
            else:
                break


# ── CLI mode ─────────────────────────────────────────────────────
def cli_mode(args, df):
    """Direct CLI flag mode."""
    filtered = apply_filters(
        df,
        role=args.role,
        location=args.location,
        salary=args.salary,
        work_type=args.type,
        company=args.company,
        platform=args.platform,
    )

    if args.sort:
        filtered = sort_data(filtered, args.sort, descending=args.desc)

    if filtered.empty:
        print(f"\n  {C.RED}No jobs matched your filters.{C.RESET}")
        return

    print_summary(filtered)
    print_table(filtered, page=1, per_page=args.limit)

    if args.export:
        export_path = Path(args.export)
        if not export_path.is_absolute():
            export_path = OUTPUT_DIR / export_path
        filtered.to_csv(export_path, index=False, encoding="utf-8-sig")
        print(f"\n  {C.GREEN}Exported to: {export_path}{C.RESET}")

    # Show pagination hint if more results
    if len(filtered) > args.limit:
        remaining = len(filtered) - args.limit
        print(f"\n  {C.DIM}+{remaining} more results. Use --limit {len(filtered)} to see all, or run without flags for interactive mode.{C.RESET}")


# ── Main ─────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Query scraped job data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("-r", "--role", type=str, help="Filter by job title (e.g. 'CMO', 'Marketing Director')")
    parser.add_argument("-l", "--location", type=str, help="Filter by location (e.g. 'Bangalore', 'India', 'Remote')")
    parser.add_argument("-s", "--salary", type=str, help="Filter by salary range (e.g. '20L+', '10L-50L', '$100K+')")
    parser.add_argument("-t", "--type", type=str, choices=["remote", "hybrid", "onsite"], help="Filter by work type")
    parser.add_argument("-p", "--platform", type=str, help="Filter by platform (linkedin, indeed, naukri, cutshort, instahyre, wellfound, iimjobs, hirist, weekday)")
    parser.add_argument("-c", "--company", type=str, help="Filter by company name")
    parser.add_argument("--sort", type=str, help="Sort by: title, company, salary, location, platform, date")
    parser.add_argument("--desc", action="store_true", help="Sort descending")
    parser.add_argument("--limit", type=int, default=20, help="Max rows to display (default: 20)")
    parser.add_argument("--export", type=str, help="Export filtered results to CSV file")
    parser.add_argument("-f", "--file", type=str, help="Path to jobs CSV (default: latest in output/)")

    args = parser.parse_args()

    # Load data
    if args.file:
        csv_path = Path(args.file)
    else:
        csv_path = find_latest_csv(OUTPUT_DIR)

    if not csv_path or not csv_path.exists():
        print(f"\n  {C.RED}No jobs CSV found in {OUTPUT_DIR}{C.RESET}")
        print(f"  Run {C.BOLD}python main.py{C.RESET} first to scrape jobs.")
        return

    print(f"\n  {C.DIM}Loading: {csv_path.name}{C.RESET}")
    df = pd.read_csv(csv_path)
    print(f"  {C.DIM}{len(df)} jobs loaded{C.RESET}")

    # If no filter flags given, enter interactive mode
    has_filters = any([args.role, args.location, args.salary, args.type, args.company, args.platform])
    if not has_filters and not args.sort:
        interactive_mode(df)
    else:
        cli_mode(args, df)


if __name__ == "__main__":
    main()
