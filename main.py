#!/usr/bin/env python3
"""
Job Hunter CLI — Search senior marketing roles and find HR contacts.

Usage:
    # Search all platforms with defaults
    python main.py

    # Custom search
    python main.py -k "CMO,Marketing Director" -l "Bangalore,Mumbai" -p linkedin,naukri -n 25

    # Full pipeline with HR search and email enrichment
    python main.py -k "VP Marketing" -l "India" --find-hr --enrich-emails

    # Only find HR contacts (uses last saved job results)
    python main.py --find-hr --jobs-file output/jobs_20260311.csv

    # Only enrich emails (uses last saved HR contacts)
    python main.py --enrich-emails --hr-file output/hr_contacts_20260311.csv
"""

import argparse
import os
import subprocess
import sys
import threading
import webbrowser
from pathlib import Path

# Add project root to path so imports work
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd

from config import (
    DEFAULT_KEYWORDS,
    DEFAULT_LOCATIONS,
    RESULTS_PER_PLATFORM,
    HOURS_OLD,
    OUTPUT_DIR,
)
from scrapers.linkedin_indeed import search_linkedin_indeed
from scrapers.naukri import search_naukri
from scrapers.cutshort import search_cutshort
from scrapers.instahyre import search_instahyre
from scrapers.wellfound import search_wellfound
from scrapers.iimjobs import search_iimjobs
from scrapers.hirist import search_hirist
from scrapers.weekday import search_weekday
from scrapers.crescendo import search_crescendo
from scrapers.michaelpage import search_michaelpage
from scrapers.sutrahr import search_sutrahr
from scrapers.antal import search_antal
from scrapers.cielhr import search_cielhr
from scrapers.topgear import search_topgear
from scrapers.abcconsultants import search_abcconsultants
from scrapers.kornferry import search_kornferry
from hr_finder.linkedin_people import find_hr_contacts
from hr_finder.email_enricher import enrich_with_emails
from scrapers.utils import filter_india_jobs
from utils.export import export_jobs, export_hr_contacts, export_summary
from utils.enricher import enrich_jobs


def parse_args():
    parser = argparse.ArgumentParser(
        description="Search marketing roles and find HR contacts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "-k", "--keywords",
        type=str,
        default=None,
        help="Comma-separated job title keywords (default: VP Marketing, CMO, etc.)",
    )
    parser.add_argument(
        "-l", "--location",
        type=str,
        default=None,
        help="Comma-separated locations (default: India, Bangalore, Mumbai, etc.)",
    )
    parser.add_argument(
        "-p", "--platforms",
        type=str,
        default="all",
        help="Platforms: linkedin, indeed, naukri, cutshort, instahyre, wellfound, iimjobs, hirist, weekday, crescendo, michaelpage, sutrahr, antal, cielhr, topgear, abcconsultants, kornferry, or 'all' (default: all)",
    )
    parser.add_argument(
        "-n", "--max-results",
        type=int,
        default=RESULTS_PER_PLATFORM,
        help=f"Max results per platform per search (default: {RESULTS_PER_PLATFORM})",
    )
    parser.add_argument(
        "--hours-old",
        type=int,
        default=None,
        help=f"Only jobs posted within this many hours (default: {HOURS_OLD})",
    )
    parser.add_argument(
        "--find-hiring",
        action="store_true",
        help="Find people on LinkedIn who are actively hiring (#Hiring) for the given keywords",
    )
    parser.add_argument(
        "--find-posts",
        action="store_true",
        help="Scan LinkedIn posts for hiring hashtags (#hiring, #wearehiring, #jobalert, etc.)",
    )
    parser.add_argument(
        "--find-consultants",
        action="store_true",
        help="Find recruitment consultants/agencies on LinkedIn (focused: tied to keywords)",
    )
    parser.add_argument(
        "--consultants-broad",
        action="store_true",
        help="Broad consultant search: recruiter titles x locations only (ignores keywords)",
    )
    parser.add_argument(
        "--find-firm-posts",
        action="store_true",
        help="Search LinkedIn posts from employees of known recruiting firms (config.py RECRUITING_FIRMS)",
    )
    parser.add_argument(
        "--target-companies",
        action="store_true",
        help="Search for HR/TA contacts at target companies from config.py (independent of scraped jobs)",
    )
    parser.add_argument(
        "--find-hr",
        action="store_true",
        help="Search LinkedIn for HR contacts at companies found in job results",
    )
    parser.add_argument(
        "--enrich-emails",
        action="store_true",
        help="Use Hunter.io to find HR email addresses",
    )
    parser.add_argument(
        "--jobs-file",
        type=str,
        default=None,
        help="Path to existing jobs CSV (skip scraping, use for --find-hr)",
    )
    parser.add_argument(
        "--hr-file",
        type=str,
        default=None,
        help="Path to existing HR contacts CSV (skip HR search, use for --enrich-emails)",
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=str,
        default=str(OUTPUT_DIR),
        help=f"Output directory (default: {OUTPUT_DIR})",
    )
    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Launch the web dashboard to view results (auto-loads latest CSVs)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8050,
        help="Port for dashboard server (default: 8050)",
    )
    return parser.parse_args()


def launch_dashboard(port: int):
    """Launch the full-featured dashboard server (serve_dashboard.py) + open browser."""
    base_dir = Path(__file__).parent
    server_script = base_dir / "serve_dashboard.py"

    url = f"http://127.0.0.1:{port}"
    print(f"\n  Starting dashboard at: {url}")
    print(f"  Press Ctrl+C to stop\n")

    env = {**os.environ, "DASHBOARD_PORT": str(port)}
    proc = subprocess.Popen(
        [sys.executable, str(server_script)],
        cwd=str(base_dir),
        env=env,
    )

    # Open browser after a short delay
    threading.Timer(1.0, lambda: webbrowser.open(url)).start()

    try:
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        print("\n  Dashboard stopped.")


def main():
    args = parse_args()

    # Dashboard-only mode
    if args.dashboard and not args.keywords and not args.jobs_file:
        launch_dashboard(args.port)
        return

    # Parse inputs
    keywords = [k.strip() for k in args.keywords.split(",")] if args.keywords else DEFAULT_KEYWORDS
    locations = [l.strip() for l in args.location.split(",")] if args.location else DEFAULT_LOCATIONS

    platform_input = args.platforms.lower().strip()
    if platform_input == "all":
        platforms = ["linkedin", "indeed", "naukri", "cutshort", "instahyre", "wellfound", "iimjobs", "hirist", "weekday", "crescendo", "michaelpage", "sutrahr", "antal", "cielhr", "topgear", "abcconsultants", "kornferry"]
    else:
        platforms = [p.strip() for p in platform_input.split(",")]

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("  JOB HUNTER — Senior Marketing Role Finder")
    print("=" * 60)
    print(f"  Keywords:   {', '.join(keywords)}")
    print(f"  Locations:  {', '.join(locations)}")
    print(f"  Platforms:  {', '.join(platforms)}")
    print(f"  Max/search: {args.max_results}")
    print(f"  Hiring:     {'Yes' if args.find_hiring else 'No'}")
    print(f"  Posts:      {'Yes' if args.find_posts else 'No'}")
    consultant_mode = "Broad" if args.consultants_broad else ("Focused" if args.find_consultants else "No")
    print(f"  Consultants:{consultant_mode}")
    print(f"  Firm Posts: {'Yes' if args.find_firm_posts else 'No'}")
    print(f"  Target Cos: {'Yes' if args.target_companies else 'No'}")
    print(f"  Find HR:    {'Yes' if args.find_hr else 'No'}")
    print(f"  Emails:     {'Yes' if args.enrich_emails else 'No'}")
    print("=" * 60)

    # ---- Step 1: Scrape Jobs ----
    jobs_df = pd.DataFrame()

    if args.jobs_file:
        # Load from file instead of scraping
        print(f"\nLoading jobs from: {args.jobs_file}")
        jobs_df = pd.read_csv(args.jobs_file)
        print(f"  Loaded {len(jobs_df)} jobs")
    else:
        all_frames = []

        # LinkedIn + Indeed via python-jobspy
        jobspy_platforms = [p for p in platforms if p in ("linkedin", "indeed")]
        if jobspy_platforms:
            print(f"\n--- Searching {', '.join(jobspy_platforms)} ---")
            li_jobs = search_linkedin_indeed(
                keywords=keywords,
                locations=locations,
                platforms=jobspy_platforms,
                max_results=args.max_results,
                hours_old=args.hours_old or HOURS_OLD,
            )
            if not li_jobs.empty:
                all_frames.append(li_jobs)

        # Naukri via custom scraper
        if "naukri" in platforms:
            print(f"\n--- Searching Naukri ---")
            # Filter to India-relevant locations for Naukri
            india_locations = [
                loc for loc in locations
                if loc.lower() not in ("remote", "usa", "uk", "europe")
            ] or ["India"]
            naukri_jobs = search_naukri(
                keywords=keywords,
                locations=india_locations,
                max_results=args.max_results,
            )
            if not naukri_jobs.empty:
                all_frames.append(naukri_jobs)

        # Cutshort via SSR scraping (no browser needed)
        if "cutshort" in platforms:
            print(f"\n--- Searching Cutshort ---")
            cutshort_jobs = search_cutshort(
                keywords=keywords,
                locations=locations,
                max_results=args.max_results,
            )
            if not cutshort_jobs.empty:
                all_frames.append(cutshort_jobs)

        # Instahyre via Playwright
        if "instahyre" in platforms:
            print(f"\n--- Searching Instahyre ---")
            instahyre_jobs = search_instahyre(
                keywords=keywords,
                locations=locations,
                max_results=args.max_results,
            )
            if not instahyre_jobs.empty:
                all_frames.append(instahyre_jobs)

        # Wellfound via Playwright + auth (requires credentials)
        if "wellfound" in platforms:
            print(f"\n--- Searching Wellfound ---")
            wellfound_jobs = search_wellfound(
                keywords=keywords,
                locations=locations,
                max_results=args.max_results,
            )
            if not wellfound_jobs.empty:
                all_frames.append(wellfound_jobs)

        # IIMJobs via Playwright (premium/MBA-level roles)
        if "iimjobs" in platforms:
            print(f"\n--- Searching IIMJobs ---")
            iimjobs_jobs = search_iimjobs(
                keywords=keywords,
                locations=locations,
                max_results=args.max_results,
            )
            if not iimjobs_jobs.empty:
                all_frames.append(iimjobs_jobs)

        # Hirist via Playwright (by IIMJobs, tech & premium roles)
        if "hirist" in platforms:
            print(f"\n--- Searching Hirist ---")
            hirist_jobs = search_hirist(
                keywords=keywords,
                locations=locations,
                max_results=args.max_results,
            )
            if not hirist_jobs.empty:
                all_frames.append(hirist_jobs)

        # Weekday via SSR scraping (HTTP-first, Playwright fallback)
        if "weekday" in platforms:
            print(f"\n--- Searching Weekday ---")
            weekday_jobs = search_weekday(
                keywords=keywords,
                locations=locations,
                max_results=args.max_results,
            )
            if not weekday_jobs.empty:
                all_frames.append(weekday_jobs)

        # Crescendo Global via Playwright
        if "crescendo" in platforms:
            print(f"\n--- Searching Crescendo Global ---")
            crescendo_jobs = search_crescendo(
                keywords=keywords,
                locations=locations,
                max_results=args.max_results,
            )
            if not crescendo_jobs.empty:
                all_frames.append(crescendo_jobs)

        # Michael Page India via HTTP scraping
        if "michaelpage" in platforms:
            print(f"\n--- Searching Michael Page ---")
            michaelpage_jobs = search_michaelpage(
                keywords=keywords,
                locations=locations,
                max_results=args.max_results,
            )
            if not michaelpage_jobs.empty:
                all_frames.append(michaelpage_jobs)

        # SutraHR via HTTP scraping
        if "sutrahr" in platforms:
            print(f"\n--- Searching SutraHR ---")
            sutrahr_jobs = search_sutrahr(
                keywords=keywords,
                locations=locations,
                max_results=args.max_results,
            )
            if not sutrahr_jobs.empty:
                all_frames.append(sutrahr_jobs)

        # Antal International via HTTP scraping
        if "antal" in platforms:
            print(f"\n--- Searching Antal International ---")
            antal_jobs = search_antal(
                keywords=keywords,
                locations=locations,
                max_results=args.max_results,
            )
            if not antal_jobs.empty:
                all_frames.append(antal_jobs)

        # CIEL HR via Playwright
        if "cielhr" in platforms:
            print(f"\n--- Searching CIEL HR ---")
            cielhr_jobs = search_cielhr(
                keywords=keywords,
                locations=locations,
                max_results=args.max_results,
            )
            if not cielhr_jobs.empty:
                all_frames.append(cielhr_jobs)

        # TopGear Consultants via Playwright
        if "topgear" in platforms:
            print(f"\n--- Searching TopGear Consultants ---")
            topgear_jobs = search_topgear(
                keywords=keywords,
                locations=locations,
                max_results=args.max_results,
            )
            if not topgear_jobs.empty:
                all_frames.append(topgear_jobs)

        # ABC Consultants via Playwright
        if "abcconsultants" in platforms:
            print(f"\n--- Searching ABC Consultants ---")
            abc_jobs = search_abcconsultants(
                keywords=keywords,
                locations=locations,
                max_results=args.max_results,
            )
            if not abc_jobs.empty:
                all_frames.append(abc_jobs)

        # Korn Ferry via Playwright
        if "kornferry" in platforms:
            print(f"\n--- Searching Korn Ferry ---")
            kf_jobs = search_kornferry(
                keywords=keywords,
                locations=locations,
                max_results=args.max_results,
            )
            if not kf_jobs.empty:
                all_frames.append(kf_jobs)

        if all_frames:
            jobs_df = pd.concat(all_frames, ignore_index=True)
            # Final dedup across platforms
            if "company" in jobs_df.columns and "title" in jobs_df.columns:
                before = len(jobs_df)
                jobs_df = jobs_df.drop_duplicates(
                    subset=["company", "title"], keep="first"
                )
                dupes = before - len(jobs_df)
                if dupes:
                    print(f"\n  Removed {dupes} cross-platform duplicates")

            # India-only safety net: remove any non-India jobs that slipped through
            jobs_df = filter_india_jobs(jobs_df, strict=False)

    # ---- Enrich: company type, role type, experience, summary ----
    if not jobs_df.empty:
        print(f"\n--- Enriching job data ---")
        jobs_df = enrich_jobs(jobs_df)
        print(f"  Added: company_type, role_type, experience_required, role_summary")
        export_jobs(jobs_df, output_dir)

    if jobs_df.empty and not args.find_hiring:
        print("\nNo jobs found. Try broader keywords or different locations.")
        if not args.find_hr:
            return

    # ---- Step 2: Find Hiring Signals ----
    hiring_df = pd.DataFrame()

    if args.find_hiring or args.find_posts:
        from hr_finder.hiring_signals import (
            find_hiring_people,
            find_hiring_posts,
            merge_hiring_results,
        )
        from utils.export import export_hiring_signals

        people_df = pd.DataFrame()
        posts_df = pd.DataFrame()

        if args.find_hiring:
            print(f"\n--- Finding #Hiring Profiles on LinkedIn ---")
            people_df = find_hiring_people(
                keywords=keywords,
                locations=locations,
                max_results=args.max_results,
            )

        if args.find_posts:
            print(f"\n--- Scanning LinkedIn Posts for Hiring Signals ---")
            posts_df = find_hiring_posts(
                keywords=keywords,
                locations=locations,
                max_results=args.max_results,
            )

        hiring_df = merge_hiring_results(people_df, posts_df)

        if not hiring_df.empty:
            export_hiring_signals(hiring_df, output_dir)

    # ---- Step 2.5: Find Recruitment Consultants ----
    consultants_df = pd.DataFrame()

    if args.find_consultants or args.consultants_broad:
        from hr_finder.hiring_signals import find_consultants
        from utils.export import export_consultants

        is_broad = args.consultants_broad
        mode_label = "Broad" if is_broad else "Focused"
        print(f"\n--- Finding Recruitment Consultants on LinkedIn ({mode_label}) ---")
        consultants_df = find_consultants(
            keywords=keywords,
            locations=locations,
            max_results=args.max_results,
            broad=is_broad,
        )
        if not consultants_df.empty:
            export_consultants(consultants_df, output_dir)

    # ---- Step 2.6: Find Recruiting Firm Posts ----
    firm_posts_df = pd.DataFrame()

    if args.find_firm_posts:
        from config import RECRUITING_FIRMS
        from hr_finder.hiring_signals import find_firm_posts
        from utils.export import export_firm_posts

        print(f"\n--- Searching Posts from Recruiting Firms ---")
        firm_posts_df = find_firm_posts(
            firms=RECRUITING_FIRMS,
            keywords=keywords,
            locations=locations,
            max_results=args.max_results,
        )
        if not firm_posts_df.empty:
            export_firm_posts(firm_posts_df, output_dir)

    # ---- Step 3: Find HR Contacts ----
    hr_df = pd.DataFrame()

    if args.hr_file:
        print(f"\nLoading HR contacts from: {args.hr_file}")
        hr_df = pd.read_csv(args.hr_file)
        print(f"  Loaded {len(hr_df)} contacts")
    elif args.find_hr and not jobs_df.empty:
        print(f"\n--- Finding HR Contacts on LinkedIn ---")
        companies = jobs_df["company"].dropna().unique().tolist()
        hr_df = find_hr_contacts(companies)
        if not hr_df.empty:
            export_hr_contacts(hr_df, output_dir)

    # ---- Step 3.5: Target Company HR Search ----
    if args.target_companies:
        from config import TARGET_COMPANIES
        print(f"\n--- Finding HR/TA at Target Companies ---")
        print(f"  Companies: {', '.join(TARGET_COMPANIES[:5])}{'...' if len(TARGET_COMPANIES) > 5 else ''}")
        target_hr_df = find_hr_contacts(TARGET_COMPANIES)
        if not target_hr_df.empty:
            export_hr_contacts(target_hr_df, output_dir)
            # Merge into main hr_df for downstream use
            if hr_df.empty:
                hr_df = target_hr_df
            else:
                hr_df = pd.concat([hr_df, target_hr_df], ignore_index=True)
                if "linkedin_url" in hr_df.columns:
                    hr_df = hr_df.drop_duplicates(subset="linkedin_url", keep="first")

    # ---- Step 4: Enrich Emails ----
    if args.enrich_emails and hr_df.empty:
        print("\n[Warning] --enrich-emails requires --find-hr or --hr-file to have HR contacts.")
        print("  No contacts to enrich. Skipping email enrichment.")
    if args.enrich_emails and not hr_df.empty:
        print(f"\n--- Enriching Emails via Hunter.io ---")
        hr_df = enrich_with_emails(hr_df)
        # Re-export with emails
        export_hr_contacts(hr_df, output_dir)

    # ---- Step 5: Export Summary ----
    if not jobs_df.empty:
        export_summary(jobs_df, hr_df, output_dir)

    # ---- Step 5.5: Job + Recruiter Matched ----
    has_recruiter_data = (
        not hr_df.empty or not consultants_df.empty or not firm_posts_df.empty
    )
    if not jobs_df.empty and has_recruiter_data:
        from utils.export import export_job_recruiter_matched
        export_job_recruiter_matched(
            jobs_df, hr_df, consultants_df, firm_posts_df, output_dir
        )

    # ---- Done ----
    print("\n" + "=" * 60)
    print("  DONE!")
    print(f"  Results in: {output_dir.resolve()}")
    if not jobs_df.empty:
        print(f"  Jobs found: {len(jobs_df)}")
        if "company" in jobs_df.columns:
            print(f"  Companies:  {jobs_df['company'].nunique()}")
    if not hiring_df.empty:
        print(f"  Hiring signals: {len(hiring_df)}")
        if "signal_source" in hiring_df.columns:
            people_count = (hiring_df["signal_source"] == "people_search").sum()
            posts_count = (hiring_df["signal_source"] == "post_search").sum()
            if people_count:
                print(f"    - From profiles: {people_count}")
            if posts_count:
                print(f"    - From posts:    {posts_count}")
        if "hiring_for_role" in hiring_df.columns:
            role_counts = hiring_df["hiring_for_role"].value_counts().head(5)
            if not role_counts.empty:
                print(f"  Top hiring roles: {', '.join(f'{r}({c})' for r, c in role_counts.items())}")
    if not firm_posts_df.empty:
        print(f"  Firm posts:  {len(firm_posts_df)}")
        if "recruiter_firm" in firm_posts_df.columns:
            firm_counts = firm_posts_df["recruiter_firm"].value_counts().head(5)
            if not firm_counts.empty:
                print(f"    - By firm: {', '.join(f'{f}({c})' for f, c in firm_counts.items())}")
    if not consultants_df.empty:
        print(f"  Consultants: {len(consultants_df)}")
        if "is_agency" in consultants_df.columns:
            agency_count = consultants_df["is_agency"].sum()
            print(f"    - Confirmed agency: {agency_count}")
        if "consultant_type" in consultants_df.columns:
            type_counts = consultants_df["consultant_type"].value_counts().head(5)
            if not type_counts.empty:
                print(f"    - Types: {', '.join(f'{t}({c})' for t, c in type_counts.items())}")
    if not hr_df.empty:
        print(f"  HR contacts: {len(hr_df)}")
        if "email" in hr_df.columns:
            emails_found = hr_df["email"].notna().sum() - (hr_df["email"] == "").sum()
            print(f"  Emails:     {emails_found}")

    # Master sheet totals
    master_files = {
        "master_jobs.csv": "Jobs",
        "master_hiring_signals.csv": "Hiring signals",
        "master_consultants.csv": "Consultants",
        "master_firm_posts.csv": "Firm posts",
        "master_hr_contacts.csv": "HR contacts",
    }
    master_lines = []
    for mf, label in master_files.items():
        mp = output_dir / mf
        if mp.exists():
            try:
                count = sum(1 for _ in open(mp, encoding="utf-8-sig")) - 1
                master_lines.append(f"    {label}: {count}")
            except Exception:
                pass
    if master_lines:
        print("  ----- Master Sheets -----")
        for line in master_lines:
            print(line)

    print("=" * 60)

    # Offer to launch dashboard
    if args.dashboard and not jobs_df.empty:
        launch_dashboard(args.port)


if __name__ == "__main__":
    main()
