"""CSV export utilities with centralised master sheet system.

Every export function:
  1. Saves a timestamped file (e.g. jobs_20260313_143000.csv)  -- your daily snapshot
  2. Merges into a master file  (e.g. master_jobs.csv)          -- cumulative dataset

Master files live in the same output/ directory and are deduplicated on a key
column so you never get duplicate rows across daily runs.
"""

from datetime import datetime
from pathlib import Path

import pandas as pd

from config import OUTPUT_DIR


# ── Master sheet merge engine ────────────────────────────────────────

def _merge_to_master(
    new_df: pd.DataFrame,
    master_filename: str,
    dedup_key: str,
    output_dir: Path = OUTPUT_DIR,
    label: str = "records",
) -> dict:
    """Merge new rows into a master CSV, deduplicating on `dedup_key`.

    - If master exists: load it, concat new rows, drop duplicates
      keeping the *latest* row (new data wins for updated fields).
    - If master doesn't exist: create it from scratch.

    Args:
        new_df:          New data from this scrape run.
        master_filename: Name of the master CSV (e.g. "master_jobs.csv").
        dedup_key:       Column used for deduplication (e.g. "job_url").
        output_dir:      Directory for master file.
        label:           Human-readable label for print output.

    Returns:
        dict with stats: {"new": int, "updated": int, "total": int}
    """
    master_path = output_dir / master_filename
    stats = {"new": 0, "updated": 0, "total": 0}

    if new_df.empty:
        if master_path.exists():
            stats["total"] = len(pd.read_csv(master_path, nrows=0).columns)
            try:
                stats["total"] = sum(1 for _ in open(master_path, encoding="utf-8-sig")) - 1
            except Exception:
                pass
        return stats

    # Ensure dedup key exists in new data
    if dedup_key not in new_df.columns:
        # Can't dedup without the key — just save and return
        new_df.to_csv(master_path, index=False, encoding="utf-8-sig")
        stats["new"] = len(new_df)
        stats["total"] = len(new_df)
        return stats

    new_df = new_df.copy()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if master_path.exists():
        try:
            master_df = pd.read_csv(master_path, dtype=str)
        except Exception:
            master_df = pd.DataFrame()

        if not master_df.empty and dedup_key in master_df.columns:
            existing_keys = set(master_df[dedup_key].dropna().tolist())
            new_keys = set(new_df[dedup_key].dropna().tolist())

            # New = keys not seen before
            truly_new = new_keys - existing_keys
            stats["new"] = len(truly_new)
            # Updated = keys that already existed (will be overwritten)
            stats["updated"] = len(new_keys & existing_keys)

            # Mark new rows with first_seen + is_new
            new_df["is_new"] = new_df[dedup_key].apply(
                lambda k: "Yes" if k in truly_new else "No"
            )
            if "first_seen" not in new_df.columns:
                new_df["first_seen"] = ""
            # New items get first_seen = now; returning items keep master's first_seen
            new_df.loc[new_df["is_new"] == "Yes", "first_seen"] = now_str

            # Preserve first_seen from master for returning items
            if "first_seen" in master_df.columns:
                fs_map = dict(zip(
                    master_df[dedup_key].dropna(),
                    master_df["first_seen"].fillna(""),
                ))
                mask = (new_df["is_new"] == "No") & new_df[dedup_key].notna()
                new_df.loc[mask, "first_seen"] = new_df.loc[mask, dedup_key].map(
                    lambda k: fs_map.get(k, "")
                )

            new_df["last_seen"] = now_str

            # For master rows NOT in this run: mark is_new=No, keep their timestamps
            if "is_new" not in master_df.columns:
                master_df["is_new"] = "No"
            else:
                master_df["is_new"] = "No"  # Reset all old rows
            if "last_seen" not in master_df.columns:
                master_df["last_seen"] = master_df.get("scraped_at", "")

            # Concat: new rows go FIRST so they win on drop_duplicates(keep='first')
            combined = pd.concat([new_df, master_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=[dedup_key], keep="first")
            stats["total"] = len(combined)

            combined.to_csv(master_path, index=False, encoding="utf-8-sig")
        else:
            # Master exists but is empty or missing the key column
            new_df["is_new"] = "Yes"
            new_df["first_seen"] = now_str
            new_df["last_seen"] = now_str
            new_df.to_csv(master_path, index=False, encoding="utf-8-sig")
            stats["new"] = len(new_df)
            stats["total"] = len(new_df)
    else:
        # First run — create master from scratch
        deduped = new_df.drop_duplicates(subset=[dedup_key], keep="first")
        deduped["is_new"] = "Yes"
        deduped["first_seen"] = now_str
        deduped["last_seen"] = now_str
        deduped.to_csv(master_path, index=False, encoding="utf-8-sig")
        stats["new"] = len(deduped)
        stats["total"] = len(deduped)

    return stats


def _print_master_stats(stats: dict, label: str, master_filename: str):
    """Pretty-print master sheet merge stats."""
    parts = []
    if stats["new"]:
        parts.append(f"{stats['new']} new")
    if stats["updated"]:
        parts.append(f"{stats['updated']} updated")
    parts.append(f"{stats['total']} total")

    detail = ", ".join(parts)
    print(f"  Master ({master_filename}): {detail}")


# ── Timestamp + scraped_at helper ────────────────────────────────────

def _stamp_scraped_at(df: pd.DataFrame) -> pd.DataFrame:
    """Add/fill scraped_at timestamp on the DataFrame (in-place)."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if "scraped_at" not in df.columns:
        df["scraped_at"] = now_str
    else:
        df["scraped_at"] = df["scraped_at"].fillna(now_str)
        df.loc[df["scraped_at"] == "", "scraped_at"] = now_str
    return df


# ── Export functions ─────────────────────────────────────────────────

def export_jobs(jobs_df: pd.DataFrame, output_dir: Path = OUTPUT_DIR) -> Path:
    """Export job listings to timestamped CSV + merge into master_jobs.csv."""
    if jobs_df.empty:
        print("No jobs to export.")
        return Path()

    jobs_df = _stamp_scraped_at(jobs_df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = output_dir / f"jobs_{timestamp}.csv"

    # Select and order key columns (include whatever's available)
    priority_cols = [
        "platform", "title", "company", "location", "salary",
        "job_url", "date_posted", "experience",
        "min_experience_years", "max_experience_years",
        "description", "search_keyword", "search_location", "scraped_at",
        "shortlist", "relevance_score",
        "is_new", "first_seen", "last_seen",
    ]
    cols = [c for c in priority_cols if c in jobs_df.columns]
    extra = [c for c in jobs_df.columns if c not in cols]
    cols.extend(extra)

    ordered = jobs_df[cols]
    ordered.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"\nJobs exported to: {filepath}")

    # Merge into master
    stats = _merge_to_master(
        ordered, "master_jobs.csv", dedup_key="job_url",
        output_dir=output_dir, label="jobs",
    )
    _print_master_stats(stats, "jobs", "master_jobs.csv")

    return filepath


def export_hr_contacts(hr_df: pd.DataFrame, output_dir: Path = OUTPUT_DIR) -> Path:
    """Export HR contacts to timestamped CSV + merge into master_hr_contacts.csv."""
    if hr_df.empty:
        print("No HR contacts to export.")
        return Path()

    hr_df = _stamp_scraped_at(hr_df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = output_dir / f"hr_contacts_{timestamp}.csv"

    priority_cols = [
        "company", "hr_name", "hr_title", "linkedin_url",
        "email", "email_confidence", "scraped_at",
    ]
    cols = [c for c in priority_cols if c in hr_df.columns]
    extra = [c for c in hr_df.columns if c not in cols]
    cols.extend(extra)

    ordered = hr_df[cols]
    ordered.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"HR contacts exported to: {filepath}")

    # Merge into master
    stats = _merge_to_master(
        ordered, "master_hr_contacts.csv", dedup_key="linkedin_url",
        output_dir=output_dir, label="HR contacts",
    )
    _print_master_stats(stats, "HR contacts", "master_hr_contacts.csv")

    return filepath


def export_hiring_signals(
    df: pd.DataFrame, output_dir: Path = OUTPUT_DIR
) -> Path:
    """Export hiring signal profiles to timestamped CSV + merge into master_hiring_signals.csv."""
    if df.empty:
        print("No hiring signals to export.")
        return Path()

    df = _stamp_scraped_at(df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = output_dir / f"hiring_signals_{timestamp}.csv"

    priority_cols = [
        "name", "title", "headline", "about", "location",
        "linkedin_url", "email", "phone",
        "company", "company_linkedin_url", "company_website",
        "company_industry", "company_size",
        "hiring_for_role", "signal_source",
        "post_text", "post_url",
        "search_keyword", "scraped_at",
    ]
    cols = [c for c in priority_cols if c in df.columns]
    extra = [c for c in df.columns if c not in cols]
    cols.extend(extra)

    ordered = df[cols]
    ordered.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"Hiring signals exported to: {filepath}")

    # Merge into master
    stats = _merge_to_master(
        ordered, "master_hiring_signals.csv", dedup_key="linkedin_url",
        output_dir=output_dir, label="hiring signals",
    )
    _print_master_stats(stats, "hiring signals", "master_hiring_signals.csv")

    return filepath


def export_consultants(
    df: pd.DataFrame, output_dir: Path = OUTPUT_DIR
) -> Path:
    """Export recruitment consultants to timestamped CSV + merge into master_consultants.csv."""
    if df.empty:
        print("No consultants to export.")
        return Path()

    df = _stamp_scraped_at(df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = output_dir / f"consultants_{timestamp}.csv"

    priority_cols = [
        "name", "title", "headline", "about", "location",
        "linkedin_url", "email", "phone",
        "company", "company_linkedin_url", "company_website",
        "company_industry", "company_size",
        "consultant_type", "is_agency", "domains_served",
        "signal_source", "search_keyword", "scraped_at",
    ]
    cols = [c for c in priority_cols if c in df.columns]
    extra = [c for c in df.columns if c not in cols]
    cols.extend(extra)

    ordered = df[cols]
    ordered.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"Consultants exported to: {filepath}")

    # Merge into master
    stats = _merge_to_master(
        ordered, "master_consultants.csv", dedup_key="linkedin_url",
        output_dir=output_dir, label="consultants",
    )
    _print_master_stats(stats, "consultants", "master_consultants.csv")

    return filepath


def export_firm_posts(
    df: pd.DataFrame, output_dir: Path = OUTPUT_DIR
) -> Path:
    """Export recruiting firm posts to timestamped CSV + merge into master_firm_posts.csv."""
    if df.empty:
        print("No firm posts to export.")
        return Path()

    df = _stamp_scraped_at(df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = output_dir / f"firm_posts_{timestamp}.csv"

    priority_cols = [
        "name", "title", "recruiter_firm", "company", "location",
        "linkedin_url", "post_text", "post_url",
        "hiring_for_role", "signal_source",
        "search_keyword", "scraped_at",
    ]
    cols = [c for c in priority_cols if c in df.columns]
    extra = [c for c in df.columns if c not in cols]
    cols.extend(extra)

    ordered = df[cols]
    ordered.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"Firm posts exported to: {filepath}")

    # Merge into master — dedup by linkedin_url + post_url combo
    # Since one person can have multiple posts, we use post_url as dedup key
    dedup_col = "post_url" if "post_url" in df.columns else "linkedin_url"
    stats = _merge_to_master(
        ordered, "master_firm_posts.csv", dedup_key=dedup_col,
        output_dir=output_dir, label="firm posts",
    )
    _print_master_stats(stats, "firm posts", "master_firm_posts.csv")

    return filepath


def export_job_recruiter_matched(
    jobs_df: pd.DataFrame,
    hr_df: pd.DataFrame,
    consultants_df: pd.DataFrame,
    firm_posts_df: pd.DataFrame,
    output_dir: Path = OUTPUT_DIR,
) -> Path:
    """Cross-reference jobs with recruiter contacts at the same company.

    Produces a CSV that matches each job listing with any known recruiter
    contact from the same company — whether that's an HR contact,
    a recruiter from a consulting firm, or someone who posted about hiring.

    Columns:
        job_title, company, job_url, platform,
        recruiter_name, recruiter_title, recruiter_linkedin_url,
        recruiter_type (In-house HR | Consulting Firm | Hiring Signal),
        match_confidence (High | Medium).
    """
    if jobs_df.empty:
        print("No jobs data for matching.")
        return Path()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = output_dir / f"job_recruiter_matched_{timestamp}.csv"

    matches = []

    # Normalise company names for matching
    def _norm(name):
        if not name or str(name).lower() in ("", "nan"):
            return ""
        return str(name).strip().lower()

    # Build lookup: normalised company → list of recruiter dicts
    recruiter_lookup: dict[str, list[dict]] = {}

    # Source 1: HR contacts (In-house HR)
    if not hr_df.empty and "company" in hr_df.columns:
        for _, row in hr_df.iterrows():
            co = _norm(row.get("company", ""))
            if not co:
                continue
            recruiter_lookup.setdefault(co, []).append({
                "recruiter_name": str(row.get("hr_name", "")),
                "recruiter_title": str(row.get("hr_title", "")),
                "recruiter_linkedin_url": str(row.get("linkedin_url", "")),
                "recruiter_type": "In-house HR",
                "match_confidence": "High",
            })

    # Source 2: Firm posts (Consulting Firm)
    if not firm_posts_df.empty and "company" in firm_posts_df.columns:
        # For firm posts, the "company" field might be the firm name
        # We need to check if the post_text mentions any job company
        # For now, use direct company match and also check post_text
        for _, row in firm_posts_df.iterrows():
            # Use the company extracted from the poster's title
            co = _norm(row.get("company", ""))
            firm = str(row.get("recruiter_firm", ""))
            if co:
                recruiter_lookup.setdefault(co, []).append({
                    "recruiter_name": str(row.get("name", "")),
                    "recruiter_title": f"{row.get('title', '')} ({firm})",
                    "recruiter_linkedin_url": str(row.get("linkedin_url", "")),
                    "recruiter_type": "Consulting Firm",
                    "match_confidence": "Medium",
                })

    # Source 3: Consultants (Recruitment Agency / Consultant)
    if not consultants_df.empty and "company" in consultants_df.columns:
        for _, row in consultants_df.iterrows():
            co = _norm(row.get("company", ""))
            if not co:
                continue
            recruiter_lookup.setdefault(co, []).append({
                "recruiter_name": str(row.get("name", "")),
                "recruiter_title": str(row.get("title", "")),
                "recruiter_linkedin_url": str(row.get("linkedin_url", "")),
                "recruiter_type": "Recruitment Consultant",
                "match_confidence": "Medium",
            })

    if not recruiter_lookup:
        print("No recruiter data to match against jobs.")
        return Path()

    # Match each job to recruiters at the same company
    matched_count = 0
    for _, job in jobs_df.iterrows():
        job_co = _norm(job.get("company", ""))
        if not job_co:
            continue

        # Try exact match first, then substring match
        matched_recruiters = recruiter_lookup.get(job_co, [])

        # Substring match: "razorpay" matches "razorpay software private limited"
        if not matched_recruiters:
            for co_key, recruiters in recruiter_lookup.items():
                if job_co in co_key or co_key in job_co:
                    matched_recruiters = recruiters
                    break

        for recruiter in matched_recruiters:
            matches.append({
                "job_title": str(job.get("title", "")),
                "company": str(job.get("company", "")),
                "job_url": str(job.get("job_url", "")),
                "platform": str(job.get("platform", "")),
                "salary": str(job.get("salary", "")),
                **recruiter,
            })
            matched_count += 1

    if not matches:
        print("No job-recruiter matches found (no company overlap).")
        return Path()

    result_df = pd.DataFrame(matches)

    # Dedup: same job_url + recruiter_linkedin_url
    result_df = result_df.drop_duplicates(
        subset=["job_url", "recruiter_linkedin_url"], keep="first"
    )

    result_df.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"\nJob-Recruiter matched: {filepath}")
    print(f"  {len(result_df)} matches across {result_df['company'].nunique()} companies")

    return filepath


def export_summary(
    jobs_df: pd.DataFrame,
    hr_df: pd.DataFrame,
    output_dir: Path = OUTPUT_DIR,
) -> Path:
    """Export a per-company summary CSV."""
    if jobs_df.empty:
        print("No data for summary.")
        return Path()

    required_cols = {"company", "title", "platform"}
    missing = required_cols - set(jobs_df.columns)
    if missing:
        print(f"Summary skipped: missing columns {missing}.")
        return Path()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = output_dir / f"summary_{timestamp}.csv"

    # Count jobs per company
    agg_dict = {
        "total_openings": ("title", "count"),
        "roles": ("title", lambda x: " | ".join(x.unique()[:3])),
        "platforms": ("platform", lambda x: ", ".join(x.unique())),
    }
    if "location" in jobs_df.columns:
        agg_dict["locations"] = ("location", lambda x: ", ".join(x.dropna().unique()[:3]))

    company_jobs = (
        jobs_df.groupby("company")
        .agg(**agg_dict)
        .reset_index()
    )

    # Merge HR contact counts
    if not hr_df.empty and "company" in hr_df.columns:
        agg_hr = {
            "hr_contacts_found": ("hr_name", "count"),
            "hr_names": ("hr_name", lambda x: " | ".join(x.unique()[:3])),
        }
        if "email" in hr_df.columns:
            agg_hr["emails_found"] = (
                "email",
                lambda x: sum(1 for e in x if e and str(e).strip()),
            )
        hr_summary = (
            hr_df.groupby("company")
            .agg(**agg_hr)
            .reset_index()
        )
        if "emails_found" not in hr_summary.columns:
            hr_summary["emails_found"] = 0
        company_jobs = company_jobs.merge(hr_summary, on="company", how="left")
    else:
        company_jobs["hr_contacts_found"] = 0
        company_jobs["hr_names"] = ""
        company_jobs["emails_found"] = 0

    company_jobs = company_jobs.fillna({"hr_contacts_found": 0, "emails_found": 0})
    company_jobs = company_jobs.sort_values("total_openings", ascending=False)

    company_jobs.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"Summary exported to: {filepath}")
    return filepath
