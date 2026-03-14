#!/usr/bin/env python3
"""
Scheduled auto-runs for Job Hunter.

Usage:
    # Run once (for Windows Task Scheduler / cron):
    python scheduler.py --once

    # Run every 12 hours continuously:
    python scheduler.py --every 12

    # Run daily at 9 AM:
    python scheduler.py --daily 09:00

    # Run with custom flags:
    python scheduler.py --every 24 --flags "--find-hr --enrich-emails"

    # Dry run (show what would execute):
    python scheduler.py --once --dry-run

Windows Task Scheduler setup:
    1. Open Task Scheduler → Create Basic Task
    2. Trigger: Daily (or your preferred frequency)
    3. Action: Start a program
       Program: python
       Arguments: scheduler.py --once
       Start in: C:\\path\\to\\job_hunter
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"
STATE_FILE = BASE_DIR / ".scheduler_state.json"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Scheduled auto-runs for Job Hunter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit (for cron / Task Scheduler)",
    )
    parser.add_argument(
        "--every",
        type=float,
        default=None,
        help="Run every N hours continuously",
    )
    parser.add_argument(
        "--daily",
        type=str,
        default=None,
        help="Run daily at HH:MM (24h format, e.g. 09:00)",
    )
    parser.add_argument(
        "--flags",
        type=str,
        default="",
        help="Extra flags to pass to main.py (e.g. '--find-hr --enrich-emails')",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the command that would run, but don't execute",
    )
    parser.add_argument(
        "--platforms",
        type=str,
        default="all",
        help="Platforms to scrape (default: all)",
    )
    parser.add_argument(
        "-n", "--max-results",
        type=int,
        default=50,
        help="Max results per platform (default: 50)",
    )
    return parser.parse_args()


def load_state() -> dict:
    """Load scheduler state (last run time, run count, etc.)."""
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"runs": [], "total_runs": 0}


def save_state(state: dict):
    """Save scheduler state."""
    STATE_FILE.write_text(json.dumps(state, indent=2))


def build_command(args) -> list[str]:
    """Build the main.py command with all flags."""
    cmd = [sys.executable, str(BASE_DIR / "main.py")]
    cmd.extend(["-p", args.platforms])
    cmd.extend(["-n", str(args.max_results)])

    if args.flags:
        # Split extra flags (handles quoted strings)
        import shlex
        cmd.extend(shlex.split(args.flags))

    return cmd


def run_scrape(args) -> dict:
    """Execute a single scrape run."""
    cmd = build_command(args)
    started = datetime.now()
    run_info = {
        "started": started.isoformat(),
        "command": " ".join(cmd),
        "status": "running",
    }

    LOG_DIR.mkdir(exist_ok=True)
    log_file = LOG_DIR / f"run_{started.strftime('%Y%m%d_%H%M%S')}.log"

    print(f"\n{'='*60}")
    print(f"  SCHEDULED RUN — {started.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Command: {' '.join(cmd)}")
    print(f"  Log: {log_file}")
    print(f"{'='*60}\n")

    if args.dry_run:
        print("[DRY RUN] Would execute:", " ".join(cmd))
        run_info["status"] = "dry_run"
        return run_info

    try:
        with open(log_file, "w", encoding="utf-8") as lf:
            result = subprocess.run(
                cmd,
                cwd=str(BASE_DIR),
                stdout=lf,
                stderr=subprocess.STDOUT,
                timeout=7200,  # 2 hour timeout
            )
        finished = datetime.now()
        duration = (finished - started).total_seconds()
        run_info["finished"] = finished.isoformat()
        run_info["duration_seconds"] = round(duration, 1)
        run_info["exit_code"] = result.returncode
        run_info["status"] = "success" if result.returncode == 0 else "error"
        run_info["log_file"] = str(log_file)

        status_emoji = "✓" if result.returncode == 0 else "✗"
        print(f"\n  {status_emoji} Run completed in {duration:.0f}s (exit code: {result.returncode})")
        print(f"    Log saved to: {log_file}")

        # Print summary from log (last 15 lines)
        try:
            lines = log_file.read_text(encoding="utf-8").strip().splitlines()
            summary_lines = [l for l in lines[-15:] if l.strip()]
            if summary_lines:
                print("\n  --- Run Summary ---")
                for line in summary_lines:
                    print(f"  {line}")
        except Exception:
            pass

    except subprocess.TimeoutExpired:
        run_info["status"] = "timeout"
        print(f"\n  ✗ Run timed out after 2 hours")
    except Exception as e:
        run_info["status"] = "error"
        run_info["error"] = str(e)
        print(f"\n  ✗ Run failed: {e}")

    return run_info


def run_once(args):
    """Execute a single run and save state."""
    state = load_state()
    run_info = run_scrape(args)

    state["total_runs"] = state.get("total_runs", 0) + 1
    state["last_run"] = run_info
    # Keep last 50 run records
    state.setdefault("runs", []).append(run_info)
    state["runs"] = state["runs"][-50:]
    save_state(state)

    return run_info


def run_every_n_hours(args):
    """Run continuously every N hours."""
    hours = args.every
    print(f"  Scheduler started — running every {hours}h")
    print(f"  Press Ctrl+C to stop\n")

    while True:
        run_once(args)
        next_run = datetime.now() + timedelta(hours=hours)
        print(f"\n  Next run at: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Sleeping for {hours}h...\n")
        time.sleep(hours * 3600)


def run_daily_at(args):
    """Run daily at a specific time."""
    target_time = args.daily
    try:
        hour, minute = map(int, target_time.split(":"))
    except ValueError:
        print(f"  Error: Invalid time format '{target_time}'. Use HH:MM (e.g. 09:00)")
        sys.exit(1)

    print(f"  Scheduler started — running daily at {target_time}")
    print(f"  Press Ctrl+C to stop\n")

    while True:
        now = datetime.now()
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

        # If target time already passed today, schedule for tomorrow
        if target <= now:
            target += timedelta(days=1)

        wait_seconds = (target - now).total_seconds()
        print(f"  Next run at: {target.strftime('%Y-%m-%d %H:%M:%S')} ({wait_seconds/3600:.1f}h from now)")

        time.sleep(wait_seconds)
        run_once(args)


def show_status():
    """Show scheduler history."""
    state = load_state()
    total = state.get("total_runs", 0)
    runs = state.get("runs", [])

    print(f"\n  Scheduler Status")
    print(f"  Total runs: {total}")

    if runs:
        last = runs[-1]
        print(f"  Last run:   {last.get('started', 'N/A')}")
        print(f"  Status:     {last.get('status', 'N/A')}")
        if last.get("duration_seconds"):
            print(f"  Duration:   {last['duration_seconds']}s")
        print(f"\n  Recent runs:")
        for r in runs[-5:]:
            s = r.get("status", "?")
            t = r.get("started", "?")[:19]
            d = r.get("duration_seconds", "?")
            print(f"    {t}  {s}  ({d}s)")
    else:
        print("  No runs yet.")
    print()


def main():
    args = parse_args()

    if args.once:
        run_once(args)
    elif args.every:
        try:
            run_every_n_hours(args)
        except KeyboardInterrupt:
            print("\n  Scheduler stopped.")
    elif args.daily:
        try:
            run_daily_at(args)
        except KeyboardInterrupt:
            print("\n  Scheduler stopped.")
    else:
        # Default: show status and usage
        show_status()
        print("  Usage:")
        print("    python scheduler.py --once                  # Run once")
        print("    python scheduler.py --every 12              # Every 12 hours")
        print("    python scheduler.py --daily 09:00           # Daily at 9 AM")
        print('    python scheduler.py --once --flags "--find-hr"  # With extra flags')
        print()


if __name__ == "__main__":
    main()
