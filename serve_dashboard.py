"""Standalone dashboard server with scrape API."""
import json
import os
import subprocess
import sys
import threading
from datetime import datetime
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

base_dir = Path(__file__).parent
output_dir = base_dir / "output"

# ── Thread-safe scrape state ──────────────────────────────────
_state_lock = threading.Lock()

_scrape_state = {
    "status": "idle",       # idle | running | completed | error
    "log_lines": [],
    "process": None,
    "started_at": None,
    "finished_at": None,
    "output_files": [],
    "exit_code": None,
}


def start_scrape(config):
    """Spawn main.py as a subprocess with the given config."""
    global _scrape_state

    # Build CLI command
    cmd = [sys.executable, str(base_dir / "main.py")]

    keywords = config.get("keywords", [])
    if keywords:
        cmd += ["-k", ",".join(keywords)]

    locations = config.get("locations", [])
    if locations:
        cmd += ["-l", ",".join(locations)]

    platforms = config.get("platforms", [])
    if not platforms:
        with _state_lock:
            _scrape_state["status"] = "error"
            _scrape_state["log_lines"] = ["ERROR: No platforms selected. Please select at least one platform."]
        return
    cmd += ["-p", ",".join(platforms)]

    max_results = config.get("max_results")
    if max_results:
        cmd += ["-n", str(max_results)]

    hours_old = config.get("hours_old")
    if hours_old:
        cmd += ["--hours-old", str(hours_old)]

    if config.get("find_hiring"):
        cmd.append("--find-hiring")

    if config.get("find_posts"):
        cmd.append("--find-posts")

    if config.get("consultants_broad"):
        cmd.append("--consultants-broad")
    elif config.get("find_consultants"):
        cmd.append("--find-consultants")

    if config.get("find_firm_posts"):
        cmd.append("--find-firm-posts")

    if config.get("target_companies"):
        cmd.append("--target-companies")

    if config.get("find_hr"):
        cmd.append("--find-hr")

    if config.get("enrich_emails"):
        cmd.append("--enrich-emails")

    # Snapshot existing CSVs right before Popen to detect new ones later
    existing_files = set(f.name for f in output_dir.glob("*.csv"))

    # Reset state
    with _state_lock:
        _scrape_state["status"] = "running"
        _scrape_state["log_lines"] = []
        _scrape_state["process"] = None
        _scrape_state["started_at"] = datetime.now().isoformat()
        _scrape_state["finished_at"] = None
        _scrape_state["output_files"] = []
        _scrape_state["exit_code"] = None

    # Spawn subprocess
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        cwd=str(base_dir),
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    with _state_lock:
        _scrape_state["process"] = proc

    # Background thread captures stdout line-by-line
    def _capture(proc=proc, existing=existing_files):
        for line in proc.stdout:
            with _state_lock:
                _scrape_state["log_lines"].append(line.rstrip("\n"))
        proc.wait()
        with _state_lock:
            _scrape_state["exit_code"] = proc.returncode
            _scrape_state["status"] = "completed" if proc.returncode == 0 else "error"
            _scrape_state["finished_at"] = datetime.now().isoformat()
            # Detect newly created files
            current_files = set(f.name for f in output_dir.glob("*.csv"))
            _scrape_state["output_files"] = sorted(current_files - existing)

    thread = threading.Thread(target=_capture, daemon=True)
    thread.start()


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(base_dir), **kwargs)

    # ── JSON helper ──────────────────────────────────────────
    def _json_response(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    # ── CORS preflight ───────────────────────────────────────
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    # ── GET routes ───────────────────────────────────────────
    def do_GET(self):
        # Scrape status
        if self.path == "/api/scrape/status":
            with _state_lock:
                snapshot = {
                    "status": _scrape_state["status"],
                    "log_lines": list(_scrape_state["log_lines"]),
                    "log_count": len(_scrape_state["log_lines"]),
                    "started_at": _scrape_state["started_at"],
                    "finished_at": _scrape_state["finished_at"],
                    "output_files": list(_scrape_state["output_files"]),
                    "exit_code": _scrape_state["exit_code"],
                }
            self._json_response(200, snapshot)
            return

        # File listing
        if self.path == "/api/files":
            files = sorted(
                [f.name for f in output_dir.glob("*.csv")],
                key=lambda x: os.path.getmtime(output_dir / x),
                reverse=True,
            )
            self._json_response(200, files)
            return

        # Serve CSVs from output/
        if self.path.startswith("/output/"):
            fn = self.path[len("/output/"):]
            fp = (output_dir / fn).resolve()
            # Validate path stays within output directory
            if not str(fp).startswith(str(output_dir.resolve())):
                self.send_error(403)
                return
            if fp.exists() and fp.suffix == ".csv":
                data = fp.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/csv; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(data)
                return
            self.send_error(404)
            return

        # Default: serve dashboard
        if self.path in ("/", ""):
            self.path = "/dashboard.html"
        super().do_GET()

    # ── POST routes ──────────────────────────────────────────
    def do_POST(self):
        if self.path == "/api/scrape":
            # Guard: one scrape at a time
            with _state_lock:
                is_running = _scrape_state["status"] == "running"
            if is_running:
                self._json_response(409, {"error": "A scrape is already running"})
                return

            # Parse JSON body
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}

            start_scrape(body)
            self._json_response(200, {"status": "running", "message": "Scrape started"})
            return

        if self.path == "/api/scrape/cancel":
            with _state_lock:
                proc = _scrape_state.get("process")
                if proc and _scrape_state["status"] == "running":
                    try:
                        proc.terminate()
                    except Exception:
                        pass
                    _scrape_state["status"] = "idle"
                    _scrape_state["finished_at"] = datetime.now().isoformat()
                    _scrape_state["exit_code"] = -1
                    cancelled = True
                else:
                    cancelled = False
            if cancelled:
                self._json_response(200, {"status": "cancelled"})
            else:
                self._json_response(200, {"status": "nothing_to_cancel"})
            return

        self.send_error(405)


if __name__ == "__main__":
    port = int(os.environ.get("DASHBOARD_PORT", 8056))
    server = HTTPServer(("127.0.0.1", port), DashboardHandler)
    print(f"Dashboard server running at http://127.0.0.1:{port}")
    print("  POST /api/scrape         — start a scrape job")
    print("  POST /api/scrape/cancel  — cancel running scrape")
    print("  GET  /api/scrape/status  — poll scrape progress")
    server.serve_forever()
