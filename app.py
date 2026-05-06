import os
import re
import csv
import time
import uuid
import shutil
import sqlite3
import threading
import requests
import urllib3
from concurrent.futures import ThreadPoolExecutor
from urllib3.exceptions import InsecureRequestWarning
from flask import Flask, render_template, request, jsonify, send_file, abort

urllib3.disable_warnings(InsecureRequestWarning)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 32 * 1024 * 1024

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JOBS_DIR = os.path.join(BASE_DIR, "jobs")
DB_PATH = os.path.join(BASE_DIR, "jobs.db")
os.makedirs(JOBS_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0 Safari/537.36"
}
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
JOB_ID_RE = re.compile(r"^[a-f0-9]{32}$")

# In-memory live counters for currently running jobs.
# Persistent metadata lives in SQLite; this dict only drives the progress bar.
live_jobs = {}
live_lock = threading.Lock()


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id           TEXT PRIMARY KEY,
                filename     TEXT,
                total        INTEGER NOT NULL,
                threads      INTEGER,
                status       TEXT NOT NULL,
                alive        INTEGER NOT NULL DEFAULT 0,
                dead         INTEGER NOT NULL DEFAULT 0,
                done         INTEGER NOT NULL DEFAULT 0,
                error        TEXT,
                alive_path   TEXT,
                dead_path    TEXT,
                created_at   INTEGER NOT NULL,
                finished_at  INTEGER
            )
        """)
        # Any job marked running on startup must have died with a previous process
        conn.execute(
            "UPDATE jobs SET status='interrupted', finished_at=? "
            "WHERE status='running'",
            (int(time.time()),),
        )
        conn.commit()


def extract_title(html):
    if not html:
        return ""
    m = TITLE_RE.search(html)
    if not m:
        return ""
    return re.sub(r"\s+", " ", m.group(1).strip())[:200]


def check_domain(domain, job_id, alive_writer, alive_fp,
                 dead_writer, dead_fp, file_lock):
    domain = domain.strip()
    if not domain:
        with live_lock:
            live_jobs[job_id]["done"] += 1
        return

    if domain.startswith("http://"):
        domain = domain[7:]
    elif domain.startswith("https://"):
        domain = domain[8:]

    is_alive = False
    last_error = ""

    for scheme in ("https", "http"):
        url = f"{scheme}://{domain}"
        try:
            resp = requests.get(
                url, headers=HEADERS, timeout=5,
                verify=False, allow_redirects=True,
            )
            try:
                html = resp.content[:100 * 1024].decode(
                    resp.encoding or "utf-8", errors="ignore"
                )
                title = extract_title(html)
            except Exception:
                title = ""

            row = {
                "url": url,
                "final_url": resp.url,
                "status": resp.status_code,
                "size": len(resp.content),
                "title": title,
                "server": resp.headers.get("Server", ""),
            }
            with file_lock:
                alive_writer.writerow(row)
                alive_fp.flush()
            with live_lock:
                live_jobs[job_id]["alive"] += 1
                live_jobs[job_id]["done"] += 1
            is_alive = True
            break
        except requests.exceptions.Timeout:
            last_error = "timeout"
        except requests.exceptions.ConnectionError:
            last_error = "connection_error"
        except requests.exceptions.RequestException as e:
            last_error = type(e).__name__

    if not is_alive:
        with file_lock:
            dead_writer.writerow({"domain": domain, "reason": last_error})
            dead_fp.flush()
        with live_lock:
            live_jobs[job_id]["dead"] += 1
            live_jobs[job_id]["done"] += 1


def run_job(job_id, domains, threads, alive_path, dead_path):
    file_lock = threading.Lock()

    alive_fp = open(alive_path, "w", encoding="utf-8-sig", newline="")
    alive_writer = csv.DictWriter(
        alive_fp,
        fieldnames=["url", "final_url", "status", "size", "title", "server"],
    )
    alive_writer.writeheader()
    alive_fp.flush()

    dead_fp = open(dead_path, "w", encoding="utf-8-sig", newline="")
    dead_writer = csv.DictWriter(dead_fp, fieldnames=["domain", "reason"])
    dead_writer.writeheader()
    dead_fp.flush()

    error_msg = None
    try:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [
                executor.submit(
                    check_domain, d, job_id,
                    alive_writer, alive_fp,
                    dead_writer, dead_fp, file_lock,
                )
                for d in domains
            ]
            for f in futures:
                f.result()
    except Exception as e:
        error_msg = str(e)
    finally:
        alive_fp.close()
        dead_fp.close()

    with live_lock:
        snapshot = dict(live_jobs[job_id])
    final_status = "error" if error_msg else "finished"
    with get_db() as conn:
        conn.execute(
            "UPDATE jobs SET status=?, alive=?, dead=?, done=?, "
            "error=?, finished_at=? WHERE id=?",
            (
                final_status,
                snapshot["alive"], snapshot["dead"], snapshot["done"],
                error_msg, int(time.time()), job_id,
            ),
        )
        conn.commit()
    with live_lock:
        live_jobs.pop(job_id, None)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    file = request.files.get("file")
    try:
        threads = int(request.form.get("threads", 30))
    except ValueError:
        threads = 30
    threads = max(1, min(200, threads))

    if not file or file.filename == "":
        return jsonify({"error": "未选择文件"}), 400

    content = file.read().decode("utf-8", errors="ignore")
    domains = [line.strip() for line in content.splitlines() if line.strip()]
    if not domains:
        return jsonify({"error": "文件为空"}), 400

    job_id = uuid.uuid4().hex
    job_dir = os.path.join(JOBS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    alive_path = os.path.join(job_dir, "alive.csv")
    dead_path = os.path.join(job_dir, "dead.csv")

    with live_lock:
        live_jobs[job_id] = {
            "total": len(domains),
            "alive": 0, "dead": 0, "done": 0,
        }

    with get_db() as conn:
        conn.execute(
            "INSERT INTO jobs (id, filename, total, threads, status, "
            "alive_path, dead_path, created_at) "
            "VALUES (?, ?, ?, ?, 'running', ?, ?, ?)",
            (
                job_id, file.filename, len(domains), threads,
                alive_path, dead_path, int(time.time()),
            ),
        )
        conn.commit()

    threading.Thread(
        target=run_job,
        args=(job_id, domains, threads, alive_path, dead_path),
        daemon=True,
    ).start()

    return jsonify({"job_id": job_id, "total": len(domains), "threads": threads})


@app.route("/status/<job_id>")
def status(job_id):
    if not JOB_ID_RE.match(job_id or ""):
        return jsonify({"error": "非法 ID"}), 400

    with live_lock:
        live = live_jobs.get(job_id)
        if live:
            return jsonify({
                "status": "running",
                "total": live["total"],
                "alive": live["alive"],
                "dead": live["dead"],
                "done": live["done"],
                "error": None,
            })

    with get_db() as conn:
        row = conn.execute(
            "SELECT status, total, alive, dead, done, error "
            "FROM jobs WHERE id=?", (job_id,),
        ).fetchone()
    if not row:
        return jsonify({"error": "任务不存在"}), 404
    return jsonify(dict(row))


@app.route("/jobs")
def jobs_list():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, filename, total, threads, status, alive, dead, done, "
            "error, created_at, finished_at FROM jobs ORDER BY created_at DESC"
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/jobs/<job_id>", methods=["DELETE"])
def delete_job(job_id):
    if not JOB_ID_RE.match(job_id or ""):
        return jsonify({"error": "非法 ID"}), 400

    with live_lock:
        if job_id in live_jobs:
            return jsonify({"error": "任务正在运行,无法删除"}), 409

    with get_db() as conn:
        row = conn.execute(
            "SELECT status FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
        if not row:
            return jsonify({"error": "任务不存在"}), 404
        if row["status"] == "running":
            return jsonify({"error": "任务正在运行,无法删除"}), 409
        conn.execute("DELETE FROM jobs WHERE id=?", (job_id,))
        conn.commit()

    job_dir = os.path.join(JOBS_DIR, job_id)
    if os.path.isdir(job_dir):
        shutil.rmtree(job_dir, ignore_errors=True)

    return jsonify({"ok": True})


@app.route("/download/<job_id>/<kind>")
def download(job_id, kind):
    if kind not in ("alive", "dead"):
        abort(404)
    if not JOB_ID_RE.match(job_id or ""):
        abort(404)

    with get_db() as conn:
        row = conn.execute(
            "SELECT alive_path, dead_path FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
    if not row:
        abort(404)
    path = row["alive_path"] if kind == "alive" else row["dead_path"]
    if not path or not os.path.exists(path):
        abort(404)
    return send_file(path, as_attachment=True, download_name=f"{kind}.csv")


init_db()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
