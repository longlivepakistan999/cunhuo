import os
import re
import csv
import time
import uuid
import shutil
import base64
import secrets
import sqlite3
import threading
import traceback
import requests
import urllib3
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from urllib3.exceptions import InsecureRequestWarning
from flask import Flask, render_template, request, jsonify, send_file, abort, Response

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

# Basic Auth: only enforced when both env vars are set. Leave one empty
# to disable auth (useful for local dev or when fronting with another
# auth layer like nginx).
AUTH_USER = os.environ.get("CUNHUO_USER", "")
AUTH_PASS = os.environ.get("CUNHUO_PASS", "")
AUTH_REQUIRED = bool(AUTH_USER and AUTH_PASS)
AUTH_REALM = "cunhuo"

# Live counters + cancellation handle for the currently running job.
live_jobs = {}
live_lock = threading.Lock()

# Wakes the queue worker when a new job is enqueued.
worker_wakeup = threading.Event()


@contextmanager
def get_db():
    # `with conn:` only manages transactions; we wrap with @contextmanager
    # so the connection is actually closed on exit and we don't leak fds.
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id           TEXT PRIMARY KEY,
                name         TEXT,
                filename     TEXT,
                total        INTEGER NOT NULL,
                threads      INTEGER,
                status       TEXT NOT NULL,
                alive        INTEGER NOT NULL DEFAULT 0,
                dead         INTEGER NOT NULL DEFAULT 0,
                done         INTEGER NOT NULL DEFAULT 0,
                error        TEXT,
                input_path   TEXT,
                alive_path   TEXT,
                dead_path    TEXT,
                position     INTEGER,
                created_at   INTEGER NOT NULL,
                started_at   INTEGER,
                finished_at  INTEGER
            )
        """)
        existing = {
            r["name"] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()
        }
        for col, ddl in [
            ("name", "TEXT"),
            ("input_path", "TEXT"),
            ("position", "INTEGER"),
            ("started_at", "INTEGER"),
        ]:
            if col not in existing:
                conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} {ddl}")
        # Backfill positions for legacy rows so ordering stays sane.
        rows = conn.execute(
            "SELECT id FROM jobs WHERE position IS NULL ORDER BY created_at ASC"
        ).fetchall()
        for i, r in enumerate(rows, start=1):
            conn.execute("UPDATE jobs SET position=? WHERE id=?", (i, r["id"]))
        # Anything still 'running' must belong to a dead process.
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
    with live_lock:
        live = live_jobs.get(job_id)
        if live and live["cancel_event"].is_set():
            live["done"] += 1
            return

    domain = domain.strip()
    if not domain:
        with live_lock:
            if job_id in live_jobs:
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
                if job_id in live_jobs:
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
            if job_id in live_jobs:
                live_jobs[job_id]["dead"] += 1
                live_jobs[job_id]["done"] += 1


def run_job(job_id):
    with get_db() as conn:
        row = conn.execute(
            "SELECT total, threads, input_path, alive_path, dead_path "
            "FROM jobs WHERE id=?", (job_id,),
        ).fetchone()
    if not row:
        return

    try:
        with open(row["input_path"], "r", encoding="utf-8") as f:
            domains = [line.strip() for line in f if line.strip()]
    except OSError as e:
        with get_db() as conn:
            conn.execute(
                "UPDATE jobs SET status='error', error=?, finished_at=? "
                "WHERE id=?",
                (f"读取输入文件失败: {e}", int(time.time()), job_id),
            )
            conn.commit()
        return

    cancel_event = threading.Event()
    with live_lock:
        live_jobs[job_id] = {
            "total": row["total"],
            "alive": 0, "dead": 0, "done": 0,
            "cancel_event": cancel_event,
        }
    # Conditional UPDATE: if a /cancel arrived during our setup above and
    # already flipped status to 'cancelled', leave it alone and bail out.
    # Without this WHERE clause the worker would silently overwrite the
    # user's cancel.
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE jobs SET status='running', started_at=? "
            "WHERE id=? AND status='queued'",
            (int(time.time()), job_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            with live_lock:
                live_jobs.pop(job_id, None)
            return

    file_lock = threading.Lock()
    alive_fp = open(row["alive_path"], "w", encoding="utf-8-sig", newline="")
    alive_writer = csv.DictWriter(
        alive_fp,
        fieldnames=["url", "final_url", "status", "size", "title", "server"],
    )
    alive_writer.writeheader()
    alive_fp.flush()

    dead_fp = open(row["dead_path"], "w", encoding="utf-8-sig", newline="")
    dead_writer = csv.DictWriter(dead_fp, fieldnames=["domain", "reason"])
    dead_writer.writeheader()
    dead_fp.flush()

    error_msg = None
    try:
        with ThreadPoolExecutor(max_workers=row["threads"]) as executor:
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
        was_cancelled = cancel_event.is_set()

    if was_cancelled:
        final_status = "cancelled"
    elif error_msg:
        final_status = "error"
    else:
        final_status = "finished"

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


def queue_worker():
    # Outer try keeps the daemon thread alive forever; without it any
    # unexpected DB error would silently kill the queue.
    while True:
        try:
            with get_db() as conn:
                row = conn.execute(
                    "SELECT id FROM jobs WHERE status='queued' "
                    "ORDER BY position ASC, created_at ASC LIMIT 1"
                ).fetchone()
            if not row:
                worker_wakeup.wait(timeout=30)
                worker_wakeup.clear()
                continue
            try:
                run_job(row["id"])
            except Exception as e:
                with get_db() as conn:
                    conn.execute(
                        "UPDATE jobs SET status='error', error=?, finished_at=? "
                        "WHERE id=?",
                        (str(e), int(time.time()), row["id"]),
                    )
                    conn.commit()
        except Exception:
            traceback.print_exc()
            time.sleep(5)


def _check_basic_auth(header):
    if not header or not header.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(header[6:], validate=True).decode("utf-8")
    except Exception:
        return False
    if ":" not in decoded:
        return False
    user, pwd = decoded.split(":", 1)
    # compare_digest is constant-time to thwart timing attacks
    return (
        secrets.compare_digest(user, AUTH_USER)
        and secrets.compare_digest(pwd, AUTH_PASS)
    )


@app.before_request
def _enforce_auth():
    if not AUTH_REQUIRED:
        return None
    if _check_basic_auth(request.headers.get("Authorization", "")):
        return None
    return Response(
        "Authentication required\n",
        status=401,
        headers={"WWW-Authenticate": f'Basic realm="{AUTH_REALM}"'},
    )


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    file = request.files.get("file")
    name = (request.form.get("name") or "").strip()
    try:
        threads = int(request.form.get("threads", 30))
    except ValueError:
        threads = 30
    threads = max(1, min(200, threads))

    if not file or file.filename == "":
        return jsonify({"error": "未选择文件"}), 400

    # utf-8-sig strips a leading BOM if Notepad-style files include one;
    # otherwise the first domain ends up prefixed with U+FEFF and never
    # resolves.
    content = file.read().decode("utf-8-sig", errors="ignore")
    domains = [line.strip() for line in content.splitlines() if line.strip()]
    if not domains:
        return jsonify({"error": "文件为空"}), 400

    job_id = uuid.uuid4().hex
    job_dir = os.path.join(JOBS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    input_path = os.path.join(job_dir, "input.txt")
    alive_path = os.path.join(job_dir, "alive.csv")
    dead_path = os.path.join(job_dir, "dead.csv")

    with open(input_path, "w", encoding="utf-8") as f:
        for d in domains:
            f.write(d + "\n")

    if not name:
        name = file.filename or job_id[:8]

    # BEGIN IMMEDIATE acquires the write lock up front so two concurrent
    # uploads can't both read the same MAX(position) and end up with
    # duplicate queue positions.
    with get_db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        max_pos = conn.execute(
            "SELECT COALESCE(MAX(position), 0) FROM jobs"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO jobs (id, name, filename, total, threads, status, "
            "input_path, alive_path, dead_path, position, created_at) "
            "VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?)",
            (
                job_id, name, file.filename, len(domains), threads,
                input_path, alive_path, dead_path,
                max_pos + 1, int(time.time()),
            ),
        )
        conn.commit()

    worker_wakeup.set()
    return jsonify({"job_id": job_id, "total": len(domains), "name": name})


def _serialize_job(row):
    d = dict(row)
    live = live_jobs.get(d["id"])
    if live:
        d["alive"] = live["alive"]
        d["dead"] = live["dead"]
        d["done"] = live["done"]
        d["cancelling"] = live["cancel_event"].is_set()
    else:
        d["cancelling"] = False
    # Tell the frontend whether download buttons are worth showing.
    # A queued task that was cancelled never opened the CSV files.
    d["has_csv"] = bool(
        d.get("alive_path") and os.path.exists(d["alive_path"])
    )
    # Don't leak server filesystem paths to the browser.
    d.pop("alive_path", None)
    return d


@app.route("/jobs")
def jobs_list():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, filename, total, threads, status, "
            "alive, dead, done, error, position, alive_path, "
            "created_at, started_at, finished_at "
            "FROM jobs "
            "ORDER BY "
            "  CASE status "
            "    WHEN 'running' THEN 0 "
            "    WHEN 'queued'  THEN 1 "
            "    ELSE 2 END, "
            "  CASE WHEN status = 'queued' THEN position END ASC, "
            "  created_at DESC"
        ).fetchall()
    with live_lock:
        out = [_serialize_job(r) for r in rows]
    # Add visible queue rank for queued tasks (1-based).
    rank = 0
    for j in out:
        if j["status"] == "queued":
            rank += 1
            j["queue_rank"] = rank
        else:
            j["queue_rank"] = None
    return jsonify(out)


@app.route("/jobs/<job_id>/cancel", methods=["POST"])
def cancel_job(job_id):
    if not JOB_ID_RE.match(job_id or ""):
        return jsonify({"error": "非法 ID"}), 400

    with live_lock:
        live = live_jobs.get(job_id)
        if live:
            live["cancel_event"].set()
            return jsonify({"ok": True, "msg": "已请求终止,等待当前请求结束"})

    with get_db() as conn:
        row = conn.execute(
            "SELECT status FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
        if not row:
            return jsonify({"error": "任务不存在"}), 404
        if row["status"] != "queued":
            return jsonify({"error": "任务已结束,无法终止"}), 400
        # Conditional UPDATE: if the worker raced us and already moved this
        # job to 'running', rowcount==0 and we fall through to set the
        # in-memory cancel flag instead. Otherwise run_job would later
        # overwrite our 'cancelled' status with finished/error.
        cur = conn.execute(
            "UPDATE jobs SET status='cancelled', finished_at=? "
            "WHERE id=? AND status='queued'",
            (int(time.time()), job_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            with live_lock:
                live = live_jobs.get(job_id)
                if live:
                    live["cancel_event"].set()
                    return jsonify({"ok": True, "msg": "任务已开始,正在终止"})
            return jsonify({"error": "任务状态已变更,请刷新"}), 409
    return jsonify({"ok": True})


@app.route("/jobs/<job_id>/move_up", methods=["POST"])
def move_up(job_id):
    if not JOB_ID_RE.match(job_id or ""):
        return jsonify({"error": "非法 ID"}), 400

    with get_db() as conn:
        row = conn.execute(
            "SELECT position, status FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
        if not row:
            return jsonify({"error": "任务不存在"}), 404
        if row["status"] != "queued":
            return jsonify({"error": "只能调整等待中的任务"}), 400

        prev = conn.execute(
            "SELECT id, position FROM jobs WHERE status='queued' AND position<? "
            "ORDER BY position DESC LIMIT 1",
            (row["position"],),
        ).fetchone()
        if not prev:
            return jsonify({"ok": True, "msg": "已在队首"})

        # Swap positions via a temporary value to avoid UNIQUE collisions
        # (no UNIQUE here, but safer pattern).
        conn.execute("UPDATE jobs SET position=? WHERE id=?",
                     (prev["position"], job_id))
        conn.execute("UPDATE jobs SET position=? WHERE id=?",
                     (row["position"], prev["id"]))
        conn.commit()
    return jsonify({"ok": True})


@app.route("/jobs/<job_id>", methods=["DELETE"])
def delete_job(job_id):
    if not JOB_ID_RE.match(job_id or ""):
        return jsonify({"error": "非法 ID"}), 400

    with live_lock:
        if job_id in live_jobs:
            return jsonify({"error": "任务正在运行,请先终止"}), 409

    with get_db() as conn:
        row = conn.execute(
            "SELECT status FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
        if not row:
            return jsonify({"error": "任务不存在"}), 404
        if row["status"] == "running":
            return jsonify({"error": "任务正在运行,请先终止"}), 409
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
            "SELECT name, alive_path, dead_path FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
    if not row:
        abort(404)
    path = row["alive_path"] if kind == "alive" else row["dead_path"]
    if not path or not os.path.exists(path):
        abort(404)

    safe_name = re.sub(r"[^\w\-.]+", "_", row["name"] or "result").strip("_") or "result"
    return send_file(
        path, as_attachment=True,
        download_name=f"{safe_name}_{kind}.csv",
    )


init_db()
threading.Thread(target=queue_worker, daemon=True).start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
