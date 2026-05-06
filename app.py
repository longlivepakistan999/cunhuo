import os
import re
import csv
import uuid
import threading
import requests
import urllib3
from concurrent.futures import ThreadPoolExecutor
from urllib3.exceptions import InsecureRequestWarning
from flask import Flask, render_template, request, jsonify, send_from_directory, abort

urllib3.disable_warnings(InsecureRequestWarning)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 32 * 1024 * 1024

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JOBS_DIR = os.path.join(BASE_DIR, "jobs")
os.makedirs(JOBS_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0 Safari/537.36"
}
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)

jobs = {}
jobs_lock = threading.Lock()


def extract_title(html):
    if not html:
        return ""
    m = TITLE_RE.search(html)
    if not m:
        return ""
    title = re.sub(r"\s+", " ", m.group(1).strip())
    return title[:200]


def check_domain(domain, job, alive_writer, alive_fp, dead_writer, dead_fp, file_lock):
    domain = domain.strip()
    if not domain:
        with jobs_lock:
            job["done"] += 1
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
            with jobs_lock:
                job["alive"] += 1
                job["done"] += 1
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
        with jobs_lock:
            job["dead"] += 1
            job["done"] += 1


def run_job(job_id, domains, threads):
    job_dir = os.path.join(JOBS_DIR, job_id)
    alive_path = os.path.join(job_dir, "alive.csv")
    dead_path = os.path.join(job_dir, "dead.csv")

    file_lock = threading.Lock()
    job = jobs[job_id]

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

    try:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [
                executor.submit(
                    check_domain, d, job,
                    alive_writer, alive_fp,
                    dead_writer, dead_fp, file_lock,
                )
                for d in domains
            ]
            for f in futures:
                f.result()
        with jobs_lock:
            job["status"] = "finished"
    except Exception as e:
        with jobs_lock:
            job["status"] = "error"
            job["error"] = str(e)
    finally:
        alive_fp.close()
        dead_fp.close()


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

    with jobs_lock:
        jobs[job_id] = {
            "status": "running",
            "total": len(domains),
            "alive": 0,
            "dead": 0,
            "done": 0,
            "error": None,
        }

    t = threading.Thread(
        target=run_job, args=(job_id, domains, threads), daemon=True
    )
    t.start()

    return jsonify({"job_id": job_id, "total": len(domains), "threads": threads})


@app.route("/status/<job_id>")
def status(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return jsonify({"error": "任务不存在"}), 404
        return jsonify(dict(job))


@app.route("/download/<job_id>/<kind>")
def download(job_id, kind):
    if kind not in ("alive", "dead"):
        abort(404)
    if not re.fullmatch(r"[a-f0-9]{32}", job_id or ""):
        abort(404)
    job_dir = os.path.join(JOBS_DIR, job_id)
    filename = f"{kind}.csv"
    path = os.path.join(job_dir, filename)
    if not os.path.exists(path):
        abort(404)
    return send_from_directory(job_dir, filename, as_attachment=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
