import requests
import threading
import argparse
import csv
import re
from concurrent.futures import ThreadPoolExecutor
from urllib3.exceptions import InsecureRequestWarning
import urllib3
from tqdm import tqdm

urllib3.disable_warnings(InsecureRequestWarning)

# 全局锁,保护文件写入和计数器
lock = threading.Lock()
alive_count = 0
dead_count = 0

# 文件句柄(在 main 里初始化,worker 里写入)
alive_writer = None
dead_writer = None
alive_fp = None
dead_fp = None

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0 Safari/537.36"
}

TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)


def extract_title(html):
    if not html:
        return ""
    m = TITLE_RE.search(html)
    if not m:
        return ""
    title = m.group(1).strip()
    title = re.sub(r"\s+", " ", title)
    return title[:200]


def check_domain(domain, pbar):
    global alive_count, dead_count
    domain = domain.strip()
    if not domain:
        pbar.update(1)
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

            # 加锁写文件 + flush,保证立即落盘
            with lock:
                alive_writer.writerow(row)
                alive_fp.flush()
                alive_count += 1
                pbar.set_postfix(alive=alive_count, dead=dead_count, refresh=False)
            is_alive = True
            break
        except requests.exceptions.Timeout:
            last_error = "timeout"
        except requests.exceptions.ConnectionError:
            last_error = "connection_error"
        except requests.exceptions.RequestException as e:
            last_error = type(e).__name__

    if not is_alive:
        with lock:
            if dead_writer:
                dead_writer.writerow({"domain": domain, "reason": last_error})
                dead_fp.flush()
            dead_count += 1
            pbar.set_postfix(alive=alive_count, dead=dead_count, refresh=False)

    pbar.update(1)


def main():
    global alive_writer, dead_writer, alive_fp, dead_fp

    parser = argparse.ArgumentParser(description="资产存活探测")
    parser.add_argument("-f", "--file", required=True, help="域名文件")
    parser.add_argument("-t", "--threads", type=int, default=30, help="线程数")
    parser.add_argument("-o", "--output", default="alive.csv", help="存活结果文件")
    parser.add_argument("--dead", default="dead.csv", help="失败结果文件")
    parser.add_argument("--no-dead", action="store_true", help="不输出失败列表")
    args = parser.parse_args()

    with open(args.file, "r", encoding="utf-8") as f:
        domains = [line.strip() for line in f if line.strip()]

    total = len(domains)
    print(f"[*] 待测域名总数: {total}")
    print(f"[*] 线程数: {args.threads}\n")

    # 提前打开文件并写表头
    alive_fp = open(args.output, "w", encoding="utf-8-sig", newline="")
    alive_writer = csv.DictWriter(
        alive_fp,
        fieldnames=["url", "final_url", "status", "size", "title", "server"],
    )
    alive_writer.writeheader()
    alive_fp.flush()

    if not args.no_dead:
        dead_fp = open(args.dead, "w", encoding="utf-8-sig", newline="")
        dead_writer = csv.DictWriter(dead_fp, fieldnames=["domain", "reason"])
        dead_writer.writeheader()
        dead_fp.flush()

    try:
        with tqdm(total=total, desc="探测进度", ncols=100, unit="个",
                  bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} "
                             "[{elapsed}<{remaining}, {rate_fmt}] {postfix}") as pbar:
            with ThreadPoolExecutor(max_workers=args.threads) as executor:
                for domain in domains:
                    executor.submit(check_domain, domain, pbar)
    except KeyboardInterrupt:
        print("\n[!] 用户中断,已写入的结果已保存")
    finally:
        # 无论怎么退出都关闭文件,保证数据完整
        alive_fp.close()
        if dead_fp:
            dead_fp.close()

    print(f"\n[*] 探测完成: 存活 {alive_count} / 失败 {dead_count} / 总数 {total}")
    print(f"[*] 存活结果: {args.output}")
    if not args.no_dead:
        print(f"[*] 失败结果: {args.dead}")


if __name__ == "__main__":
    main()