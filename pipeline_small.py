"""
meor.com — Pipeline 1: Small TLDs (runs every hour)
Processes all approved TLDs under 50MB zone files.
Skips large TLDs (net, org, info, biz) — handled by pipeline_large.py
"""

import os, gzip, time, json, logging, requests, shutil
from datetime import datetime, date
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from supabase import create_client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("meor.small")

BASE_DIR  = Path(__file__).parent
DATA_DIR  = BASE_DIR / "data"
CACHE_DIR = BASE_DIR / "zone_cache"
DATA_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

CZDS_USER    = os.getenv("CZDS_USERNAME", "")
CZDS_PASS    = os.getenv("CZDS_PASSWORD", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# These are handled by pipeline_large.py
LARGE_TLDS = {"net", "org", "info", "biz", "mobi", "com"}

# Max file size to process in this pipeline (50MB)
MAX_SIZE_MB = 50


def get_token():
    log.info("Logging into ICANN CZDS...")
    try:
        r = requests.post(
            "https://account-api.icann.org/api/authenticate",
            json={"username": CZDS_USER, "password": CZDS_PASS}, timeout=30)
        r.raise_for_status()
        log.info("Login successful.")
        return r.json().get("accessToken")
    except Exception as e:
        log.error(f"Login failed: {e}")
        return None


def get_all_approved_tlds():
    ap = DATA_DIR / "approved_tlds.json"
    if ap.exists():
        data = json.load(open(ap))
        tlds = data.get("tlds", [])
    else:
        tlds = []
    # Remove large TLDs — handled separately
    tlds = [t for t in tlds if t not in LARGE_TLDS]
    log.info(f"Processing {len(tlds)} small TLDs this run")
    return tlds


def get_links(token):
    """Get all approved download links from CZDS."""
    try:
        r = requests.get(
            "https://czds-api.icann.org/czds/downloads/links",
            headers={"Authorization": f"Bearer {token}"}, timeout=30)
        r.raise_for_status()
        return {link.split("/")[-1].replace(".zone", ""): link
                for link in r.json()}
    except Exception as e:
        log.error(f"Failed to get links: {e}")
        return {}


def process_tld(tld, link, token):
    """Download, compare, return drops for one TLD."""
    today_path     = CACHE_DIR / f"{tld}_today.txt.gz"
    yesterday_path = CACHE_DIR / f"{tld}_yesterday.txt.gz"

    # Rotate today → yesterday
    if today_path.exists():
        shutil.copy2(today_path, yesterday_path)

    # Download
    try:
        with requests.get(link, headers={"Authorization": f"Bearer {token}"},
                          stream=True, timeout=60) as r:
            if r.status_code in (403, 404):
                return []
            r.raise_for_status()
            size = 0
            with open(today_path, "wb") as f:
                for chunk in r.iter_content(524288):
                    f.write(chunk)
                    size += len(chunk)
                    # Skip if file is getting too large
                    if size > MAX_SIZE_MB * 1024 * 1024:
                        log.warning(f"  .{tld} too large ({size/1048576:.0f}MB), skipping")
                        today_path.unlink(missing_ok=True)
                        return []
    except Exception as e:
        log.error(f"  .{tld} download failed: {e}")
        return []

    # Parse both files
    today_domains = parse_zone(today_path)
    if not yesterday_path.exists():
        return []  # First run, cached for next time

    yesterday_domains = parse_zone(yesterday_path)

    # Find drops
    dropped = yesterday_domains - today_domains
    if not dropped:
        return []

    log.info(f"  .{tld} — {len(dropped):,} drops! 🎯")
    ts = date.today().isoformat()
    return [{"domain": f"{n}.{tld}", "tld": f".{tld}", "name": n,
             "drop_date": ts, "source": "zone_file", "status": "dropped",
             "dns_verified": False, "checked_at": datetime.utcnow().isoformat(),
             "is_arabic_idn": False} for n in list(dropped)[:200]]


def parse_zone(path):
    domains = set()
    opener = gzip.open if str(path).endswith(".gz") else open
    try:
        with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
            for line in f:
                p = line.split()
                if len(p) >= 4 and p[2].upper() == "IN" and p[3].upper() == "NS":
                    d = p[0].rstrip(".").lower()
                    if d: domains.add(d)
    except Exception as e:
        log.error(f"Parse error: {e}")
    return domains


def save_to_supabase(drops):
    if not drops: return
    if not SUPABASE_AVAILABLE or not SUPABASE_URL or not SUPABASE_KEY:
        save_csv(drops); return
    try:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        for i in range(0, len(drops), 100):
            sb.table("dropped_domains").upsert(
                drops[i:i+100], on_conflict="domain,drop_date").execute()
        log.info(f"✅ Saved {len(drops)} drops to Supabase")
    except Exception as e:
        log.error(f"Supabase error: {e}"); save_csv(drops)


def save_csv(drops):
    import csv
    path = DATA_DIR / f"drops_small_{date.today().isoformat()}.csv"
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=drops[0].keys())
        if path.stat().st_size == 0: w.writeheader()
        w.writerows(drops)
    log.info(f"Saved {len(drops)} to {path.name}")


def main():
    start = time.time()
    log.info(f"\n{'='*50}")
    log.info(f"meor.com Small TLD Pipeline — {date.today()}")
    log.info(f"{'='*50}")

    if not CZDS_USER or not CZDS_PASS:
        log.error("No CZDS credentials"); return

    token = get_token()
    if not token: return

    tlds = get_all_approved_tlds()
    links = get_links(token)

    all_drops = []
    processed = 0
    skipped = 0

    for tld in tlds:
        if tld not in links:
            skipped += 1
            continue
        drops = process_tld(tld, links[tld], token)
        all_drops.extend(drops)
        processed += 1
        if processed % 50 == 0:
            log.info(f"Progress: {processed}/{len(tlds)} TLDs, {len(all_drops)} drops so far")

    log.info(f"\nProcessed: {processed} TLDs, Skipped: {skipped}")
    log.info(f"Total drops: {len(all_drops)}")

    if all_drops:
        save_to_supabase(all_drops)

    log.info(f"✅ Done in {time.time()-start:.0f}s\n{'='*50}")


if __name__ == "__main__":
    main()
