"""
meor.com — Pipeline 2: Large TLDs (runs once daily)
Processes .net, .org, .info, .biz, .mobi, .com
These are large files (100MB-500MB) so run separately once per day.
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
log = logging.getLogger("meor.large")

BASE_DIR  = Path(__file__).parent
DATA_DIR  = BASE_DIR / "data"
CACHE_DIR = BASE_DIR / "zone_cache_large"
DATA_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

CZDS_USER    = os.getenv("CZDS_USERNAME", "")
CZDS_PASS    = os.getenv("CZDS_PASSWORD", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

LARGE_TLDS = ["net", "org", "info", "biz", "mobi", "com"]


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


def parse_zone(path, tld):
    """
    Parse zone file — returns set of BASE names WITHOUT the tld.
    Zone lines: name.tld. IN NS ns1.example.com.
    Strip trailing dot AND tld suffix so we get just 'name'.
    """
    domains = set()
    tld_suffix = f".{tld}"
    opener = gzip.open if str(path).endswith(".gz") else open
    try:
        with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
            for line in f:
                p = line.split()
                if len(p) >= 4 and p[2].upper() == "IN" and p[3].upper() == "NS":
                    d = p[0].rstrip(".").lower()
                    if not d:
                        continue
                    # Strip tld suffix if present to get clean base name
                    if d.endswith(tld_suffix):
                        d = d[:-len(tld_suffix)]
                    if d:
                        domains.add(d)
    except Exception as e:
        log.error(f"Parse error: {e}")
    log.info(f"  Parsed {len(domains):,} domains")
    return domains


def process_tld(tld, token):
    today_path     = CACHE_DIR / f"{tld}_today.txt.gz"
    yesterday_path = CACHE_DIR / f"{tld}_yesterday.txt.gz"

    if today_path.exists():
        shutil.copy2(today_path, yesterday_path)
        log.info(f"  .{tld} — rotated yesterday")

    url = f"https://czds-api.icann.org/czds/downloads/{tld}.zone"
    log.info(f"  .{tld} — downloading (large file, may take a while)...")

    try:
        with requests.get(url, headers={"Authorization": f"Bearer {token}"},
                          stream=True, timeout=600) as r:
            if r.status_code in (403, 404):
                log.warning(f"  .{tld} — not approved")
                return []
            r.raise_for_status()
            downloaded = 0
            with open(today_path, "wb") as f:
                for chunk in r.iter_content(1048576):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if downloaded % (50 * 1048576) == 0:
                        log.info(f"  .{tld} — {downloaded/1048576:.0f}MB downloaded...")
        log.info(f"  .{tld} — saved ({today_path.stat().st_size/1048576:.0f}MB)")
    except Exception as e:
        log.error(f"  .{tld} — failed: {e}")
        return []

    log.info(f"  .{tld} — parsing today...")
    today_domains = parse_zone(today_path, tld)

    if not yesterday_path.exists():
        log.info(f"  .{tld} — first run, cached for tomorrow")
        return []

    log.info(f"  .{tld} — parsing yesterday...")
    yesterday_domains = parse_zone(yesterday_path, tld)

    dropped = yesterday_domains - today_domains
    if not dropped:
        log.info(f"  .{tld} — no drops today")
        return []

    log.info(f"  .{tld} — {len(dropped):,} drops! 🎯")
    ts = date.today().isoformat()
    tld_suffix = f".{tld}"
    results = []
    for base in list(dropped)[:2000]:
        # base is clean (no tld) — build full domain safely, never doubled
        full_domain = f"{base}{tld_suffix}"
        results.append({
            "domain":        full_domain,
            "tld":           tld_suffix,
            "name":          base,
            "drop_date":     ts,
            "source":        "zone_file",
            "status":        "dropped",
            "dns_verified":  False,
            "checked_at":    datetime.utcnow().isoformat(),
            "is_arabic_idn": False
        })
    return results


def save_to_supabase(drops):
    if not drops: return
    if not SUPABASE_AVAILABLE or not SUPABASE_URL or not SUPABASE_KEY:
        save_csv(drops); return
    try:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        saved = 0
        for i in range(0, len(drops), 100):
            sb.table("dropped_domains").upsert(
                drops[i:i+100], on_conflict="domain,drop_date").execute()
            saved += 100
        log.info(f"✅ Saved {len(drops)} drops to Supabase")
    except Exception as e:
        log.error(f"Supabase error: {e}"); save_csv(drops)


def save_csv(drops):
    import csv
    path = DATA_DIR / f"drops_large_{date.today().isoformat()}.csv"
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=drops[0].keys())
        if path.stat().st_size == 0: w.writeheader()
        w.writerows(drops)
    log.info(f"Saved {len(drops)} to {path.name}")


def main():
    start = time.time()
    log.info(f"\n{'='*50}")
    log.info(f"meor.com Large TLD Pipeline — {date.today()}")
    log.info(f"{'='*50}")

    if not CZDS_USER or not CZDS_PASS:
        log.error("No CZDS credentials"); return

    token = get_token()
    if not token: return

    all_drops = []
    for tld in LARGE_TLDS:
        log.info(f"\nProcessing .{tld}...")
        drops = process_tld(tld, token)
        all_drops.extend(drops)
        log.info(f"  Running total: {len(all_drops)} drops")

    log.info(f"\nTotal drops: {len(all_drops)}")
    if all_drops:
        save_to_supabase(all_drops)

    log.info(f"✅ Done in {(time.time()-start)/60:.1f} minutes\n{'='*50}")


if __name__ == "__main__":
    main()
