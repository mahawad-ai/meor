"""
meor.com — Pipeline 1: Small TLDs (runs every hour)
Processes all approved TLDs under 50MB.
DNS-verifies all drops before inserting to Supabase.
"""

import os, gzip, time, json, logging, requests, shutil, socket
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

LARGE_TLDS       = {"net", "org", "info", "biz", "mobi", "com"}
MAX_SIZE_MB      = 50
MAX_DROPS        = 200
DNS_VERIFY_LIMIT = 100  # verify up to this many per TLD


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
        tlds = json.load(open(ap)).get("tlds", [])
    else:
        tlds = []
    tlds = [t for t in tlds if t not in LARGE_TLDS]
    log.info(f"Processing {len(tlds)} small TLDs this run")
    return tlds


def get_links(token):
    try:
        r = requests.get(
            "https://czds-api.icann.org/czds/downloads/links",
            headers={"Authorization": f"Bearer {token}"}, timeout=30)
        r.raise_for_status()
        return {link.split("/")[-1].replace(".zone", ""): link for link in r.json()}
    except Exception as e:
        log.error(f"Failed to get links: {e}")
        return {}


def dns_available(domain):
    try:
        socket.setdefaulttimeout(3)
        socket.getaddrinfo(domain, None)
        return False
    except socket.gaierror:
        return True
    except:
        return False


def parse_zone(path, tld):
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
                    if d.endswith(tld_suffix):
                        d = d[:-len(tld_suffix)]
                    if d:
                        domains.add(d)
    except Exception as e:
        log.error(f"Parse error: {e}")
    return domains


def process_tld(tld, link, token):
    today_path     = CACHE_DIR / f"{tld}_today.txt.gz"
    yesterday_path = CACHE_DIR / f"{tld}_yesterday.txt.gz"

    if today_path.exists():
        shutil.copy2(today_path, yesterday_path)

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
                    if size > MAX_SIZE_MB * 1024 * 1024:
                        log.warning(f"  .{tld} too large, skipping")
                        today_path.unlink(missing_ok=True)
                        return []
    except Exception as e:
        log.error(f"  .{tld} download failed: {e}")
        return []

    today_domains = parse_zone(today_path, tld)
    if not yesterday_path.exists():
        return []

    yesterday_domains = parse_zone(yesterday_path, tld)
    dropped = list(yesterday_domains - today_domains)
    if not dropped:
        return []

    log.info(f"  .{tld} — {len(dropped):,} candidates, DNS verifying...")

    tld_suffix = f".{tld}"
    verified = []
    for base in dropped[:DNS_VERIFY_LIMIT]:
        full_domain = f"{base}{tld_suffix}"
        if dns_available(full_domain):
            verified.append(base)
        time.sleep(0.05)

    if not verified:
        return []

    log.info(f"  .{tld} — {len(verified)} confirmed available 🎯")
    ts = date.today().isoformat()
    results = []
    for base in verified[:MAX_DROPS]:
        full_domain = f"{base}{tld_suffix}"
        results.append({
            "domain":        full_domain,
            "tld":           tld_suffix,
            "name":          base,
            "drop_date":     ts,
            "source":        "zone_file",
            "status":        "dropped",
            "dns_verified":  True,
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

    tlds  = get_all_approved_tlds()
    links = get_links(token)

    all_drops = []
    processed = 0
    skipped   = 0

    for tld in tlds:
        if tld not in links:
            skipped += 1
            continue
        drops = process_tld(tld, links[tld], token)
        all_drops.extend(drops)
        processed += 1
        if processed % 50 == 0:
            log.info(f"Progress: {processed}/{len(tlds)} TLDs, {len(all_drops)} drops")

    log.info(f"\nProcessed: {processed}, Skipped: {skipped}")
    log.info(f"Total verified drops: {len(all_drops)}")

    if all_drops:
        save_to_supabase(all_drops)

    log.info(f"✅ Done in {time.time()-start:.0f}s\n{'='*50}")


if __name__ == "__main__":
    main()
