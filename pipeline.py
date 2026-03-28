"""
meor.com — Fixed Domain Drop Pipeline
Uses GitHub Actions cache to persist zone files between runs.
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
log = logging.getLogger("meor")

BASE_DIR  = Path(__file__).parent
DATA_DIR  = BASE_DIR / "data"
CACHE_DIR = BASE_DIR / "zone_cache"
DATA_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

CZDS_USER    = os.getenv("CZDS_USERNAME", "")
CZDS_PASS    = os.getenv("CZDS_PASSWORD", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
BATCH_SIZE   = 5

PRIORITY_TLDS = [
    "net","org","info","biz","online","store","tech","shop","site","media",
    "digital","agency","services","solutions","travel","hotel","finance",
    "money","property","realestate","house","homes","estate","marketing",
    "consulting","management","group","global","center","world","today",
]
MENA_TLDS = ["ae","sa","eg","ma","jo","qa","kw","bh","om"]


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


def get_todays_batch():
    ap = DATA_DIR / "approved_tlds.json"
    all_tlds = json.load(open(ap)).get("tlds", []) if ap.exists() else PRIORITY_TLDS
    ordered = PRIORITY_TLDS + [t for t in all_tlds if t not in PRIORITY_TLDS]
    now = datetime.utcnow()
    offset = ((now.timetuple().tm_yday * 24 + now.hour) * BATCH_SIZE) % max(len(ordered), 1)
    batch = ordered[offset:offset + BATCH_SIZE]
    log.info(f"Today's batch: {['.'+t for t in batch]}")
    return batch


def download_zone(tld, token):
    today_path     = CACHE_DIR / f"{tld}_today.txt.gz"
    yesterday_path = CACHE_DIR / f"{tld}_yesterday.txt.gz"

    # Rotate today → yesterday
    if today_path.exists():
        shutil.copy2(today_path, yesterday_path)
        log.info(f"  .{tld} — rotated to yesterday")

    url = f"https://czds-api.icann.org/czds/downloads/{tld}.zone"
    log.info(f"  .{tld} — downloading...")
    try:
        with requests.get(url, headers={"Authorization": f"Bearer {token}"},
                          stream=True, timeout=120) as r:
            if r.status_code == 403:
                log.warning(f"  .{tld} — not approved, skipping.")
                return None, None
            r.raise_for_status()
            with open(today_path, "wb") as f:
                for chunk in r.iter_content(524288):
                    f.write(chunk)
        log.info(f"  .{tld} — saved ({today_path.stat().st_size/1048576:.1f} MB)")
        return today_path, yesterday_path if yesterday_path.exists() else None
    except Exception as e:
        log.error(f"  .{tld} — failed: {e}")
        return None, None


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
    log.info(f"  Parsed {len(domains):,} domains")
    return domains


def find_drops(tld, today, yesterday):
    dropped = yesterday - today
    if not dropped:
        log.info(f"  .{tld} — no drops")
        return []
    log.info(f"  .{tld} — {len(dropped):,} drops! 🎯")
    ts = date.today().isoformat()
    return [{"domain": f"{n}.{tld}", "tld": f".{tld}", "name": n,
             "drop_date": ts, "source": "zone_file", "status": "dropped",
             "dns_verified": False, "checked_at": datetime.utcnow().isoformat(),
             "is_arabic_idn": False} for n in list(dropped)[:500]]


def dns_available(domain):
    try:
        import socket; socket.getaddrinfo(domain, None); return False
    except: return True


def poll_mena():
    known_path = DATA_DIR / "mena_known_domains.txt"
    if not known_path.exists():
        log.info("  Seeding MENA domains...")
        seed_mena(known_path)
        return []

    drops = []
    ts = date.today().isoformat()
    lines = [l.strip() for l in open(known_path) if l.strip() and not l.startswith("#")]
    sample = lines[:300]
    log.info(f"  Polling {len(sample)} MENA domains...")

    for line in sample:
        domain = line.split(",")[0].strip()
        if dns_available(domain):
            tld = "." + domain.split(".")[-1]
            drops.append({"domain": domain, "tld": tld,
                "name": domain.replace(tld, "").rstrip("."),
                "drop_date": ts, "source": "dns_poll", "status": "dropped",
                "dns_verified": True, "checked_at": datetime.utcnow().isoformat(),
                "is_arabic_idn": any(ord(c) > 0x0600 for c in domain)})
            log.info(f"  🎯 {domain}")
        time.sleep(0.05)

    dropped_set = {d["domain"] for d in drops}
    remaining = [l for l in lines if l.split(",")[0] not in dropped_set]
    with open(known_path, "w") as f:
        f.write("# MENA domains\n")
        for l in remaining: f.write(l + "\n")

    log.info(f"  MENA: {len(drops)} drops")
    return drops


def seed_mena(known_path):
    from urllib.parse import urlparse
    domains = set()
    headers = {"User-Agent": "meor.com/1.0"}
    for tld in MENA_TLDS:
        try:
            r = requests.get("http://web.archive.org/cdx/search/cdx",
                params={"url": f"*.{tld}", "output": "text", "fl": "original",
                        "collapse": "urlkey", "limit": 300, "filter": "statuscode:200"},
                headers=headers, timeout=20)
            for line in r.text.strip().split("\n"):
                line = line.strip()
                if not line: continue
                try:
                    host = urlparse(line if "://" in line else "http://"+line).netloc
                    host = host.lower().split(":")[0]
                    if f".{tld}" in host: domains.add(host)
                except: pass
            log.info(f"  Seeded .{tld}")
        except Exception as e:
            log.warning(f"  Seed failed .{tld}: {e}")
        time.sleep(0.3)
    with open(known_path, "w") as f:
        f.write("# MENA domains\n")
        for d in sorted(domains): f.write(f"{d},{date.today().isoformat()}\n")
    log.info(f"  Seeded {len(domains)} MENA domains")


def save_to_supabase(drops):
    if not drops: return
    if not SUPABASE_AVAILABLE or not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("No Supabase — saving CSV")
        save_csv(drops); return
    try:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        saved = 0
        for i in range(0, len(drops), 100):
            sb.table("dropped_domains").upsert(
                drops[i:i+100], on_conflict="domain,drop_date").execute()
            saved += 100
        log.info(f"  ✅ Saved {len(drops)} to Supabase")
    except Exception as e:
        log.error(f"  Supabase error: {e}"); save_csv(drops)


def save_csv(drops):
    import csv
    path = DATA_DIR / f"drops_{date.today().isoformat()}.csv"
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=drops[0].keys())
        if path.stat().st_size == 0: w.writeheader()
        w.writerows(drops)
    log.info(f"  Saved {len(drops)} to {path.name}")


def main():
    start = time.time()
    log.info(f"\n{'='*50}\nmeor.com Pipeline — {date.today()}\n{'='*50}")
    all_drops = []

    if not CZDS_USER or not CZDS_PASS:
        log.warning("No CZDS credentials")
    else:
        token = get_token()
        if token:
            for tld in get_todays_batch():
                log.info(f"\nProcessing .{tld}...")
                today_p, yest_p = download_zone(tld, token)
                if not today_p: continue
                today_d = parse_zone(today_p)
                if not yest_p:
                    log.info(f"  .{tld} — cached for next run")
                    continue
                yest_d = parse_zone(yest_p)
                all_drops.extend(find_drops(tld, today_d, yest_d))

    log.info("\nPolling MENA...")
    all_drops.extend(poll_mena())

    log.info(f"\nTotal drops: {len(all_drops)}")
    if all_drops:
        save_to_supabase(all_drops)
    else:
        log.info("  Zone files cached — drops will appear next run")

    log.info(f"\n✅ Done in {time.time()-start:.0f}s\n{'='*50}")


if __name__ == "__main__":
    main()
