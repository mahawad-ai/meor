"""
meor.com — Fast Domain Drop Pipeline
=====================================
Designed to run every hour on GitHub Actions.
Processes a small batch of TLDs per run (fast, under 2 minutes).
Rotates through all approved TLDs over time.

Each run:
1. Picks 5 TLDs from the approved list (rotates daily)
2. Downloads their zone files from ICANN
3. Compares with previous run to find drops
4. Saves drops to Supabase
5. Also polls MENA ccTLDs via DNS (fast, no zone file needed)
"""

import os
import gzip
import time
import json
import logging
import requests
import hashlib
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

# ─── LOGGING ────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger("meor")

# ─── CONFIG ─────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DATA_DIR  = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

CZDS_USER = os.getenv("CZDS_USERNAME", "")
CZDS_PASS = os.getenv("CZDS_PASSWORD", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# How many TLDs to process per run (keeps each run under 2 minutes)
BATCH_SIZE = 5

# Priority TLDs — always check these first (high value for MENA users)
PRIORITY_TLDS = [
    "net", "org", "info", "biz", "online",
    "store", "tech", "shop", "site", "media",
    "digital", "agency", "services", "solutions",
    "travel", "hotel", "finance", "money", "property"
]

# MENA ccTLDs — tracked via DNS polling (no zone file needed)
MENA_TLDS = ["ae", "sa", "eg", "ma", "jo", "qa", "kw", "bh", "om"]

# ─── ICANN AUTH ─────────────────────────────────────
def get_token():
    log.info("Logging into ICANN CZDS...")
    try:
        resp = requests.post(
            "https://account-api.icann.org/api/authenticate",
            json={"username": CZDS_USER, "password": CZDS_PASS},
            timeout=30
        )
        resp.raise_for_status()
        token = resp.json().get("accessToken")
        log.info("Login successful.")
        return token
    except Exception as e:
        log.error(f"Login failed: {e}")
        return None

# ─── GET TODAY'S BATCH ──────────────────────────────
def get_todays_batch():
    """
    Returns 5 TLDs to process this run.
    Rotates daily through the approved list so all TLDs
    get covered over time.
    """
    approved_path = DATA_DIR / "approved_tlds.json"

    # Load approved TLDs if available
    if approved_path.exists():
        with open(approved_path) as f:
            data = json.load(f)
        all_tlds = data.get("tlds", [])
    else:
        all_tlds = PRIORITY_TLDS

    # Always start with priority TLDs
    ordered = PRIORITY_TLDS + [t for t in all_tlds if t not in PRIORITY_TLDS]

    # Pick batch based on day of year (rotates through all TLDs)
    day_of_year = datetime.utcnow().timetuple().tm_yday
    hour = datetime.utcnow().hour
    offset = ((day_of_year * 24 + hour) * BATCH_SIZE) % len(ordered)
    batch = ordered[offset:offset + BATCH_SIZE]

    log.info(f"Today's batch: {['.'+t for t in batch]}")
    return batch

# ─── DOWNLOAD ZONE FILE ─────────────────────────────
def download_zone(tld, token):
    """Download zone file for a TLD. Returns path or None."""
    today = date.today().isoformat()
    path = DATA_DIR / f"{tld}_{today}.txt.gz"

    if path.exists():
        log.info(f"  .{tld} — already downloaded today.")
        return path

    url = f"https://czds-api.icann.org/czds/downloads/{tld}.zone"
    headers = {"Authorization": f"Bearer {token}"}

    log.info(f"  .{tld} — downloading...")
    try:
        with requests.get(url, headers=headers, stream=True, timeout=120) as r:
            if r.status_code == 403:
                log.warning(f"  .{tld} — not approved yet, skipping.")
                return None
            r.raise_for_status()
            with open(path, "wb") as f:
                for chunk in r.iter_content(1024 * 512):  # 512KB chunks
                    f.write(chunk)
        size_mb = path.stat().st_size / (1024 * 1024)
        log.info(f"  .{tld} — saved ({size_mb:.1f} MB)")
        return path
    except Exception as e:
        log.error(f"  .{tld} — download failed: {e}")
        return None

# ─── PARSE ZONE FILE ────────────────────────────────
def parse_zone(path):
    """Extract domain names from zone file."""
    domains = set()
    opener = gzip.open if str(path).endswith(".gz") else open
    try:
        with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 4 and parts[2].upper() == "IN" and parts[3].upper() == "NS":
                    domain = parts[0].rstrip(".").lower()
                    if domain:
                        domains.add(domain)
    except Exception as e:
        log.error(f"Parse error: {e}")
    log.info(f"  Parsed {len(domains):,} domains")
    return domains

# ─── FIND DROPS ─────────────────────────────────────
def find_drops(tld, today_domains, yesterday_domains):
    """Find domains in yesterday's list but not today's = dropped."""
    dropped_names = yesterday_domains - today_domains
    if not dropped_names:
        return []
    log.info(f"  .{tld} — {len(dropped_names):,} drops detected!")
    today = date.today().isoformat()
    return [{
        "domain":     f"{name}.{tld}",
        "tld":        f".{tld}",
        "name":       name,
        "drop_date":  today,
        "source":     "zone_file",
        "status":     "dropped",
        "dns_verified": False,
        "checked_at": datetime.utcnow().isoformat(),
        "is_arabic_idn": False,
    } for name in list(dropped_names)[:1000]]  # cap at 1000 per TLD per run

# ─── DNS CHECK ──────────────────────────────────────
def dns_available(domain):
    """Check if domain is available via DNS."""
    try:
        import socket
        socket.getaddrinfo(domain, None)
        return False  # resolves = still registered
    except socket.gaierror:
        return True   # doesn't resolve = available

# ─── MENA DNS POLLING ───────────────────────────────
def poll_mena():
    """
    Poll MENA ccTLD domains via DNS.
    Reads known domains from file, checks each one.
    """
    known_path = DATA_DIR / "mena_known_domains.txt"
    if not known_path.exists():
        log.info("  No MENA domains file yet — seeding from common domains list...")
        seed_mena_domains()
        return []

    drops = []
    today = date.today().isoformat()
    updated = {}

    with open(known_path) as f:
        lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]

    # Only check a sample each run to stay fast
    sample = lines[:200]
    log.info(f"  Polling {len(sample)} MENA domains...")

    for line in sample:
        parts = line.split(",")
        domain = parts[0].strip()
        if dns_available(domain):
            tld = "." + domain.split(".")[-1]
            drops.append({
                "domain":       domain,
                "tld":          tld,
                "name":         domain.replace(tld, ""),
                "drop_date":    today,
                "source":       "dns_poll",
                "status":       "dropped",
                "dns_verified": True,
                "checked_at":   datetime.utcnow().isoformat(),
                "is_arabic_idn": any(ord(c) > 0x0600 for c in domain),
            })
            log.info(f"  🎯 Drop: {domain}")
        else:
            updated[domain] = today
        time.sleep(0.02)

    # Update known domains file
    remaining = [l for l in lines if l.split(",")[0] not in [d["domain"] for d in drops]]
    with open(known_path, "w") as f:
        f.write("# MENA domains monitored by meor.com\n")
        for line in remaining:
            f.write(line + "\n")

    log.info(f"  MENA polling done: {len(drops)} drops")
    return drops

def seed_mena_domains():
    """Seed initial MENA domain list from Wayback CDX."""
    known_path = DATA_DIR / "mena_known_domains.txt"
    domains = set()
    headers = {"User-Agent": "meor.com domain research tool/1.0"}

    for tld in MENA_TLDS:
        try:
            url = "http://web.archive.org/cdx/search/cdx"
            params = {
                "url": f"*.{tld}",
                "output": "text",
                "fl": "original",
                "collapse": "urlkey",
                "limit": 500,
                "filter": "statuscode:200"
            }
            resp = requests.get(url, params=params, headers=headers, timeout=20)
            for line in resp.text.strip().split("\n"):
                line = line.strip()
                if line and f".{tld}" in line:
                    try:
                        from urllib.parse import urlparse
                        host = urlparse(line if "://" in line else "http://"+line).netloc
                        host = host.lower().split(":")[0]
                        if host.endswith(f".{tld}") or host == tld:
                            domains.add(host)
                    except Exception:
                        pass
            log.info(f"  Seeded {tld}: found domains")
        except Exception as e:
            log.warning(f"  Seed failed for {tld}: {e}")
        time.sleep(0.5)

    with open(known_path, "w") as f:
        f.write("# MENA domains monitored by meor.com\n")
        for d in sorted(domains):
            f.write(f"{d},{date.today().isoformat()}\n")

    log.info(f"  Seeded {len(domains)} MENA domains.")

# ─── SAVE TO SUPABASE ───────────────────────────────
def save_to_supabase(drops):
    """Save dropped domains to Supabase."""
    if not drops:
        return
    if not SUPABASE_AVAILABLE or not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("Supabase not available — saving to CSV")
        save_to_csv(drops)
        return

    try:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        batch_size = 100
        saved = 0
        for i in range(0, len(drops), batch_size):
            batch = drops[i:i+batch_size]
            sb.table("dropped_domains").upsert(
                batch, on_conflict="domain,drop_date"
            ).execute()
            saved += len(batch)
        log.info(f"  ✅ Saved {saved} domains to Supabase")
    except Exception as e:
        log.error(f"  Supabase error: {e}")
        save_to_csv(drops)

def save_to_csv(drops):
    """Fallback: save to CSV."""
    import csv
    path = DATA_DIR / f"drops_{date.today().isoformat()}.csv"
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=drops[0].keys())
        if path.stat().st_size == 0:
            w.writeheader()
        w.writerows(drops)
    log.info(f"  Saved {len(drops)} domains to {path}")

# ─── CLEANUP ────────────────────────────────────────
def cleanup():
    """Delete zone files older than 3 days."""
    from datetime import timedelta
    cutoff = datetime.utcnow() - timedelta(days=3)
    for p in DATA_DIR.glob("*.txt.gz"):
        try:
            date_str = p.stem.split("_")[-1]
            if datetime.strptime(date_str, "%Y-%m-%d") < cutoff:
                p.unlink()
                log.info(f"  Deleted old file: {p.name}")
        except Exception:
            pass

# ─── MAIN ───────────────────────────────────────────
def main():
    start = time.time()
    today = date.today().isoformat()

    log.info(f"\n{'='*50}")
    log.info(f"meor.com Pipeline — {today}")
    log.info(f"{'='*50}")

    all_drops = []

    # ── PART A: gTLD zone files ──────────────────────
    if not CZDS_USER or not CZDS_PASS:
        log.warning("No CZDS credentials — skipping gTLD processing")
    else:
        token = get_token()
        if token:
            batch = get_todays_batch()
            for tld in batch:
                log.info(f"\nProcessing .{tld}...")
                today_path = download_zone(tld, token)
                if not today_path:
                    continue

                today_domains = parse_zone(today_path)

                # Find yesterday's file
                yesterday_files = sorted(DATA_DIR.glob(f"{tld}_*.txt.gz"))
                yesterday_files = [f for f in yesterday_files if f != today_path]

                if not yesterday_files:
                    log.info(f"  .{tld} — first run, no comparison yet")
                    continue

                yesterday_domains = parse_zone(yesterday_files[-1])
                drops = find_drops(tld, today_domains, yesterday_domains)
                all_drops.extend(drops)

    # ── PART B: MENA DNS polling ─────────────────────
    log.info("\nPolling MENA ccTLDs...")
    mena_drops = poll_mena()
    all_drops.extend(mena_drops)

    # ── PART C: Save ─────────────────────────────────
    log.info(f"\nTotal drops found: {len(all_drops)}")
    if all_drops:
        save_to_supabase(all_drops)

    # ── PART D: Cleanup ──────────────────────────────
    cleanup()

    elapsed = time.time() - start
    log.info(f"\n✅ Done in {elapsed:.0f} seconds")
    log.info(f"{'='*50}\n")

if __name__ == "__main__":
    main()
