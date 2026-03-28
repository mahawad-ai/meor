"""
meor.com — Domain Drop Detection Pipeline
==========================================
What this script does, in plain language:
  1. Downloads the master list of every registered .com, .net, .ae etc domain
     from ICANN (it's a free public file called a "zone file")
  2. Compares today's list with yesterday's list
  3. Any domain in yesterday's list but NOT in today's = it was dropped
  4. Saves all dropped domains to your database
  5. Also does a quick DNS check to confirm the domain is actually available

Run this once a day (the scheduler at the bottom handles that automatically).

HOW TO RUN:
  1. Install dependencies:   pip install requests dnspython python-dotenv supabase schedule
  2. Fill in your .env file (copy .env.example to .env)
  3. Run:  python pipeline.py
"""

import os
import gzip
import time
import logging
import hashlib
import requests
import schedule
import dns.resolver
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client

# ─────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────

load_dotenv()  # reads your .env file

# Folders
BASE_DIR   = Path(__file__).parent
DATA_DIR   = BASE_DIR / "data"
LOG_DIR    = BASE_DIR / "logs"
DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# Logging — writes to both screen and a log file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "pipeline.log")
    ]
)
log = logging.getLogger("meor")

# Supabase connection
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = None  # connected later in main()

# ICANN credentials (you get these after CZDS approval)
CZDS_USER = os.getenv("CZDS_USERNAME")
CZDS_PASS = os.getenv("CZDS_PASSWORD")

# ─────────────────────────────────────────────
# WHICH TLDs TO TRACK
# ─────────────────────────────────────────────
# These are the zone files ICANN gives us for free.
# Arabic ccTLDs (.ae, .sa etc) we track separately via DNS polling below.

GTLD_ZONE_FILES = [
    "com",    # biggest — ~160 million domains
    "net",
    "org",
    "info",
    "biz",
    "mobi",
    # new gTLDs — add more after CZDS approved
    "online",
    "store",
    "site",
    "tech",
    "shop",
]

# Arabic / MENA ccTLDs — we track these via DNS polling
# because their registries don't share zone files publicly
MENA_CCTLDS = {
    ".ae":  "xn--mgbaam7a8h",   # .امارات
    ".sa":  "xn--mgberp4a5d4ar", # .السعودية
    ".eg":  "xn--wgbh1c",        # .مصر
    ".ma":  "xn--mgbc0a9azcg",   # .المغرب
    ".jo":  "xn--mgbayh7gpa",    # .الاردن
    ".qa":  "xn--wgbl6a",        # .قطر
}

# ─────────────────────────────────────────────
# STEP 1 — DOWNLOAD ZONE FILE FROM ICANN CZDS
# ─────────────────────────────────────────────

def get_czds_token() -> str:
    """
    Log into ICANN's system and get an access token.
    Think of this like logging into a website to download a file.
    """
    log.info("Logging into ICANN CZDS...")
    resp = requests.post(
        "https://account-api.icann.org/api/authenticate",
        json={"username": CZDS_USER, "password": CZDS_PASS},
        timeout=30
    )
    resp.raise_for_status()
    token = resp.json().get("accessToken")
    log.info("ICANN login successful.")
    return token


def download_zone_file(tld: str, token: str) -> Path:
    """
    Downloads the zone file for a given TLD (e.g. 'com').
    The zone file is a compressed text file with one domain per line.
    We save it locally so we can compare with yesterday's version.

    Returns the path to the saved file.
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    save_path = DATA_DIR / f"{tld}_{today}.txt.gz"

    # Don't re-download if we already have today's file
    if save_path.exists():
        log.info(f"  .{tld} — already downloaded today, skipping.")
        return save_path

    url = f"https://czds-api.icann.org/czds/downloads/{tld}.zone"
    log.info(f"  .{tld} — downloading zone file...")

    headers = {"Authorization": f"Bearer {token}"}
    with requests.get(url, headers=headers, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(save_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                f.write(chunk)

    size_mb = save_path.stat().st_size / (1024 * 1024)
    log.info(f"  .{tld} — saved ({size_mb:.1f} MB)")
    return save_path


def parse_zone_file(path: Path) -> set:
    """
    Reads a zone file and extracts just the domain names.
    Zone files have lots of technical DNS records — we only want
    lines that look like:  domainname  IN  NS  nameserver

    Returns a set of domain names (lowercase, no duplicates).
    Example: {'google', 'facebook', 'amazon', ...}
    """
    domains = set()
    opener = gzip.open if str(path).endswith(".gz") else open

    with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith(";"):  # skip comments
                continue
            parts = line.split()
            # Zone file format: NAME  TTL  CLASS  TYPE  DATA
            # We want lines where TYPE is NS (nameserver) — these are registered domains
            if len(parts) >= 4 and parts[2].upper() == "IN" and parts[3].upper() == "NS":
                domain = parts[0].rstrip(".").lower()
                if domain:
                    domains.add(domain)

    log.info(f"  Parsed {len(domains):,} domains from {path.name}")
    return domains

# ─────────────────────────────────────────────
# STEP 2 — COMPARE TODAY vs YESTERDAY
# ─────────────────────────────────────────────

def get_yesterday_path(tld: str) -> Path | None:
    """
    Finds yesterday's zone file for a given TLD.
    Returns None if we don't have yesterday's file yet
    (this happens on the first run).
    """
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    path = DATA_DIR / f"{tld}_{yesterday}.txt.gz"
    return path if path.exists() else None


def find_dropped_domains(tld: str, today_domains: set, yesterday_domains: set) -> list:
    """
    Compares two sets of domains.
    Any domain in yesterday's set but NOT in today's set = dropped.

    Returns a list of dicts ready to save to the database.
    """
    dropped_names = yesterday_domains - today_domains

    if not dropped_names:
        log.info(f"  .{tld} — no dropped domains detected today.")
        return []

    log.info(f"  .{tld} — found {len(dropped_names):,} dropped domains!")

    dropped = []
    for name in dropped_names:
        full_domain = f"{name}.{tld}"
        dropped.append({
            "domain":     full_domain,
            "tld":        f".{tld}",
            "name":       name,
            "drop_date":  datetime.utcnow().strftime("%Y-%m-%d"),
            "source":     "zone_file",
            "status":     "dropped",
            "checked_at": datetime.utcnow().isoformat(),
        })

    return dropped

# ─────────────────────────────────────────────
# STEP 3 — DNS VERIFICATION
# ─────────────────────────────────────────────

def verify_domain_available(domain: str) -> bool:
    """
    Double-checks that a domain is actually available by doing a DNS lookup.
    If the DNS lookup fails (NXDOMAIN = "domain does not exist"), 
    it means the domain is genuinely available to register.

    This catches false positives from zone file diffs.
    """
    try:
        dns.resolver.resolve(domain, "NS", lifetime=5.0)
        # Got a result = domain still has nameservers = NOT dropped yet
        return False
    except dns.resolver.NXDOMAIN:
        # NXDOMAIN = "no such domain" = it's available!
        return True
    except Exception:
        # Timeout or other error — assume available (conservative)
        return True


def verify_batch(dropped: list, sample_size: int = 100) -> list:
    """
    DNS-checks a sample of dropped domains to filter out false positives.
    We check a sample (not all) to keep things fast — zone files can have
    tens of thousands of drops per day.

    For production you'd want to check them all asynchronously.
    """
    if not dropped:
        return []

    # Check up to sample_size domains
    to_check = dropped[:sample_size]
    remaining = dropped[sample_size:]

    log.info(f"  DNS-verifying {len(to_check)} domains...")
    confirmed = []

    for item in to_check:
        available = verify_domain_available(item["domain"])
        if available:
            item["dns_verified"] = True
            confirmed.append(item)
        time.sleep(0.05)  # small delay to avoid hitting DNS rate limits

    # Add remaining without verification (marked as unverified)
    for item in remaining:
        item["dns_verified"] = False
        confirmed.append(item)

    log.info(f"  DNS check: {len([x for x in confirmed if x.get('dns_verified')])} confirmed available")
    return confirmed

# ─────────────────────────────────────────────
# STEP 4 — MENA ccTLD TRACKING (DNS POLLING)
# ─────────────────────────────────────────────
# .ae, .sa, .eg, .ma don't give us zone files.
# Instead we maintain a list of known MENA domains and poll them daily.
# Any that stop resolving = dropped.

def load_mena_known_domains() -> dict:
    """
    Loads the list of MENA domains we're tracking.
    File format: one domain per line.
    Returns a dict of {domain: last_seen_date}
    """
    known = {}
    path = DATA_DIR / "mena_known_domains.txt"

    if not path.exists():
        log.info("  No MENA known domains file yet — will create on first run.")
        return known

    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                parts = line.split(",")
                domain = parts[0].strip()
                last_seen = parts[1].strip() if len(parts) > 1 else "unknown"
                known[domain] = last_seen

    log.info(f"  Loaded {len(known):,} known MENA domains to monitor.")
    return known


def save_mena_known_domains(known: dict):
    """Saves the updated MENA domain list back to disk."""
    path = DATA_DIR / "mena_known_domains.txt"
    today = datetime.utcnow().strftime("%Y-%m-%d")

    with open(path, "w") as f:
        f.write("# MENA domains being monitored by meor.com pipeline\n")
        f.write("# Format: domain, last_seen_date\n")
        for domain, last_seen in sorted(known.items()):
            f.write(f"{domain},{last_seen}\n")


def poll_mena_domains(known: dict) -> tuple[list, dict]:
    """
    Checks each known MENA domain via DNS.
    Returns:
        - list of newly dropped domains
        - updated known dict with today's last_seen dates
    """
    dropped = []
    today = datetime.utcnow().strftime("%Y-%m-%d")
    updated_known = {}

    log.info(f"  Polling {len(known):,} MENA domains via DNS...")

    for domain, last_seen in known.items():
        available = verify_domain_available(domain)

        if available:
            # Was known, now not resolving = dropped!
            tld = "." + domain.split(".")[-1]
            dropped.append({
                "domain":     domain,
                "tld":        tld,
                "name":       domain.replace(tld, ""),
                "drop_date":  today,
                "source":     "dns_poll",
                "status":     "dropped",
                "dns_verified": True,
                "checked_at": datetime.utcnow().isoformat(),
            })
            log.info(f"  🎯 MENA drop detected: {domain}")
            # Don't add back to known — it's gone
        else:
            # Still registered — update last seen
            updated_known[domain] = today
            time.sleep(0.02)

    log.info(f"  MENA polling complete: {len(dropped)} new drops found.")
    return dropped, updated_known


def seed_mena_domains_from_web(tld: str, limit: int = 1000):
    """
    Seeds our MENA known-domains list by scraping public domain lists.
    We use the Wayback CDX API — it has records of Arabic domains from
    web crawls. Free, no API key needed.

    This runs once to build your initial list. After that, daily DNS
    polling keeps it updated.
    """
    log.info(f"  Seeding known domains for {tld} from Wayback CDX...")

    url = "http://web.archive.org/cdx/search/cdx"
    params = {
        "url":        f"*.{tld}",
        "output":     "text",
        "fl":         "original",
        "collapse":   "urlkey",
        "limit":      limit,
        "filter":     "statuscode:200",
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()

        domains = set()
        for line in resp.text.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            # Extract just the domain from a full URL
            try:
                from urllib.parse import urlparse
                parsed = urlparse(line if line.startswith("http") else "http://" + line)
                host = parsed.netloc or parsed.path.split("/")[0]
                host = host.lower().strip()
                if host.endswith(f".{tld}") or host == tld:
                    domains.add(host)
            except Exception:
                continue

        log.info(f"  Found {len(domains)} {tld} domains from Wayback CDX.")
        return domains

    except Exception as e:
        log.warning(f"  Wayback CDX seed failed for {tld}: {e}")
        return set()

# ─────────────────────────────────────────────
# STEP 5 — SAVE TO DATABASE
# ─────────────────────────────────────────────

def save_to_database(dropped: list):
    """
    Saves dropped domains to your Supabase database.
    Uses "upsert" which means: insert if new, update if already exists.
    This prevents duplicate entries if the script runs twice in a day.
    """
    if not dropped:
        return

    if not supabase:
        log.warning("  Supabase not connected — saving to local file instead.")
        save_to_local_file(dropped)
        return

    # Split into batches of 500 to avoid request size limits
    batch_size = 500
    total_saved = 0

    for i in range(0, len(dropped), batch_size):
        batch = dropped[i:i + batch_size]
        try:
            result = supabase.table("dropped_domains").upsert(
                batch,
                on_conflict="domain,drop_date"  # don't duplicate same domain+date
            ).execute()
            total_saved += len(batch)
            log.info(f"  Saved batch {i//batch_size + 1}: {len(batch)} domains")
        except Exception as e:
            log.error(f"  Database error on batch {i//batch_size + 1}: {e}")
            save_to_local_file(batch)  # fallback to local file

    log.info(f"  ✅ Total saved to database: {total_saved:,} domains")


def save_to_local_file(dropped: list):
    """
    Fallback: saves dropped domains to a local CSV file.
    Useful for testing before you have a database set up.
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    path = DATA_DIR / f"dropped_{today}.csv"

    import csv
    write_header = not path.exists()

    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "domain", "tld", "name", "drop_date",
            "source", "status", "dns_verified", "checked_at"
        ])
        if write_header:
            writer.writeheader()
        writer.writerows(dropped)

    log.info(f"  Saved {len(dropped)} domains to {path}")

# ─────────────────────────────────────────────
# STEP 6 — CLEANUP OLD FILES
# ─────────────────────────────────────────────

def cleanup_old_zone_files(keep_days: int = 3):
    """
    Zone files are large (the .com zone file is ~10GB compressed).
    We only need the last 2 days to detect drops, so we delete older files.
    """
    cutoff = datetime.utcnow() - timedelta(days=keep_days)

    for path in DATA_DIR.glob("*.txt.gz"):
        try:
            # Extract date from filename like "com_2025-03-15.txt.gz"
            date_str = path.stem.split("_")[1].replace(".txt", "")
            file_date = datetime.strptime(date_str, "%Y-%m-%d")
            if file_date < cutoff:
                path.unlink()
                log.info(f"  Deleted old zone file: {path.name}")
        except (IndexError, ValueError):
            pass

# ─────────────────────────────────────────────
# MAIN DAILY JOB
# ─────────────────────────────────────────────

def run_daily_pipeline():
    """
    The main function that runs every day.
    Orchestrates all the steps above.
    """
    start_time = time.time()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    log.info(f"\n{'='*60}")
    log.info(f"meor.com Pipeline — {today}")
    log.info(f"{'='*60}")

    all_dropped = []

    # ── PART A: gTLD zone files (com, net, org etc) ──────────────
    log.info("\n[PART A] Processing gTLD zone files via ICANN CZDS")

    if not CZDS_USER or not CZDS_PASS:
        log.warning("  CZDS credentials not set in .env — skipping gTLD processing.")
        log.warning("  Apply at czds.icann.org and add credentials to your .env file.")
    else:
        try:
            token = get_czds_token()

            for tld in GTLD_ZONE_FILES:
                log.info(f"\n  Processing .{tld}...")

                # Download today's zone file
                today_path = download_zone_file(tld, token)
                today_domains = parse_zone_file(today_path)

                # Get yesterday's file
                yesterday_path = get_yesterday_path(tld)
                if not yesterday_path:
                    log.info(f"  .{tld} — no yesterday file yet (first run). Skipping diff.")
                    continue

                yesterday_domains = parse_zone_file(yesterday_path)

                # Find drops
                dropped = find_dropped_domains(tld, today_domains, yesterday_domains)

                # Verify via DNS
                if dropped:
                    dropped = verify_batch(dropped)
                    all_dropped.extend(dropped)

        except Exception as e:
            log.error(f"  gTLD processing failed: {e}")

    # ── PART B: MENA ccTLDs via DNS polling ──────────────────────
    log.info("\n[PART B] Processing MENA ccTLDs via DNS polling")

    known_mena = load_mena_known_domains()

    # First run: seed the known domains list from Wayback CDX
    if not known_mena:
        log.info("  First run detected — seeding MENA domain list from Wayback CDX...")
        for tld in ["ae", "sa", "eg", "ma"]:
            seeded = seed_mena_domains_from_web(tld, limit=2000)
            for domain in seeded:
                known_mena[domain] = today
        save_mena_known_domains(known_mena)
        log.info(f"  Seeded {len(known_mena):,} MENA domains. Will start detecting drops tomorrow.")
    else:
        # Regular run: poll known domains
        mena_dropped, updated_known = poll_mena_domains(known_mena)
        save_mena_known_domains(updated_known)
        all_dropped.extend(mena_dropped)

    # ── PART C: Save everything ────────────────────────────────────
    log.info(f"\n[PART C] Saving results")
    log.info(f"  Total dropped domains found today: {len(all_dropped):,}")

    if all_dropped:
        save_to_database(all_dropped)

    # ── PART D: Cleanup ────────────────────────────────────────────
    log.info("\n[PART D] Cleanup")
    cleanup_old_zone_files(keep_days=3)

    elapsed = time.time() - start_time
    log.info(f"\n✅ Pipeline complete in {elapsed/60:.1f} minutes")
    log.info(f"{'='*60}\n")


# ─────────────────────────────────────────────
# SCHEDULER — runs the pipeline every day at 6am UTC
# ─────────────────────────────────────────────

def main():
    global supabase

    log.info("meor.com Data Pipeline starting...")

    # Connect to Supabase
    if SUPABASE_URL and SUPABASE_KEY:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        log.info("✅ Connected to Supabase database")
    else:
        log.warning("⚠️  No Supabase credentials — will save to local CSV files")
        log.warning("   Add SUPABASE_URL and SUPABASE_KEY to your .env file")

    # Run once immediately on startup
    log.info("Running initial pipeline...")
    run_daily_pipeline()

    # Then schedule to run every day at 06:00 UTC
    # (ICANN zone files are typically updated between 00:00-05:00 UTC)
    schedule.every().day.at("06:00").do(run_daily_pipeline)
    log.info("Scheduled to run daily at 06:00 UTC")
    log.info("Press Ctrl+C to stop.\n")

    while True:
        schedule.run_pending()
        time.sleep(60)  # check every minute


if __name__ == "__main__":
    main()
