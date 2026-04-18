"""
meor.com — MENA ccTLD Drop Detection Pipeline
Runs daily.

MENA registries (.ae .sa .eg .ma .jo .qa .kw .bh .om) don't
participate in ICANN CZDS, so we can't get zone files.
Instead we maintain a watchlist and poll via DNS.

How it works:
  1. Load watchlist from data/mena_watchlist.txt (persisted via GitHub Actions cache)
  2. If watchlist is too small (< 1000), reseed from multiple sources
  3. Poll a rotating batch of 600 domains via DNS
     - Resolves = still registered (skip)
     - NXDOMAIN  = domain dropped → insert to Supabase
  4. Rotate: move polled domains to end of list so every domain gets checked
  5. Dropped domains are kept in watchlist (they may re-register and drop again)

Watchlist format (CSV): domain,added_date,last_checked
"""

import os, time, json, logging, requests, socket
from datetime import datetime, date
from pathlib import Path

try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

try:
    from supabase import create_client; SUPA = True
except ImportError:
    SUPA = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("meor.mena")

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

WATCHLIST_PATH = DATA_DIR / "mena_watchlist.txt"

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

MENA_TLDS = ["ae", "sa", "eg", "ma", "jo", "qa", "kw", "bh", "om"]

ARABIC_IDN_TLDS = [
    "السعودية", "مصر", "امارات", "قطر", "البحرين", "الكويت", "عمان", "الاردن"
]

# Domain batch per run — large enough to find drops, small enough to finish in time
POLL_BATCH   = 600
RESEED_BELOW = 1000   # reseed if watchlist shrinks below this
DNS_TIMEOUT  = 3


# ── DNS check ────────────────────────────────────────────────────────────────

def is_dropped(domain):
    """
    Returns True if domain is NOT resolving (dropped/expired).
    NXDOMAIN = available. Any resolution = still registered.
    """
    try:
        socket.setdefaulttimeout(DNS_TIMEOUT)
        socket.getaddrinfo(domain, None)
        return False   # resolves → still registered
    except socket.gaierror:
        return True    # NXDOMAIN → dropped
    except Exception:
        return False   # timeout or error → assume registered


# ── Watchlist management ──────────────────────────────────────────────────────

def load_watchlist():
    if not WATCHLIST_PATH.exists():
        return []
    lines = []
    for line in WATCHLIST_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            lines.append(line)
    return lines


def save_watchlist(entries):
    WATCHLIST_PATH.write_text(
        "# meor.com MENA watchlist — domain,added_date,last_checked\n"
        + "\n".join(entries) + "\n",
        encoding="utf-8",
    )
    log.info(f"Watchlist saved: {len(entries)} domains")


def parse_domain(entry):
    return entry.split(",")[0].strip().lower()


def rotate(entries, polled_set):
    """Move polled entries to end of list so every domain gets checked over time."""
    not_polled = [e for e in entries if parse_domain(e) not in polled_set]
    polled     = [e for e in entries if parse_domain(e) in polled_set]
    return not_polled + polled


# ── Seeding ───────────────────────────────────────────────────────────────────

def seed_from_wayback(tld, limit=300):
    """Fetch known domains for a TLD from Wayback Machine CDX."""
    from urllib.parse import urlparse
    domains = set()
    try:
        r = requests.get(
            "http://web.archive.org/cdx/search/cdx",
            params={
                "url": f"*.{tld}",
                "output": "text",
                "fl": "original",
                "collapse": "urlkey",
                "limit": limit,
            },
            headers={"User-Agent": "meor.com/1.0"},
            timeout=20,
        )
        if r.status_code == 200:
            for line in r.text.strip().splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    host = urlparse(line if "://" in line else f"http://{line}").netloc
                    host = host.lower().split(":")[0].lstrip("www.")
                    if f".{tld}" in host and len(host) > len(tld) + 2:
                        domains.add(host)
                except Exception:
                    pass
        log.info(f"  Wayback .{tld}: {len(domains)} domains")
    except Exception as e:
        log.warning(f"  Wayback .{tld} failed: {e}")
    return domains


def seed_generated(tld):
    """
    Generate likely high-value domain names for a MENA TLD.
    Focuses on industries and keywords that have real demand in the region.
    """
    keywords = [
        # Generic high value
        "shop", "store", "online", "web", "digital", "tech", "media", "news",
        "sport", "sports", "food", "travel", "hotel", "hotels", "real", "estate",
        # Finance
        "bank", "finance", "money", "invest", "insurance", "gold", "crypto",
        # Real estate
        "home", "homes", "property", "properties", "land", "villa", "apartment",
        # Government adjacent
        "gov", "health", "education", "university", "school",
        # MENA-specific
        "dubai", "abudhabi", "riyadh", "jeddah", "cairo", "casablanca", "amman",
        "doha", "kuwait", "manama", "muscat", "beirut",
        # Business
        "company", "group", "holding", "international", "global", "services",
        "solutions", "consulting", "agency", "marketing",
        # E-commerce
        "mall", "market", "buy", "sell", "trade",
        # Arabic words transliterated
        "souq", "medina", "khalij", "noor", "baraka", "aman",
        # Short 3-4 letter combos (high value)
        "uae", "ksa", "gcc", "mena",
    ]
    return {f"{kw}.{tld}" for kw in keywords}


def reseed():
    """Build a fresh watchlist from Wayback + generated domains."""
    log.info("Reseeding MENA watchlist...")
    all_domains = set()

    for tld in MENA_TLDS:
        all_domains |= seed_from_wayback(tld, limit=400)
        all_domains |= seed_generated(tld)
        time.sleep(0.5)

    # Also include Arabic IDN TLDs
    for idn in ARABIC_IDN_TLDS:
        all_domains |= seed_generated(idn)

    today = date.today().isoformat()
    entries = [f"{d},{today}," for d in sorted(all_domains)]
    save_watchlist(entries)
    log.info(f"Seeded {len(entries)} MENA domains")
    return entries


# ── Poll ─────────────────────────────────────────────────────────────────────

def poll_batch(entries, batch_size):
    """
    Poll up to batch_size domains via DNS.
    Returns (drops, polled_domains_set).
    Dropped domains get inserted to Supabase.
    """
    drops      = []
    polled_set = set()
    ts         = date.today().isoformat()
    now        = datetime.utcnow().isoformat()

    batch = entries[:batch_size]
    log.info(f"Polling {len(batch)} MENA domains...")

    for entry in batch:
        domain = parse_domain(entry)
        if not domain:
            continue

        polled_set.add(domain)

        if is_dropped(domain):
            tld_part   = domain.split(".")[-1]
            tld_suffix = f".{tld_part}"
            base       = domain[: -len(tld_suffix)] if domain.endswith(tld_suffix) else domain
            is_idn     = any(ord(c) > 0x0600 for c in domain)

            drops.append({
                "domain":        domain,
                "tld":           tld_suffix,
                "name":          base,
                "drop_date":     ts,
                "source":        "dns_poll",
                "status":        "dropped",
                "dns_verified":  True,
                "checked_at":    now,
                "is_arabic_idn": is_idn,
            })
            log.info(f"  🎯 Drop: {domain}")

        time.sleep(0.05)

    log.info(f"Polled {len(polled_set)} domains, found {len(drops)} drops")
    return drops, polled_set


# ── Save ─────────────────────────────────────────────────────────────────────

def save_drops(sb, drops):
    if not drops:
        return
    for i in range(0, len(drops), 100):
        try:
            sb.table("dropped_domains").upsert(
                drops[i:i + 100], on_conflict="domain,drop_date"
            ).execute()
        except Exception as e:
            log.error(f"Supabase error: {e}")
    log.info(f"Saved {len(drops)} MENA drops")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    start = time.time()
    log.info(f"\n{'='*60}")
    log.info(f"meor.com MENA Pipeline — {date.today()}")
    log.info(f"{'='*60}")

    if not SUPA or not SUPABASE_URL or not SUPABASE_KEY:
        log.error("Supabase not configured"); return

    # Load or seed watchlist
    entries = load_watchlist()
    if len(entries) < RESEED_BELOW:
        log.info(f"Watchlist has {len(entries)} entries (< {RESEED_BELOW}), reseeding...")
        entries = reseed()

    log.info(f"Watchlist: {len(entries)} domains")

    # Poll a rotating batch
    drops, polled_set = poll_batch(entries, POLL_BATCH)

    # Save drops
    if drops:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        save_drops(sb, drops)

    # Rotate list: move polled domains to end (round-robin coverage)
    updated = rotate(entries, polled_set)
    save_watchlist(updated)

    log.info(f"Done in {time.time()-start:.0f}s")


if __name__ == "__main__":
    main()
