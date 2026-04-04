"""
meor.com — Pipeline 3: MENA DNS Polling (runs every 30 minutes)
Monitors Arabic ccTLDs: .ae .sa .eg .ma .jo .qa .kw .bh .om
No zone files needed — uses DNS checking directly.
Also tracks Arabic IDN domains.
"""

import os, time, json, logging, requests, socket
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
log = logging.getLogger("meor.mena")

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

MENA_TLDS = ["ae", "sa", "eg", "ma", "jo", "qa", "kw", "bh", "om"]

ARABIC_IDN_TLDS = {
    "السعودية": "xn--mgberp4a5d4ar",
    "مصر": "xn--wgbh1c",
    "امارات": "xn--mgbaam7a8h",
    "قطر": "xn--wgbl6a",
    "البحرين": "xn--ygbi2ammx",
    "الكويت": "xn--mgbayh7gpa",
    "عمان": "xn--mgb9awbf",
    "الاردن": "xn--ygbk9c7a",
}


def dns_available(domain):
    try:
        socket.setdefaulttimeout(3)
        socket.getaddrinfo(domain, None)
        return False
    except socket.gaierror:
        return True
    except:
        return False


def load_known_domains():
    path = DATA_DIR / "mena_known_domains.txt"
    if not path.exists():
        log.info("No MENA domains file — will seed now")
        seed_from_multiple_sources(path)
    if not path.exists():
        return []
    lines = [l.strip() for l in open(path) if l.strip() and not l.startswith("#")]
    return lines


def seed_from_multiple_sources(path):
    domains = set()
    headers = {"User-Agent": "meor.com/1.0 domain research tool"}
    for tld in MENA_TLDS:
        try:
            r = requests.get(
                "http://web.archive.org/cdx/search/cdx",
                params={"url": f"*.{tld}", "output": "text", "fl": "original",
                        "collapse": "urlkey", "limit": 500, "filter": "statuscode:200"},
                headers=headers, timeout=15)
            if r.status_code == 200:
                from urllib.parse import urlparse
                for line in r.text.strip().split("\n"):
                    line = line.strip()
                    if not line: continue
                    try:
                        host = urlparse(line if "://" in line else "http://"+line).netloc
                        host = host.lower().split(":")[0]
                        if f".{tld}" in host and len(host) > len(tld) + 2:
                            domains.add(host)
                    except: pass
            log.info(f"  Wayback seeded .{tld}: {len([d for d in domains if d.endswith('.'+tld)])} domains")
        except Exception as e:
            log.warning(f"  Wayback failed .{tld}: {e}")
        time.sleep(0.5)
    common_words = [
        "shop","store","online","web","digital","tech","media","news","sport",
        "food","travel","hotel","real","estate","dubai","riyadh","cairo","doha",
        "kuwait","bahrain","uae","ksa","egypt","qatar","oman","jordan",
    ]
    for tld in MENA_TLDS:
        for word in common_words:
            domains.add(f"{word}.{tld}")
    log.info(f"Total seeded: {len(domains)} MENA domains")
    with open(path, "w", encoding="utf-8") as f:
        f.write("# MENA domains monitored by meor.com\n")
        for d in sorted(domains):
            f.write(f"{d},{date.today().isoformat()}\n")


def poll_batch(lines, batch_size=500):
    drops = []
    ts = date.today().isoformat()
    sample = lines[:batch_size]
    log.info(f"Polling {len(sample)} MENA domains...")
    for line in sample:
        domain = line.split(",")[0].strip()
        if not domain: continue
        if dns_available(domain):
            # Build tld and base safely
            tld_part  = domain.split(".")[-1]
            tld_suffix = f".{tld_part}"
            # Strip tld from end to get base — never double
            base = domain[:-len(tld_suffix)] if domain.endswith(tld_suffix) else domain
            is_idn = any(ord(c) > 0x0600 for c in domain)
            drops.append({
                "domain":        domain,        # e.g. food.ae
                "tld":           tld_suffix,    # e.g. .ae
                "name":          base,          # e.g. food
                "drop_date":     ts,
                "source":        "dns_poll",
                "status":        "dropped",
                "dns_verified":  True,
                "checked_at":    datetime.utcnow().isoformat(),
                "is_arabic_idn": is_idn,
            })
            log.info(f"  🎯 Drop: {domain}")
        time.sleep(0.05)
    return drops, sample


def update_known_domains(path, lines, drops, polled):
    dropped_set = {d["domain"] for d in drops}
    polled_set  = {l.split(",")[0] for l in polled}
    unpolled   = [l for l in lines if l.split(",")[0] not in polled_set and l.split(",")[0] not in dropped_set]
    was_polled = [l for l in lines if l.split(",")[0] in polled_set and l.split(",")[0] not in dropped_set]
    updated = unpolled + was_polled
    with open(path, "w", encoding="utf-8") as f:
        f.write("# MENA domains monitored by meor.com\n")
        for l in updated:
            f.write(l + "\n")
    log.info(f"  Known domains: {len(updated)} remaining ({len(drops)} dropped)")


def save_to_supabase(drops):
    if not drops: return
    if not SUPABASE_AVAILABLE or not SUPABASE_URL or not SUPABASE_KEY:
        save_csv(drops); return
    try:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        for i in range(0, len(drops), 100):
            sb.table("dropped_domains").upsert(
                drops[i:i+100], on_conflict="domain,drop_date").execute()
        log.info(f"✅ Saved {len(drops)} MENA drops to Supabase")
    except Exception as e:
        log.error(f"Supabase error: {e}"); save_csv(drops)


def save_csv(drops):
    import csv
    path = DATA_DIR / f"drops_mena_{date.today().isoformat()}.csv"
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=drops[0].keys())
        if path.stat().st_size == 0: w.writeheader()
        w.writerows(drops)
    log.info(f"Saved {len(drops)} to {path.name}")


def main():
    start = time.time()
    log.info(f"\n{'='*50}")
    log.info(f"meor.com MENA Pipeline — {date.today()} {datetime.utcnow().strftime('%H:%M')} UTC")
    log.info(f"{'='*50}")
    known_path = DATA_DIR / "mena_known_domains.txt"
    lines = load_known_domains()
    if not lines:
        log.info("No domains to poll yet — seeded for next run")
        return
    log.info(f"Known MENA domains: {len(lines)}")
    drops, polled = poll_batch(lines, batch_size=500)
    log.info(f"\nMENA drops found: {len(drops)}")
    if drops:
        save_to_supabase(drops)
    update_known_domains(known_path, lines, drops, polled)
    log.info(f"✅ Done in {time.time()-start:.0f}s\n{'='*50}")


if __name__ == "__main__":
    main()
