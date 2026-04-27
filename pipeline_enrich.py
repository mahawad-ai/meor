"""
meor.com — Enrichment Pipeline
================================
Runs every 6 hours via GitHub Actions.
Enriches detected domains with SEO metrics, history, and DNS verification.

Key fixes in this version:
  - TLD priority: MENA + major generic TLDs enriched first
  - Junk TLD skip list: don't waste Wayback API calls on throwaway TLDs
  - Concurrent Wayback calls: 5x faster than sequential
  - Larger batch: 2000 domains per run (up from 500)
  - Better ordering: newest drops first for priority TLDs

Sources (all free):
  1. Open PageRank API — domain authority score 0-10
  2. Wayback Machine CDX API — first seen year, snapshot count
  3. DNS verification — confirms domain is actually available
"""

import os, time, logging, requests
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

try:
    from supabase import create_client; SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("meor.enrich")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
OPR_API_KEY  = os.getenv("OPR_API_KEY", "")

OPR_BATCH_SIZE = 100    # OPR API max per request
MAX_PER_RUN    = 2000   # domains enriched per run (was 500)
WAYBACK_WORKERS = 8     # concurrent Wayback calls (was 1)

# ── TLD Priority ──────────────────────────────────────────────────────────────
# These TLDs are enriched first — they have real value for MENA investors
PRIORITY_TLDS = {
    # MENA ccTLDs (highest priority — Meor's unique value)
    ".ae", ".sa", ".eg", ".ma", ".jo", ".qa", ".kw", ".bh", ".om",
    # Arabic IDN TLDs
    ".مصر", ".السعودية", ".امارات", ".قطر", ".البحرين", ".الكويت",
    # Major generic TLDs (high resale value)
    ".com", ".net", ".org", ".io", ".co", ".app", ".dev", ".ai",
    # Quality new gTLDs
    ".tech", ".store", ".blog", ".digital", ".media", ".agency",
    ".online", ".site", ".shop", ".cloud", ".pro", ".info",
}

# These TLDs are skipped entirely — cheap throwaway domains with near-zero value
# Saving Wayback API quota for domains that matter
SKIP_TLDS = {
    ".cyou", ".cfd", ".sbs", ".buzz", ".lol", ".icu", ".vip",
    ".click", ".fun", ".space", ".club", ".fun", ".sbs", ".top",
    ".xyz", ".pw", ".cc", ".tk", ".ml", ".ga", ".cf",
    ".loan", ".bid", ".trade", ".cricket", ".racing", ".party",
    ".review", ".science", ".accountant", ".webcam", ".faith",
    ".date", ".download", ".stream", ".gdn", ".men", ".win",
    ".xin", ".bid",
}


# ── Fetch unenriched domains ──────────────────────────────────────────────────

def get_unenriched(sb, limit=MAX_PER_RUN):
    """
    Fetch domains that need enrichment.
    Priority order:
      1. MENA ccTLDs + major generics (newest drops first)
      2. Everything else (newest first, skip junk TLDs)
    """
    domains = []

    # Pass 1: Priority TLDs — get the freshest drops first
    try:
        tld_filter = ",".join(PRIORITY_TLDS)
        result = sb.table("dropped_domains") \
            .select("id,domain,tld,name") \
            .is_("meor_score", "null") \
            .in_("tld", list(PRIORITY_TLDS)) \
            .order("drop_date", desc=True) \
            .order("created_at", desc=True) \
            .limit(limit) \
            .execute()
        domains = result.data or []
        log.info(f"Priority TLDs: {len(domains)} unenriched domains")
    except Exception as e:
        log.error(f"Failed to fetch priority domains: {e}")

    # Pass 2: Fill remaining slots with any non-junk TLD
    remaining = limit - len(domains)
    if remaining > 0:
        try:
            result = sb.table("dropped_domains") \
                .select("id,domain,tld,name") \
                .is_("meor_score", "null") \
                .not_.in_("tld", list(SKIP_TLDS | PRIORITY_TLDS)) \
                .order("drop_date", desc=True) \
                .order("created_at", desc=True) \
                .limit(remaining) \
                .execute()
            extra = result.data or []
            log.info(f"Other TLDs: {len(extra)} additional domains")
            domains.extend(extra)
        except Exception as e:
            log.error(f"Failed to fetch secondary domains: {e}")

    log.info(f"Total to enrich: {len(domains)}")
    return domains


# ── Open PageRank ─────────────────────────────────────────────────────────────

def get_pagerank_batch(domains):
    """Fetch OPR scores for up to 100 domains per request."""
    if not OPR_API_KEY:
        return {}

    results = {}
    for i in range(0, len(domains), OPR_BATCH_SIZE):
        batch = domains[i:i + OPR_BATCH_SIZE]
        try:
            params = "&".join(f"domains[]={d}" for d in batch)
            r = requests.get(
                f"https://openpagerank.com/api/v1.0/getPageRank?{params}",
                headers={"API-OPR": OPR_API_KEY},
                timeout=30,
            )
            r.raise_for_status()
            for item in r.json().get("response", []):
                if item.get("status_code") == 200:
                    results[item["domain"]] = {
                        "opr_score": item.get("page_rank_integer", 0),
                    }
        except Exception as e:
            log.error(f"OPR API error: {e}")
        time.sleep(0.5)

    log.info(f"OPR: data for {len(results)}/{len(domains)} domains")
    return results


# ── Wayback Machine ───────────────────────────────────────────────────────────

def get_wayback_data(domain):
    """
    Fetch first seen year and monthly snapshot count.
    No statuscode filter — captures parked pages and redirects too.
    Uses collapse=timestamp:6 for month-level deduplication.
    """
    base_url = "http://web.archive.org/cdx/search/cdx"
    headers  = {"User-Agent": "meor.com/1.0"}
    try:
        # First snapshot
        r = requests.get(base_url, params={
            "url": domain, "output": "json", "fl": "timestamp",
            "limit": 1, "collapse": "timestamp:6",
        }, headers=headers, timeout=10)

        if r.status_code == 200 and r.text.strip():
            data = r.json()
            if len(data) > 1:
                first_year = int(data[1][0][:4])

                # Monthly snapshot count
                rc = requests.get(base_url, params={
                    "url": domain, "output": "json", "fl": "timestamp",
                    "limit": 9999, "collapse": "timestamp:6",
                }, headers=headers, timeout=10)
                count = max(0, len(rc.json()) - 1) if rc.status_code == 200 else 0
                return first_year, count
    except Exception:
        pass
    return None, 0


def get_wayback_batch(domains):
    """Fetch Wayback data for multiple domains concurrently."""
    results = {}
    with ThreadPoolExecutor(max_workers=WAYBACK_WORKERS) as pool:
        futures = {pool.submit(get_wayback_data, d): d for d in domains}
        for future in as_completed(futures):
            domain = futures[future]
            try:
                first_year, count = future.result()
                results[domain] = (first_year, count)
            except Exception as e:
                log.error(f"Wayback error for {domain}: {e}")
                results[domain] = (None, 0)
    return results


# ── DNS verification ──────────────────────────────────────────────────────────

def verify_available(domain):
    """True if domain has no DNS record (NXDOMAIN = actually dropped)."""
    import socket
    try:
        socket.setdefaulttimeout(3)
        socket.getaddrinfo(domain, None)
        return False  # still resolves
    except socket.gaierror:
        return True   # NXDOMAIN
    except Exception:
        return False


# ── Score & verdict ───────────────────────────────────────────────────────────

def calc_meor_score(opr_score, first_year, wayback_count, dns_verified):
    """
    0-100 composite score:
      Age        → 0-35 pts  (primary — most reliable for expired domains)
      Wayback    → 0-30 pts  (logarithmic — 10 snaps=13pts, 1000 snaps=36pts)
      OPR        → 0-25 pts  (most expired domains score 0-2, so low weight)
      DNS verify → 0-10 pts
    """
    import math
    score = 0
    if first_year:
        age = max(0, 2025 - first_year)
        score += min(35, age * 2.5)
    if wayback_count and wayback_count > 0:
        score += min(30, math.log10(wayback_count + 1) * 12)
    score += min(25, (opr_score / 10) * 25)
    if dns_verified:
        score += 10
    return min(100, round(score))


def build_verdict(domain, meor_score, first_year, wb_count, tld):
    age = (2025 - first_year) if first_year else 0
    is_mena = tld in PRIORITY_TLDS and tld.startswith(".") and len(tld) <= 4

    if meor_score >= 65:
        ar = f"دومين قوي — عمره {age} {'سنة' if age == 1 else 'سنوات'} مع {wb_count} لقطة ويباك. يستحق التسجيل."
        en = f"Strong domain — {age} years old with {wb_count} Wayback snapshots. Worth registering."
    elif meor_score >= 40:
        ar = f"دومين متوسط — عمره {age} {'سنة' if age == 1 else 'سنوات'} مع {wb_count} لقطة. راجع التاريخ قبل التسجيل."
        en = f"Average domain — {age} years old with {wb_count} snapshots. Review history before registering."
    else:
        ar = f"دومين ضعيف — تاريخ محدود ({wb_count} لقطة ويباك). أولوية منخفضة."
        en = f"Weak domain — limited history ({wb_count} Wayback snapshots). Lower priority."

    return ar, en


# ── Supabase update ───────────────────────────────────────────────────────────

def bulk_update(sb, updates_list):
    """Batch update domains in Supabase."""
    saved = 0
    for upd in updates_list:
        domain_id = upd.pop("_id")
        try:
            sb.table("dropped_domains").update(upd).eq("id", domain_id).execute()
            saved += 1
        except Exception as e:
            log.error(f"Update failed for id {domain_id}: {e}")
    return saved


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    start = time.time()
    log.info(f"\n{'='*50}")
    log.info(f"meor.com Enrichment Pipeline — {date.today()}")
    log.info(f"{'='*50}")

    if not SUPABASE_AVAILABLE or not SUPABASE_URL or not SUPABASE_KEY:
        log.error("Supabase not configured"); return

    sb      = create_client(SUPABASE_URL, SUPABASE_KEY)
    domains = get_unenriched(sb)

    if not domains:
        log.info("Nothing to enrich — all caught up!"); return

    domain_names = [d["domain"] for d in domains]

    # OPR — batch fetch (fast)
    log.info(f"\nFetching OPR scores for {len(domain_names)} domains...")
    opr_data = get_pagerank_batch(domain_names)

    # Wayback — concurrent fetch (was the bottleneck)
    log.info(f"\nFetching Wayback data ({WAYBACK_WORKERS} concurrent)...")
    wayback_data = get_wayback_batch(domain_names)

    # DNS — quick check per domain
    log.info("\nRunning DNS checks...")
    dns_data = {}
    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(verify_available, d["domain"]): d["domain"] for d in domains}
        for future in as_completed(futures):
            dns_data[futures[future]] = future.result()

    # Build updates
    updates_list = []
    for d in domains:
        domain     = d["domain"]
        tld        = d.get("tld", "")
        opr_score  = opr_data.get(domain, {}).get("opr_score", 0)
        first_year, wb_count = wayback_data.get(domain, (None, 0))
        dns_ok     = dns_data.get(domain, False)
        meor_score = calc_meor_score(opr_score, first_year, wb_count, dns_ok)
        ar, en     = build_verdict(domain, meor_score, first_year, wb_count, tld)

        upd = {
            "_id":             d["id"],
            "meor_score":      meor_score,
            "wayback_snaps":   wb_count,
            "first_seen_year": first_year,
            "dns_verified":    dns_ok,
            "ai_verdict":      ar,
            "ai_verdict_en":   en,
            "checked_at":      datetime.utcnow().isoformat(),
        }
        if opr_score > 0:
            upd["moz_da"] = opr_score * 10
        updates_list.append(upd)

    log.info(f"\nSaving {len(updates_list)} enriched domains to Supabase...")
    saved = bulk_update(sb, updates_list)

    elapsed = time.time() - start
    log.info(f"\nEnriched {saved}/{len(domains)} domains in {elapsed:.0f}s")
    log.info(f"{'='*50}\n")


if __name__ == "__main__":
    main()
