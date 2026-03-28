"""
meor.com — Enrichment Pipeline
================================
Runs after drop detection to fill in SEO metrics for each domain.

Sources (all free):
1. Open PageRank API — page rank score 0-10
2. Wayback Machine CDX API — first seen year, snapshot count
3. DNS verification — confirms domain is actually available

Runs every 6 hours via GitHub Actions.
Processes up to 500 unenriched domains per run.
"""

import os, time, json, logging, requests
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
log = logging.getLogger("meor.enrich")

SUPABASE_URL  = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY", "")
OPR_API_KEY   = os.getenv("OPR_API_KEY", "")  # Open PageRank API key

BATCH_SIZE    = 100   # domains per OPR API call (max 100)
MAX_PER_RUN   = 500   # max domains to enrich per run


# ─── FETCH UNENRICHED DOMAINS FROM SUPABASE ─────────
def get_unenriched(sb, limit=MAX_PER_RUN):
    """Get domains that haven't been enriched yet (meor_score is null)."""
    try:
        result = sb.table("dropped_domains") \
            .select("id,domain,tld,name") \
            .is_("meor_score", "null") \
            .order("created_at", desc=True) \
            .limit(limit) \
            .execute()
        domains = result.data or []
        log.info(f"Found {len(domains)} unenriched domains")
        return domains
    except Exception as e:
        log.error(f"Failed to fetch domains: {e}")
        return []


# ─── OPEN PAGERANK API ───────────────────────────────
def get_pagerank_batch(domains):
    """
    Fetch Open PageRank scores for up to 100 domains at once.
    Returns dict: {domain: {page_rank_integer, page_rank_decimal, rank}}
    """
    if not OPR_API_KEY:
        log.warning("No OPR_API_KEY set — skipping PageRank enrichment")
        return {}

    results = {}
    # API accepts up to 100 domains per request
    for i in range(0, len(domains), BATCH_SIZE):
        batch = domains[i:i+BATCH_SIZE]
        try:
            params = "&".join(f"domains[]={d}" for d in batch)
            url = f"https://openpagerank.com/api/v1.0/getPageRank?{params}"
            r = requests.get(
                url,
                headers={"API-OPR": OPR_API_KEY},
                timeout=30
            )
            r.raise_for_status()
            data = r.json()

            for item in data.get("response", []):
                if item.get("status_code") == 200:
                    results[item["domain"]] = {
                        "opr_score":   item.get("page_rank_integer", 0),
                        "opr_decimal": item.get("page_rank_decimal", 0),
                        "opr_rank":    item.get("rank"),
                    }
            log.info(f"  OPR: got data for {len(results)} domains so far")
        except Exception as e:
            log.error(f"  OPR API error: {e}")
        time.sleep(0.5)

    return results


# ─── WAYBACK MACHINE ────────────────────────────────
def get_wayback_data(domain):
    """
    Get Wayback Machine data for a domain:
    - First seen year
    - Number of snapshots
    """
    try:
        url = "http://web.archive.org/cdx/search/cdx"
        params = {
            "url": domain,
            "output": "json",
            "fl": "timestamp",
            "limit": 1,
            "from": "19900101",
            "to": "20250101",
            "filter": "statuscode:200",
        }
        r = requests.get(url, params=params,
                         headers={"User-Agent": "meor.com/1.0"},
                         timeout=10)
        if r.status_code == 200 and r.text.strip():
            data = r.json()
            if len(data) > 1:  # first row is headers
                first_ts = data[1][0]  # timestamp like 20081205120000
                first_year = int(first_ts[:4])

                # Get count
                count_params = {**params, "limit": 9999, "fl": "timestamp"}
                count_params.pop("from", None)
                rc = requests.get(url, params=count_params,
                                  headers={"User-Agent": "meor.com/1.0"},
                                  timeout=10)
                count = max(0, len(rc.json()) - 1) if rc.status_code == 200 else 0
                return first_year, count
    except Exception as e:
        pass
    return None, 0


# ─── DNS VERIFICATION ───────────────────────────────
def verify_available(domain):
    """Confirm domain is actually available via DNS."""
    import socket
    try:
        socket.setdefaulttimeout(3)
        socket.getaddrinfo(domain, None)
        return False  # resolves = still registered
    except socket.gaierror:
        return True   # NXDOMAIN = available
    except:
        return False


# ─── CALCULATE MEOR SCORE ───────────────────────────
def calc_meor_score(opr_score, first_year, wayback_count, dns_verified):
    """
    Calculate a 0-100 composite score for meor.com display.

    Components:
    - OPR score (0-10) → 40% weight → 0-40 points
    - Domain age → 30% weight → 0-30 points
    - Wayback snapshots → 20% weight → 0-20 points
    - DNS verified → 10% weight → 0-10 points
    """
    score = 0

    # OPR component (0-10 scale → 0-40 points)
    opr_points = min(40, (opr_score / 10) * 40)
    score += opr_points

    # Age component
    if first_year:
        age = 2025 - first_year
        age_points = min(30, age * 2)  # 2 points per year, max 30
        score += age_points

    # Wayback component
    if wayback_count:
        wb_points = min(20, wayback_count / 50)  # 1 point per 50 snapshots, max 20
        score += wb_points

    # DNS verified
    if dns_verified:
        score += 10

    return min(100, round(score))


# ─── UPDATE SUPABASE ────────────────────────────────
def update_domain(sb, domain_id, updates):
    """Update a domain record with enriched data."""
    try:
        sb.table("dropped_domains") \
            .update(updates) \
            .eq("id", domain_id) \
            .execute()
        return True
    except Exception as e:
        log.error(f"  Update failed for id {domain_id}: {e}")
        return False


# ─── MAIN ───────────────────────────────────────────
def main():
    start = time.time()
    log.info(f"\n{'='*50}")
    log.info(f"meor.com Enrichment Pipeline — {date.today()}")
    log.info(f"{'='*50}")

    if not SUPABASE_AVAILABLE or not SUPABASE_URL or not SUPABASE_KEY:
        log.error("Supabase not configured"); return

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Get unenriched domains
    domains = get_unenriched(sb)
    if not domains:
        log.info("All domains already enriched!")
        return

    domain_names = [d["domain"] for d in domains]

    # Batch fetch OPR scores
    log.info(f"\nFetching Open PageRank scores for {len(domain_names)} domains...")
    opr_data = get_pagerank_batch(domain_names)
    log.info(f"Got OPR data for {len(opr_data)} domains")

    # Enrich each domain
    enriched = 0
    for d in domains:
        domain = d["domain"]
        domain_id = d["id"]

        log.info(f"  Enriching {domain}...")

        # Get OPR score
        opr = opr_data.get(domain, {})
        opr_score = opr.get("opr_score", 0)

        # Get Wayback data
        first_year, wb_count = get_wayback_data(domain)
        time.sleep(0.3)  # be nice to Wayback

        # DNS verification
        dns_ok = verify_available(domain)

        # Calculate meor score
        meor_score = calc_meor_score(opr_score, first_year, wb_count, dns_ok)

        # Build AI verdict based on score
        if meor_score >= 75:
            verdict_ar = f"دومين قوي — نقاط OPR {opr_score}/10. يستحق التسجيل الفوري."
            verdict_en = f"Strong domain — OPR score {opr_score}/10. Worth registering immediately."
        elif meor_score >= 50:
            verdict_ar = f"دومين متوسط — نقاط OPR {opr_score}/10. راجع التاريخ قبل التسجيل."
            verdict_en = f"Average domain — OPR score {opr_score}/10. Review history before registering."
        else:
            verdict_ar = f"دومين ضعيف — نقاط OPR {opr_score}/10. لا يُنصح بالتسجيل."
            verdict_en = f"Weak domain — OPR score {opr_score}/10. Not recommended."

        updates = {
            "meor_score":      meor_score,
            "wayback_snaps":   wb_count,
            "first_seen_year": first_year,
            "dns_verified":    dns_ok,
            "ai_verdict":      verdict_ar,
            "ai_verdict_en":   verdict_en,
            "checked_at":      datetime.utcnow().isoformat(),
        }

        # Add OPR data to moz_da field (repurposing for OPR score * 10)
        if opr_score > 0:
            updates["moz_da"] = opr_score * 10  # scale 0-10 → 0-100

        if update_domain(sb, domain_id, updates):
            enriched += 1
            log.info(f"  ✅ {domain} — Score: {meor_score} (OPR:{opr_score}, Age:{first_year}, WB:{wb_count})")
        else:
            log.warning(f"  ❌ Failed to update {domain}")

    log.info(f"\nEnriched {enriched}/{len(domains)} domains")
    log.info(f"✅ Done in {time.time()-start:.0f}s\n{'='*50}")


if __name__ == "__main__":
    main()
