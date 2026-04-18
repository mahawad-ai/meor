"""
meor.com — Unified Zone File Drop Detection Pipeline
Runs once daily at 6am UTC via GitHub Actions.

How it works:
  1. Fetch list of approved TLDs from CZDS API
  2. For each TLD, download today's zone file
  3. Rotate: today → yesterday (cached from previous run)
  4. Parse both files; domains in yesterday but not today = dropped
  5. Insert all drops to Supabase — NO DNS verification at this stage.

Why no DNS verification here:
  Zone file absence IS proof of a drop. When a domain leaves the zone
  file its DNS record stays cached for 24-48h, so dns_available() would
  wrongly discard real drops. The enrichment pipeline checks DNS 24h later.
"""

import os, gzip, time, json, logging, requests, shutil, subprocess, tempfile
from datetime import datetime, date
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

try:
    from supabase import create_client; SUPA = True
except ImportError:
    SUPA = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("meor.zones")

BASE_DIR  = Path(__file__).parent
CACHE_DIR = BASE_DIR / "zone_cache"
DATA_DIR  = BASE_DIR / "data"
CACHE_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

CZDS_USER    = os.getenv("CZDS_USERNAME", "")
CZDS_PASS    = os.getenv("CZDS_PASSWORD", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# Large TLDs processed sequentially (files can be 1GB+)
LARGE_TLDS      = {"com", "net", "org", "info", "biz", "mobi"}
MAX_FILE_MB     = 80       # skip small TLDs larger than this
MAX_DROPS_LARGE = 5000     # cap per large TLD
MAX_DROPS_SMALL = 500      # cap per small TLD
WORKERS         = 5        # concurrent downloads for small TLDs


# ── Auth ─────────────────────────────────────────────────────────────────────

def auth():
    log.info("Authenticating with ICANN CZDS...")
    r = requests.post(
        "https://account-api.icann.org/api/authenticate",
        json={"username": CZDS_USER, "password": CZDS_PASS},
        timeout=30,
    )
    r.raise_for_status()
    token = r.json().get("accessToken")
    log.info("Authenticated.")
    return token


def get_approved_links(token):
    """Return {tld_name: download_url} for every approved TLD."""
    r = requests.get(
        "https://czds-api.icann.org/czds/downloads/links",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    r.raise_for_status()
    links = {}
    for url in r.json():
        tld = url.split("/")[-1].replace(".zone", "")
        links[tld] = url
    log.info(f"Approved TLDs available: {len(links)}")
    return links


# ── Download ──────────────────────────────────────────────────────────────────

def download_zone(tld, url, token, max_mb=None):
    """
    Download zone file to zone_cache/{tld}_today.txt.gz.
    Before downloading, rotate any existing today → yesterday.

    Returns (today_path, yesterday_path) — yesterday_path is None on first run.
    Returns (None, None) on download failure or size limit exceeded.
    """
    today_path     = CACHE_DIR / f"{tld}_today.txt.gz"
    yesterday_path = CACHE_DIR / f"{tld}_yesterday.txt.gz"

    # Rotate: today becomes yesterday before we download the new today
    if today_path.exists():
        shutil.copy2(today_path, yesterday_path)
        log.info(f"  .{tld} rotated to yesterday")

    headers = {"Authorization": f"Bearer {token}"}
    try:
        with requests.get(url, headers=headers, stream=True, timeout=300) as r:
            if r.status_code in (403, 404):
                log.warning(f"  .{tld} not approved (HTTP {r.status_code})")
                return None, None
            r.raise_for_status()

            downloaded = 0
            with open(today_path, "wb") as f:
                for chunk in r.iter_content(1 << 20):  # 1MB chunks
                    f.write(chunk)
                    downloaded += len(chunk)
                    if max_mb and downloaded > max_mb * 1024 * 1024:
                        log.warning(f"  .{tld} exceeds {max_mb}MB limit, skipping")
                        today_path.unlink(missing_ok=True)
                        return None, None

    except Exception as e:
        log.error(f"  .{tld} download failed: {e}")
        today_path.unlink(missing_ok=True)
        return None, None

    size_mb = today_path.stat().st_size / 1048576
    log.info(f"  .{tld} downloaded ({size_mb:.1f}MB)")
    return today_path, (yesterday_path if yesterday_path.exists() else None)


# ── Parse & Diff ──────────────────────────────────────────────────────────────

def extract_names_to_sorted_file(zone_path, tld, out_path):
    """
    Stream-parse a gzip zone file and write sorted base names to out_path.
    Uses external sort (subprocess) so memory usage stays flat regardless
    of file size — critical for .com (4GB+ compressed, 175M+ domains).
    """
    tld_dot = f".{tld}"
    opener  = gzip.open if str(zone_path).endswith(".gz") else open

    # Write all NS base-names to a temp unsorted file, then sort externally
    unsorted = out_path.with_suffix(".unsorted")
    try:
        with opener(zone_path, "rt", encoding="utf-8", errors="ignore") as zf, \
             open(unsorted, "w") as uf:
            for line in zf:
                parts = line.split()
                if len(parts) >= 4 and parts[2].upper() == "IN" and parts[3].upper() == "NS":
                    name = parts[0].rstrip(".").lower()
                    if name.endswith(tld_dot):
                        name = name[: -len(tld_dot)]
                    if name:
                        uf.write(name + "\n")

        # External sort — uses disk, not RAM; handles hundreds of millions of lines
        subprocess.run(
            ["sort", "-u", str(unsorted), "-o", str(out_path)],
            check=True,
        )
    finally:
        unsorted.unlink(missing_ok=True)


def detect_drops_streaming(tld, today_path, yesterday_path, max_drops):
    """
    Find dropped domains using sorted file comparison (comm -23).
    Memory usage is O(1) — safe for .com and any size zone file.
    dropped = in yesterday but not in today.
    """
    log.info(f"  .{tld} extracting & sorting names...")

    with tempfile.TemporaryDirectory() as tmp:
        tmp     = Path(tmp)
        today_s = tmp / "today_sorted.txt"
        yest_s  = tmp / "yesterday_sorted.txt"

        extract_names_to_sorted_file(today_path,     tld, today_s)
        extract_names_to_sorted_file(yesterday_path, tld, yest_s)

        # comm -23: lines only in file1 (yesterday) = dropped
        result = subprocess.run(
            ["comm", "-23", str(yest_s), str(today_s)],
            capture_output=True, text=True, check=True,
        )

    dropped = [line.strip() for line in result.stdout.splitlines() if line.strip()]

    if not dropped:
        log.info(f"  .{tld} — no drops detected")
        return []

    log.info(f"  .{tld} — {len(dropped):,} drops 🎯 (capped at {max_drops})")

    ts      = date.today().isoformat()
    now     = datetime.utcnow().isoformat()
    tld_dot = f".{tld}"

    return [
        {
            "domain":        f"{base}{tld_dot}",
            "tld":           tld_dot,
            "name":          base,
            "drop_date":     ts,
            "source":        "zone_file",
            "status":        "dropped",
            "dns_verified":  False,
            "checked_at":    now,
            "is_arabic_idn": any(ord(c) > 0x0600 for c in base),
        }
        for base in dropped[:max_drops]
    ]


def parse_zone(path, tld):
    """In-memory parse for small TLDs only (< MAX_FILE_MB)."""
    names   = set()
    tld_dot = f".{tld}"
    opener  = gzip.open if str(path).endswith(".gz") else open
    try:
        with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 4 and parts[2].upper() == "IN" and parts[3].upper() == "NS":
                    name = parts[0].rstrip(".").lower()
                    if name.endswith(tld_dot):
                        name = name[: -len(tld_dot)]
                    if name:
                        names.add(name)
    except Exception as e:
        log.error(f"  Parse error ({path.name}): {e}")
    return names


def detect_drops(tld, today_path, yesterday_path, max_drops, large=False):
    """
    Route to streaming (large TLDs) or in-memory (small TLDs) comparison.
    """
    if large:
        return detect_drops_streaming(tld, today_path, yesterday_path, max_drops)

    log.info(f"  .{tld} parsing...")
    today_names     = parse_zone(today_path, tld)
    yesterday_names = parse_zone(yesterday_path, tld)
    dropped         = list(yesterday_names - today_names)

    if not dropped:
        log.info(f"  .{tld} — no drops detected")
        return []

    log.info(f"  .{tld} — {len(dropped):,} drops 🎯 (capped at {max_drops})")

    ts      = date.today().isoformat()
    now     = datetime.utcnow().isoformat()
    tld_dot = f".{tld}"

    return [
        {
            "domain":        f"{base}{tld_dot}",
            "tld":           tld_dot,
            "name":          base,
            "drop_date":     ts,
            "source":        "zone_file",
            "status":        "dropped",
            "dns_verified":  False,
            "checked_at":    now,
            "is_arabic_idn": any(ord(c) > 0x0600 for c in base),
        }
        for base in dropped[:max_drops]
    ]


# ── Supabase ──────────────────────────────────────────────────────────────────

def save_drops(sb, drops):
    if not drops:
        return 0
    saved = 0
    for i in range(0, len(drops), 100):
        batch = drops[i:i + 100]
        try:
            sb.table("dropped_domains").upsert(
                batch, on_conflict="domain,drop_date"
            ).execute()
            saved += len(batch)
        except Exception as e:
            log.error(f"  Supabase batch error: {e}")
    return saved


# ── Per-TLD orchestration ─────────────────────────────────────────────────────

def process_tld(tld, url, token, max_mb, max_drops, large=False):
    """Full pipeline for one TLD. Returns list of drop records."""
    today_path, yesterday_path = download_zone(tld, url, token, max_mb)
    if not today_path:
        return []
    if not yesterday_path:
        log.info(f"  .{tld} — first run, cached for tomorrow")
        return []
    return detect_drops(tld, today_path, yesterday_path, max_drops, large=large)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    start = time.time()
    log.info(f"\n{'='*60}")
    log.info(f"meor.com Zone Pipeline — {date.today()}")
    log.info(f"{'='*60}")

    if not CZDS_USER or not CZDS_PASS:
        log.error("CZDS_USERNAME / CZDS_PASSWORD not set"); return
    if not SUPA or not SUPABASE_URL or not SUPABASE_KEY:
        log.error("Supabase not configured"); return

    token = auth()
    links = get_approved_links(token)
    sb    = create_client(SUPABASE_URL, SUPABASE_KEY)

    total_saved = 0

    # ── Large TLDs: sequential (huge files) ──────────────────────────────────
    large = {t: u for t, u in links.items() if t in LARGE_TLDS}
    log.info(f"\nLarge TLDs ({len(large)}): processing sequentially (streaming sort)...")
    for tld, url in large.items():
        log.info(f"\nProcessing .{tld}...")
        drops = process_tld(tld, url, token, max_mb=None, max_drops=MAX_DROPS_LARGE, large=True)
        if drops:
            saved = save_drops(sb, drops)
            total_saved += saved
            log.info(f"  .{tld} saved {saved} drops")

    # ── Small TLDs: concurrent downloads ─────────────────────────────────────
    small = {t: u for t, u in links.items() if t not in LARGE_TLDS}
    log.info(f"\nSmall TLDs ({len(small)}): processing with {WORKERS} workers...")

    def run_small(item):
        tld, url = item
        return tld, process_tld(tld, url, token, max_mb=MAX_FILE_MB, max_drops=MAX_DROPS_SMALL)

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(run_small, item): item[0] for item in small.items()}
        for future in as_completed(futures):
            try:
                tld, drops = future.result()
                if drops:
                    saved = save_drops(sb, drops)
                    total_saved += saved
                    log.info(f"  .{tld} saved {saved} drops")
            except Exception as e:
                log.error(f"  Worker error: {e}")

    # ── Log run ───────────────────────────────────────────────────────────────
    elapsed = time.time() - start
    log.info(f"\n{'='*60}")
    log.info(f"Total drops saved: {total_saved:,} in {elapsed/60:.1f} min")
    log.info(f"{'='*60}\n")

    try:
        sb.table("pipeline_runs").insert({
            "run_date":       date.today().isoformat(),
            "finished_at":    datetime.utcnow().isoformat(),
            "domains_found":  total_saved,
            "tlds_processed": len(large) + len(small),
            "status":         "success",
            "duration_mins":  round(elapsed / 60, 2),
        }).execute()
    except Exception:
        pass


if __name__ == "__main__":
    main()
