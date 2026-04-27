"""
meor.com — Unified Zone File Drop Detection Pipeline
Runs once daily at 6am UTC via GitHub Actions.

How it works:
  1. Fetch list of approved TLDs from CZDS API
  2. For each TLD, download today's zone file to a temp location
  3. Extract + sort domain names from zone file
  4. Diff sorted names against cached yesterday's names → drops
  5. Update cache with today's names (for tomorrow's diff)
  6. Insert all drops to Supabase

Cache strategy (v2 — names-only):
  Previous approach stored raw zone gz files (4.3GB for .com alone), causing
  the GitHub Actions 10GB cache limit to be exceeded on alternate days → 0 drops.

  New approach: cache only gzip-compressed sorted names files per TLD.
    zone_cache/{tld}.names.gz  →  sorted domain base names from yesterday
  This keeps total cache size ~1-2GB regardless of how many TLDs we track.
  Raw zone files are downloaded to temp, processed, and deleted immediately.

Why no DNS verification here:
  Zone file absence IS proof of a drop. DNS stays cached 24-48h after zone
  removal, so dns_available() would wrongly discard real drops. The enrichment
  pipeline checks DNS 24h later.
"""

import os, gzip as gz_mod, time, logging, requests, shutil, subprocess, tempfile
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
CACHE_DIR.mkdir(exist_ok=True)

CZDS_USER    = os.getenv("CZDS_USERNAME", "")
CZDS_PASS    = os.getenv("CZDS_PASSWORD", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# Large TLDs use external sort (disk-based) to avoid OOM on 175M+ domains
LARGE_TLDS      = {"com", "net", "org", "info", "biz", "mobi"}
MAX_FILE_MB     = 80       # skip small TLDs larger than this (spam farms)
MAX_DROPS_LARGE = 5000     # cap per large TLD to avoid Supabase write overload
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
    Download zone file to a temp file in zone_cache.
    Returns Path to temp gz file, or None on failure.
    Caller is responsible for deleting the temp file after processing.
    """
    tmp_path = CACHE_DIR / f"{tld}.zone.tmp.gz"
    headers  = {"Authorization": f"Bearer {token}"}
    try:
        with requests.get(url, headers=headers, stream=True, timeout=300) as r:
            if r.status_code in (403, 404):
                log.warning(f"  .{tld} not approved (HTTP {r.status_code})")
                return None
            r.raise_for_status()

            downloaded = 0
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(1 << 20):   # 1MB chunks
                    f.write(chunk)
                    downloaded += len(chunk)
                    if max_mb and downloaded > max_mb * 1024 * 1024:
                        log.warning(f"  .{tld} exceeds {max_mb}MB limit, skipping")
                        tmp_path.unlink(missing_ok=True)
                        return None

        size_mb = tmp_path.stat().st_size / 1048576
        log.info(f"  .{tld} downloaded ({size_mb:.1f}MB)")
        return tmp_path

    except Exception as e:
        log.error(f"  .{tld} download failed: {e}")
        tmp_path.unlink(missing_ok=True)
        return None


# ── Parse & Sort ──────────────────────────────────────────────────────────────

def parse_zone_to_sorted(zone_path, tld, out_path, external_sort=True):
    """
    Parse zone file and write sorted unique base names to out_path (plain text).

    external_sort=True  → uses `sort -u` subprocess (O(1) RAM, required for .com)
    external_sort=False → in-memory sort (fine for small TLDs)
    """
    tld_dot = f".{tld}"
    opener  = gz_mod.open if str(zone_path).endswith(".gz") else open

    if external_sort:
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
            subprocess.run(
                ["sort", "-u", str(unsorted), "-o", str(out_path)],
                check=True,
            )
        finally:
            unsorted.unlink(missing_ok=True)
    else:
        names = set()
        try:
            with opener(zone_path, "rt", encoding="utf-8", errors="ignore") as zf:
                for line in zf:
                    parts = line.split()
                    if len(parts) >= 4 and parts[2].upper() == "IN" and parts[3].upper() == "NS":
                        name = parts[0].rstrip(".").lower()
                        if name.endswith(tld_dot):
                            name = name[: -len(tld_dot)]
                        if name:
                            names.add(name)
        except Exception as e:
            log.error(f"  Parse error ({zone_path.name}): {e}")
        with open(out_path, "w") as f:
            for name in sorted(names):
                f.write(name + "\n")


# ── Cache helpers ─────────────────────────────────────────────────────────────

def names_cache_path(tld):
    """Path to cached sorted names file for this TLD."""
    return CACHE_DIR / f"{tld}.names.gz"


def load_cached_names(tld, dest_path):
    """Decompress cached names to dest_path. Returns True if cache existed."""
    cache = names_cache_path(tld)
    if not cache.exists():
        return False
    with gz_mod.open(cache, "rb") as f_in, open(dest_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    return True


def save_names_cache(tld, sorted_path):
    """Gzip-compress sorted_path into the cache."""
    cache = names_cache_path(tld)
    with open(sorted_path, "rb") as f_in, gz_mod.open(cache, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


# ── Drop detection ────────────────────────────────────────────────────────────

def build_drops(tld, dropped_names, max_drops):
    """Build Supabase-ready records from a list of dropped base names."""
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
        for base in dropped_names[:max_drops]
    ]


def process_tld(tld, url, token, max_mb=None, max_drops=500, large=False):
    """
    Full pipeline for one TLD:
      1. Download zone to temp file
      2. Parse + sort names
      3. Diff against cached names  →  dropped = in cache but not today
      4. Update cache with today's names
      5. Delete raw zone file
      6. Return drop records

    Cache stores only gzip-compressed sorted names (~600MB for .com vs 8GB+
    for raw zone files) — this is the key fix for the alternating-0 bug.
    """
    zone_tmp = None
    try:
        zone_tmp = download_zone(tld, url, token, max_mb)
        if not zone_tmp:
            return []

        if large:
            log.info(f"  .{tld} extracting & sorting (external sort)...")

        # Use a temp directory for sorting scratch space
        with tempfile.TemporaryDirectory(dir=CACHE_DIR) as _tmp:
            tmp       = Path(_tmp)
            today_srt = tmp / "today.txt"
            yest_srt  = tmp / "yesterday.txt"

            # Parse + sort today's zone
            parse_zone_to_sorted(zone_tmp, tld, today_srt, external_sort=large)

            # Raw zone no longer needed — delete immediately to free disk
            zone_tmp.unlink(missing_ok=True)
            zone_tmp = None

            # Diff against cached names from yesterday
            dropped_names = []
            has_cache = load_cached_names(tld, yest_srt)

            if has_cache:
                result = subprocess.run(
                    ["comm", "-23", str(yest_srt), str(today_srt)],
                    capture_output=True, text=True, check=True,
                )
                dropped_names = [
                    l.strip() for l in result.stdout.splitlines() if l.strip()
                ]
            else:
                log.info(f"  .{tld} — no cache yet, caching for tomorrow")

            # Always update cache with today's sorted names
            save_names_cache(tld, today_srt)

        if not dropped_names:
            if has_cache:
                log.info(f"  .{tld} — no drops detected")
            return []

        log.info(f"  .{tld} — {len(dropped_names):,} drops 🎯 (capped at {max_drops})")
        return build_drops(tld, dropped_names, max_drops)

    except Exception as e:
        log.error(f"  .{tld} failed: {e}")
        return []
    finally:
        if zone_tmp and zone_tmp.exists():
            zone_tmp.unlink(missing_ok=True)


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

    # ── Large TLDs: sequential (external sort needed for .com) ───────────────
    large = {t: u for t, u in links.items() if t in LARGE_TLDS}
    log.info(f"\nLarge TLDs ({len(large)}): processing sequentially...")
    for tld, url in large.items():
        log.info(f"\nProcessing .{tld}...")
        drops = process_tld(
            tld, url, token,
            max_mb=None, max_drops=MAX_DROPS_LARGE, large=True,
        )
        if drops:
            saved = save_drops(sb, drops)
            total_saved += saved
            log.info(f"  .{tld} saved {saved} drops")

    # ── Small TLDs: concurrent downloads ─────────────────────────────────────
    small = {t: u for t, u in links.items() if t not in LARGE_TLDS}
    log.info(f"\nSmall TLDs ({len(small)}): processing with {WORKERS} workers...")

    def run_small(item):
        tld, url = item
        return tld, process_tld(
            tld, url, token,
            max_mb=MAX_FILE_MB, max_drops=MAX_DROPS_SMALL, large=False,
        )

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

    # ── Summary ───────────────────────────────────────────────────────────────
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
