"""
meor.com — CZDS Auto-Discovery
================================
Run this ONCE after getting CZDS approval.

It will:
1. Log into ICANN CZDS with your credentials
2. Fetch the complete list of TLDs you are approved for
3. Print them all so you can see what you have
4. Save the list to a file so the pipeline uses it automatically

HOW TO RUN:
  python czds_discover.py
"""

import os
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

CZDS_USER = os.getenv("CZDS_USERNAME")
CZDS_PASS = os.getenv("CZDS_PASSWORD")

BASE = Path(__file__).parent
DATA = BASE / "data"
DATA.mkdir(exist_ok=True)


def get_token():
    """Log into ICANN and get access token."""
    print("Logging into ICANN CZDS...")
    resp = requests.post(
        "https://account-api.icann.org/api/authenticate",
        json={"username": CZDS_USER, "password": CZDS_PASS},
        timeout=30
    )
    if resp.status_code != 200:
        print(f"Login failed: {resp.status_code} — {resp.text}")
        print("\nCheck your .env file:")
        print("  CZDS_USERNAME=your-email@example.com")
        print("  CZDS_PASSWORD=your-password")
        return None
    token = resp.json().get("accessToken")
    print("Login successful.")
    return token


def get_approved_tlds(token):
    """
    Fetch the complete list of zone file download URLs
    you are approved for. Each URL ends in /tld.zone
    """
    print("\nFetching your approved TLD list...")
    resp = requests.get(
        "https://czds-api.icann.org/czds/downloads/links",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30
    )
    if resp.status_code != 200:
        print(f"Failed to fetch TLD list: {resp.status_code}")
        return []

    links = resp.json()  # list of download URLs
    return links


def extract_tld_name(url):
    """
    Pull the TLD name from a download URL.
    e.g. https://czds-api.icann.org/czds/downloads/com.zone → com
    """
    filename = url.split("/")[-1]       # com.zone
    tld = filename.replace(".zone", "") # com
    return tld


def main():
    if not CZDS_USER or not CZDS_PASS:
        print("ERROR: Missing credentials in .env file")
        print("Add these lines to your .env file:")
        print("  CZDS_USERNAME=your-email@example.com")
        print("  CZDS_PASSWORD=your-password")
        return

    # Login
    token = get_token()
    if not token:
        return

    # Get approved list
    links = get_approved_tlds(token)
    if not links:
        print("No approved TLDs found. Check your CZDS account.")
        return

    # Extract TLD names
    tlds = [extract_tld_name(url) for url in links]
    tlds.sort()

    # Print summary
    print(f"\n{'='*50}")
    print(f"You are approved for {len(tlds)} TLDs")
    print(f"{'='*50}")

    # Group by category for readability
    big = [t for t in tlds if t in ['com','net','org','info','biz','mobi']]
    mena = [t for t in tlds if t in ['ae','sa','eg','ma','jo','qa','kw','bh','om','ly','tn','dz','sd']]
    arabic_idn = [t for t in tlds if any(c > '\u0600' for c in t)]
    new_gtld = [t for t in tlds if t not in big+mena+arabic_idn]

    if big:
        print(f"\n Major gTLDs ({len(big)}):")
        print("  " + "  ".join(f".{t}" for t in big))

    if mena:
        print(f"\n MENA ccTLDs ({len(mena)}):")
        print("  " + "  ".join(f".{t}" for t in mena))

    if arabic_idn:
        print(f"\n Arabic IDN ({len(arabic_idn)}):")
        print("  " + "  ".join(f".{t}" for t in arabic_idn))

    if new_gtld:
        print(f"\n New gTLDs ({len(new_gtld)}):")
        # print in rows of 10
        for i in range(0, len(new_gtld), 10):
            print("  " + "  ".join(f".{t}" for t in new_gtld[i:i+10]))

    # Save to file — pipeline reads this automatically
    approved_path = DATA / "approved_tlds.json"
    with open(approved_path, "w") as f:
        json.dump({
            "tlds": tlds,
            "links": links,
            "total": len(tlds)
        }, f, indent=2)

    print(f"\n Saved to: {approved_path}")
    print(f"\n Your pipeline will now automatically process all {len(tlds)} TLDs.")
    print("\nNext step: run  python pipeline.py")


if __name__ == "__main__":
    main()
