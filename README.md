# meor.com Data Pipeline
## Setup Guide — No Coding Experience Needed

---

## What This Does

Every day at 6am, this script:
1. Downloads the master list of all registered .com, .net, .org etc domains from ICANN
2. Compares it with yesterday's list
3. Any domain that disappeared = it was dropped and is now available
4. Saves all dropped domains to your database
5. Also monitors Arabic domains (.ae, .sa, .eg, .ma) by checking them directly

---

## Step 1 — Install Python

If you don't have Python installed:
- Go to **python.org/downloads**
- Download Python 3.11 or newer
- Install it (check "Add to PATH" during installation on Windows)

Verify it worked by opening Terminal (Mac) or Command Prompt (Windows) and typing:
```
python --version
```
You should see something like: `Python 3.11.5`

---

## Step 2 — Set Up Your Free Database (Supabase)

1. Go to **supabase.com** and create a free account
2. Click "New Project" — give it any name (e.g. "meor")
3. Choose a region close to you (Europe West is fine for Egypt)
4. Wait ~2 minutes for it to set up
5. Go to **SQL Editor** (left sidebar icon that looks like a database)
6. Click "New Query"
7. Open the file `schema.sql` from this folder, copy everything, paste it in
8. Click **Run**
9. You should see: `meor.com database schema created successfully ✅`

Now get your credentials:
- Go to **Settings → API** (left sidebar)
- Copy the **Project URL** (looks like: https://abcdef.supabase.co)
- Copy the **anon/public key** (a long string starting with "eyJ...")

---

## Step 3 — Apply for ICANN Zone File Access

This is the free master list of all domains. Takes 2–4 weeks to get approved.

1. Go to **czds.icann.org**
2. Create a free account
3. Go to "Zone File Access Requests"
4. Apply for these TLDs: .com, .net, .org, .info, .biz
5. In the reason field write:
   > "I am building meor.com, an Arabic-language expired domain search engine for the MENA market. Zone file access is needed to detect daily domain drops for our users."
6. Submit and wait for approval email

**While you wait:** The pipeline will still work for MENA domains (.ae, .sa, .eg, .ma) via DNS polling. You don't need CZDS approval for that part.

---

## Step 4 — Configure Your Settings

1. In this folder, find the file called `.env.example`
2. Copy it and rename the copy to `.env` (just remove the `.example` part)
3. Open `.env` in any text editor (Notepad on Windows, TextEdit on Mac)
4. Fill in your values:

```
SUPABASE_URL=https://your-project-id.supabase.co    ← paste from Step 2
SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6...        ← paste from Step 2
CZDS_USERNAME=your-email@example.com                  ← your ICANN login
CZDS_PASSWORD=your-icann-password                     ← your ICANN password
```

Save the file.

---

## Step 5 — Install Python Packages

Open Terminal / Command Prompt, navigate to this folder, and run:

```bash
pip install -r requirements.txt
```

This installs all the code libraries the pipeline needs. Takes about 1 minute.

---

## Step 6 — Run the Pipeline

```bash
python pipeline.py
```

**What you'll see on screen:**
```
meor.com Pipeline starting...
✅ Connected to Supabase database
Running initial pipeline...
============================================================
meor.com Pipeline — 2025-03-26
============================================================

[PART A] Processing gTLD zone files via ICANN CZDS
  .com — downloading zone file...
  .com — saved (312.4 MB)
  Parsed 162,847,293 domains from com_2025-03-26.txt.gz
  .com — no yesterday file yet (first run). Skipping diff.

[PART B] Processing MENA ccTLDs via DNS polling
  First run detected — seeding MENA domain list from Wayback CDX...
  Found 1,847 ae domains from Wayback CDX.
  Seeded 4,231 MENA domains. Will start detecting drops tomorrow.

✅ Pipeline complete in 8.3 minutes
Scheduled to run daily at 06:00 UTC
Press Ctrl+C to stop.
```

On the **second day**, you'll start seeing real dropped domains appearing in your database.

---

## Step 7 — Keep It Running (Deploy to a Server)

You need this running 24/7. The free option is **Render.com**:

1. Go to **render.com** and create a free account
2. Connect your GitHub account
3. Upload this folder to a GitHub repository
4. In Render: New → Background Worker → connect your repo
5. Set environment variables (the same values from your .env file)
6. Deploy

It will run automatically forever for free.

---

## Frequently Asked Questions

**Q: The .com zone file is huge — how much disk space do I need?**
A: About 2GB for .com + .net + .org combined (compressed). The script keeps only the last 3 days then deletes old files automatically.

**Q: How many drops per day will I find?**
A: Typically 50,000–150,000 domains drop from .com alone every day. Most are worthless (expired spam sites). Your website filters these down to the valuable ones using the AI score.

**Q: Will ICANN really give me zone file access for free?**
A: Yes. It's a public service they're required to offer. Approval is usually granted within 2–4 weeks. The reason you provide matters — use the text in Step 3.

**Q: What if I don't have CZDS access yet?**
A: The pipeline still runs and collects MENA ccTLD drops via DNS polling. You'll have real data flowing from day one. gTLD drops get added once CZDS is approved.

**Q: Something crashed — what do I do?**
A: Check the file `logs/pipeline.log` — it shows exactly what happened and where it failed.
