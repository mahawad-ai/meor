# CLAUDE.md — meor.com

## What this is

meor.com is an AI services agency for MENA businesses. It offers two paths:

1. **Domain search** — a GoDaddy reseller redirect (no inventory, no backend)
2. **Business assessment** — a 5/6-step bilingual form that qualifies leads and captures contact info

## Stack

| Layer | Technology |
|---|---|
| Hosting | Vercel (static HTML + serverless functions) |
| Pages | Vanilla HTML/CSS/JS — no framework, no build step |
| Database | Supabase (PostgreSQL) — `assessment_submissions` table only |
| Email | Resend — for lead notifications |
| Domain reseller | WildWestDomains via GoDaddy (`plid=600707` on secureserver.net) |

## Files

```
index.html          — homepage (two-path router)
assessment.html     — the 5/6-step assessment (full bilingual)
services.html       — services overview (4 solutions listed)
api/submit.js       — Vercel serverless: save to Supabase + email via Resend
vercel.json         — routing + security headers
robots.txt          — noindex on assessment, api
meor-favicon.svg    — brand icon (keep)
env / env.example   — Supabase credentials (never commit real values)
```

## Environment variables

Set these in Vercel dashboard (Settings > Environment Variables):

| Variable | Value |
|---|---|
| `SUPABASE_URL` | `https://rtwxcljyetfdcgudcrye.supabase.co` |
| `SUPABASE_KEY` | anon/public key from Supabase |
| `RESEND_API_KEY` | from resend.com (free tier: 3000 emails/month) |
| `NOTIFY_EMAIL` | email address to receive lead notifications |
| `FROM_EMAIL` | verified sender in Resend (e.g. `leads@meor.com`) |

## Configuration in HTML

Both `index.html` and `assessment.html` have a `CFG` / `CONFIG` object at the top of their `<script>` block:

```javascript
const CFG = {
  whatsapp: '+201XXXXXXXXXX',  // ← your WhatsApp number
  reseller: 'https://www.secureserver.net/products/domain-registration/find?plid=600707&domainToCheck='
};
```

**Update the WhatsApp number** before going live. The reseller URL and plid are correct and should not change.

## Database

Single table: `assessment_submissions`

```sql
id, ref_id, industry, scale, pain_text, pains (JSONB),
volume, budget, timeline, name, business, whatsapp,
notes, language, created_at
```

No RLS configured. Anon key has INSERT access. No other tables.

## Assessment flow

```
Step 1  → Industry selection (clinic / ecom / restaurant / realestate / factory)
Step 1.5→ Factory scale only (small / mid / large) — inserts into sequence only if factory
Step 2  → Pain identification (free text + multi-select checkboxes, industry-specific)
Step 3  → Volume slider (0–200, dynamic helper text)
Step 4  → Qualification (budget posture + timeline)
Step 5  → Personalized insight card + contact form
Done    → Confirmation with ref ID (MEOR-XXXXX)
```

Total: 5 steps for non-factory, 6 for factory.

## Bilingual

- Language toggled via `localStorage` key `meor_lang` (`'en'` or `'ar'`)
- Toggle sets `html[lang]` and `html[dir]` (RTL for Arabic)
- All copy lives in a `T = { en: {...}, ar: {...} }` object in each page
- State is preserved across language toggles (re-render restores values)

## Domain reseller

The reseller integration is a labeled redirect only. meor.com does no domain search, no API calls, no inventory lookup:

```javascript
window.open(CFG.reseller + encodeURIComponent(query), '_blank', 'noopener,noreferrer');
```

## What is NOT here

- No domain inventory (old `dropped_domains` table deleted)
- No GitHub Actions pipelines (all old workflows deleted)
- No Python scripts
- No user authentication or accounts
- No CRM, no email sequences, no analytics
- No pricing pages or checkout flows

## Before going live checklist

- [ ] Set all 5 env vars in Vercel dashboard
- [ ] Update `CFG.whatsapp` in `index.html` with real number
- [ ] Sign up for Resend, verify `meor.com` domain, set `FROM_EMAIL`
- [ ] Test assessment end-to-end on mobile (375px)
- [ ] Test Arabic RTL on iOS Safari
- [ ] Confirm Supabase anon key allows INSERT on `assessment_submissions`
