-- ─────────────────────────────────────────────────────
-- meor.com — Database Schema
-- ─────────────────────────────────────────────────────
-- HOW TO USE:
--   1. Go to supabase.com and create a free account
--   2. Create a new project
--   3. Go to SQL Editor (left sidebar)
--   4. Paste this entire file and click "Run"
--   That's it — your database is ready.
-- ─────────────────────────────────────────────────────


-- ── TABLE 1: dropped_domains ─────────────────────────
-- Stores every domain drop detected by the pipeline.
-- This is the core table powering the meor.com search tool.

CREATE TABLE IF NOT EXISTS dropped_domains (
    id              BIGSERIAL PRIMARY KEY,

    -- The domain name and its parts
    domain          TEXT NOT NULL,          -- e.g. "travelinsider.com"
    tld             TEXT NOT NULL,          -- e.g. ".com"
    name            TEXT NOT NULL,          -- e.g. "travelinsider"

    -- When and how it was detected
    drop_date       DATE NOT NULL,          -- e.g. "2025-03-26"
    source          TEXT DEFAULT 'zone_file', -- 'zone_file' or 'dns_poll'
    status          TEXT DEFAULT 'dropped', -- 'dropped', 'registered', 'pending'
    dns_verified    BOOLEAN DEFAULT FALSE,  -- true = confirmed available via DNS
    checked_at      TIMESTAMPTZ,            -- when we last checked this domain

    -- SEO metrics (filled in by enrichment script, not pipeline)
    moz_da          INTEGER,                -- Domain Authority (0-100)
    moz_pa          INTEGER,                -- Page Authority (0-100)
    majestic_tf     INTEGER,                -- Trust Flow (0-100)
    majestic_cf     INTEGER,                -- Citation Flow (0-100)
    backlinks       INTEGER,                -- total backlink count
    ref_domains     INTEGER,                -- number of unique referring domains
    spam_score      INTEGER,                -- spam score % (lower = better)
    wayback_snaps   INTEGER,                -- number of Wayback Machine snapshots
    first_seen_year INTEGER,                -- year domain was first registered

    -- AI scoring (filled in by scoring script)
    meor_score      INTEGER,                -- our 0-100 AI composite score
    ai_verdict      TEXT,                   -- AI analysis text in Arabic
    ai_verdict_en   TEXT,                   -- AI analysis text in English
    is_arabic_idn   BOOLEAN DEFAULT FALSE,  -- true = Arabic IDN domain

    -- Timestamps
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),

    -- Prevent duplicate entries for same domain on same day
    UNIQUE(domain, drop_date)
);

-- Speed up searches (these make queries much faster)
CREATE INDEX IF NOT EXISTS idx_dropped_tld       ON dropped_domains(tld);
CREATE INDEX IF NOT EXISTS idx_dropped_date      ON dropped_domains(drop_date DESC);
CREATE INDEX IF NOT EXISTS idx_dropped_score     ON dropped_domains(meor_score DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_dropped_da        ON dropped_domains(moz_da DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_dropped_tf        ON dropped_domains(majestic_tf DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_dropped_status    ON dropped_domains(status);
CREATE INDEX IF NOT EXISTS idx_dropped_domain    ON dropped_domains(domain);
-- Full text search on domain names (enables keyword search)
CREATE INDEX IF NOT EXISTS idx_dropped_name_text ON dropped_domains USING gin(to_tsvector('english', name));


-- ── TABLE 2: domain_alerts ───────────────────────────
-- Stores user-configured alerts (Premium feature).
-- When a drop matches an alert's criteria, we send a WhatsApp message.

CREATE TABLE IF NOT EXISTS domain_alerts (
    id              BIGSERIAL PRIMARY KEY,
    user_id         TEXT NOT NULL,          -- Supabase auth user ID
    name            TEXT,                   -- alert name, e.g. "Travel domains"

    -- Filter criteria (NULL = no filter on that field)
    keyword         TEXT,                   -- domain must contain this keyword
    tld             TEXT,                   -- e.g. ".ae" or ".com"
    min_da          INTEGER,                -- minimum Domain Authority
    min_tf          INTEGER,                -- minimum Trust Flow
    min_backlinks   INTEGER,                -- minimum backlink count
    max_spam        INTEGER DEFAULT 30,     -- maximum spam score %
    min_age_years   INTEGER,                -- minimum domain age in years
    arabic_idn_only BOOLEAN DEFAULT FALSE,  -- only Arabic IDN domains

    -- Delivery settings
    whatsapp_number TEXT,                   -- phone number with country code
    email           TEXT,                   -- email address for alerts
    is_active       BOOLEAN DEFAULT TRUE,

    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_user ON domain_alerts(user_id);
CREATE INDEX IF NOT EXISTS idx_alerts_active ON domain_alerts(is_active);


-- ── TABLE 3: alert_notifications ────────────────────
-- Log of every notification sent (prevents duplicate alerts).

CREATE TABLE IF NOT EXISTS alert_notifications (
    id          BIGSERIAL PRIMARY KEY,
    alert_id    BIGINT REFERENCES domain_alerts(id),
    domain_id   BIGINT REFERENCES dropped_domains(id),
    domain      TEXT NOT NULL,
    channel     TEXT NOT NULL,              -- 'whatsapp' or 'email'
    sent_at     TIMESTAMPTZ DEFAULT NOW(),
    status      TEXT DEFAULT 'sent'         -- 'sent', 'failed'
);


-- ── TABLE 4: pipeline_runs ───────────────────────────
-- Tracks each time the pipeline runs (for monitoring and debugging).

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              BIGSERIAL PRIMARY KEY,
    run_date        DATE NOT NULL,
    started_at      TIMESTAMPTZ DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    domains_found   INTEGER DEFAULT 0,
    tlds_processed  INTEGER DEFAULT 0,
    status          TEXT DEFAULT 'running',  -- 'running', 'completed', 'failed'
    error_message   TEXT,
    duration_mins   NUMERIC(6,2)
);


-- ── FUNCTION: auto-update updated_at timestamp ───────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_dropped_domains_updated_at
    BEFORE UPDATE ON dropped_domains
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();


-- ── VIEW: today's best drops ─────────────────────────
-- A convenient pre-filtered view showing today's best drops
-- sorted by meor_score. This is what powers the default search results.

CREATE OR REPLACE VIEW todays_best_drops AS
SELECT
    domain,
    tld,
    name,
    drop_date,
    meor_score,
    moz_da,
    majestic_tf,
    backlinks,
    ref_domains,
    spam_score,
    wayback_snaps,
    first_seen_year,
    is_arabic_idn,
    ai_verdict,
    ai_verdict_en,
    dns_verified
FROM dropped_domains
WHERE
    drop_date = CURRENT_DATE
    AND status = 'dropped'
    AND (spam_score IS NULL OR spam_score < 50)
ORDER BY
    meor_score DESC NULLS LAST,
    majestic_tf DESC NULLS LAST,
    backlinks DESC NULLS LAST;


-- ── SAMPLE DATA: insert a few test domains ───────────
-- This lets you see something in your dashboard immediately.
-- Delete these once real data starts coming in.

INSERT INTO dropped_domains
    (domain, tld, name, drop_date, source, status, dns_verified,
     moz_da, majestic_tf, majestic_cf, backlinks, ref_domains,
     spam_score, wayback_snaps, first_seen_year, meor_score,
     ai_verdict, ai_verdict_en, is_arabic_idn)
VALUES
    ('travelinsider.com', '.com', 'travelinsider', CURRENT_DATE,
     'zone_file', 'dropped', true,
     34, 42, 38, 4820, 312, 2, 418, 2008, 87,
     'دومين سفر نظيف عمره 15 سنة. 94% من الباك لينكات من مواقع سفر ذات صلة. لا سبام، لا PBN.',
     'Clean 15-year travel domain. 94% relevant backlinks. No spam, no PBN.',
     false),

    ('luxurytravel.org', '.org', 'luxurytravel', CURRENT_DATE,
     'zone_file', 'dropped', true,
     61, 58, 52, 7300, 540, 1, 640, 2006, 93,
     'دومين سفر فاخر استثنائي. روابط من Forbes وCNTraveler وNYT. صفر إشارات سبام.',
     'Exceptional luxury travel domain. Links from Forbes, CNTraveler, NYT. Zero spam signals.',
     false),

    ('عقارات.السعودية', '.السعودية', 'عقارات', CURRENT_DATE,
     'dns_poll', 'dropped', true,
     29, 33, 28, 1840, 145, 4, 185, 2013, 78,
     'دومين IDN عربي نادر في نيش العقارات السعودية. تاريخ نظيف وباك لينكات موثوقة.',
     'Rare Arabic IDN domain in Saudi real estate niche. Clean history.',
     true),

    ('مطاعم-دبي.ae', '.ae', 'مطاعم-دبي', CURRENT_DATE,
     'dns_poll', 'dropped', true,
     21, 25, 20, 620, 88, 8, 72, 2016, 69,
     'دومين عربي في نيش مطاعم دبي. روابط ذات صلة وتاريخ نظيف.',
     'Arabic domain in Dubai restaurants niche. Relevant links, clean history.',
     true)

ON CONFLICT (domain, drop_date) DO NOTHING;

-- Done! Your database is ready.
SELECT 'meor.com database schema created successfully ✅' AS status;
