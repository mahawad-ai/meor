/**
 * POST /api/submit
 * Saves assessment submission to Supabase and sends an email notification
 * via Resend.
 *
 * Required env vars:
 *   SUPABASE_URL       — your Supabase project URL
 *   SUPABASE_KEY       — anon/public key (INSERT on assessment_submissions allowed)
 *   RESEND_API_KEY     — get from resend.com
 *   NOTIFY_EMAIL       — where to send lead notifications (e.g. mahawad@gmail.com)
 *   FROM_EMAIL         — verified sender in Resend (e.g. leads@meor.com)
 */

const INDUSTRY_LABELS = {
  clinic: 'Clinic / Medical', ecom: 'E-commerce', restaurant: 'Restaurant / Café',
  realestate: 'Real Estate', factory: 'Factory / Manufacturing'
};
const SCALE_LABELS   = { small: 'Small (5–30)', mid: 'Mid (30–150)', large: 'Large (150+)' };
const BUDGET_LABELS  = { explore: 'Just exploring', light: 'Small budget', real: 'Ready to invest', unsure: 'Need cost info first' };
const TIMELINE_LABELS= { asap: 'ASAP', '30': 'Within a month', '90': 'Within 3 months', no: 'No rush' };

function genRef() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let s = 'MEOR-';
  for (let i = 0; i < 5; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s;
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ ok: false, error: 'Method not allowed' });
  }

  const SUPABASE_URL  = process.env.SUPABASE_URL;
  const SUPABASE_KEY  = process.env.SUPABASE_KEY;
  const RESEND_KEY    = process.env.RESEND_API_KEY;
  const NOTIFY_EMAIL  = process.env.NOTIFY_EMAIL  || 'mahawad@gmail.com';
  const FROM_EMAIL    = process.env.FROM_EMAIL     || 'leads@meor.com';

  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return res.status(500).json({ ok: false, error: 'Server not configured' });
  }

  let body;
  try {
    body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
  } catch {
    return res.status(400).json({ ok: false, error: 'Invalid JSON' });
  }

  const { industry, scale, painText, pains, volume, budget, timeline, name, business, whatsapp, notes, language } = body;

  /* basic validation */
  if (!industry || !budget || !timeline || !name || !business || !whatsapp || whatsapp.trim().length < 6) {
    return res.status(400).json({ ok: false, error: 'Missing required fields' });
  }

  const ref = genRef();

  /* ── Save to Supabase ────────────────────────────────────────────── */
  try {
    const sbRes = await fetch(`${SUPABASE_URL}/rest/v1/assessment_submissions`, {
      method: 'POST',
      headers: {
        'Content-Type':  'application/json',
        'apikey':        SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Prefer':        'return=minimal',
      },
      body: JSON.stringify({
        ref_id:    ref,
        industry,
        scale:     scale || null,
        pain_text: painText || null,
        pains:     JSON.stringify(pains || []),
        volume:    parseInt(volume) || 50,
        budget,
        timeline,
        name:      name.trim(),
        business:  business.trim(),
        whatsapp:  whatsapp.trim(),
        notes:     notes?.trim() || null,
        language:  language || 'en',
      }),
    });
    if (!sbRes.ok) {
      const err = await sbRes.text();
      console.error('Supabase error:', err);
      return res.status(500).json({ ok: false, error: 'Database write failed' });
    }
  } catch (err) {
    console.error('Supabase fetch error:', err);
    return res.status(500).json({ ok: false, error: 'Database unreachable' });
  }

  /* ── Send email notification ─────────────────────────────────────── */
  if (RESEND_KEY) {
    const pct  = Math.round((parseInt(volume) / 200) * 100);
    const painItems = Array.isArray(pains) && pains.length > 0
      ? `\n  Selected: ${pains.map(i => `[${i}]`).join(', ')}`
      : '\n  None selected';
    const emailBody = `
New assessment submission from meor.com

Ref: ${ref}
Language: ${(language || 'en').toUpperCase()}

CONTACT
Name:      ${name.trim()}
Business:  ${business.trim()}
WhatsApp:  ${whatsapp.trim()}

PROFILE
Industry:  ${INDUSTRY_LABELS[industry] || industry}${scale ? `\nScale:     ${SCALE_LABELS[scale] || scale}` : ''}
Volume:    ${volume >= 200 ? '200+' : volume} messages/day
Budget:    ${BUDGET_LABELS[budget] || budget}
Timeline:  ${TIMELINE_LABELS[timeline] || timeline}

PAIN POINTS
Free text: ${painText?.trim() || '(none)'}
Checkboxes:${painItems}

Notes: ${notes?.trim() || '(none)'}
`.trim();

    try {
      await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: {
          'Content-Type':  'application/json',
          'Authorization': `Bearer ${RESEND_KEY}`,
        },
        body: JSON.stringify({
          from:    FROM_EMAIL,
          to:      [NOTIFY_EMAIL],
          subject: `New meor lead — ${business.trim()} (${INDUSTRY_LABELS[industry] || industry}) [${ref}]`,
          text:    emailBody,
        }),
      });
    } catch (err) {
      /* email failure is non-fatal — submission is already saved */
      console.error('Resend error:', err);
    }
  }

  return res.status(200).json({ ok: true, ref });
}
