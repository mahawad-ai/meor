/**
 * inquiry.js — drop-in inquiry modal for meor.com
 *
 * Usage:
 *   <script src="/inquiry.js" defer></script>
 *   ...
 *   <button onclick="openInquiry('WhatsApp Inquiry Handler')">Inquire</button>
 *
 * Or attach via [data-inquire="Service Name"] on any element.
 *
 * Injects: <style>, modal HTML at end of <body>, global window.openInquiry(name).
 * Picks up document.documentElement.lang for bilingual EN/AR + persists with the
 * page's existing meor_lang localStorage key. Re-translates whenever opened or
 * when the language attribute changes.
 */

(function () {
  /* ─── CONFIG ─────────────────────────────────────────────────── */
  const CFG = {
    whatsapp: '201000000000', // ← replace with the real meor.com WhatsApp number
    apiUrl:   '/api/submit'
  };

  /* ─── TRANSLATIONS ───────────────────────────────────────────── */
  const T = {
    en: {
      eyebrow:      'Get this set up',
      title:        'Inquire about this service',
      name:         'Your name',
      biz:          'Business name',
      whatsapp:     'WhatsApp number',
      whatsappPh:   '+20 10x xxx xxxx',
      email:        'Email (optional)',
      emailPh:      'you@business.com',
      notes:        'Anything specific? (optional)',
      notesPh:      'A specific situation, deadline, or anything that helps us prepare.',
      submit:       'Send inquiry',
      submitting:   'Sending…',
      or:           'or',
      wa:           'Message us directly on WhatsApp',
      errFields:    'Please fill in name, business, and WhatsApp number.',
      errSubmit:    'Something went wrong. Please try WhatsApp instead.',
      doneTitle:    "Thanks — we'll be in touch.",
      doneBody:     "We've received your details. Expect a personal message on WhatsApp within 24 hours.",
      closeAria:    'Close',
      waMsg:        (svc) => `Hi! I'm interested in the "${svc}" service from meor.com.`,
    },
    ar: {
      eyebrow:      'احصل على هذا الحل',
      title:        'استفسر عن هذه الخدمة',
      name:         'اسمك',
      biz:          'اسم النشاط',
      whatsapp:     'رقم الواتساب',
      whatsappPh:   '+20 10x xxx xxxx',
      email:        'البريد الإلكتروني (اختياري)',
      emailPh:      'you@business.com',
      notes:        'هل هناك تفاصيل محددة؟ (اختياري)',
      notesPh:      'موقف معين، موعد نهائي، أو أي شيء يساعدنا في التحضير.',
      submit:       'إرسال الاستفسار',
      submitting:   'جارٍ الإرسال…',
      or:           'أو',
      wa:           'راسلنا مباشرة على واتساب',
      errFields:    'يرجى تعبئة الاسم واسم النشاط ورقم الواتساب.',
      errSubmit:    'حدث خطأ. حاول عبر واتساب بدلاً من ذلك.',
      doneTitle:    'شكراً — سنتواصل معك.',
      doneBody:     'استلمنا بياناتك. توقع رسالة شخصية على واتساب خلال 24 ساعة.',
      closeAria:    'إغلاق',
      waMsg:        (svc) => `مرحباً، أنا مهتم بخدمة "${svc}" من meor.com.`,
    }
  };

  /* ─── STYLE INJECTION ────────────────────────────────────────── */
  const CSS = `
.inq-modal {
  position: fixed;
  inset: 0;
  z-index: 100;
  display: none;
  align-items: center;
  justify-content: center;
  padding: 24px;
}
.inq-modal.open { display: flex; }
.inq-backdrop {
  position: absolute;
  inset: 0;
  background: rgba(10,10,10,0.55);
  -webkit-backdrop-filter: blur(4px);
  backdrop-filter: blur(4px);
  animation: inq-fade 0.2s ease;
}
.inq-card {
  position: relative;
  background: var(--surface, #fff);
  border-radius: 24px;
  max-width: 500px;
  width: 100%;
  max-height: 90vh;
  overflow-y: auto;
  padding: 40px 36px 32px;
  animation: inq-rise 0.32s cubic-bezier(0.16, 1, 0.3, 1);
  box-shadow: 0 40px 100px -20px rgba(0,0,0,0.5);
}
.inq-close {
  position: absolute;
  top: 16px;
  right: 16px;
  width: 36px;
  height: 36px;
  border-radius: 50%;
  border: none;
  background: transparent;
  cursor: pointer;
  color: var(--ink-3, #9A9A95);
  display: flex;
  align-items: center;
  justify-content: center;
  transition: background 0.15s, color 0.15s;
  font-family: inherit;
}
.inq-close:hover { background: var(--border, #ECEAE5); color: var(--ink, #0A0A0A); }
html[dir="rtl"] .inq-close { right: auto; left: 16px; }

.inq-eyebrow {
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--ink-2, #555);
  margin-bottom: 12px;
  display: inline-flex;
  align-items: center;
  gap: 10px;
}
.inq-eyebrow::before {
  content: "";
  width: 24px;
  height: 1px;
  background: var(--ink-2, #555);
}
.inq-title {
  font-size: 26px;
  font-weight: 800;
  letter-spacing: -0.03em;
  line-height: 1.18;
  color: var(--ink, #0A0A0A);
  margin: 0 0 6px 0;
}
.inq-service {
  font-size: 15px;
  color: var(--ink-2, #555);
  margin-bottom: 22px;
  font-weight: 500;
}
.inq-service strong { color: var(--ink, #0A0A0A); font-weight: 700; }

.inq-form { display: flex; flex-direction: column; gap: 14px; }
.inq-field { display: flex; flex-direction: column; gap: 6px; }
.inq-label {
  font-size: 12px;
  font-weight: 600;
  letter-spacing: 0.04em;
  color: var(--ink-2, #555);
}
.inq-input, .inq-textarea {
  width: 100%;
  padding: 14px 16px;
  border: 1.5px solid var(--border-2, #E2DFD8);
  border-radius: 10px;
  font-size: 15px;
  font-weight: 500;
  font-family: inherit;
  background: var(--surface, #fff);
  color: var(--ink, #0A0A0A);
  outline: none;
  transition: border-color 0.15s, box-shadow 0.15s;
}
.inq-input:focus, .inq-textarea:focus {
  border-color: var(--ink, #0A0A0A);
  box-shadow: 0 0 0 1px var(--ink, #0A0A0A) inset;
}
.inq-input::placeholder, .inq-textarea::placeholder {
  color: var(--ink-3, #9A9A95);
  font-weight: 400;
}
.inq-textarea { resize: vertical; min-height: 80px; line-height: 1.5; }

.inq-submit {
  margin-top: 10px;
  background: var(--ink, #0A0A0A);
  color: white;
  border: none;
  padding: 16px 24px;
  border-radius: 999px;
  font-size: 14px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  font-family: inherit;
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  transition: gap 0.2s, background 0.15s, opacity 0.15s;
}
.inq-submit:not(:disabled):hover { gap: 14px; background: #1a1a1a; }
.inq-submit:disabled { opacity: 0.5; cursor: not-allowed; }
html[dir="rtl"] .inq-submit svg { transform: scaleX(-1); }

.inq-divider {
  display: flex;
  align-items: center;
  gap: 14px;
  margin: 22px 0 16px;
  color: var(--ink-3, #9A9A95);
}
.inq-divider::before, .inq-divider::after {
  content: "";
  flex: 1;
  height: 1px;
  background: var(--border-2, #E2DFD8);
}
.inq-divider span {
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.2em;
  text-transform: uppercase;
}

.inq-wa-link {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 12px;
  padding: 16px 24px;
  background: #25D366;
  color: white;
  text-decoration: none;
  border-radius: 999px;
  font-size: 14px;
  font-weight: 700;
  letter-spacing: 0.04em;
  transition: background 0.15s, transform 0.15s;
  font-family: inherit;
}
.inq-wa-link:hover {
  background: #20BD5B;
  transform: translateY(-1px);
  color: white;
}
.inq-wa-link svg { width: 20px; height: 20px; flex-shrink: 0; }

.inq-error {
  font-size: 13px;
  color: #C62828;
  margin-top: 4px;
  display: none;
  font-weight: 500;
}

.inq-done {
  text-align: center;
  padding: 20px 0 8px;
  display: none;
}
.inq-done.show { display: block; }
.inq-form-wrap.hidden { display: none; }
.inq-done-icon {
  width: 56px;
  height: 56px;
  border-radius: 50%;
  background: var(--ink, #0A0A0A);
  color: white;
  display: flex;
  align-items: center;
  justify-content: center;
  margin: 0 auto 24px;
}
.inq-done-title {
  font-size: 22px;
  font-weight: 800;
  letter-spacing: -0.025em;
  margin: 0 0 12px 0;
  color: var(--ink, #0A0A0A);
}
.inq-done-body {
  font-size: 15px;
  color: var(--ink-2, #555);
  line-height: 1.6;
  margin: 0 auto 22px;
  max-width: 380px;
}
.inq-done-ref {
  display: inline-block;
  font-size: 12px;
  font-weight: 600;
  padding: 6px 14px;
  border: 1px solid var(--border-2, #E2DFD8);
  border-radius: 999px;
  color: var(--ink-2, #555);
  letter-spacing: 0.08em;
  font-variant-numeric: tabular-nums;
}

@keyframes inq-fade { from { opacity: 0; } to { opacity: 1; } }
@keyframes inq-rise {
  from { opacity: 0; transform: translateY(24px) scale(0.98); }
  to   { opacity: 1; transform: translateY(0) scale(1); }
}

@media (max-width: 560px) {
  .inq-modal { padding: 0; align-items: flex-end; }
  .inq-card {
    max-width: 100%;
    border-radius: 24px 24px 0 0;
    max-height: 95vh;
    padding: 32px 22px 28px;
  }
  .inq-title { font-size: 22px; }
}
@media (prefers-reduced-motion: reduce) {
  .inq-backdrop, .inq-card { animation: none; }
}
`;

  /* ─── HTML INJECTION ─────────────────────────────────────────── */
  const HTML = `
<div class="inq-modal" id="inq-modal" role="dialog" aria-modal="true" aria-labelledby="inq-title" aria-hidden="true">
  <div class="inq-backdrop" data-close></div>
  <div class="inq-card">
    <button type="button" class="inq-close" data-close aria-label="Close" id="inq-close-btn">
      <svg width="18" height="18" viewBox="0 0 18 18" fill="none" aria-hidden="true">
        <path d="M4 4l10 10M14 4L4 14" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
      </svg>
    </button>

    <div class="inq-form-wrap" id="inq-form-wrap">
      <div class="inq-eyebrow" id="inq-eyebrow"></div>
      <h2 class="inq-title" id="inq-title"></h2>
      <p class="inq-service" id="inq-service-display"></p>

      <form class="inq-form" id="inq-form" novalidate>
        <div class="inq-field">
          <label class="inq-label" for="inq-name" id="inq-label-name"></label>
          <input type="text" id="inq-name" class="inq-input" autocomplete="name" required>
        </div>
        <div class="inq-field">
          <label class="inq-label" for="inq-biz" id="inq-label-biz"></label>
          <input type="text" id="inq-biz" class="inq-input" autocomplete="organization" required>
        </div>
        <div class="inq-field">
          <label class="inq-label" for="inq-wa" id="inq-label-wa"></label>
          <input type="tel" id="inq-wa" class="inq-input" autocomplete="tel" required>
        </div>
        <div class="inq-field">
          <label class="inq-label" for="inq-email" id="inq-label-email"></label>
          <input type="email" id="inq-email" class="inq-input" autocomplete="email">
        </div>
        <div class="inq-field">
          <label class="inq-label" for="inq-notes" id="inq-label-notes"></label>
          <textarea id="inq-notes" class="inq-textarea" rows="3"></textarea>
        </div>
        <button type="submit" class="inq-submit" id="inq-submit">
          <span id="inq-submit-label"></span>
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none" aria-hidden="true">
            <path d="M2 7H12M12 7L7.5 2.5M12 7L7.5 11.5" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
        </button>
        <p class="inq-error" id="inq-error" role="alert"></p>
      </form>

      <div class="inq-divider"><span id="inq-or"></span></div>

      <a href="#" id="inq-wa-link" class="inq-wa-link" target="_blank" rel="noopener noreferrer">
        <svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
          <path d="M17.5 14.4c-.3-.1-1.6-.8-1.9-.9-.3-.1-.4-.1-.6.1-.2.3-.7.9-.8 1-.2.2-.3.2-.6.1-.3-.1-1.2-.5-2.3-1.4-.9-.8-1.4-1.7-1.6-2-.2-.3 0-.5.1-.6.1-.1.3-.3.4-.5.1-.2.2-.3.2-.5.1-.2 0-.4 0-.5 0-.1-.6-1.4-.8-1.9-.2-.5-.4-.4-.6-.4h-.5c-.2 0-.5.1-.7.3-.2.3-.9.9-.9 2.1 0 1.2.9 2.4 1 2.6.1.2 1.7 2.6 4.2 3.7 1.4.6 2 .7 2.7.6.4-.1 1.2-.5 1.4-1 .2-.5.2-.9.1-1-.1-.1-.2-.2-.5-.3z"/>
          <path d="M12 2C6.5 2 2 6.5 2 12c0 1.7.4 3.4 1.3 4.9L2 22l5.3-1.3c1.5.8 3.1 1.2 4.7 1.3 5.5 0 10-4.5 10-10S17.5 2 12 2zm0 18.3c-1.4 0-2.8-.4-4.1-1.1l-.3-.2-3 .8.8-3-.2-.3c-.8-1.3-1.2-2.7-1.2-4.2 0-4.5 3.6-8.1 8.1-8.1s8.1 3.6 8.1 8.1c-.1 4.5-3.7 8-8.2 8z"/>
        </svg>
        <span id="inq-wa-label"></span>
      </a>
    </div>

    <div class="inq-done" id="inq-done">
      <div class="inq-done-icon">
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
          <polyline points="20 6 9 17 4 12"/>
        </svg>
      </div>
      <h3 class="inq-done-title" id="inq-done-title"></h3>
      <p class="inq-done-body" id="inq-done-body"></p>
      <span class="inq-done-ref" id="inq-done-ref"></span>
    </div>
  </div>
</div>
`;

  /* ─── INJECT ─────────────────────────────────────────────────── */
  function inject() {
    const style = document.createElement('style');
    style.id = 'inq-styles';
    style.textContent = CSS;
    document.head.appendChild(style);

    const wrap = document.createElement('div');
    wrap.innerHTML = HTML.trim();
    document.body.appendChild(wrap.firstChild);
  }

  /* ─── STATE ──────────────────────────────────────────────────── */
  let currentService = '';
  let modal, formWrap, doneEl;

  /* ─── TRANSLATIONS APPLY ─────────────────────────────────────── */
  function getLang() {
    return (document.documentElement.lang || 'en').toLowerCase().startsWith('ar') ? 'ar' : 'en';
  }
  function applyTranslations() {
    const c = T[getLang()];
    const set = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
    const setPh = (id, val) => { const el = document.getElementById(id); if (el) el.placeholder = val; };
    set('inq-eyebrow', c.eyebrow);
    set('inq-title', c.title);
    set('inq-label-name', c.name);
    set('inq-label-biz', c.biz);
    set('inq-label-wa', c.whatsapp);
    setPh('inq-wa', c.whatsappPh);
    set('inq-label-email', c.email);
    setPh('inq-email', c.emailPh);
    set('inq-label-notes', c.notes);
    setPh('inq-notes', c.notesPh);
    set('inq-submit-label', c.submit);
    set('inq-or', c.or);
    set('inq-wa-label', c.wa);
    set('inq-done-title', c.doneTitle);
    set('inq-done-body', c.doneBody);
    const closeBtn = document.getElementById('inq-close-btn');
    if (closeBtn) closeBtn.setAttribute('aria-label', c.closeAria);
  }

  /* ─── OPEN / CLOSE ───────────────────────────────────────────── */
  function open(serviceName) {
    currentService = serviceName || '';
    applyTranslations();
    const display = document.getElementById('inq-service-display');
    display.innerHTML = currentService ? `<strong>${escapeHtml(currentService)}</strong>` : '';
    formWrap.classList.remove('hidden');
    doneEl.classList.remove('show');
    modal.classList.add('open');
    modal.setAttribute('aria-hidden', 'false');
    document.body.style.overflow = 'hidden';

    const c = T[getLang()];
    const waLink = document.getElementById('inq-wa-link');
    waLink.href = `https://wa.me/${CFG.whatsapp}?text=${encodeURIComponent(c.waMsg(currentService))}`;

    setTimeout(() => document.getElementById('inq-name').focus(), 80);
  }

  function close() {
    modal.classList.remove('open');
    modal.setAttribute('aria-hidden', 'true');
    document.body.style.overflow = '';
    setTimeout(() => {
      document.getElementById('inq-form').reset();
      document.getElementById('inq-error').style.display = 'none';
      const submitBtn = document.getElementById('inq-submit');
      submitBtn.disabled = false;
      document.getElementById('inq-submit-label').textContent = T[getLang()].submit;
    }, 200);
  }

  function escapeHtml(s) {
    return String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  /* ─── SUBMIT ─────────────────────────────────────────────────── */
  async function onSubmit(e) {
    e.preventDefault();
    const c = T[getLang()];
    const submitBtn = document.getElementById('inq-submit');
    const submitLabel = document.getElementById('inq-submit-label');
    const errEl = document.getElementById('inq-error');
    errEl.style.display = 'none';

    const name = document.getElementById('inq-name').value.trim();
    const business = document.getElementById('inq-biz').value.trim();
    const whatsapp = document.getElementById('inq-wa').value.trim();
    const email = document.getElementById('inq-email').value.trim();
    const notes = document.getElementById('inq-notes').value.trim();

    if (!name || !business || whatsapp.length < 6) {
      errEl.textContent = c.errFields;
      errEl.style.display = 'block';
      return;
    }

    submitBtn.disabled = true;
    submitLabel.textContent = c.submitting;

    try {
      const res = await fetch(CFG.apiUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          type:     'inquiry',
          service:  currentService,
          name, business, whatsapp, email, notes,
          language: getLang(),
        })
      });
      const data = await res.json();
      if (data.ok) {
        formWrap.classList.add('hidden');
        doneEl.classList.add('show');
        const refEl = document.getElementById('inq-done-ref');
        refEl.textContent = data.ref || '';
        refEl.style.display = data.ref ? 'inline-block' : 'none';
      } else {
        throw new Error(data.error || 'submit failed');
      }
    } catch (err) {
      errEl.textContent = c.errSubmit;
      errEl.style.display = 'block';
      submitBtn.disabled = false;
      submitLabel.textContent = c.submit;
    }
  }

  /* ─── WIRE EVENTS ────────────────────────────────────────────── */
  function wire() {
    modal = document.getElementById('inq-modal');
    formWrap = document.getElementById('inq-form-wrap');
    doneEl = document.getElementById('inq-done');

    modal.querySelectorAll('[data-close]').forEach(el => {
      el.addEventListener('click', close);
    });
    document.addEventListener('keydown', e => {
      if (e.key === 'Escape' && modal.classList.contains('open')) close();
    });
    document.getElementById('inq-form').addEventListener('submit', onSubmit);

    /* Auto-wire any element with [data-inquire="Service Name"] */
    document.addEventListener('click', e => {
      const trigger = e.target.closest('[data-inquire]');
      if (trigger) {
        e.preventDefault();
        open(trigger.getAttribute('data-inquire'));
      }
    });

    /* Re-translate when language toggles on the host page */
    const obs = new MutationObserver(() => applyTranslations());
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ['lang', 'dir'] });

    applyTranslations();
  }

  /* ─── INIT ───────────────────────────────────────────────────── */
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => { inject(); wire(); });
  } else {
    inject(); wire();
  }

  /* Expose public API */
  window.openInquiry = open;
  window.closeInquiry = close;
})();
