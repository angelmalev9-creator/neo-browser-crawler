import http from "http";
import crypto from "crypto";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

// Worker config - за изпращане на SiteMap
const WORKER_URL = process.env.NEO_WORKER_URL || "https://neo-worker.onrender.com";
const WORKER_SECRET = process.env.NEO_WORKER_SECRET || "";

// Supabase config - за записване на SiteMap
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";

let crawlInProgress = false;
let crawlFinished = false;
let lastResult = null;
let lastCrawlUrl = null;
let lastCrawlTime = 0;
const RESULT_TTL_MS = 5 * 60 * 1000;
const visited = new Set();

// ================= LIMITS =================
const MAX_SECONDS = 180;
const MIN_WORDS = 20;
const PARALLEL_TABS = 32;          // 6-core VPS: ~5 tabs per core
const BROWSERS = 2;                // split tabs across 2 browser instances
const PAGE_BUDGET_MS = 7000;      // max ms per page total (soft, never throws)

const SKIP_URL_RE =
  /(wp-content\/uploads|media|gallery|video|photo|attachment|privacy|terms|cookies|gdpr)/i;

// ================= DEADLINE HELPER =================
// Races a promise against a timeout. On timeout returns the fallback — never throws.
function withDeadline(promise, ms, fallback = null) {
  let t;
  const timer = new Promise(r => { t = setTimeout(() => r(fallback), ms); });
  return Promise.race([
    promise.then(v => { clearTimeout(t); return v; }).catch(() => { clearTimeout(t); return fallback; }),
    timer,
  ]);
}


// ================= UTILS =================
const clean = (t = "") =>
  t.replace(/\r/g, "").replace(/[ \t]+/g, " ").replace(/\n{3,}/g, "\n\n").trim();

const countWordsExact = (t = "") => t.split(/\s+/).filter(Boolean).length;

// ================= BG NUMBER NORMALIZER =================
// IMPORTANT FIX: do NOT convert money amounts to words.
// Keep numeric prices intact for downstream extraction.
const BG_0_19 = [
  "нула","едно","две","три","четири","пет","шест","седем","осем","девет",
  "десет","единадесет","дванадесет","тринадесет","четиринадесет",
  "петнадесет","шестнадесет","седемнадесет","осемнадесет","деветнадесет"
];
const BG_TENS = ["", "", "двадесет","тридесет","четиридесет","петдесет","шестдесет","седемдесет","осемдесет","деветдесет"];

function numberToBgWords(n) {
  n = Number(n);
  if (Number.isNaN(n)) return n;
  if (n < 20) return BG_0_19[n];
  if (n < 100) {
    const t = Math.floor(n / 10);
    const r = n % 10;
    return BG_TENS[t] + (r ? " и " + BG_0_19[r] : "");
  }
  return String(n);
}

function normalizeNumbers(text = "") {
  try {
    // ✅ Exclude money units (лв/лева/€/$/EUR/BGN) from normalization.
    // Keep digits for prices so pack/pricing extraction works reliably.
    return text.replace(
      /(\d+)\s?(стая|стаи|човек|човека|нощувка|нощувки|кв\.?|sqm)/gi,
      (_, num, unit) => `${numberToBgWords(num)} ${unit}`
    );
  } catch { return text; }
}

// ================= URL NORMALIZER =================
const normalizeUrl = (u) => {
  try {
    const url = new URL(u);
    url.hash = "";
    url.search = "";
    url.hostname = url.hostname.replace(/^www\./, "");
    if (url.pathname.endsWith("/") && url.pathname !== "/") {
      url.pathname = url.pathname.slice(0, -1);
    }
    return url.toString();
  } catch { return u; }
};

function normalizeDomain(u) {
  try {
    const url = new URL(u);
    return url.hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

// ================= CONTACT EXTRACTION =================
const EMAIL_RE = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi;
// broad phone-ish: +90 532 155 86 96, (052) 123-45-67, 0888 123 456 etc.
const PHONE_CANDIDATE_RE = /(\+?\d[\d\s().-]{6,}\d)/g;
const DATE_DOT_RE = /\b\d{1,2}\.\d{1,2}\.\d{4}\b/;

function normalizePhone(raw) {
  const s = String(raw || "").trim();
  if (!s) return "";
  if (DATE_DOT_RE.test(s)) return ""; // avoid dates like 10.12.2025

  // keep leading +, strip other non-digits
  const hasPlus = s.trim().startsWith("+");
  const digits = s.replace(/[^\d]/g, "");
  // typical phone length guard (tolerant)
  if (digits.length < 8 || digits.length > 15) return "";

  return hasPlus ? `+${digits}` : digits;
}

function extractContactsFromText(text) {
  const out = { emails: [], phones: [] };

  if (!text) return out;

  const emails = (text.match(EMAIL_RE) || [])
    .map(e => e.trim())
    .filter(Boolean);

  const phonesRaw = [];
  let m;
  while ((m = PHONE_CANDIDATE_RE.exec(text)) !== null) {
    phonesRaw.push(m[1]);
  }
  const phones = phonesRaw
    .map(normalizePhone)
    .filter(Boolean);

  // dedupe
  out.emails = Array.from(new Set(emails)).slice(0, 12);
  out.phones = Array.from(new Set(phones)).slice(0, 12);

  return out;
}

async function extractContactsFromPage(page) {
  try {
    const dom = await page.evaluate(() => {
      const norm = (s) => (s || "").replace(/\s+/g, " ").trim();

      const emails = new Set();
      const phones = new Set();

      // mailto/tel links
      document.querySelectorAll('a[href^="mailto:"]').forEach(a => {
        const href = a.getAttribute("href") || "";
        const v = href.replace(/^mailto:/i, "").split("?")[0];
        if (v) emails.add(v.trim());
      });

      document.querySelectorAll('a[href^="tel:"]').forEach(a => {
        const href = a.getAttribute("href") || "";
        const v = href.replace(/^tel:/i, "");
        if (v) phones.add(v.trim());
      });

      // visible text hints near footer/contact areas
      const candidates = [];
      const footer = document.querySelector("footer");
      if (footer) candidates.push(footer.innerText || "");
      const contactSection =
        document.querySelector("[id*='contact'],[class*='contact'],[id*='kontakti'],[class*='kontakti']");
      if (contactSection) candidates.push(contactSection.innerText || "");

      return {
        emails: Array.from(emails).map(norm).filter(Boolean).slice(0, 12),
        phones: Array.from(phones).map(norm).filter(Boolean).slice(0, 12),
        textHints: candidates.map(norm).filter(Boolean).join("\n"),
      };
    });

    return dom || { emails: [], phones: [], textHints: "" };
  } catch {
    return { emails: [], phones: [], textHints: "" };
  }
}

// ================= PAGE TYPE =================
function detectPageType(url = "", title = "") {
  const s = (url + " " + title).toLowerCase();
  if (/za-nas|about/.test(s)) return "about";
  if (/uslugi|services|pricing|price|ceni|tseni/.test(s)) return "services";
  if (/kontakti|contact/.test(s)) return "contact";
  if (/faq|vuprosi|questions/.test(s)) return "faq";
  if (/blog|news|article/.test(s)) return "blog";
  return "general";
}

// ═══════════════════════════════════════════════════════════════════════════
// SITEMAP EXTRACTION - EXISTING
// ═══════════════════════════════════════════════════════════════════════════

// Keyword mappings for buttons and fields
const KEYWORD_MAP = {
  // Booking
  "резерв": ["book", "reserve", "booking"],
  "запази": ["book", "reserve"],
  "резервация": ["booking", "reservation"],
  "резервирай": ["book", "reserve"],
  // Search
  "търси": ["search", "find"],
  "провери": ["check", "verify"],
  "покажи": ["show", "display"],
  // Dates
  "настаняване": ["check-in", "checkin", "arrival"],
  "напускане": ["check-out", "checkout", "departure"],
  "пристигане": ["arrival", "check-in"],
  "заминаване": ["departure", "check-out"],
  // Contact
  "контакт": ["contact"],
  "контакти": ["contact", "contacts"],
  "свържи": ["contact", "reach"],
  // Rooms
  "стаи": ["rooms", "accommodation"],
  "стая": ["room"],
  // Other
  "цени": ["prices", "rates"],
  "услуги": ["services"],
  "изпрати": ["send", "submit"],
};

function generateKeywords(text) {
  const lower = text.toLowerCase().trim();
  const keywords = new Set([lower]);

  // Split into words
  const words = lower.split(/\s+/);
  words.forEach(w => {
    if (w.length > 2) keywords.add(w);
  });

  // Add mapped keywords
  for (const [bg, en] of Object.entries(KEYWORD_MAP)) {
    if (lower.includes(bg)) {
      en.forEach(k => keywords.add(k));
    }
  }

  return Array.from(keywords).filter(k => k.length > 1);
}

function detectActionType(text) {
  const lower = text.toLowerCase();

  if (/резерв|book|запази|reserve/i.test(lower)) return "booking";
  if (/контакт|contact|свържи/i.test(lower)) return "contact";
  if (/търси|search|провери|check|submit|изпрати/i.test(lower)) return "submit";
  if (/стаи|rooms|услуги|services|за нас|about|галерия|gallery/i.test(lower)) return "navigation";

  return "other";
}

function detectFieldType(name, type, placeholder, label) {
  const searchText = `${name} ${type} ${placeholder} ${label}`.toLowerCase();

  if (type === "date") return "date";
  if (type === "number") return "number";
  if (/date|дата/i.test(searchText)) return "date";
  if (/guest|човек|брой|count|number/i.test(searchText)) return "number";
  if (/select/i.test(type)) return "select";

  return "text";
}

function generateFieldKeywords(name, placeholder, label) {
  const keywords = new Set();
  const searchText = `${name} ${placeholder} ${label}`.toLowerCase();

  // Check-in patterns
  if (/check-?in|checkin|arrival|от|настаняване|пристигане|from|start/i.test(searchText)) {
    ["check-in", "checkin", "от", "настаняване", "arrival", "from"].forEach(k => keywords.add(k));
  }

  // Check-out patterns
  if (/check-?out|checkout|departure|до|напускане|заминаване|to|end/i.test(searchText)) {
    ["check-out", "checkout", "до", "напускане", "departure", "to"].forEach(k => keywords.add(k));
  }

  // Guests patterns
  if (/guest|adult|човек|гост|брой|persons|pax/i.test(searchText)) {
    ["guests", "гости", "човека", "adults", "persons", "брой"].forEach(k => keywords.add(k));
  }

  // Name patterns
  if (/name|име/i.test(searchText)) {
    ["name", "име"].forEach(k => keywords.add(k));
  }

  // Email patterns
  if (/email|имейл|e-mail/i.test(searchText)) {
    ["email", "имейл", "e-mail"].forEach(k => keywords.add(k));
  }

  // Phone patterns
  if (/phone|телефон|тел/i.test(searchText)) {
    ["phone", "телефон"].forEach(k => keywords.add(k));
  }

  // Add name/id as keywords
  if (name) keywords.add(name.toLowerCase());

  return Array.from(keywords);
}

// Enrich raw SiteMap with keywords (runs in Node.js)
function enrichSiteMap(raw, siteId, siteUrl) {
  return {
    site_id: siteId,
    url: siteUrl || raw.url || "",

    buttons: (raw.buttons || []).map(btn => ({
      text: btn.text,
      selector: btn.selector,
      keywords: generateKeywords(btn.text),
      action_type: detectActionType(btn.text),
    })),

    forms: (raw.forms || []).map(form => ({
      selector: form.selector,
      submit_button: form.submit_button,
      fields: form.fields.map(field => ({
        name: field.name,
        selector: field.selector,
        type: detectFieldType(field.name, field.type, field.placeholder, field.label),
        keywords: generateFieldKeywords(field.name, field.placeholder, field.label),
      })),
    })),

    prices: (raw.prices || []).map(p => ({
      text: p.text,
      context: p.context || "",
    })),
  };
}

// Build combined SiteMap from all crawled pages
function buildCombinedSiteMap(pageSiteMaps, siteId, siteUrl) {
  const combined = {
    site_id: siteId,
    url: siteUrl,
    buttons: [],
    forms: [],
    prices: [],
  };

  const seenButtons = new Set();
  const seenForms = new Set();
  const seenPrices = new Set();

  for (const pageMap of pageSiteMaps) {
    for (const btn of pageMap.buttons || []) {
      const key = btn.text.toLowerCase();
      if (!seenButtons.has(key)) {
        seenButtons.add(key);
        combined.buttons.push(btn);
      }
    }

    for (const form of pageMap.forms || []) {
      const key = form.fields.map(f => f.name).sort().join(",");
      if (!seenForms.has(key)) {
        seenForms.add(key);
        combined.forms.push(form);
      }
    }

    for (const price of pageMap.prices || []) {
      const key = `${price.text}|${price.context}`;
      if (!seenPrices.has(key)) {
        seenPrices.add(key);
        combined.prices.push(price);
      }
    }
  }

  combined.buttons = combined.buttons.slice(0, 50);
  combined.forms = combined.forms.slice(0, 15);
  combined.prices = combined.prices.slice(0, 30);

  console.log(`[SITEMAP] Combined: ${combined.buttons.length} buttons, ${combined.forms.length} forms, ${combined.prices.length} prices`);

  return combined;
}

// Save SiteMap to Supabase
async function saveSiteMapToSupabase(siteMap) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    console.log("[SITEMAP] Supabase not configured, skipping save");
    return false;
  }

  try {
    const response = await fetch(`${SUPABASE_URL}/rest/v1/sites_map`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
        "Prefer": "resolution=merge-duplicates",
      },
      body: JSON.stringify({
        site_id: siteMap.site_id,
        url: siteMap.url,
        site_map: siteMap,
        updated_at: new Date().toISOString(),
      }),
    });

    if (response.ok) {
      console.log(`[SITEMAP] ✓ Saved to Supabase`);
      return true;
    } else {
      const error = await response.text();
      console.error(`[SITEMAP] ✗ Supabase error:`, error);
      return false;
    }
  } catch (error) {
    console.error(`[SITEMAP] ✗ Save error:`, error.message);
    return false;
  }
}

// Send SiteMap to Worker to prepare hot session
async function sendSiteMapToWorker(siteMap) {
  if (!WORKER_URL || !WORKER_SECRET) {
    console.log("[SITEMAP] Worker not configured, skipping");
    return false;
  }

  try {
    console.log(`[SITEMAP] Sending to worker: ${WORKER_URL}/prepare-session`);

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 30000);

    const response = await fetch(`${WORKER_URL}/prepare-session`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${WORKER_SECRET}`,
      },
      body: JSON.stringify({
        site_id: siteMap.site_id,
        site_map: siteMap,
      }),
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (response.ok) {
      const result = await response.json();
      console.log(`[SITEMAP] ✓ Worker response:`, result);
      return result.success === true;
    } else {
      console.error(`[SITEMAP] ✗ Worker error:`, response.status);
      return false;
    }
  } catch (error) {
    console.error(`[SITEMAP] ✗ Worker send error:`, error.message);
    return false;
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// NEW: CAPABILITIES EXTRACTION (FOR form_schemas)
// ═══════════════════════════════════════════════════════════════════════════

function stableSortObject(value) {
  if (Array.isArray(value)) return value.map(stableSortObject);
  if (value && typeof value === "object") {
    const out = {};
    Object.keys(value).sort().forEach(k => {
      out[k] = stableSortObject(value[k]);
    });
    return out;
  }
  return value;
}

function stableStringify(obj) {
  try {
    return JSON.stringify(stableSortObject(obj));
  } catch {
    return JSON.stringify(obj);
  }
}

function sha256Hex(str) {
  return crypto.createHash("sha256").update(str).digest("hex");
}

function guessVendorFromText(s = "") {
  const t = s.toLowerCase();
  if (t.includes("quendoo")) return "quendoo";
  if (t.includes("cloudbeds")) return "cloudbeds";
  if (t.includes("simplybook")) return "simplybook";
  if (t.includes("calendly")) return "calendly";
  if (t.includes("bookero")) return "bookero";
  if (t.includes("amelia")) return "amelia";
  if (t.includes("hotelrunner")) return "hotelrunner";
  if (t.includes("beds24")) return "beds24";
  if (t.includes("synxis")) return "synxis";
  if (t.includes("mews")) return "mews";
  if (t.includes("sabeeapp")) return "sabeeapp";
  if (t.includes("littlehotelier")) return "littlehotelier";
  if (t.includes("bookingengine") || t.includes("booking-engine")) return "booking_engine";
  if (t.includes("wordpress")) return "wordpress";
  return "unknown";
}


// ═══════════════════════════════════════════════════════════════════════════
// MEGA EVALUATE — 1 CDP round-trip, извлича ВСИЧКО наведнъж:
// текст, линкове, контакти, цени, sitemap бутони/форми, capabilities
// ═══════════════════════════════════════════════════════════════════════════

async function extractEverything(page, base, url) {
  try {
    return await page.evaluate(({ base, url }) => {
      // ── HELPERS ───────────────────────────────────────────
      const norm = (s = "") => String(s || "").replace(/\s+/g, " ").trim();
      const seenText = new Set();
      const addUniq = (t, min = 8) => {
        const n = norm(t); if (!n || n.length < min || seenText.has(n)) return ""; seenText.add(n); return n;
      };
      const isVis = (el) => {
        try { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; } catch { return false; }
      };
      const getText = (el) => norm(el?.innerText || el?.textContent || "");
      const base_origin = (() => { try { return new URL(base).origin; } catch { return ""; } })();
      const pathLower = (() => { try { return new URL(url).pathname.toLowerCase(); } catch { return ""; } })();

      // ── 1. TEXT CONTENT ───────────────────────────────────
      const parts = [];
      const main = document.querySelector("main,article,[role='main'],#content,.content,#main");
      if (main) { const t = addUniq(main.innerText || "", 20); if (t) parts.push(t); }
      document.querySelectorAll("h1,h2,h3,h4,p,li,td,th").forEach(el => {
        try { const t = addUniq(el.innerText || el.textContent || ""); if (t) parts.push(t); } catch {}
      });
      document.querySelectorAll("details").forEach(el => {
        try { el.open = true; const t = addUniq(el.innerText || "", 5); if (t) parts.push(t); } catch {}
      });
      const rawContent = parts.join("\n");

      // ── 2. LINKS ──────────────────────────────────────────
      const links = [];
      document.querySelectorAll("a[href]").forEach(a => {
        try { const u = new URL(a.href, base); if (u.origin === base_origin) links.push(u.href.split("#")[0]); } catch {}
      });

      // ── 3. CONTACTS ───────────────────────────────────────
      const emails = new Set();
      const phones = new Set();
      document.querySelectorAll('a[href^="mailto:"]').forEach(a => {
        const v = (a.getAttribute("href") || "").replace(/^mailto:/i, "").split("?")[0].trim(); if (v) emails.add(v);
      });
      document.querySelectorAll('a[href^="tel:"]').forEach(a => {
        const v = (a.getAttribute("href") || "").replace(/^tel:/i, "").trim(); if (v) phones.add(v);
      });
      const footer = document.querySelector("footer");
      const contactEl = document.querySelector("[id*='contact'],[class*='contact']");
      const contactHints = [footer, contactEl].filter(Boolean).map(el => norm(el.innerText || "")).join("\n");

      // ── 4. PRICING (само на ценови страници) ──────────────
      let pricing = null;
      const isPricingPage = /tseni|ceni|price|pricing|package|plan|услуги|пакет/i.test(pathLower + " " + document.title);
      if (isPricingPage) {
        try {
          const moneyRe = /(\d{1,3}(?:[ \u00A0]\d{3})*(?:[.,]\d{1,2})?)\s*(лв\.?|лева|BGN|EUR|€|\$)/i;
          const cards = [];
          const seenCards = new Set();
          const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
          let node;
          while (node = walker.nextNode()) {
            const txt = norm(node.textContent || "");
            if (!moneyRe.test(txt)) continue;
            let el = node.parentElement;
            for (let i = 0; i < 6 && el; i++) {
              const cls = String(el.className || "");
              if (/card|pricing|package|plan|tier/i.test(cls) || ["article","section"].includes((el.tagName||"").toLowerCase())) {
                const title = getText(el.querySelector("h1,h2,h3,h4,strong,b") || el).slice(0, 80);
                const mm = getText(el).match(moneyRe);
                const price_text = mm ? norm(mm[0]) : "";
                const key = `${title}|${price_text}`;
                if (!seenCards.has(key) && title) {
                  seenCards.add(key);
                  const features = Array.from(el.querySelectorAll("li")).map(li => getText(li)).filter(t => t.length > 2 && t.length < 120).slice(0, 10);
                  cards.push({ title, price_text, features });
                }
                break;
              }
              el = el.parentElement;
            }
          }
          pricing = { pricing_cards: cards.slice(0, 12), installment_plans: [] };
        } catch {}
      }

      // ── 5. SITEMAP — buttons & forms (само homepage & key pages) ──
      let siteMap = null;
      const isKeyPage = pathLower === "/" || pathLower === "" ||
        /contact|kontakt|about|services|uslugi/i.test(pathLower);
      if (isKeyPage) {
        try {
          const buttons = [];
          const seenBtns = new Set();
          document.querySelectorAll("button,a[href],[role='button'],input[type='submit']").forEach((el, i) => {
            if (!isVis(el)) return;
            const text = norm(el.textContent || el.value || "").slice(0, 80);
            if (!text || text.length < 2 || seenBtns.has(text.toLowerCase())) return;
            const href = el.href || "";
            if (/^(#|javascript:|mailto:|tel:)/.test(href)) return;
            seenBtns.add(text.toLowerCase());
            buttons.push({ text, selector: el.id ? `#${el.id}` : el.tagName.toLowerCase() });
          });
          const forms = [];
          document.querySelectorAll("form").forEach(form => {
            if (!isVis(form)) return;
            const fields = [];
            form.querySelectorAll("input:not([type='hidden']):not([type='submit']),select,textarea").forEach(inp => {
              if (!isVis(inp)) return;
              fields.push({ name: inp.name || inp.id || "", type: inp.type || inp.tagName.toLowerCase(), placeholder: inp.placeholder || "" });
            });
            if (fields.length) forms.push({ fields: fields.slice(0, 10) });
          });
          siteMap = { buttons: buttons.slice(0, 20), forms: forms.slice(0, 5) };
        } catch {}
      }

      // ── 6. CAPABILITIES — iframes & date inputs (всички страници) ──
      let capabilities = null;
      try {
        const iframes = [];
        document.querySelectorAll("iframe[src]").forEach(fr => {
          const src = fr.getAttribute("src") || "";
          const t = src.toLowerCase();
          const vendor = ["cloudbeds","simplybook","calendly","bookero","beds24","synxis","mews","sabeeapp"].find(v => t.includes(v)) || "unknown";
          const bookingLike = vendor !== "unknown" || /book|reserv|availab|check/i.test(src);
          if (bookingLike) iframes.push({ src, vendor });
        });
        const dateInputs = Array.from(document.querySelectorAll("input[type='date']")).filter(isVis).length;
        if (iframes.length || dateInputs) {
          capabilities = { iframes, dateInputs };
        }
      } catch {}

      return {
        rawContent,
        links: [...new Set(links)],
        emails: [...emails].slice(0, 12),
        phones: [...phones].slice(0, 12),
        contactHints,
        pricing,
        siteMap,
        capabilities,
        title: document.title,
      };
    }, { base, url });
  } catch {
    return { rawContent: "", links: [], emails: [], phones: [], contactHints: "", pricing: null, siteMap: null, capabilities: null, title: "" };
  }
}

// ================= PROCESS SINGLE PAGE =================
async function processPage(page, url, base, stats, siteMaps, capabilitiesMaps) {
  const startTime = Date.now();

  await withDeadline(
    page.goto(url, { timeout: 0, waitUntil: "domcontentloaded" })
      .catch(e => { if (!e.message?.includes("interrupted by another navigation")) throw e; }),
    PAGE_BUDGET_MS,
    null
  );

  const loadMs = Date.now() - startTime;
  if (loadMs > PAGE_BUDGET_MS - 300) {
    stats.errors++;
    return { links: [], page: null };
  }

  // ── ОДИН evaluate — всичко наведнъж ──────────────────────
  const all = await withDeadline(
    extractEverything(page, base, url),
    PAGE_BUDGET_MS - loadMs - 100,
    { rawContent: "", links: [], emails: [], phones: [], contactHints: "", pricing: null, siteMap: null, capabilities: null, title: "" }
  );

  // Sitemap
  if (all.siteMap?.buttons?.length || all.siteMap?.forms?.length) {
    siteMaps.push({ url, ...all.siteMap });
  }

  // Capabilities
  if (all.capabilities?.iframes?.length || all.capabilities?.dateInputs) {
    capabilitiesMaps.push({ url, ...all.capabilities });
  }

  const title = clean(all.title || "");
  const pageType = detectPageType(url, title);
  stats.byType[pageType] = (stats.byType[pageType] || 0) + 1;

  const htmlContent = normalizeNumbers(clean(all.rawContent || ""));
  const textContacts = extractContactsFromText(`${htmlContent}\n${all.contactHints || ""}`);
  const mergedEmails = Array.from(new Set([...(all.emails || []), ...(textContacts.emails || [])])).slice(0, 12);
  const mergedPhones = Array.from(new Set([
    ...(all.phones || []).map(normalizePhone).filter(Boolean),
    ...(textContacts.phones || [])
  ])).slice(0, 12);

  const totalWords = countWordsExact(htmlContent);
  console.log(`[PAGE] ✓ ${url.split("/").pop() || "/"} ${totalWords}w ${Date.now() - startTime}ms`);

  if (pageType !== "services" && totalWords < MIN_WORDS) {
    return { links: all.links || [], page: null };
  }

  return {
    links: all.links || [],
    page: {
      url, title, pageType,
      content: `=== HTML_CONTENT_START ===\n${htmlContent}\n=== HTML_CONTENT_END ===`,
      structured: { pricing: all.pricing, contacts: { emails: mergedEmails, phones: mergedPhones } },
      wordCount: totalWords,
      status: "ok",
    }
  };
}


// ================= PARALLEL CRAWLER =================

// Chromium args for performance (no duplicates with Playwright defaults)
const CHROMIUM_ARGS = [
  "--no-sandbox",
  "--disable-dev-shm-usage",
  "--disable-gpu",
  "--disable-software-rasterizer",
  "--disable-domain-reliability",
  "--mute-audio",
  // NOTE: --single-process and --no-zygote intentionally omitted
  // — they crash Chromium when opening multiple pages/contexts
];

// Block resources we don't need (no OCR = no images needed)
const BLOCKED_TYPES = new Set(["image", "media", "font", "stylesheet"]);

async function setupPage(ctx) {
  const pg = await ctx.newPage();
  await pg.route("**/*", (route) => {
    if (BLOCKED_TYPES.has(route.request().resourceType())) {
      route.abort();
    } else {
      route.continue();
    }
  });
  return pg;
}

async function crawlSmart(startUrl, siteId = null) {
  const deadline = Date.now() + MAX_SECONDS * 1000;
  console.log("\n[CRAWL START]", startUrl);
  console.log(`[CONFIG] ${PARALLEL_TABS} tabs across ${BROWSERS} browsers`);
  if (siteId) console.log(`[SITE ID] ${siteId}`);

  // Launch multiple browser instances for better CPU distribution
  const browsers = await Promise.all(
    Array(BROWSERS).fill(0).map(() => chromium.launch({ headless: true, args: CHROMIUM_ARGS }))
  );

  const stats = {
    visited: 0,
    saved: 0,
    byType: {},
    errors: 0,
  };

  const pages = [];
  // Use Set for O(1) queue dedup instead of array.includes()
  const queue = [];
  const queued = new Set();
  const siteMaps = [];
  const capabilitiesMaps = [];
  let base = "";

  const contactAgg = { emails: new Set(), phones: new Set() };

  const enqueue = (url) => {
    const nl = normalizeUrl(url);
    if (!visited.has(nl) && !SKIP_URL_RE.test(nl) && !queued.has(nl)) {
      queued.add(nl);
      queue.push(nl);
    }
  };

  try {
    const initCtx = await browsers[0].newContext({
      viewport: { width: 1280, height: 800 },
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    });
    const initPage = await setupPage(initCtx);

    await withDeadline(
      initPage.goto(startUrl, { timeout: 0, waitUntil: "domcontentloaded" }).catch(() => null),
      12000, null
    );
    base = new URL(initPage.url()).origin;

    const initData = await withDeadline(
      extractEverything(initPage, base, initPage.url()),
      8000,
      { links: [] }
    );
    enqueue(initPage.url());
    (initData?.links || []).forEach(enqueue);

    await initPage.close();
    await initCtx.close();

    console.log(`[CRAWL] Found ${queue.length} URLs`);

    const createWorker = async (browser) => {
      const ctx = await browser.newContext({
        viewport: { width: 1280, height: 800 },
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      });
      const pg = await setupPage(ctx);

      while (Date.now() < deadline) {
        let url = null;
        while (queue.length > 0) {
          const candidate = queue.shift();
          if (!visited.has(candidate) && !SKIP_URL_RE.test(candidate)) {
            visited.add(candidate);
            url = candidate;
            break;
          }
        }

        if (!url) {
          await new Promise(r => setTimeout(r, 20));
          if (queue.length === 0) break;
          continue;
        }

        stats.visited++;

        const result = await processPage(pg, url, base, stats, siteMaps, capabilitiesMaps);

        if (result.page) {
          const c = result.page?.structured?.contacts;
          if (c?.emails?.length) c.emails.forEach(e => contactAgg.emails.add(String(e).trim()));
          if (c?.phones?.length) c.phones.forEach(p => contactAgg.phones.add(String(p).trim()));
          pages.push(result.page);
          stats.saved++;
        }

        result.links.forEach(enqueue);
      }

      await pg.close();
      await ctx.close();
    };

    // Distribute tabs evenly across browser instances
    const tabsPerBrowser = Math.ceil(PARALLEL_TABS / BROWSERS);
    const workerPromises = browsers.flatMap((browser, i) =>
      Array(tabsPerBrowser).fill(0).map(() => createWorker(browser))
    );
    await Promise.all(workerPromises);

  } finally {
    await Promise.all(browsers.map(b => b.close()));
    console.log(`\n[CRAWL DONE] ${stats.saved}/${stats.visited} pages`);
  }

  let combinedSiteMap = null;
  if (siteMaps.length > 0 && siteId) {
    console.log(`\n[SITEMAP] Building combined map from ${siteMaps.length} pages...`);

    const enrichedMaps = siteMaps.map(raw => enrichSiteMap(raw, siteId, base));
    combinedSiteMap = buildCombinedSiteMap(enrichedMaps, siteId, base);

    await saveSiteMapToSupabase(combinedSiteMap);
    await sendSiteMapToWorker(combinedSiteMap);
  }

  let combinedCapabilities = [];
  if (capabilitiesMaps.length > 0) {
    combinedCapabilities = buildCombinedCapabilities(capabilitiesMaps, base);
    console.log(`[CAPS] Combined: ${combinedCapabilities.length} capabilities (forms/wizards/widgets/availability)`);
  }

  const contacts = {
    emails: Array.from(contactAgg.emails).filter(Boolean).slice(0, 20),
    phones: Array.from(contactAgg.phones).filter(Boolean).slice(0, 20),
  };

  if (contacts.emails.length || contacts.phones.length) {
    console.log(`[CONTACTS] Combined: ${contacts.phones.length} phones, ${contacts.emails.length} emails`);
  }

  return { pages, stats, siteMap: combinedSiteMap, capabilities: combinedCapabilities, contacts };
}

// ================= HTTP SERVER =================
http
  .createServer((req, res) => {
    if (req.method === "GET") {
      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({
        status: crawlInProgress ? "crawling" : (crawlFinished ? "ready" : "idle"),
        crawlInProgress,
        crawlFinished,
        lastCrawlUrl,
        lastCrawlTime: lastCrawlTime ? new Date(lastCrawlTime).toISOString() : null,
        resultAvailable: !!lastResult,
        pagesCount: lastResult?.pages?.length || 0,
        capabilitiesCount: lastResult?.capabilities?.length || 0,
        contacts: lastResult?.contacts || null,
        config: { PARALLEL_TABS, BROWSERS, MAX_SECONDS, MIN_WORDS }
      }));
    }

    if (req.method !== "POST") {
      res.writeHead(405, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ error: "Method not allowed" }));
    }

    let body = "";
    req.on("data", c => (body += c));
    req.on("error", err => {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: "Request error" }));
    });

    req.on("end", async () => {
      try {
        const parsed = JSON.parse(body || "{}");

        if (!parsed.url) {
          res.writeHead(400, { "Content-Type": "application/json" });
          return res.end(JSON.stringify({
            success: false,
            error: "Missing 'url' parameter"
          }));
        }

        const requestedUrl = normalizeUrl(parsed.url);
        const siteId = parsed.site_id || null;
        const now = Date.now();

        if (crawlFinished && lastResult && lastCrawlUrl === requestedUrl) {
          if (now - lastCrawlTime < RESULT_TTL_MS) {
            console.log("[CACHE HIT] Returning cached result for:", requestedUrl);
            res.writeHead(200, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({ success: true, cached: true, ...lastResult }));
          }
        }

        if (crawlInProgress) {
          if (lastCrawlUrl === requestedUrl) {
            res.writeHead(202, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({
              success: false,
              status: "in_progress",
              message: "Crawl in progress for this URL"
            }));
          } else {
            res.writeHead(429, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({
              success: false,
              error: "Crawler busy with different URL"
            }));
          }
        }

        crawlInProgress = true;
        crawlFinished = false;
        lastCrawlUrl = requestedUrl;
        visited.clear();

        console.log("[CRAWL START] New crawl for:", requestedUrl);
        if (siteId) console.log("[SITE ID]", siteId);

        const result = await crawlSmart(parsed.url, siteId);

        crawlInProgress = false;
        crawlFinished = true;
        lastResult = result;
        lastCrawlTime = Date.now();

        console.log("[CRAWL DONE] Result ready for:", requestedUrl);

        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: true, ...result }));
      } catch (e) {
        crawlInProgress = false;
        console.error("[CRAWL ERROR]", e.message);
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({
          success: false,
          error: e instanceof Error ? e.message : String(e),
        }));
      }
    });
  })
  .listen(PORT, () => {
    console.log("Crawler running on", PORT);
    console.log(`Config: ${PARALLEL_TABS} tabs across ${BROWSERS} browsers`);
    console.log(`Worker: ${WORKER_URL}`);
  });
