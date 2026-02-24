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
const PARALLEL_TABS = 5;
const PARALLEL_OCR = 10;
const OCR_TIMEOUT_MS = 6000;

const SKIP_URL_RE =
  /(wp-content\/uploads|media|gallery|video|photo|attachment|privacy|terms|cookies|gdpr)/i;

const SKIP_OCR_RE = /\/(logo|favicon|spinner|avatar|pixel|spacer|blank|transparent)\.|\/icons?\//i;

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

// Extract SiteMap from a page
async function extractSiteMapFromPage(page) {
  return await page.evaluate(() => {
    const getSelector = (el, idx) => {
      if (el.id) return `#${el.id}`;
      if (el.className && typeof el.className === "string") {
        const cls = el.className.trim().split(/\s+/)[0];
        if (cls && !cls.includes(":") && !cls.includes("[")) {
          const matches = document.querySelectorAll(`.${cls}`);
          if (matches.length === 1) return `.${cls}`;
        }
      }
      const tag = el.tagName.toLowerCase();
      const parent = el.parentElement;
      if (parent) {
        const siblings = Array.from(parent.children).filter(c => c.tagName === el.tagName);
        const index = siblings.indexOf(el) + 1;
        if (el.className) {
          const cls = el.className.split(/\s+/)[0];
          if (cls) return `${tag}.${cls}`;
        }
        return `${tag}:nth-of-type(${index})`;
      }
      return `${tag}:nth-of-type(${idx + 1})`;
    };

    const isVisible = (el) => {
      const rect = el.getBoundingClientRect();
      const style = window.getComputedStyle(el);
      return rect.width > 0 && rect.height > 0 &&
             style.display !== "none" &&
             style.visibility !== "hidden";
    };

    const getLabel = (el) => {
      const id = el.id;
      if (id) {
        const label = document.querySelector(`label[for="${id}"]`);
        if (label) return label.textContent?.trim();
      }
      const parent = el.closest("label");
      if (parent) return parent.textContent?.trim();
      const prev = el.previousElementSibling;
      if (prev?.tagName === "LABEL") return prev.textContent?.trim();
      return "";
    };

    // EXTRACT BUTTONS
    const buttons = [];
    const btnElements = document.querySelectorAll(
      "button, a[href], [role='button'], input[type='submit'], input[type='button'], .btn, .button"
    );

    btnElements.forEach((el, i) => {
      if (!isVisible(el)) return;

      const text = (el.textContent?.trim() || el.value || "").slice(0, 100);
      if (!text || text.length < 2) return;

      const href = el.href || "";
      if (/^(#|javascript:|mailto:|tel:)/.test(href)) return;
      if (href && !href.includes(window.location.hostname)) return;

      buttons.push({
        text,
        selector: getSelector(el, i),
      });
    });

    // EXTRACT FORMS
    const forms = [];
    document.querySelectorAll("form").forEach((form, formIdx) => {
      if (!isVisible(form)) return;

      const fields = [];

      form.querySelectorAll("input:not([type='hidden']):not([type='submit']), select, textarea")
        .forEach((input, inputIdx) => {
          if (!isVisible(input)) return;

          fields.push({
            name: input.name || input.id || `field_${inputIdx}`,
            selector: getSelector(input, inputIdx),
            type: input.type || input.tagName.toLowerCase(),
            placeholder: input.placeholder || "",
            label: getLabel(input),
          });
        });

      let submitSelector = "";
      const submitBtn = form.querySelector('button[type="submit"], input[type="submit"], button:not([type])');
      if (submitBtn) {
        submitSelector = getSelector(submitBtn, 0);
      }

      if (fields.length > 0) {
        forms.push({
          selector: getSelector(form, formIdx),
          fields,
          submit_button: submitSelector,
        });
      }
    });

    // EXTRACT PRICES (legacy, context-light)
    const prices = [];
    const priceRegex = /(\d+[\s,.]?\d*)\s*(лв\.?|BGN|EUR|€|\$|лева)/gi;

    const walker = document.createTreeWalker(
      document.body,
      NodeFilter.SHOW_TEXT,
      null
    );

    let node;
    while (node = walker.nextNode()) {
      const text = node.textContent || "";
      const matches = [...text.matchAll(priceRegex)];

      matches.forEach(match => {
        const parent = node.parentElement;
        let context = "";

        if (parent) {
          const container = parent.closest("div, article, section, li, tr");
          if (container) {
            const heading = container.querySelector("h1, h2, h3, h4, h5, h6, strong, b, .title");
            if (heading) context = heading.textContent?.trim().slice(0, 50) || "";
          }
        }

        if (!prices.some(p => p.text === match[0] && p.context === context)) {
          prices.push({
            text: match[0],
            context,
          });
        }
      });
    }

    return {
      url: window.location.href,
      title: document.title,
      buttons: buttons.slice(0, 30),
      forms: forms.slice(0, 10),
      prices: prices.slice(0, 20),
    };
  });
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
// NEW: PRICING/PACKAGES STRUCTURED EXTRACTION
// ═══════════════════════════════════════════════════════════════════════════

async function extractPricingFromPage(page) {
  return await page.evaluate(() => {
    const isVisible = (el) => {
      const rect = el.getBoundingClientRect();
      const style = window.getComputedStyle(el);
      return rect.width > 0 && rect.height > 0 &&
        style.display !== "none" &&
        style.visibility !== "hidden";
    };

    const norm = (s) => (s || "").replace(/\s+/g, " ").trim();

    const moneyRe = /(\d{1,3}(?:[ \u00A0]\d{3})*(?:[.,]\d{1,2})?)\s*(лв\.?|лева|BGN|EUR|€|\$|eur)/i;

    const getText = (el) => norm(el?.innerText || el?.textContent || "");
    const pickTitle = (root) => {
      const h = root.querySelector("h1,h2,h3,h4,[class*='title'],strong,b");
      const t = getText(h);
      if (t && t.length <= 80) return t;
      const lines = getText(root).split("\n").map(norm).filter(Boolean);
      return (lines.find(l => l.length >= 3 && l.length <= 80) || "");
    };

    const pickBadge = (root) => {
      const b = root.querySelector("[class*='badge'],[class*='label'],[class*='tag']");
      const t = getText(b);
      if (t && t.length <= 40) return t;
      const all = getText(root);
      if (/популярен|най-популярен|special|оферта/i.test(all)) {
        const m = all.match(/(популярен|най-популярен|специална оферта)/i);
        return m ? m[0] : "";
      }
      return "";
    };

    const pickFeatures = (root) => {
      const items = [];
      root.querySelectorAll("li").forEach(li => {
        const t = getText(li);
        if (!t) return;
        if (t.length < 3 || t.length > 140) return;
        items.push(t);
      });
      return Array.from(new Set(items)).slice(0, 30);
    };

    const pickPeriod = (root) => {
      const t = getText(root);
      if (/\/\s*месец|на месец|месец/i.test(t)) return "monthly";
      if (/еднократно|one[-\s]?time|еднократ/i.test(t)) return "one_time";
      return null;
    };

    const findCardRoot = (startEl) => {
      let el = startEl;
      for (let i = 0; i < 8 && el; i++) {
        const cls = (el.className && typeof el.className === "string") ? el.className : "";
        const tag = (el.tagName || "").toLowerCase();
        const looksCard =
          /card|pricing|package|plan|tier|column/i.test(cls) ||
          ["article","section"].includes(tag);

        const txt = getText(el);
        const hasTitle = !!pickTitle(el);
        const hasFeatures = el.querySelectorAll("li").length >= 3;

        if (looksCard && (hasTitle || hasFeatures) && txt.length >= 60) return el;
        el = el.parentElement;
      }
      return null;
    };

    const cards = [];
    const seen = new Set();

    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
    let node;
    while (node = walker.nextNode()) {
      const txt = norm(node.textContent || "");
      if (!txt) continue;
      if (!moneyRe.test(txt) && !/по договаряне/i.test(txt)) continue;

      const parent = node.parentElement;
      if (!parent || !isVisible(parent)) continue;

      const root = findCardRoot(parent);
      if (!root || !isVisible(root)) continue;

      const title = pickTitle(root);
      if (!title) continue;

      const rootText = getText(root);
      const moneyMatch = rootText.match(moneyRe);
      const price_text = moneyMatch ? norm(moneyMatch[0]) : (/по договаряне/i.test(rootText) ? "По договаряне" : "");

      const period = pickPeriod(root);

      const badge = pickBadge(root);
      const features = pickFeatures(root);

      const key = `${title}|${price_text}|${period || ""}`;
      if (seen.has(key)) continue;
      seen.add(key);

      cards.push({
        title,
        price_text,
        period,
        badge,
        features,
      });
    }

    const installment_plans = cards.filter(c => c.period === "monthly" || /месец/i.test((c.title || "") + " " + (c.price_text || "")));
    const pricing_cards = cards.filter(c => !installment_plans.includes(c));

    installment_plans.forEach(p => {
      p.title = norm(p.title.replace(/\/\s*месец/i, "").replace(/пакет\s*\/\s*месец/i, "пакет")).trim();
    });

    return {
      pricing_cards: pricing_cards.slice(0, 12),
      installment_plans: installment_plans.slice(0, 12),
    };
  });
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
  if (t.includes("calendly")) return "calendly";
  if (t.includes("simplybook")) return "simplybook";
  if (t.includes("bookero")) return "bookero";
  if (t.includes("cloudbeds")) return "cloudbeds";
  if (t.includes("amelia")) return "amelia";
  if (t.includes("wordpress")) return "wordpress";
  return "unknown";
}

async function extractCapabilitiesFromPage(page) {
  // Universal capability extraction with dynamic rescans.
  // Goal: detect "input surfaces" even when UI is not wrapped in <form> and steps appear after filling.
  return await page.evaluate(async () => {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

    const isVisible = (el) => {
      const rect = el.getBoundingClientRect();
      const style = window.getComputedStyle(el);
      return rect.width > 0 && rect.height > 0 &&
        style.display !== "none" &&
        style.visibility !== "hidden" &&
        style.opacity !== "0";
    };

    const norm = (s) => (s || "").replace(/\s+/g, " ").trim();

    const getLabel = (el) => {
      try {
        const id = el.id;
        if (id) {
          const label = document.querySelector(`label[for="${CSS.escape(id)}"]`);
          if (label) return norm(label.textContent);
        }
      } catch {}

      const parentLabel = el.closest("label");
      if (parentLabel) return norm(parentLabel.textContent);

      const aria = el.getAttribute("aria-label");
      if (aria) return norm(aria);

      const labelledBy = el.getAttribute("aria-labelledby");
      if (labelledBy) {
        const ref = document.getElementById(labelledBy);
        if (ref) return norm(ref.textContent);
      }

      const prev = el.previousElementSibling;
      if (prev && prev.tagName === "LABEL") return norm(prev.textContent);

      return "";
    };

    const selectorCandidates = (el) => {
      const out = [];
      const tag = (el.tagName || "").toLowerCase();

      try { if (el.id) out.push(`#${CSS.escape(el.id)}`); } catch {}
      try {
        const name = el.getAttribute("name");
        if (name) out.push(`${tag}[name="${CSS.escape(name)}"]`);
      } catch {}
      try {
        const type = el.getAttribute("type");
        if (type) out.push(`${tag}[type="${CSS.escape(type)}"]`);
      } catch {}
      try {
        const role = el.getAttribute("role");
        if (role) out.push(`${tag}[role="${CSS.escape(role)}"]`);
      } catch {}
      try {
        const ac = el.getAttribute("autocomplete");
        if (ac) out.push(`${tag}[autocomplete="${CSS.escape(ac)}"]`);
      } catch {}
      try {
        const ph = el.getAttribute("placeholder");
        if (ph && ph.length >= 2) {
          const frag = ph.slice(0, 14).replace(/"/g, "");
          out.push(`${tag}[placeholder*="${frag}"]`);
        }
      } catch {}
      try {
        const cls = (el.className && typeof el.className === "string")
          ? el.className.trim().split(/\s+/).filter(Boolean)[0]
          : "";
        if (cls && !cls.includes(":") && !cls.includes("[") && !cls.includes("]")) out.push(`${tag}.${cls}`);
      } catch {}

      return Array.from(new Set(out)).slice(0, 8);
    };

    const fieldType = (el) => {
      const tag = (el.tagName || "").toLowerCase();
      const typeAttr = (el.getAttribute("type") || "").toLowerCase();
      const role = (el.getAttribute("role") || "").toLowerCase();

      if (tag === "textarea") return "textarea";
      if (tag === "select") return "select";
      if (typeAttr === "email") return "email";
      if (typeAttr === "tel") return "tel";
      if (typeAttr === "number") return "number";
      if (typeAttr === "date") return "date";
      if (typeAttr === "file") return "file";
      if (typeAttr === "checkbox") return "checkbox";
      if (typeAttr === "radio") return "radio";
      if (tag === "input") return typeAttr || "text";

      if (el.isContentEditable) return "contenteditable";
      if (role === "combobox") return "combobox";
      if (role === "listbox") return "listbox";

      return "unknown";
    };

    const isInteractiveInput = (el) => {
      const tag = (el.tagName || "").toLowerCase();
      const typeAttr = (el.getAttribute("type") || "").toLowerCase();
      const role = (el.getAttribute("role") || "").toLowerCase();

      if (!isVisible(el)) return false;

      if (tag === "input") {
        if (["hidden", "submit", "button", "image", "reset"].includes(typeAttr)) return false;
        return true;
      }
      if (tag === "select" || tag === "textarea") return true;
      if (el.isContentEditable) return true;
      if (role === "combobox" || role === "listbox") return true;

      const tab = el.getAttribute("tabindex");
      const hasPopup = (el.getAttribute("aria-haspopup") || "").toLowerCase() === "listbox";
      if (tab === "0" && hasPopup) return true;

      return false;
    };

    const isClickable = (el) => {
      const tag = (el.tagName || "").toLowerCase();
      const role = (el.getAttribute("role") || "").toLowerCase();
      if (!isVisible(el)) return false;
      if (tag === "button") return true;
      if (tag === "a" && (el.getAttribute("href") || "").trim()) return true;
      if (tag === "input") {
        const t = (el.getAttribute("type") || "").toLowerCase();
        if (["button", "submit"].includes(t)) return true;
      }
      if (role === "button") return true;
      return false;
    };

    const isDisabled = (el) => {
      return el.hasAttribute("disabled") || el.getAttribute("aria-disabled") === "true";
    };

    const clickablesIn = (root) => {
      const out = [];
      root.querySelectorAll("button,[role='button'],a,input[type='button'],input[type='submit']").forEach((b) => {
        if (!isClickable(b)) return;
        const text = norm(b.textContent || b.getAttribute("value") || b.getAttribute("aria-label") || "");
        out.push({
          text: text || "",
          disabled: isDisabled(b),
          selector_candidates: selectorCandidates(b),
        });
      });
      return out.slice(0, 25);
    };

    const dumpSnapshot = () => {
      // 1) Standard <form> extraction (kept)
      const forms = [];
      document.querySelectorAll("form").forEach((form) => {
        if (!isVisible(form)) return;

        const fields = [];
        form.querySelectorAll("input,select,textarea,[role='combobox'],[role='listbox'],[contenteditable='true']")
          .forEach((el) => {
            if (!isInteractiveInput(el)) return;

            const tag = (el.tagName || "").toLowerCase();
            const type = fieldType(el);
            const name = el.getAttribute("name") || el.id || "";
            const placeholder = el.getAttribute("placeholder") || "";
            const label = getLabel(el);

            const required =
              el.hasAttribute("required") ||
              el.getAttribute("aria-required") === "true" ||
              (label && /(\*|задължително|required)/i.test(label));

            fields.push({
              tag,
              type,
              name,
              label,
              placeholder: placeholder || "",
              required,
              autocomplete: el.getAttribute("autocomplete") || "",
              aria_label: el.getAttribute("aria-label") || "",
              aria_describedby: el.getAttribute("aria-describedby") || "",
              selector_candidates: selectorCandidates(el),
            });
          });

        if (fields.length === 0) return;

        const actions = clickablesIn(form);

        let dom_snapshot = "";
        try { dom_snapshot = (form.outerHTML || "").slice(0, 4000); } catch {}

        forms.push({
          kind: "form",
          schema: { fields, actions },
          dom_snapshot,
        });
      });

      // 2) Universal INPUT GROUP extraction
      const inputEls = Array.from(
        document.querySelectorAll("input,select,textarea,[role='combobox'],[role='listbox'],[contenteditable='true'],[aria-haspopup='listbox'][tabindex='0']")
      ).filter(isInteractiveInput);

      const groups = new Map();

      const pickRoot = (el) => {
        let cur = el.parentElement;
        for (let depth = 0; depth < 9 && cur; depth++) {
          if (cur === document.body) break;

          const txtLen = (cur.innerText || "").trim().length;
          if (txtLen > 6000) { cur = cur.parentElement; continue; }

          const inputsHere = cur.querySelectorAll(
            "input,select,textarea,[role='combobox'],[role='listbox'],[contenteditable='true'],[aria-haspopup='listbox'][tabindex='0']"
          ).length;
          const hasActions = cur.querySelectorAll("button,[role='button'],input[type='submit'],input[type='button']").length > 0;

          if (inputsHere >= 2 || (hasActions && inputsHere >= 1)) return cur;

          cur = cur.parentElement;
        }
        return null;
      };

      inputEls.forEach((el) => {
        const root = pickRoot(el);
        if (!root) return;
        const arr = groups.get(root) || [];
        arr.push(el);
        groups.set(root, arr);
      });

      const input_groups = [];
      const seenGroupKeys = new Set();

      groups.forEach((els, root) => {
        const uniq = Array.from(new Set(els));
        if (uniq.length === 0) return;

        const fields = [];
        uniq.forEach((el) => {
          const tag = (el.tagName || "").toLowerCase();
          const type = fieldType(el);
          const name = el.getAttribute("name") || el.id || "";
          const placeholder = el.getAttribute("placeholder") || "";
          const label = getLabel(el);

          const required =
            el.hasAttribute("required") ||
            el.getAttribute("aria-required") === "true" ||
            (label && /(\*|задължително|required)/i.test(label));

          fields.push({
            tag,
            type,
            name,
            label,
            placeholder: placeholder || "",
            required,
            autocomplete: el.getAttribute("autocomplete") || "",
            aria_label: el.getAttribute("aria-label") || "",
            aria_describedby: el.getAttribute("aria-describedby") || "",
            selector_candidates: selectorCandidates(el),
          });
        });

        // Keep also single "file upload" groups.
        const hasFile = fields.some(f => f.type === "file");
        if (fields.length < 2 && !hasFile) return;

        const key = fields
          .map(f => (f.name || f.label || f.placeholder || f.type).toLowerCase().slice(0, 40))
          .sort()
          .join("|");
        if (!key || key.length < 3) return;
        if (seenGroupKeys.has(key)) return;
        seenGroupKeys.add(key);

        const actions = clickablesIn(root);

        // Option buttons (segmented controls, etc.)
        const option_buttons = [];
        const clickableCandidates = Array.from(root.querySelectorAll("button,[role='button'],a,div,span"))
          .filter(isVisible)
          .map(el => ({
            el,
            text: norm(el.textContent || el.getAttribute("aria-label") || ""),
          }))
          .filter(x => x.text && x.text.length >= 2 && x.text.length <= 18);

        const seenOpt = new Set();
        for (const x of clickableCandidates) {
          const t = x.text.toLowerCase();
          if (seenOpt.has(t)) continue;
          // filter obvious navigation words (still universal, just avoiding false positives)
          if (/(back|next|назад|напред|submit|изпрати|continue|продължи)/i.test(x.text)) continue;
          seenOpt.add(t);
          option_buttons.push({ text: x.text, selector_candidates: selectorCandidates(x.el) });
          if (option_buttons.length >= 14) break;
        }

        let dom_snapshot = "";
        try { dom_snapshot = (root.outerHTML || "").slice(0, 4000); } catch {}

        input_groups.push({
          kind: "input_group",
          schema: {
            fields,
            actions,
            option_buttons,
            root_selector_candidates: selectorCandidates(root),
          },
          dom_snapshot,
        });
      });

      // 3) Widgets / iframes
      const iframes = [];
      document.querySelectorAll("iframe").forEach((fr) => {
        const src = fr.getAttribute("src") || "";
        if (!src) return;
        iframes.push({
          kind: "booking_widget",
          schema: {
            src,
            title: fr.getAttribute("title") || "",
            name: fr.getAttribute("name") || "",
          },
        });
      });

      // 4) Availability signals (kept)
      const availability = [];
      const dateInputs = Array.from(document.querySelectorAll("input[type='date']")).filter(isVisible).slice(0, 10);
      if (dateInputs.length > 0) {
        availability.push({
          kind: "availability",
          schema: {
            date_inputs: dateInputs.map(inp => ({
              name: inp.getAttribute("name") || inp.id || "",
              label: getLabel(inp),
              selector_candidates: selectorCandidates(inp),
              required: inp.hasAttribute("required") || inp.getAttribute("aria-required") === "true",
            })),
          },
        });
      }

      const calendarLike = Array.from(
        document.querySelectorAll("[class*='calendar'],[class*='datepicker'],[id*='calendar'],[id*='datepicker']")
      ).filter(isVisible).slice(0, 8);

      if (calendarLike.length > 0) {
        availability.push({
          kind: "availability",
          schema: {
            calendar_containers: calendarLike.map(el => ({
              selector_candidates: selectorCandidates(el),
              text_hint: norm((el.textContent || "")).slice(0, 120),
            })),
          },
        });
      }

      return {
        url: window.location.href,
        forms,
        input_groups,
        iframes,
        availability,
      };
    };

    const fillDummyValue = (el) => {
      const tag = (el.tagName || "").toLowerCase();
      const typeAttr = (el.getAttribute("type") || "").toLowerCase();
      const role = (el.getAttribute("role") || "").toLowerCase();

      // Skip disabled/readOnly
      if (el.hasAttribute("disabled")) return false;
      if (el.hasAttribute("readonly")) return false;

      const setVal = (v) => {
        try {
          el.focus();
          if ("value" in el) el.value = v;
          // trigger events
          el.dispatchEvent(new Event("input", { bubbles: true }));
          el.dispatchEvent(new Event("change", { bubbles: true }));
          return true;
        } catch {
          return false;
        }
      };

      if (tag === "select") {
        const opts = Array.from(el.querySelectorAll("option")).filter(o => (o.value || "").trim());
        if (opts.length > 0) {
          el.value = opts[0].value;
          el.dispatchEvent(new Event("change", { bubbles: true }));
          return true;
        }
        return false;
      }

      if (tag === "textarea") return setVal("Тест");
      if (el.isContentEditable) {
        try {
          el.focus();
          el.textContent = "Тест";
          el.dispatchEvent(new Event("input", { bubbles: true }));
          return true;
        } catch { return false; }
      }

      if (tag === "input") {
        if (typeAttr === "email") return setVal("test@example.com");
        if (typeAttr === "tel") return setVal("+359888888888");
        if (typeAttr === "number") return setVal("30");
        if (typeAttr === "date") {
          // today + 7
          const d = new Date();
          const pad = (n) => String(n).padStart(2, "0");
          const iso = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
          return setVal(iso);
        }
        if (typeAttr === "checkbox" || typeAttr === "radio") {
          try {
            if (!el.checked) el.click();
            return true;
          } catch { return false; }
        }
        if (typeAttr === "file") {
          // Can't attach files in crawler discovery
          return false;
        }
        return setVal("Тест");
      }

      if (role === "combobox") {
        // try to type into combobox
        return setVal("Тест");
      }

      return false;
    };

    const visibleRequiredInputs = (root) => {
      const out = [];
      root.querySelectorAll("input,select,textarea,[role='combobox'],[contenteditable='true']").forEach((el) => {
        if (!isInteractiveInput(el)) return;

        const label = getLabel(el);
        const required =
          el.hasAttribute("required") ||
          el.getAttribute("aria-required") === "true" ||
          (label && /(\*|задължително|required)/i.test(label));

        // only if empty-ish
        let empty = true;
        const tag = (el.tagName || "").toLowerCase();
        if (tag === "select") empty = !(el.value && String(el.value).trim());
        else if (el.isContentEditable) empty = !(norm(el.textContent || ""));
        else empty = !((el.value || "").trim());

        if (required && empty) out.push(el);
      });
      return out;
    };

    const pickBestAction = (root) => {
      // Prefer enabled buttons / role=button / submit-ish.
      const candidates = Array.from(root.querySelectorAll("button,[role='button'],input[type='submit'],input[type='button']"))
        .filter(isClickable)
        .filter((b) => !isDisabled(b));

      // If multiple, prefer ones with "primary" styling heuristics.
      const scored = candidates.map((b) => {
        const cls = (b.className && typeof b.className === "string") ? b.className.toLowerCase() : "";
        const type = (b.getAttribute("type") || "").toLowerCase();
        const txt = norm(b.textContent || b.getAttribute("value") || b.getAttribute("aria-label") || "").toLowerCase();

        let score = 0;
        if (type === "submit") score += 5;
        if (/(primary|btn-primary|button-primary|cta|main)/.test(cls)) score += 4;
        if (txt.length >= 2 && txt.length <= 30) score += 1;
        // Avoid obvious "cancel/close"
        if (/(cancel|затвори|close)/.test(txt)) score -= 5;

        return { b, score };
      }).sort((a, b) => b.score - a.score);

      return scored[0]?.b || null;
    };

    const countVisibleInteractive = () => {
      const els = Array.from(document.querySelectorAll("input,select,textarea,[role='combobox'],[contenteditable='true']"))
        .filter(isInteractiveInput);
      return els.length;
    };

    // --- Dynamic rounds ---
    const rounds = [];
    const baselineCount = countVisibleInteractive();

    // round 0 snapshot
    rounds.push(dumpSnapshot());

    // up to N rounds: try to unlock next step by filling required and clicking best action in each discovered group.
    const MAX_ROUNDS = 5;

    for (let round = 1; round <= MAX_ROUNDS; round++) {
      let progressed = false;

      // Find candidate roots by looking at clusters of inputs + actions.
      const roots = Array.from(document.querySelectorAll("div,section,article,form"))
        .filter(isVisible)
        .filter((el) => {
          const inputsHere = el.querySelectorAll("input,select,textarea,[role='combobox'],[contenteditable='true']").length;
          const btnHere = el.querySelectorAll("button,[role='button'],input[type='submit'],input[type='button']").length;
          return inputsHere >= 2 && btnHere >= 1;
        })
        .sort((a, b) => (a.innerText || "").length - (b.innerText || "").length)
        .slice(0, 8);

      for (const root of roots) {
        const reqs = visibleRequiredInputs(root);
        if (reqs.length > 0) {
          reqs.slice(0, 8).forEach((el) => fillDummyValue(el));
        }

        // Try select option buttons (segmented) if exist: click first pressed=false
        const optionCandidates = Array.from(root.querySelectorAll("button,[role='button']"))
          .filter(isClickable)
          .filter((b) => {
            const t = norm(b.textContent || b.getAttribute("aria-label") || "");
            if (!t || t.length > 18) return false;
            if (/(back|next|назад|напред|submit|изпрати|continue|продължи)/i.test(t)) return false;
            return true;
          })
          .slice(0, 6);
        if (optionCandidates.length > 0) {
          try {
            optionCandidates[0].click();
          } catch {}
        }

        const action = pickBestAction(root);
        if (!action) continue;

        const before = countVisibleInteractive();
        try {
          action.click();
        } catch {
          continue;
        }
        await sleep(900);

        const after = countVisibleInteractive();
        if (after !== before) {
          progressed = true;
          break;
        }
      }

      // If nothing changed, stop
      if (!progressed) break;

      // store snapshot after progress
      rounds.push(dumpSnapshot());
    }

    // Union rounds by concatenation (node-side will dedupe by fingerprint anyway)
    // Return as a single snapshot-like object plus debug info.
    const merged = {
      url: window.location.href,
      forms: [],
      input_groups: [],
      iframes: [],
      availability: [],
      _dynamic_rounds: rounds.length,
      _baseline_inputs: baselineCount,
    };

    for (const r of rounds) {
      merged.forms.push(...(r.forms || []));
      merged.input_groups.push(...(r.input_groups || []));
      merged.iframes.push(...(r.iframes || []));
      merged.availability.push(...(r.availability || []));
    }

    return merged;
  });
}

function buildCombinedCapabilities(perPageCaps, baseOrigin) {
  const combined = [];
  const seen = new Set();

  for (const p of perPageCaps) {
    const url = p.url || "";
    const domain = normalizeDomain(url || baseOrigin || "");

    const pushCap = (kind, schema, dom_snapshot) => {
      const normalized = { url, domain, kind, schema };
      const fp = sha256Hex(stableStringify(normalized));
      const key = `${url}|${kind}|${fp}`;
      if (seen.has(key)) return;
      seen.add(key);

      combined.push({
        url,
        domain,
        kind,
        fingerprint: fp,
        schema,
        dom_snapshot: dom_snapshot || null,
      });
    };

    for (const f of p.forms || []) pushCap("form", f.schema, f.dom_snapshot);
    for (const g of p.input_groups || []) pushCap("input_group", g.schema, g.dom_snapshot);

    for (const w of p.iframes || []) {
      const src = w.schema?.src || "";
      pushCap("booking_widget", { ...w.schema, vendor: guessVendorFromText(src) });
    }

    for (const a of p.availability || []) pushCap("availability", a.schema);
  }

  // Prefer actionable input surfaces first
  const forms = combined.filter(c => c.kind === "form" || c.kind === "input_group").slice(0, 80);
  const widgets = combined.filter(c => c.kind === "booking_widget").slice(0, 30);
  const avail = combined.filter(c => c.kind === "availability").slice(0, 30);
  const other = combined.filter(c => !["form","input_group","booking_widget","availability"].includes(c.kind)).slice(0, 20);

  return [...forms, ...widgets, ...avail, ...other];
}

// ═══════════════════════════════════════════════════════════════════════════
// EXISTING EXTRACTION FUNCTIONS (unchanged)
// ═══════════════════════════════════════════════════════════════════════════

async function extractStructured(page) {
  try {
    await page.waitForSelector("body", { timeout: 1500 });
  } catch {}

  try {
    return await page.evaluate(() => {
      const seenTexts = new Set();

      function addUniqueText(text, minLength = 10) {
        const normalized = text.trim().replace(/\s+/g, ' ');
        if (normalized.length < minLength || seenTexts.has(normalized)) return "";
        seenTexts.add(normalized);
        return normalized;
      }

      const sections = [];
      let current = null;
      const processedElements = new Set();

      document.querySelectorAll("h1,h2,h3,p,li").forEach(el => {
        if (processedElements.has(el)) return;
        let parent = el.parentElement;
        while (parent) {
          if (processedElements.has(parent)) return;
          parent = parent.parentElement;
        }
        const text = el.innerText?.trim();
        if (!text) return;
        const uniqueText = addUniqueText(text, 5);
        if (!uniqueText) return;
        if (el.tagName.startsWith("H")) {
          current = { heading: uniqueText, text: "" };
          sections.push(current);
        } else if (current) {
          current.text += " " + uniqueText;
        }
        processedElements.add(el);
      });

      const overlaySelectors = [
        '[class*="overlay"]', '[class*="modal"]', '[class*="popup"]',
        '[class*="tooltip"]', '[class*="banner"]', '[class*="notification"]',
        '[class*="alert"]', '[style*="position: fixed"]',
        '[style*="position: absolute"]', '[role="dialog"]', '[role="alertdialog"]',
      ];

      const overlayTexts = [];
      overlaySelectors.forEach(selector => {
        try {
          document.querySelectorAll(selector).forEach(el => {
            if (processedElements.has(el)) return;
            const text = el.innerText?.trim();
            if (!text) return;
            const uniqueText = addUniqueText(text);
            if (uniqueText) {
              overlayTexts.push(uniqueText);
              processedElements.add(el);
            }
          });
        } catch {}
      });

      const pseudoTexts = [];
      try {
        document.querySelectorAll("*").forEach(el => {
          const before = window.getComputedStyle(el, "::before").content;
          const after = window.getComputedStyle(el, "::after").content;
          if (before && before !== "none" && before !== '""') {
            const cleaned = before.replace(/^["']|["']$/g, "");
            const uniqueText = addUniqueText(cleaned, 3);
            if (uniqueText) pseudoTexts.push(uniqueText);
          }
          if (after && after !== "none" && after !== '""') {
            const cleaned = after.replace(/^["']|["']$/g, "");
            const uniqueText = addUniqueText(cleaned, 3);
            if (uniqueText) pseudoTexts.push(uniqueText);
          }
        });
      } catch {}

      let mainContent = "";
      const mainEl = document.querySelector("main") || document.querySelector("article");
      if (mainEl && !processedElements.has(mainEl)) {
        const text = mainEl.innerText?.trim();
        if (text) mainContent = addUniqueText(text) || "";
      }

      return {
        rawContent: [
          sections.map(s => `${s.heading}\n${s.text}`).join("\n\n"),
          mainContent,
          overlayTexts.join("\n"),
          pseudoTexts.join(" "),
        ].filter(Boolean).join("\n\n"),
      };
    });
  } catch (e) {
    return { rawContent: "" };
  }
}

// ================= GLOBAL OCR CACHE =================
const globalOcrCache = new Map();
const API_KEY = "AIzaSyCoai4BCKJtnnryHbhsPKxJN35UMcMAKrk";

async function fastOCR(buffer) {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), OCR_TIMEOUT_MS);

    const res = await fetch(
      `https://vision.googleapis.com/v1/images:annotate?key=${API_KEY}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          requests: [{
            image: { content: buffer.toString("base64") },
            features: [{ type: "TEXT_DETECTION" }],
            imageContext: { languageHints: ["bg", "en", "tr", "ru"] }
          }]
        }),
        signal: controller.signal
      }
    );

    clearTimeout(timeout);
    if (!res.ok) return "";

    const json = await res.json();
    return json.responses?.[0]?.fullTextAnnotation?.text?.trim() || "";
  } catch { return ""; }
}

async function ocrAllImages(page, stats) {
  const results = [];

  try {
    const imgElements = await page.$$("img");
    if (imgElements.length === 0) return results;

    const imgInfos = await Promise.all(
      imgElements.map(async (img, i) => {
        try {
          const info = await img.evaluate(el => ({
            src: el.src || "",
            alt: el.alt || "",
            w: Math.round(el.getBoundingClientRect().width),
            h: Math.round(el.getBoundingClientRect().height),
            visible: el.getBoundingClientRect().width > 0
          }));
          return { ...info, element: img, index: i };
        } catch { return null; }
      })
    );

    const validImages = imgInfos.filter(info => {
      if (!info || !info.visible) return false;
      if (info.w < 50 || info.h < 25) return false;
      if (info.w > 1800 && info.h > 700) return false;
      if (SKIP_OCR_RE.test(info.src)) {
        return false;
      }
      return true;
    });

    console.log(`[OCR] ${validImages.length}/${imgElements.length} images to process`);
    if (validImages.length === 0) return results;

    const screenshots = await Promise.all(
      validImages.map(async (img) => {
        try {
          if (globalOcrCache.has(img.src)) {
            const cachedText = globalOcrCache.get(img.src);
            return { ...img, buffer: null, cached: true, text: cachedText };
          }

          if (page.isClosed()) return null;
          const buffer = await img.element.screenshot({ type: 'png', timeout: 2500 });
          return { ...img, buffer, cached: false };
        } catch { return null; }
      })
    );

    const validScreenshots = screenshots.filter(s => s !== null);

    for (let i = 0; i < validScreenshots.length; i += PARALLEL_OCR) {
      const batch = validScreenshots.slice(i, i + PARALLEL_OCR);

      const batchResults = await Promise.all(
        batch.map(async (img) => {
          if (img.cached) {
            if (img.text && img.text.length > 2) {
              return { text: img.text, src: img.src, alt: img.alt };
            }
            return null;
          }

          if (!img.buffer) return null;

          const text = await fastOCR(img.buffer);

          globalOcrCache.set(img.src, text);

          if (text && text.length > 2) {
            stats.ocrElementsProcessed++;
            stats.ocrCharsExtracted += text.length;
            return { text, src: img.src, alt: img.alt };
          }
          return null;
        })
      );

      results.push(...batchResults.filter(r => r !== null));
    }

  } catch (e) {
    console.error("[OCR ERROR]", e.message);
  }

  return results;
}

// ================= LINK DISCOVERY =================
async function collectAllLinks(page, base) {
  try {
    return await page.evaluate(base => {
      const urls = new Set();
      document.querySelectorAll("a[href]").forEach(a => {
        try {
          const u = new URL(a.href, base);
          if (u.origin === base) urls.add(u.href.split("#")[0]);
        } catch {}
      });
      return Array.from(urls);
    }, base);
  } catch { return []; }
}

// ================= PROCESS SINGLE PAGE =================
async function processPage(page, url, base, stats, siteMaps, capabilitiesMaps) {
  const startTime = Date.now();

  try {
    console.log("[PAGE]", url);
    await page.goto(url, { timeout: 10000, waitUntil: "domcontentloaded" });

    // Scroll for lazy load
    await page.evaluate(async () => {
      const scrollStep = window.innerHeight;
      const maxScroll = document.body.scrollHeight;

      for (let pos = 0; pos < maxScroll; pos += scrollStep) {
        window.scrollTo(0, pos);
        await new Promise(r => setTimeout(r, 100));
      }

      window.scrollTo(0, maxScroll);

      document.querySelectorAll('img[loading="lazy"], img[data-src], img[data-lazy]').forEach(img => {
        img.loading = 'eager';
        if (img.dataset.src) img.src = img.dataset.src;
        if (img.dataset.lazy) img.src = img.dataset.lazy;
      });
    });

    await page.waitForTimeout(500);

    try {
      await page.waitForLoadState('networkidle', { timeout: 3000 });
    } catch {}

    const title = clean(await page.title());
    const pageType = detectPageType(url, title);
    stats.byType[pageType] = (stats.byType[pageType] || 0) + 1;

    // Extract structured content
    const data = await extractStructured(page);

    // OCR images
    const ocrResults = await ocrAllImages(page, stats);

    // NEW: Pricing/package structured extraction (cards + installment)
    let pricing = null;
    try {
      pricing = await extractPricingFromPage(page);
      if ((pricing?.pricing_cards?.length || 0) > 0 || (pricing?.installment_plans?.length || 0) > 0) {
        console.log(`[PRICING] Page: ${pricing.pricing_cards?.length || 0} cards, ${pricing.installment_plans?.length || 0} installment`);
      }
    } catch (e) {
      console.error("[PRICING] Extract error:", e.message);
    }

    // *** EXISTING: Extract SiteMap from this page ***
    try {
      const rawSiteMap = await extractSiteMapFromPage(page);
      if (rawSiteMap.buttons.length > 0 || rawSiteMap.forms.length > 0) {
        siteMaps.push(rawSiteMap);
        console.log(`[SITEMAP] Page: ${rawSiteMap.buttons.length} buttons, ${rawSiteMap.forms.length} forms`);
      }
    } catch (e) {
      console.error("[SITEMAP] Extract error:", e.message);
    }

    // *** NEW: Extract Capabilities from this page (forms/widgets/availability) ***
    try {
      const caps = await extractCapabilitiesFromPage(page);
      if ((caps.forms?.length || 0) > 0 || (caps.iframes?.length || 0) > 0 || (caps.availability?.length || 0) > 0) {
        capabilitiesMaps.push(caps);
        console.log(`[CAPS] Page: ${caps.forms?.length || 0} forms, ${caps.iframes?.length || 0} iframes, ${caps.availability?.length || 0} availability`);
      }
    } catch (e) {
      console.error("[CAPS] Extract error:", e.message);
    }

    // Format content
    const htmlContent = normalizeNumbers(clean(data.rawContent));
    const ocrTexts = ocrResults.map(r => r.text);
    const ocrContent = normalizeNumbers(clean(ocrTexts.join("\n\n")));

    const content = `
=== HTML_CONTENT_START ===
${htmlContent}
=== HTML_CONTENT_END ===

=== OCR_CONTENT_START ===
${ocrContent}
=== OCR_CONTENT_END ===
`.trim();

    // ✅ NEW: contacts extraction (DOM + combined text)
    const domContacts = await extractContactsFromPage(page);
    const textContacts = extractContactsFromText(`${htmlContent}\n\n${ocrContent}\n\n${domContacts.textHints || ""}`);

    const mergedEmails = Array.from(new Set([...(domContacts.emails || []), ...(textContacts.emails || [])])).slice(0, 12);
    const mergedPhones = Array.from(new Set([...(domContacts.phones || []).map(normalizePhone).filter(Boolean), ...(textContacts.phones || [])])).slice(0, 12);

    const contacts = {
      emails: mergedEmails,
      phones: mergedPhones,
    };

    if (contacts.emails.length || contacts.phones.length) {
      console.log(`[CONTACTS] Page: ${contacts.phones.length} phones, ${contacts.emails.length} emails`);
    }

    const htmlWords = countWordsExact(htmlContent);
    const ocrWords = countWordsExact(ocrContent);
    const totalWords = htmlWords + ocrWords;

    const elapsed = Date.now() - startTime;
    console.log(`[PAGE] ✓ ${totalWords}w (${htmlWords}+${ocrWords}ocr, ${ocrResults.length} imgs) ${elapsed}ms`);

    if (pageType !== "services" && totalWords < MIN_WORDS) {
      return { links: await collectAllLinks(page, base), page: null };
    }

    return {
      links: await collectAllLinks(page, base),
      page: {
        url,
        title,
        pageType,
        content,
        // ✅ structured output: pricing + contacts
        structured: { pricing, contacts },
        wordCount: totalWords,
        breakdown: { htmlWords, ocrWords, images: ocrResults.length },
        status: "ok",
      }
    };
  } catch (e) {
    console.error("[PAGE ERROR]", url, e.message);
    stats.errors++;
    return { links: [], page: null };
  }
}

// ================= PARALLEL CRAWLER =================
async function crawlSmart(startUrl, siteId = null) {
  const deadline = Date.now() + MAX_SECONDS * 1000;
  console.log("\n[CRAWL START]", startUrl);
  console.log(`[CONFIG] ${PARALLEL_TABS} tabs, ${PARALLEL_OCR} parallel OCR`);
  if (siteId) console.log(`[SITE ID] ${siteId}`);

  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--disable-software-rasterizer"],
  });

  const stats = {
    visited: 0,
    saved: 0,
    byType: {},
    ocrElementsProcessed: 0,
    ocrCharsExtracted: 0,
    errors: 0,
  };

  const pages = [];
  const queue = [];
  const siteMaps = []; // collect sitemaps from all pages
  const capabilitiesMaps = []; // collect capabilities from all pages
  let base = "";

  // ✅ NEW: aggregate contacts across pages
  const contactAgg = { emails: new Set(), phones: new Set() };

  try {
    const initContext = await browser.newContext({
      viewport: { width: 1920, height: 1080 },
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    });
    const initPage = await initContext.newPage();

    await initPage.goto(startUrl, { timeout: 10000, waitUntil: "domcontentloaded" });
    base = new URL(initPage.url()).origin;

    const initialLinks = await collectAllLinks(initPage, base);
    queue.push(normalizeUrl(initPage.url()));
    initialLinks.forEach(l => {
      const nl = normalizeUrl(l);
      if (!visited.has(nl) && !SKIP_URL_RE.test(nl) && !queue.includes(nl)) {
        queue.push(nl);
      }
    });

    await initPage.close();
    await initContext.close();

    console.log(`[CRAWL] Found ${queue.length} URLs`);

    const createWorker = async () => {
      const ctx = await browser.newContext({
        viewport: { width: 1920, height: 1080 },
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      });
      const pg = await ctx.newPage();

      while (Date.now() < deadline) {
        let url = null;
        while (queue.length > 0) {
          const candidate = queue.shift();
          const normalized = normalizeUrl(candidate);
          if (!visited.has(normalized) && !SKIP_URL_RE.test(normalized)) {
            visited.add(normalized);
            url = normalized;
            break;
          }
        }

        if (!url) {
          await new Promise(r => setTimeout(r, 30));
          if (queue.length === 0) break;
          continue;
        }

        stats.visited++;

        const result = await processPage(pg, url, base, stats, siteMaps, capabilitiesMaps);

        if (result.page) {
          // ✅ collect contacts
          const c = result.page?.structured?.contacts;
          if (c?.emails?.length) c.emails.forEach(e => contactAgg.emails.add(String(e).trim()));
          if (c?.phones?.length) c.phones.forEach(p => contactAgg.phones.add(String(p).trim()));

          pages.push(result.page);
          stats.saved++;
        }

        result.links.forEach(l => {
          const nl = normalizeUrl(l);
          if (!visited.has(nl) && !SKIP_URL_RE.test(nl) && !queue.includes(nl)) {
            queue.push(nl);
          }
        });
      }

      await pg.close();
      await ctx.close();
    };

    await Promise.all(Array(PARALLEL_TABS).fill(0).map(() => createWorker()));

  } finally {
    await browser.close();
    console.log(`\n[CRAWL DONE] ${stats.saved}/${stats.visited} pages`);
    console.log(`[OCR STATS] ${stats.ocrElementsProcessed} images → ${stats.ocrCharsExtracted} chars`);
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
    console.log(`[CAPS] Combined: ${combinedCapabilities.length} capabilities (forms/widgets/availability)`);
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
        config: { PARALLEL_TABS, MAX_SECONDS, MIN_WORDS, PARALLEL_OCR }
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
        globalOcrCache.clear();

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
    console.log(`Config: ${PARALLEL_TABS} tabs, ${PARALLEL_OCR} parallel OCR`);
    console.log(`Worker: ${WORKER_URL}`);
  });
