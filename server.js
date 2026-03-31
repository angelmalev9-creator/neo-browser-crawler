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
const MAX_SECONDS = 90;
const MIN_WORDS = 20;
const PARALLEL_TABS = 8;
const SCROLL_STEP_MS = 30;
const MAX_SCROLL_STEPS = 8;

// ================= UI INTERACTION LIMITS =================
const MAX_UI_CLICKS = 30;           // max interactive elements to click per page
const UI_CLICK_WAIT_MS = 300;       // wait after each click for content to appear
const UI_INTERACTION_BUDGET_MS = 8000; // max time budget for UI interactions per page

const SKIP_URL_RE =
  /(wp-content\/uploads|media|gallery|video|photo|attachment|privacy|terms|cookies|gdpr)/i;

// ================= UTILS =================
const clean = (t = "") =>
  t.replace(/\r/g, "").replace(/[ \t]+/g, " ").replace(/\n{3,}/g, "\n\n").trim();

const countWordsExact = (t = "") => t.split(/\s+/).filter(Boolean).length;

// ================= BG NUMBER NORMALIZER =================
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
const PHONE_CANDIDATE_RE = /(\+?\d[\d\s().-]{6,}\d)/g;
const DATE_DOT_RE = /\b\d{1,2}\.\d{1,2}\.\d{4}\b/;

function normalizePhone(raw) {
  const s = String(raw || "").trim();
  if (!s) return "";
  if (DATE_DOT_RE.test(s)) return "";
  const hasPlus = s.trim().startsWith("+");
  const digits = s.replace(/[^\d]/g, "");
  if (digits.length < 8 || digits.length > 15) return "";
  return hasPlus ? `+${digits}` : digits;
}

function extractContactsFromText(text) {
  const out = { emails: [], phones: [] };
  if (!text) return out;
  const emails = (text.match(EMAIL_RE) || []).map(e => e.trim()).filter(Boolean);
  const phonesRaw = [];
  let m;
  while ((m = PHONE_CANDIDATE_RE.exec(text)) !== null) {
    phonesRaw.push(m[1]);
  }
  const phones = phonesRaw.map(normalizePhone).filter(Boolean);
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
// SITEMAP EXTRACTION - EXISTING (unchanged)
// ═══════════════════════════════════════════════════════════════════════════

const KEYWORD_MAP = {
  "резерв": ["book", "reserve", "booking"],
  "запази": ["book", "reserve"],
  "резервация": ["booking", "reservation"],
  "резервирай": ["book", "reserve"],
  "търси": ["search", "find"],
  "провери": ["check", "verify"],
  "покажи": ["show", "display"],
  "настаняване": ["check-in", "checkin", "arrival"],
  "напускане": ["check-out", "checkout", "departure"],
  "пристигане": ["arrival", "check-in"],
  "заминаване": ["departure", "check-out"],
  "контакт": ["contact"],
  "контакти": ["contact", "contacts"],
  "свържи": ["contact", "reach"],
  "стаи": ["rooms", "accommodation"],
  "стая": ["room"],
  "цени": ["prices", "rates"],
  "услуги": ["services"],
  "изпрати": ["send", "submit"],
};

function generateKeywords(text) {
  const lower = text.toLowerCase().trim();
  const keywords = new Set([lower]);
  const words = lower.split(/\s+/);
  words.forEach(w => {
    if (w.length > 2) keywords.add(w);
  });
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
  if (/check-?in|checkin|arrival|от|настаняване|пристигане|from|start/i.test(searchText)) {
    ["check-in", "checkin", "от", "настаняване", "arrival", "from"].forEach(k => keywords.add(k));
  }
  if (/check-?out|checkout|departure|до|напускане|заминаване|to|end/i.test(searchText)) {
    ["check-out", "checkout", "до", "напускане", "departure", "to"].forEach(k => keywords.add(k));
  }
  if (/guest|adult|човек|гост|брой|persons|pax/i.test(searchText)) {
    ["guests", "гости", "човека", "adults", "persons", "брой"].forEach(k => keywords.add(k));
  }
  if (/name|име/i.test(searchText)) {
    ["name", "име"].forEach(k => keywords.add(k));
  }
  if (/email|имейл|e-mail/i.test(searchText)) {
    ["email", "имейл", "e-mail"].forEach(k => keywords.add(k));
  }
  if (/phone|телефон|тел/i.test(searchText)) {
    ["phone", "телефон"].forEach(k => keywords.add(k));
  }
  if (name) keywords.add(name.toLowerCase());
  return Array.from(keywords);
}

// Extract SiteMap from a page (unchanged)
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
      buttons.push({ text, selector: getSelector(el, i) });
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

    // EXTRACT PRICES
    const prices = [];
    const priceRegex = /(\d+[\s,.]?\d*)\s*(лв\.?|BGN|EUR|€|\$|лева)/gi;
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
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
          prices.push({ text: match[0], context });
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

// Enrich raw SiteMap (unchanged)
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

// Build combined SiteMap (unchanged)
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

// Save SiteMap to Supabase (unchanged)
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

// Send SiteMap to Worker (unchanged)
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
// PRICING/PACKAGES STRUCTURED EXTRACTION (unchanged)
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

      cards.push({ title, price_text, period, badge, features });
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
// CAPABILITIES EXTRACTION (unchanged from original)
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

// extractCapabilitiesFromPage - unchanged from original (very large function)
async function extractCapabilitiesFromPage(page) {
  return await page.evaluate(() => {
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
        if (label) return label.textContent?.trim() || "";
      }
      const parent = el.closest("label");
      if (parent) return parent.textContent?.trim() || "";
      const aria = el.getAttribute("aria-label");
      if (aria) return aria.trim();
      const prev = el.previousElementSibling;
      if (prev?.tagName === "LABEL") return prev.textContent?.trim() || "";
      return "";
    };

    const selectorCandidates = (el) => {
      const out = [];
      try { if (el.id) out.push(`#${CSS.escape(el.id)}`); } catch {}
      try {
        const name = el.getAttribute("name");
        if (name) out.push(`${el.tagName.toLowerCase()}[name="${CSS.escape(name)}"]`);
      } catch {}
      try {
        const type = el.getAttribute("type");
        if (type) out.push(`${el.tagName.toLowerCase()}[type="${CSS.escape(type)}"]`);
      } catch {}
      try {
        const ph = el.getAttribute("placeholder");
        if (ph && ph.length >= 2) out.push(`${el.tagName.toLowerCase()}[placeholder*="${ph.slice(0, 12).replace(/"/g, "")}"]`);
      } catch {}
      try {
        const ac = el.getAttribute("autocomplete");
        if (ac) out.push(`${el.tagName.toLowerCase()}[autocomplete="${CSS.escape(ac)}"]`);
      } catch {}
      try {
        const cls = (el.className && typeof el.className === "string")
          ? el.className.trim().split(/\s+/).filter(Boolean)[0]
          : "";
        if (cls) out.push(`${el.tagName.toLowerCase()}.${cls}`);
      } catch {}
      return Array.from(new Set(out)).slice(0, 6);
    };

    // Helper: extract radio choices + button-group choices
    const extractChoices = (root) => {
      const choices = [];

      // RADIO GROUPS
      root.querySelectorAll('input[type="radio"]').forEach(input => {
        if (!isVisible(input)) return;
        const name = input.getAttribute("name") || input.id || "";
        const label = getLabel(input);
        const groupName = name || label;
        const required =
          input.hasAttribute("required") ||
          input.getAttribute("aria-required") === "true" ||
          (label && /(\*|задължително|required)/i.test(label));

        let group = choices.find(c => c.name === groupName && c.type === "radio");
        if (!group) {
          group = {
            name: groupName,
            label: getLabel(input.closest("[role='radiogroup']") || input.parentElement) || label,
            required,
            type: "radio",
            options: []
          };
          choices.push(group);
        }
        group.options.push({
          value: input.value || label,
          label: getLabel(input) || input.value,
          selector_candidates: selectorCandidates(input)
        });
      });

      // BUTTON GROUPS
      root.querySelectorAll('button[aria-pressed], [role="radio"], .segmented button').forEach(btn => {
        if (!isVisible(btn)) return;
        const text = (btn.textContent || "").trim();
        if (!text || text.length < 2) return;
        const parentLabel = getLabel(btn.parentElement) || "";
        const groupName = parentLabel || "button_group";
        let group = choices.find(c => c.name === groupName && c.type === "button_group");
        if (!group) {
          group = { name: groupName, label: parentLabel, required: false, type: "button_group", options: [] };
          choices.push(group);
        }
        group.options.push({
          value: text, label: text, selector_candidates: selectorCandidates(btn)
        });
      });

      // SIBLING BUTTON CHOICES
      const seenBtnContainers = new Set();
      root.querySelectorAll("button").forEach(btn => {
        if (!isVisible(btn)) return;
        const parent = btn.parentElement;
        if (!parent || seenBtnContainers.has(parent)) return;
        if (btn.hasAttribute("aria-pressed") || btn.getAttribute("role") === "radio") return;
        const siblingBtns = Array.from(parent.querySelectorAll(":scope > button, :scope > * > button"))
          .filter(b => isVisible(b));
        if (siblingBtns.length < 2) return;
        const submitRe = /напред|назад|next|back|prev|submit|изпрати|запази|book|reserve|резерв|close|затвори|отказ|cancel/i;
        const optionBtns = siblingBtns.filter(b => {
          const t = (b.textContent || "").trim();
          return t.length >= 1 && t.length <= 30 && !submitRe.test(t);
        });
        if (optionBtns.length < 2) return;
        seenBtnContainers.add(parent);
        let groupLabel = "";
        const prevSib = parent.previousElementSibling;
        if (prevSib) {
          const t = (prevSib.textContent || "").trim();
          if (t.length >= 2 && t.length <= 60) groupLabel = t;
        }
        if (!groupLabel) groupLabel = getLabel(parent) || "";
        const required = /\*|задължително|required/i.test(groupLabel);
        const cleanLabel = groupLabel.replace(/\s*\*\s*$/, "").trim();
        const groupName = cleanLabel || "button_choice";
        if (choices.find(c => c.name === groupName)) return;
        const group = { name: groupName, label: cleanLabel, required, type: "button_group", options: [] };
        optionBtns.forEach(b => {
          const text = (b.textContent || "").trim();
          group.options.push({ value: text, label: text, selector_candidates: selectorCandidates(b) });
        });
        choices.push(group);
      });

      // SELECT OPTIONS
      root.querySelectorAll("select").forEach(sel => {
        if (!isVisible(sel)) return;
        const name = sel.getAttribute("name") || sel.id || "";
        const label = getLabel(sel);
        const required = sel.hasAttribute("required") || sel.getAttribute("aria-required") === "true";
        const options = [];
        sel.querySelectorAll("option").forEach(opt => {
          const val = opt.value;
          const text = (opt.textContent || "").trim();
          if (!val && !text) return;
          if (/^(--|изберете|избери|select|choose)/i.test(text) && !val) return;
          options.push({ value: val, label: text, selector_candidates: [] });
        });
        if (options.length > 0) {
          choices.push({
            name: name || label, label, required, type: "select",
            options, selector_candidates: selectorCandidates(sel)
          });
        }
      });

      return choices;
    };

    // FORMS EXTRACTION
    const forms = [];
    document.querySelectorAll("form").forEach((form) => {
      if (!isVisible(form)) return;
      const fields = [];
      form.querySelectorAll("input:not([type='hidden']):not([type='submit']), select, textarea")
        .forEach((input) => {
          if (!isVisible(input)) return;
          const tag = input.tagName.toLowerCase();
          const type = (input.getAttribute("type") || tag).toLowerCase();
          const name = input.getAttribute("name") || input.id || "";
          const placeholder = input.getAttribute("placeholder") || "";
          const label = getLabel(input);
          const required =
            input.hasAttribute("required") ||
            input.getAttribute("aria-required") === "true" ||
            (label && /(\*|задължително|required)/i.test(label));
          const autocomplete = input.getAttribute("autocomplete") || "";
          const ariaLabel = input.getAttribute("aria-label") || "";
          const ariaDesc = input.getAttribute("aria-describedby") || "";
          if (type === "radio") return;
          fields.push({
            tag, type, name, label, placeholder, required,
            autocomplete, aria_label: ariaLabel, aria_describedby: ariaDesc,
            selector_candidates: selectorCandidates(input),
          });
        });

      if (fields.length === 0) return;
      const meaningfulFields = fields.filter(f =>
        !['hidden','submit','button','reset','image'].includes(f.type)
      );
      if (meaningfulFields.length === 0) return;

      const choices = extractChoices(form);
      const submitCandidates = [];
      form.querySelectorAll("button, input[type='submit'], [role='button']").forEach((btn) => {
        if (!isVisible(btn)) return;
        const text = (btn.textContent?.trim() || btn.getAttribute("value") || "").slice(0, 80);
        if (!text) return;
        submitCandidates.push({ text, selector_candidates: selectorCandidates(btn) });
      });
      const bestSubmit =
        submitCandidates.find(b => /изпрати|send|submit|запази|резерв|book|reserve/i.test(b.text)) ||
        submitCandidates[0] || null;

      let dom_snapshot = "";
      try { dom_snapshot = (form.outerHTML || "").slice(0, 4000); } catch {}

      forms.push({
        kind: "form",
        schema: {
          fields, choices, submit: bestSubmit,
          action: form.getAttribute("action") || "",
          method: (form.getAttribute("method") || "get").toLowerCase(),
        },
        dom_snapshot,
      });
    });

    // IFRAMES EXTRACTION
    const iframes = [];
    const bookingIframes = [];
    const vendorFromText = (s = "") => {
      const t = String(s || "").toLowerCase();
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
      return "unknown";
    };
    const iframeSelectorHint = (fr, vendor) => {
      try { if (fr.id) return `#${CSS.escape(fr.id)}`; } catch {}
      try { if (vendor && vendor !== "unknown") return `iframe[src*="${vendor}"]`; } catch {}
      return "iframe";
    };
    document.querySelectorAll("iframe").forEach((fr) => {
      const src = fr.getAttribute("src") || "";
      if (!src) return;
      const title = fr.getAttribute("title") || "";
      const name = fr.getAttribute("name") || "";
      const vendor = vendorFromText(`${src} ${title} ${name}`);
      const rect = fr.getBoundingClientRect();
      const visible = isVisible(fr);
      const bookingLike = vendor !== "unknown" || /(book|booking|reserve|reservation|availability|check-?in|check-?out|guest|adult|children|room)/i.test(`${src} ${title} ${name}`);
      const selectorHint = iframeSelectorHint(fr, vendor);

      iframes.push({
        kind: "booking_widget",
        schema: {
          src, title, name, vendor, visible, booking_like: bookingLike,
          selector_candidates: [selectorHint],
        },
      });

      if (bookingLike && visible && rect.width >= 220 && rect.height >= 40) {
        bookingIframes.push({ vendor, src, title, name, selectorHint });
      }
    });

    // AVAILABILITY EXTRACTION
    const availability = [];

    bookingIframes.forEach((widget) => {
      const vendorLabel = widget.vendor && widget.vendor !== "unknown" ? widget.vendor : "iframe_booking";
      availability.push({
        kind: "availability",
        schema: {
          ui_type: "iframe_booking_widget",
          booking_vendor: vendorLabel,
          iframe_src: widget.src,
          extraction_mode: "iframe_vendor_detection",
          detection_grade: true,
          execution_grade: false,
          text_hint: `${vendorLabel} iframe booking widget`,
          date_inputs: [
            { text: "Пристигане", label: "Пристигане", concrete: false, selector_candidates: [widget.selectorHint] },
            { text: "Напускане", label: "Напускане", concrete: false, selector_candidates: [widget.selectorHint] },
          ],
          guest_fields: [
            { text: "Гости", label: "Гости", concrete: false, selector_candidates: [widget.selectorHint] },
          ],
          action_buttons: [
            { text: "Резервирай", concrete: false, selector_candidates: [widget.selectorHint] },
          ],
          detected_fields: { check_in: true, check_out: true, guests: true },
          selector_candidates: [widget.selectorHint],
        },
      });
    });

    const dateInputs = Array.from(document.querySelectorAll("input[type='date']"))
      .filter(isVisible).slice(0, 10);
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

    const calendarLike = Array.from(document.querySelectorAll("[class*='calendar'],[class*='datepicker'],[id*='calendar'],[id*='datepicker']"))
      .filter(isVisible).slice(0, 8);
    if (calendarLike.length > 0) {
      availability.push({
        kind: "availability",
        schema: {
          calendar_containers: calendarLike.map(el => ({
            selector_candidates: selectorCandidates(el),
            text_hint: (el.textContent || "").trim().slice(0, 120),
          })),
        },
      });
    }

    // INTERACTIVE BOOKING BAR DETECTION
    const pushAvailability = (schema) => {
      const key = JSON.stringify(schema || {});
      if (!pushAvailability._seen) pushAvailability._seen = new Set();
      if (pushAvailability._seen.has(key)) return;
      pushAvailability._seen.add(key);
      availability.push({ kind: "availability", schema });
    };

    const normText = (s) => String(s || "").replace(/\s+/g, " ").trim();
    const getInteractiveText = (el) => {
      if (!el) return "";
      return normText([
        el.textContent || "",
        el.getAttribute?.("aria-label") || "",
        el.getAttribute?.("placeholder") || "",
        el.getAttribute?.("value") || "",
        el.getAttribute?.("title") || "",
      ].filter(Boolean).join(" "));
    };

    const bookingRe = {
      checkIn: /(пристигане|настаняване|check\s*-?in|arrival|checkin)/i,
      checkOut: /(напускане|заминаване|check\s*-?out|departure|checkout)/i,
      guests: /(възрастни|adults?|guests?|гости|деца|children|rooms?|стаи?|promo\s*code|промо\s*код)/i,
      action: /(резервирай|резервация|book(?:\s*now)?|reserve|search|availability|провери|търси)/i,
      noise: /(jquery|document\.ready|swiper|slidesperview|pagination|navigation|autoplay|loop:|виж повече|направи запитване)/i,
      menuNoise: /(начало|home|за нас|about|контакти|contact|галерия|gallery|оферти|offers|цени|pricing|blog|новини|faq|всички стаи|стаи и апартаменти|rooms? & suites|accommodation)/i,
      roomNoise: /(делукс|double|studio|апартамент|suite|standard room|family room|superior|junior suite|икономична стая)/i,
      genericActionNoise: /(виж повече|learn more|details|прочети повече|направи запитване|изпрати запитване)/i,
    };

    const interactiveSelectors = 'button, a, input, select, textarea, [role="button"], [role="combobox"], [aria-haspopup], [aria-label], [placeholder]';
    const signalSelectors = 'button, a, input, select, textarea, label, span, div, [role="button"], [role="combobox"], [aria-haspopup], [aria-label], [placeholder]';
    const concreteInteractiveSelectors = 'input, select, textarea, button, a, [role="button"], [role="combobox"], [aria-label], [placeholder]';

    const isMenuLikeElement = (el) => {
      if (!el) return false;
      const tag = (el.tagName || '').toLowerCase();
      const cls = String(el.className || '').toLowerCase();
      const id = String(el.id || '').toLowerCase();
      const href = String(el.getAttribute?.('href') || '').toLowerCase();
      const txt = getInteractiveText(el).toLowerCase();
      if (tag === 'a' && /(^#|javascript:|\/accommodation|\/rooms|\/contact|\/offers|\/restaurant|\/about|\/home|\/blog|\/faq)/i.test(href)) return true;
      if (/menu-link|menu-text|submenu|offcanvas|nav|navbar|header-menu|mobile-menu|desktop-menu/i.test(cls)) return true;
      if (/nav|menu|header|offcanvas/i.test(id)) return true;
      if (el.closest('header, nav, [class*="menu"], [class*="nav"], [class*="header"], [class*="offcanvas"]')) return true;
      if (bookingRe.menuNoise.test(txt)) return true;
      return false;
    };

    const isHeaderActionOnly = (el) => {
      if (!el) return false;
      const tag = (el.tagName || '').toLowerCase();
      if (!/^(a|button)$/i.test(tag)) return false;
      if (!bookingRe.action.test(getInteractiveText(el))) return false;
      return !!el.closest('header, nav, [class*="menu"], [class*="nav"], [class*="header"], [class*="offcanvas"]');
    };

    const ctaCandidates = Array.from(document.querySelectorAll(interactiveSelectors))
      .filter(isVisible)
      .filter(el => bookingRe.action.test(getInteractiveText(el)) && !bookingRe.genericActionNoise.test(getInteractiveText(el)))
      .filter(el => !isMenuLikeElement(el) && !isHeaderActionOnly(el));

    const getNearbyLabel = (el) => {
      if (!el) return "";
      const base = [
        getLabel(el),
        el.getAttribute?.('aria-label') || '',
        el.getAttribute?.('placeholder') || '',
        el.getAttribute?.('title') || '',
      ].find(Boolean);
      if (base) return normText(base).slice(0, 120);
      const prev = el.previousElementSibling;
      if (prev) {
        const t = normText(prev.textContent || '').slice(0, 120);
        if (t && t.length <= 120) return t;
      }
      const parent = el.parentElement;
      if (parent) {
        const labelish = parent.querySelector('label, .label, [class*="label"], [class*="title"], [class*="caption"], span');
        if (labelish && labelish !== el) {
          const t = normText(labelish.textContent || '').slice(0, 120);
          if (t && t.length <= 120) return t;
        }
      }
      return "";
    };

    const getSignalText = (el) => {
      if (!el) return '';
      const tag = (el.tagName || '').toLowerCase();
      const own = normText([
        getNearbyLabel(el),
        el.textContent || '',
        el.getAttribute?.('aria-label') || '',
        el.getAttribute?.('placeholder') || '',
        el.getAttribute?.('value') || '',
        el.getAttribute?.('title') || '',
        tag === 'input' ? getLabel(el) : '',
      ].filter(Boolean).join(' '));
      return own.slice(0, 140);
    };

    const isConcreteSelectorSet = (selectors = []) => {
      return (selectors || []).some(sel => {
        if (!sel || /^div\b/i.test(sel) || /^section\b/i.test(sel) || /^main\b/i.test(sel) || /^header\b/i.test(sel) || /^aside\b/i.test(sel)) return false;
        if (/elementor-element|e-con-inner|desktop-menu-area|widget-container/i.test(sel)) return false;
        return /#|\[name=|\[type=|\[placeholder\*=|\[autocomplete=|^(input|select|textarea|button|a)\b|\[role=|\[aria-label\]/i.test(sel);
      });
    };

    const classifyControl = (el, text) => {
      const t = normText(text || getSignalText(el));
      if (!t) return null;
      if (bookingRe.noise.test(t)) return null;
      if (bookingRe.menuNoise.test(t)) return null;
      if (isMenuLikeElement(el)) return null;
      if (isHeaderActionOnly(el)) return null;
      if (/^a\.menu-link$/i.test((selectorCandidates(el)[0] || ''))) return null;
      if (bookingRe.roomNoise.test(t) && !bookingRe.checkIn.test(t) && !bookingRe.checkOut.test(t) && !bookingRe.guests.test(t)) return null;

      const selectors = selectorCandidates(el);
      const concrete = isConcreteSelectorSet(selectors);
      const base = {
        text: t.slice(0, 120),
        label: t.slice(0, 120),
        selector_candidates: selectors,
        concrete,
      };

      if (bookingRe.checkIn.test(t)) return { bucket: 'check_in', item: base };
      if (bookingRe.checkOut.test(t)) return { bucket: 'check_out', item: base };
      if (bookingRe.guests.test(t)) return { bucket: 'guests', item: base };
      if (bookingRe.action.test(t) && !bookingRe.genericActionNoise.test(t)) {
        return { bucket: 'action', item: { text: t.slice(0, 80), selector_candidates: selectors, concrete } };
      }
      return null;
    };

    const dedupeSignals = (items = [], max = 6) => {
      const seenItems = new Set();
      const out = [];
      for (const item of items) {
        const key = `${item.text || ''}|${(item.selector_candidates || []).join('|')}`;
        if (!item.text || seenItems.has(key)) continue;
        seenItems.add(key);
        out.push(item);
        if (out.length >= max) break;
      }
      return out;
    };

    const getSignalNodes = (container) => {
      return Array.from(container.querySelectorAll(signalSelectors))
        .filter(isVisible)
        .filter(el => {
          const rect = el.getBoundingClientRect();
          if (rect.width < 10 || rect.height < 10) return false;
          if (rect.width > window.innerWidth * 0.95 || rect.height > 180) return false;
          const txt = getSignalText(el);
          if (!txt) return false;
          if (txt.length > 140) return false;
          if (bookingRe.noise.test(txt)) return false;
          if (isMenuLikeElement(el) || isHeaderActionOnly(el)) return false;
          return bookingRe.checkIn.test(txt) || bookingRe.checkOut.test(txt) || bookingRe.guests.test(txt) || bookingRe.action.test(txt);
        })
        .slice(0, 100);
    };

    const gatherInteractiveControls = (container) => {
      return Array.from(container.querySelectorAll(concreteInteractiveSelectors))
        .filter(isVisible)
        .filter(el => {
          const rect = el.getBoundingClientRect();
          if (!(rect.width >= 8 && rect.height >= 8 && rect.width <= window.innerWidth * 0.95 && rect.height <= 120)) return false;
          if (isMenuLikeElement(el) || isHeaderActionOnly(el)) return false;
          return true;
        })
        .slice(0, 60);
    };

    const scoreContainer = (container, ctaEl) => {
      if (!container || !isVisible(container)) return null;
      const rect = container.getBoundingClientRect();
      if (rect.width < 220 || rect.height < 35) return null;
      if (rect.top > Math.max(window.innerHeight + 220, 1200)) return null;
      const raw = normText(container.innerText || container.textContent || '');
      if (!raw || raw.length > 1600) return null;
      if (bookingRe.noise.test(raw)) return null;

      const nodes = getSignalNodes(container);
      if (ctaEl && !nodes.includes(ctaEl)) nodes.unshift(ctaEl);
      const interactiveControls = gatherInteractiveControls(container);
      if (ctaEl && !interactiveControls.includes(ctaEl)) interactiveControls.unshift(ctaEl);

      const texts = nodes.map(getSignalText).filter(Boolean);
      const interactiveTexts = interactiveControls.map(getSignalText).filter(Boolean);
      if (ctaEl) texts.unshift(getInteractiveText(ctaEl));
      texts.push(raw.slice(0, 300));
      const joined = [...texts, ...interactiveTexts].join(' | ');

      const hasCheckIn = bookingRe.checkIn.test(joined);
      const hasCheckOut = bookingRe.checkOut.test(joined);
      const hasGuests = bookingRe.guests.test(joined);
      const hasAction = bookingRe.action.test(joined);
      let score = 0;
      if (hasCheckIn) score += 2;
      if (hasCheckOut) score += 2;
      if (hasGuests) score += 1;
      if (hasAction) score += 2;
      if (rect.top < Math.max(window.innerHeight + 120, 900)) score += 1;
      if (nodes.length >= 3) score += 1;
      if (interactiveControls.length >= 2) score += 1;

      const hasBookingIframe = bookingIframes.length > 0;
      const containerSelectors = selectorCandidates(container);
      const genericWrapper = containerSelectors.some(sel => /^(div\.site|#page|main|header|div\.elementor)/i.test(sel));
      if (hasBookingIframe && genericWrapper && !String(raw || '').match(/пристигане|напускане|възрастни|guests|check-?in|check-?out/i)) return null;
      if (!hasCheckIn || !(hasCheckOut || hasGuests) || !hasAction || score < 5) return null;

      const checkIn = [];
      const checkOut = [];
      const guestFields = [];
      const actionButtons = [];

      nodes.forEach((el) => {
        const hit = classifyControl(el);
        if (!hit) return;
        if (hit.bucket === 'check_in') checkIn.push(hit.item);
        if (hit.bucket === 'check_out') checkOut.push(hit.item);
        if (hit.bucket === 'guests') guestFields.push(hit.item);
        if (hit.bucket === 'action') actionButtons.push(hit.item);
      });

      interactiveControls.forEach((el) => {
        const hit = classifyControl(el);
        if (!hit) return;
        if (hit.bucket === 'check_in') checkIn.push(hit.item);
        if (hit.bucket === 'check_out') checkOut.push(hit.item);
        if (hit.bucket === 'guests') guestFields.push(hit.item);
        if (hit.bucket === 'action') actionButtons.push(hit.item);
      });

      if (ctaEl) {
        const hit = classifyControl(ctaEl, getInteractiveText(ctaEl));
        if (hit?.bucket === 'action') actionButtons.unshift(hit.item);
      }

      const dedupedCheckIn = dedupeSignals(checkIn, 4);
      const dedupedCheckOut = dedupeSignals(checkOut, 4);
      const dedupedGuests = dedupeSignals(guestFields, 6);
      const dedupedActions = dedupeSignals(actionButtons, 4);
      const dedupedDateInputs = dedupeSignals([...dedupedCheckIn, ...dedupedCheckOut], 6);

      const concreteFieldCount = [...dedupedCheckIn, ...dedupedCheckOut, ...dedupedGuests].filter(x => x.concrete).length;
      const concreteActionCount = dedupedActions.filter(x => x.concrete).length;
      const concreteControlCount = concreteFieldCount + concreteActionCount;

      const detectionGrade =
        (dedupedCheckIn.length > 0 || dedupedCheckOut.length > 0) &&
        dedupedActions.length > 0 &&
        (dedupedCheckIn.length + dedupedCheckOut.length + dedupedGuests.length) >= 2;

      const executionGrade =
        concreteFieldCount >= 1 &&
        concreteActionCount >= 1 &&
        concreteControlCount >= 2 &&
        (dedupedCheckIn.some(c => c.concrete) || dedupedCheckOut.some(c => c.concrete));

      if (!detectionGrade) return null;

      const compact = Array.from(new Set([
        ...dedupedDateInputs.map(x => x.text),
        ...dedupedGuests.map(x => x.text),
        ...dedupedActions.map(x => x.text),
      ])).join(' | ').slice(0, 260);

      return {
        score: score + (executionGrade ? 2 : 0),
        schema: {
          ui_type: "interactive_booking_bar",
          extraction_mode: "hybrid",
          detection_grade: true,
          execution_grade: executionGrade,
          text_hint: compact || raw.slice(0, 260),
          date_inputs: dedupedDateInputs.slice(0, 6),
          guest_fields: dedupedGuests.slice(0, 6),
          action_buttons: dedupedActions.slice(0, 4),
          detected_fields: { check_in: hasCheckIn, check_out: hasCheckOut, guests: hasGuests },
          selector_candidates: selectorCandidates(container),
        },
      };
    };

    const seenContainers = new Set();
    const scored = [];
    const containerSelectors = 'section, form, div, aside, header, main';
    ctaCandidates.forEach((cta) => {
      let node = cta;
      for (let i = 0; i < 8 && node; i++) {
        const candidate = node.closest?.(containerSelectors) || node.parentElement;
        if (!candidate) break;
        if (!seenContainers.has(candidate)) {
          seenContainers.add(candidate);
          const result = scoreContainer(candidate, cta);
          if (result) scored.push(result);
        }
        node = candidate.parentElement;
      }
    });

    if (scored.length === 0) {
      const topCandidates = Array.from(document.querySelectorAll(containerSelectors))
        .filter(isVisible)
        .filter(el => {
          const rect = el.getBoundingClientRect();
          return rect.top < Math.max(window.innerHeight + 200, 1100) && rect.width > 220 && rect.height > 35;
        })
        .slice(0, 80);
      topCandidates.forEach((candidate) => {
        if (seenContainers.has(candidate)) return;
        const result = scoreContainer(candidate, null);
        if (result) scored.push(result);
      });
    }

    scored.sort((a, b) => b.score - a.score);
    scored.slice(0, 6).forEach(item => pushAvailability(item.schema));

    // WIZARD / MULTI-STEP DETECTION
    const wizards = [];
    try {
      const stepSelectors = [
        '[class*="step"]', '[class*="wizard"]', '[data-step]',
        '[class*="multi-step"]', '[class*="multistep"]',
        '[class*="form-step"]', '[class*="stepper"]',
      ];

      const stepIndicatorSelectors = [
        '[class*="step-indicator"]', '[class*="progress-step"]',
        '[class*="step-nav"]', '[class*="stepper"]',
        '[class*="step-number"]', '[class*="form-progress"]',
      ];

      const wizardRoots = new Set();

      for (const sel of stepSelectors) {
        try {
          document.querySelectorAll(sel).forEach(el => {
            if (el.closest('form')) return;
            if (!isVisible(el)) return;
            const root = el.closest(
              '[class*="wizard"],[class*="step-container"],[class*="form-wrapper"],' +
              '[class*="multistep"],[class*="multi-step"],[class*="stepper"]'
            ) || el;
            if (root.closest('form')) return;
            const inputs = root.querySelectorAll('input:not([type="hidden"]):not([type="submit"]), select, textarea');
            const visibleInputs = Array.from(inputs).filter(isVisible);
            if (visibleInputs.length >= 1) wizardRoots.add(root);
          });
        } catch {}
      }

      for (const sel of stepIndicatorSelectors) {
        try {
          document.querySelectorAll(sel).forEach(indicator => {
            if (indicator.closest('form')) return;
            if (!isVisible(indicator)) return;
            const parent = indicator.parentElement;
            if (!parent || parent.closest('form')) return;
            let container = parent;
            for (let i = 0; i < 5; i++) {
              if (!container) break;
              const inputs = container.querySelectorAll('input:not([type="hidden"]):not([type="submit"]), select, textarea');
              const visibleInputs = Array.from(inputs).filter(isVisible);
              if (visibleInputs.length >= 1) { wizardRoots.add(container); break; }
              container = container.parentElement;
            }
          });
        } catch {}
      }

      const navButtonRe = /напред|назад|next|back|previous|стъпка|step/i;
      document.querySelectorAll('button, [role="button"], a[class*="btn"]').forEach(btn => {
        try {
          if (btn.closest('form')) return;
          if (!isVisible(btn)) return;
          const text = (btn.textContent || "").trim();
          if (!navButtonRe.test(text)) return;
          let container = btn.parentElement;
          for (let i = 0; i < 6; i++) {
            if (!container) break;
            if (container.closest('form')) break;
            const inputs = container.querySelectorAll('input:not([type="hidden"]):not([type="submit"]), select, textarea');
            const visibleInputs = Array.from(inputs).filter(isVisible);
            if (visibleInputs.length >= 2) { wizardRoots.add(container); break; }
            container = container.parentElement;
          }
        } catch {}
      });

      for (const root of wizardRoots) {
        const fields = [];
        root.querySelectorAll('input:not([type="hidden"]):not([type="submit"]), select, textarea').forEach(input => {
          if (!isVisible(input)) return;
          const tag = input.tagName.toLowerCase();
          const type = (input.getAttribute("type") || tag).toLowerCase();
          const name = input.getAttribute("name") || input.id || "";
          const placeholder = input.getAttribute("placeholder") || "";
          const label = getLabel(input);
          const required = input.hasAttribute("required") || input.getAttribute("aria-required") === "true" ||
            (label && /(\*|задължително|required)/i.test(label));
          if (type === "radio") return;
          fields.push({
            tag, type, name, label, placeholder, required,
            autocomplete: input.getAttribute("autocomplete") || "",
            aria_label: input.getAttribute("aria-label") || "",
            aria_describedby: input.getAttribute("aria-describedby") || "",
            selector_candidates: selectorCandidates(input),
          });
        });

        const choices = extractChoices(root);
        if (fields.length === 0 && choices.length === 0) continue;

        const stepIndicatorsArr = [];
        root.querySelectorAll('[class*="step"], [data-step], [class*="progress"]').forEach(el => {
          const t = (el.textContent || "").trim().slice(0, 80);
          if (t && t.length > 1 && t.length < 80) stepIndicatorsArr.push(t);
        });

        const submitCandidates = [];
        root.querySelectorAll('button, [role="button"], input[type="submit"]').forEach(btn => {
          if (!isVisible(btn)) return;
          const text = (btn.textContent?.trim() || btn.getAttribute("value") || "").slice(0, 80);
          if (!text) return;
          submitCandidates.push({ text, selector_candidates: selectorCandidates(btn) });
        });

        const bestSubmit =
          submitCandidates.find(b => /изпрати|send|submit|запази|напред|next|резерв|book/i.test(b.text)) ||
          submitCandidates[0] || null;

        const rootText = (root.textContent || "").slice(0, 500);
        const stepsMatch = rootText.match(/(?:стъпка|step)\s*\d+\s*(?:от|of|\/)\s*(\d+)/i);
        const totalSteps = stepsMatch ? parseInt(stepsMatch[1], 10) : null;

        let dom_snapshot = "";
        try { dom_snapshot = (root.outerHTML || "").slice(0, 4000); } catch {}

        wizards.push({
          kind: "wizard",
          schema: {
            fields, choices, submit: bestSubmit,
            is_multi_step: true, total_steps: totalSteps,
            step_indicators: [...new Set(stepIndicatorsArr)].slice(0, 10),
            action: "", method: "post",
          },
          dom_snapshot,
        });
      }
    } catch (e) {}

    return {
      url: window.location.href,
      forms,
      wizards,
      iframes,
      availability,
    };
  });
}

function buildCombinedCapabilities(perPageCaps, baseOrigin) {
  const combined = [];
  const seen = new Set();

  for (const p of perPageCaps) {
    const url = p.url || "";
    const domain = normalizeDomain(url || baseOrigin || "");
    const pushCap = (kind, schema, dom_snapshot) => {
      const normalized = { kind, schema };
      const fp = sha256Hex(stableStringify(normalized));
      const key = `${kind}|${fp}`;
      if (seen.has(key)) return;
      seen.add(key);
      combined.push({ url, domain, kind, fingerprint: fp, schema, dom_snapshot: dom_snapshot || null });
    };
    for (const f of p.forms || []) pushCap("form", f.schema, f.dom_snapshot);
    for (const w of p.wizards || []) pushCap("wizard", w.schema, w.dom_snapshot);
    for (const w of p.iframes || []) {
      const src = w.schema?.src || "";
      pushCap("booking_widget", { ...w.schema, vendor: guessVendorFromText(src) });
    }
    for (const a of p.availability || []) pushCap("availability", a.schema);
  }

  const forms = combined.filter(c => c.kind === "form").slice(0, 8);
  const wizards = combined.filter(c => c.kind === "wizard").slice(0, 5);
  const widgets = combined.filter(c => c.kind === "booking_widget").slice(0, 5);
  const avail = combined.filter(c => c.kind === "availability").slice(0, 10);
  const other = combined.filter(c => !["form","wizard","booking_widget","availability"].includes(c.kind)).slice(0, 10);

  return [...forms, ...wizards, ...widgets, ...avail, ...other];
}


// ═══════════════════════════════════════════════════════════════════════════
// ██████████████████████████████████████████████████████████████████████████
// ███  NEW: UI-AWARE INTERACTION LAYER                                  ███
// ███  Clicks tabs, accordions, dialogs, "Details", dropdowns           ███
// ███  and captures ALL revealed content                                ███
// ██████████████████████████████████████████████████████████████████████████
// ═══════════════════════════════════════════════════════════════════════════

/**
 * revealHiddenContent(page)
 *
 * This is the CORE NEW FUNCTION.
 * It clicks every interactive UI element that hides content:
 *   - tabs (role="tab", [data-toggle="tab"], .tab, .nav-link)
 *   - accordions (.accordion-header, [data-toggle="collapse"], details > summary)
 *   - "Show more" / "Details" / "Виж детайли" / "Повече" buttons
 *   - dialog triggers ([data-toggle="modal"], [data-bs-toggle="modal"])
 *   - dropdown triggers
 *
 * After each click, it waits for new content to appear and captures it.
 *
 * Returns: { revealedTexts: string[], dialogTexts: string[], clickCount: number }
 */
async function revealHiddenContent(page) {
  const startTime = Date.now();
  const results = { revealedTexts: [], dialogTexts: [], clickCount: 0 };

  try {
    // PHASE 1: Open all <details> elements (no click needed, just set open attribute)
    await page.evaluate(() => {
      document.querySelectorAll("details").forEach(d => {
        d.open = true;
        d.setAttribute("open", "");
      });
    });

    // PHASE 2: Discover all clickable UI triggers
    const triggers = await page.evaluate((maxClicks) => {
      const norm = (s) => (s || "").replace(/\s+/g, " ").trim();
      const isVisible = (el) => {
        try {
          const rect = el.getBoundingClientRect();
          const style = window.getComputedStyle(el);
          return rect.width > 0 && rect.height > 0 &&
                 style.display !== "none" && style.visibility !== "hidden";
        } catch { return false; }
      };

      // Skip navigation/footer/cookie elements
      const isJunk = (el) => {
        if (!el) return false;
        const inNav = el.closest('nav, header, footer, [class*="cookie"], [class*="gdpr"], [class*="consent"]');
        return !!inNav;
      };

      const found = [];
      const seenTexts = new Set();

      // --- TABS ---
      document.querySelectorAll(
        '[role="tab"], [data-toggle="tab"], [data-bs-toggle="tab"], ' +
        '.nav-tabs .nav-link, .tab-link, .tabs__tab, [class*="tab-btn"], ' +
        '[class*="tab-button"], [class*="tab-trigger"]'
      ).forEach(el => {
        if (!isVisible(el) || isJunk(el)) return;
        const text = norm(el.textContent).slice(0, 80);
        if (!text || text.length < 2 || seenTexts.has(text)) return;
        seenTexts.add(text);
        found.push({ type: "tab", text, index: found.length });
      });

      // --- ACCORDIONS ---
      document.querySelectorAll(
        '[data-toggle="collapse"], [data-bs-toggle="collapse"], ' +
        '.accordion-button, .accordion-header, .accordion-trigger, ' +
        '[class*="accordion"] > button, [class*="accordion"] > a, ' +
        '[class*="collapse-trigger"], [class*="expand"], ' +
        'details > summary'
      ).forEach(el => {
        if (!isVisible(el) || isJunk(el)) return;
        const text = norm(el.textContent).slice(0, 80);
        if (!text || text.length < 2 || seenTexts.has(text)) return;
        seenTexts.add(text);
        found.push({ type: "accordion", text, index: found.length });
      });

      // --- "SHOW MORE" / "DETAILS" / "ВИЖ ДЕТАЙЛИ" BUTTONS ---
      const showMoreRe = /виж (повече|детайли|детайлите|всичк)|вижте|покажи повече|повече( информация| детайли)?|подробности|more (details|info)|show more|see more|read more|details|expand|learn more|view details|show all|вижте повече|прочети повече|разгледай/i;

      document.querySelectorAll('button, a, [role="button"], span[onclick], div[onclick]').forEach(el => {
        if (!isVisible(el) || isJunk(el)) return;
        const text = norm(el.textContent).slice(0, 80);
        if (!text || text.length < 2 || text.length > 60) return;
        if (!showMoreRe.test(text)) return;
        if (seenTexts.has(text)) return;

        // Skip if it's a navigation link to another page
        const href = el.getAttribute("href") || "";
        if (href && !href.startsWith("#") && !href.startsWith("javascript:") && href !== "") {
          // Check if it's an internal anchor or JS action
          try {
            const url = new URL(href, window.location.origin);
            if (url.pathname !== window.location.pathname) return; // links to different page
          } catch {}
        }

        seenTexts.add(text);
        found.push({ type: "show_more", text, index: found.length });
      });

      // --- MODAL/DIALOG TRIGGERS ---
      // Classic Bootstrap + Radix UI + Headless UI + generic React dialogs
      document.querySelectorAll(
        '[data-toggle="modal"], [data-bs-toggle="modal"], ' +
        '[data-fancybox], [data-lightbox], [data-popup], ' +
        '[class*="modal-trigger"], [class*="dialog-trigger"], ' +
        '[class*="popup-trigger"], [data-target*="modal"], ' +
        '[data-bs-target*="modal"], ' +
        // Radix UI / Headless UI / shadcn triggers
        '[aria-haspopup="dialog"], [data-radix-collection-item], ' +
        '[data-state][role="button"], button[data-state]'
      ).forEach(el => {
        if (!isVisible(el) || isJunk(el)) return;
        const text = norm(el.textContent).slice(0, 80);
        if (!text || text.length < 2 || seenTexts.has(text)) return;
        seenTexts.add(text);
        found.push({ type: "modal", text, index: found.length });
      });

      // --- CONTENT BUTTONS (catch-all for React SPA sites) ---
      // Any button inside main content area that looks like it reveals info
      // (not in nav/header/footer, short text, likely opens dialog/details)
      const contentButtonRe = /модел|model|план|plan|пакет|package|стая|room|апартамент|apartment|къща|house|вила|villa|студио|studio|офис|office|етаж|floor|тип|type|вариант|variant|опция|option|избери|choose|select|конфигур|config|customize|поръч|order|оферт|offer|промоц|promo|включено|included|standard|basic|premium|pro|deluxe|лукс|luxury|comfort|komfort|екстра|extra/i;

      document.querySelectorAll('button, [role="button"]').forEach(el => {
        if (!isVisible(el) || isJunk(el)) return;
        const text = norm(el.textContent).slice(0, 80);
        if (!text || text.length < 3 || text.length > 60) return;
        if (seenTexts.has(text)) return;
        // Must match content-related keywords
        if (!contentButtonRe.test(text)) return;
        // Must be in main content, not nav/header
        if (el.closest('nav, header, footer, [class*="menu"], [class*="nav"]')) return;
        seenTexts.add(text);
        found.push({ type: "modal", text, index: found.length });
      });

      // --- DROPDOWN TRIGGERS (non-nav) ---
      document.querySelectorAll(
        '[data-toggle="dropdown"], [data-bs-toggle="dropdown"], ' +
        '[aria-haspopup="listbox"], [aria-haspopup="menu"]'
      ).forEach(el => {
        if (!isVisible(el) || isJunk(el)) return;
        // Only include dropdowns that look like content dropdowns, not nav menus
        if (el.closest('nav, header, [class*="menu"]')) return;
        const text = norm(el.textContent).slice(0, 80);
        if (!text || text.length < 2 || seenTexts.has(text)) return;
        seenTexts.add(text);
        found.push({ type: "dropdown", text, index: found.length });
      });

      return found.slice(0, maxClicks);
    }, MAX_UI_CLICKS);

    if (triggers.length === 0) {
      return results;
    }

    console.log(`[UI-REVEAL] Found ${triggers.length} interactive triggers`);

    // PHASE 3: Click each trigger and capture revealed content
    for (const trigger of triggers) {
      // Time budget check
      if (Date.now() - startTime > UI_INTERACTION_BUDGET_MS) {
        console.log(`[UI-REVEAL] Time budget exceeded after ${results.clickCount} clicks`);
        break;
      }

      try {
        // Get text BEFORE click
        const beforeText = await page.evaluate(() => {
          return (document.body.innerText || "").length;
        });

        // Build selector to find the element and click it
        const clicked = await page.evaluate((triggerInfo) => {
          const norm = (s) => (s || "").replace(/\s+/g, " ").trim();
          const isVisible = (el) => {
            try {
              const rect = el.getBoundingClientRect();
              const style = window.getComputedStyle(el);
              return rect.width > 0 && rect.height > 0 &&
                     style.display !== "none" && style.visibility !== "hidden";
            } catch { return false; }
          };

          // Find the element by matching type and text
          let selectors;
          switch (triggerInfo.type) {
            case "tab":
              selectors = '[role="tab"], [data-toggle="tab"], [data-bs-toggle="tab"], .nav-tabs .nav-link, .tab-link, .tabs__tab, [class*="tab-btn"], [class*="tab-button"], [class*="tab-trigger"]';
              break;
            case "accordion":
              selectors = '[data-toggle="collapse"], [data-bs-toggle="collapse"], .accordion-button, .accordion-header, .accordion-trigger, [class*="accordion"] > button, [class*="accordion"] > a, [class*="collapse-trigger"], [class*="expand"], details > summary';
              break;
            case "show_more":
              selectors = 'button, a, [role="button"], span[onclick], div[onclick]';
              break;
            case "modal":
              selectors = '[data-toggle="modal"], [data-bs-toggle="modal"], [data-fancybox], [data-lightbox], [data-popup], [class*="modal-trigger"], [class*="dialog-trigger"], [class*="popup-trigger"], [data-target*="modal"], [data-bs-target*="modal"], [aria-haspopup="dialog"], [data-radix-collection-item], button[data-state], [data-state][role="button"], button, [role="button"]';
              break;
            case "dropdown":
              selectors = '[data-toggle="dropdown"], [data-bs-toggle="dropdown"], [aria-haspopup="listbox"], [aria-haspopup="menu"]';
              break;
            default:
              return false;
          }

          const candidates = Array.from(document.querySelectorAll(selectors)).filter(isVisible);
          const match = candidates.find(el => {
            const text = norm(el.textContent).slice(0, 80);
            return text === triggerInfo.text;
          });

          if (match) {
            try {
              match.scrollIntoView({ block: "center", behavior: "instant" });
              match.click();
              return true;
            } catch { return false; }
          }
          return false;
        }, trigger);

        if (!clicked) continue;

        results.clickCount++;

        // Wait for content to appear
        await page.waitForTimeout(UI_CLICK_WAIT_MS);

        // For modals, also try to wait for dialog/modal element to appear
        if (trigger.type === "modal") {
          try {
            await page.waitForSelector(
              '[role="dialog"]:not([style*="display: none"]), ' +
              '[role="dialog"][data-state="open"], ' +
              '[data-radix-dialog-content], ' +
              '.modal.show, .modal.in, .modal[style*="display: block"], ' +
              '[class*="popup"][style*="display: block"]',
              { timeout: 1500 }
            );
          } catch {}

          // Extra wait for Radix animations
          await page.waitForTimeout(200);

          // Extract dialog content
          const dialogContent = await page.evaluate(() => {
            const norm = (s) => (s || "").replace(/\s+/g, " ").trim();
            const dialogs = document.querySelectorAll(
              // Standard
              '[role="dialog"], .modal.show, .modal.in, .modal[style*="display: block"], ' +
              '[class*="popup"][style*="display: block"], [class*="modal-content"], ' +
              '[class*="dialog-content"], .fancybox-content, .lightbox-content, ' +
              // Radix UI / Headless UI / shadcn
              '[data-radix-dialog-content], [data-state="open"][role="dialog"], ' +
              '[data-radix-popper-content-wrapper]'
            );

            const texts = [];
            dialogs.forEach(d => {
              const text = norm(d.innerText || d.textContent || "");
              if (text && text.length > 10) {
                texts.push(text.slice(0, 5000));
              }
            });
            return texts;
          });

          if (dialogContent.length > 0) {
            results.dialogTexts.push(...dialogContent);
            console.log(`[UI-REVEAL] Modal "${trigger.text.slice(0,30)}": captured ${dialogContent.length} dialog(s)`);
          }

          // Close the modal — support Radix overlay click + close buttons + Escape
          await page.evaluate(() => {
            // Try close buttons (classic + Radix)
            const closeBtn = document.querySelector(
              '.modal.show .close, .modal.show [data-dismiss="modal"], ' +
              '.modal.show [data-bs-dismiss="modal"], .modal.show .btn-close, ' +
              '[role="dialog"] button[aria-label="Close"], ' +
              '[role="dialog"] button[aria-label="Затвори"], ' +
              '.modal.in .close, [class*="popup"] .close, ' +
              '[class*="modal-close"], [class*="dialog-close"], ' +
              // Radix close button
              '[data-radix-dialog-close], ' +
              '[role="dialog"] button:first-of-type'
            );
            if (closeBtn) {
              closeBtn.click();
              return;
            }

            // Try clicking Radix overlay to dismiss
            const overlay = document.querySelector(
              '[data-radix-dialog-overlay], [data-state="open"][data-aria-hidden="true"]'
            );
            if (overlay) {
              overlay.click();
              return;
            }

            // Fallback: Escape key
            document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
            document.body.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
          });

          await page.waitForTimeout(300);
        }

        // For tabs/accordions/show-more, capture the newly revealed text
        if (trigger.type !== "modal") {
          const afterText = await page.evaluate(() => {
            return (document.body.innerText || "").length;
          });

          if (afterText > beforeText) {
            // New content appeared — we'll capture it in the final extractStructured call
            console.log(`[UI-REVEAL] ${trigger.type} "${trigger.text.slice(0,30)}": +${afterText - beforeText} chars`);
          }
        }

      } catch (err) {
        // Single trigger failure — continue with others
      }
    }

    // PHASE 4: After all clicks, capture any newly visible content
    // (tabs/accordions that are now open will be read by extractStructured)

  } catch (err) {
    console.error("[UI-REVEAL] Error:", err.message);
  }

  console.log(`[UI-REVEAL] Done: ${results.clickCount} clicks, ${results.dialogTexts.length} dialogs`);
  return results;
}


// ═══════════════════════════════════════════════════════════════════════════
// ██████████████████████████████████████████████████████████████████████████
// ███  NEW: STRUCTURED CONTENT EXTRACTION                               ███
// ███  Extracts services, packages, FAQ, features as structured JSON    ███
// ██████████████████████████████████████████████████████████████████████████
// ═══════════════════════════════════════════════════════════════════════════

/**
 * extractStructuredSections(page)
 *
 * Goes beyond rawContent — extracts structured JSON:
 *   services[], packages[], faq[], features[], contacts[]
 *
 * This is what NEO needs to NOT hallucinate.
 */
async function extractStructuredSections(page) {
  return await page.evaluate(() => {
    const norm = (s) => (s || "").replace(/\s+/g, " ").trim();
    const isVisible = (el) => {
      try {
        const rect = el.getBoundingClientRect();
        const style = window.getComputedStyle(el);
        return rect.width > 0 && rect.height > 0 &&
               style.display !== "none" && style.visibility !== "hidden";
      } catch { return false; }
    };

    // ═══════ SERVICES ═══════
    const services = [];
    const serviceSelectors = [
      '[class*="service"]', '[class*="uslugi"]', '[class*="usluga"]',
      '[class*="offering"]', '[id*="service"]', '[id*="uslugi"]',
    ];

    // Find service cards/items
    for (const sel of serviceSelectors) {
      try {
        document.querySelectorAll(sel).forEach(container => {
          if (!isVisible(container)) return;
          const heading = container.querySelector("h1,h2,h3,h4,h5,h6,strong,[class*='title']");
          const title = norm(heading?.innerText || heading?.textContent || "");
          if (!title || title.length < 3 || title.length > 120) return;

          const desc = norm(container.innerText || container.textContent || "");
          if (desc.length < 10) return;

          // Find price if present
          const priceMatch = desc.match(/(\d+[\s,.]?\d*)\s*(лв\.?|BGN|EUR|€|\$|лева)/i);
          const price = priceMatch ? norm(priceMatch[0]) : null;

          // Find features/bullets
          const features = [];
          container.querySelectorAll("li").forEach(li => {
            const t = norm(li.innerText || li.textContent || "");
            if (t && t.length >= 3 && t.length <= 200) features.push(t);
          });

          // Avoid duplicates
          if (services.some(s => s.title === title)) return;

          services.push({
            title,
            description: desc.slice(0, 500),
            price,
            features: features.slice(0, 20),
          });
        });
      } catch {}
    }

    // Also try to find services by common page structure patterns
    if (services.length === 0) {
      try {
        // Look for repeating card patterns with headings + descriptions
        const cards = document.querySelectorAll(
          '.card, .item, [class*="card"], [class*="item"], article, ' +
          '[class*="box"], [class*="feature-box"], [class*="service-box"]'
        );
        cards.forEach(card => {
          if (!isVisible(card)) return;
          const heading = card.querySelector("h2,h3,h4,h5");
          const title = norm(heading?.innerText || "");
          if (!title || title.length < 3 || title.length > 120) return;

          const desc = norm(card.innerText || "");
          if (desc.length < 20) return;

          const priceMatch = desc.match(/(\d+[\s,.]?\d*)\s*(лв\.?|BGN|EUR|€|\$|лева)/i);
          const price = priceMatch ? norm(priceMatch[0]) : null;

          const features = [];
          card.querySelectorAll("li").forEach(li => {
            const t = norm(li.innerText || "");
            if (t && t.length >= 3 && t.length <= 200) features.push(t);
          });

          if (services.some(s => s.title === title)) return;
          services.push({ title, description: desc.slice(0, 500), price, features: features.slice(0, 20) });
        });
      } catch {}
    }

    // ═══════ FAQ ═══════
    const faq = [];

    // Method 1: <details>/<summary> pattern
    document.querySelectorAll("details").forEach(d => {
      const summary = d.querySelector("summary");
      const question = norm(summary?.innerText || summary?.textContent || "");
      if (!question || question.length < 5) return;

      // Get answer: everything except summary
      const answerParts = [];
      Array.from(d.children).forEach(child => {
        if (child.tagName?.toLowerCase() === "summary") return;
        const t = norm(child.innerText || child.textContent || "");
        if (t) answerParts.push(t);
      });
      const answer = answerParts.join(" ").slice(0, 1000);
      if (answer) faq.push({ question, answer });
    });

    // Method 2: Accordion patterns (common in FAQ sections)
    const faqContainers = document.querySelectorAll(
      '[class*="faq"], [id*="faq"], [class*="accordion"], ' +
      '[class*="vuprosi"], [id*="vuprosi"], [class*="questions"]'
    );
    faqContainers.forEach(container => {
      // Look for question/answer pairs
      const items = container.querySelectorAll(
        '.accordion-item, [class*="faq-item"], [class*="question"], ' +
        '[class*="accordion-header"], dt'
      );
      items.forEach(item => {
        const questionEl = item.querySelector(
          '.accordion-button, [class*="question"], button, h3, h4, dt, summary, [class*="title"]'
        ) || item;
        const question = norm(questionEl.innerText || questionEl.textContent || "");
        if (!question || question.length < 5) return;

        // Try to find corresponding answer
        let answer = "";
        const nextSib = item.nextElementSibling;
        if (nextSib && /collapse|answer|content|body|panel/i.test(nextSib.className || "")) {
          answer = norm(nextSib.innerText || nextSib.textContent || "");
        }

        // Also check inside item for answer panel
        if (!answer) {
          const answerEl = item.querySelector(
            '.accordion-body, .accordion-content, [class*="answer"], ' +
            '[class*="content"], [class*="body"], [class*="panel"], dd'
          );
          if (answerEl) {
            answer = norm(answerEl.innerText || answerEl.textContent || "");
          }
        }

        if (answer && !faq.some(f => f.question === question)) {
          faq.push({ question, answer: answer.slice(0, 1000) });
        }
      });
    });

    // ═══════ FEATURES ═══════
    const features = [];
    const featureContainers = document.querySelectorAll(
      '[class*="feature"], [class*="benefit"], [class*="advantage"], ' +
      '[class*="why-us"], [class*="why_us"], [class*="предимств"], ' +
      '[class*="услуг"], [id*="feature"], [id*="benefit"]'
    );
    featureContainers.forEach(container => {
      if (!isVisible(container)) return;
      const heading = container.querySelector("h2,h3,h4,h5,[class*='title'],strong");
      const title = norm(heading?.innerText || "");
      const desc = norm(container.innerText || "");

      if (title && title.length >= 3 && title.length <= 120 && desc.length >= 10) {
        if (!features.some(f => f.title === title)) {
          features.push({ title, description: desc.slice(0, 300) });
        }
      }
    });

    // ═══════ PACKAGES (separate from pricing — looks for named bundles) ═══════
    const packages = [];
    const pkgContainers = document.querySelectorAll(
      '[class*="package"], [class*="plan"], [class*="пакет"], ' +
      '[class*="tier"], [class*="bundle"], [id*="package"], [id*="plan"]'
    );
    pkgContainers.forEach(container => {
      if (!isVisible(container)) return;
      const heading = container.querySelector("h2,h3,h4,h5,[class*='title'],strong,b");
      const title = norm(heading?.innerText || "");
      if (!title || title.length < 3 || title.length > 120) return;

      const desc = norm(container.innerText || "");
      const priceMatch = desc.match(/(\d+[\s,.]?\d*)\s*(лв\.?|BGN|EUR|€|\$|лева)/i);
      const price = priceMatch ? norm(priceMatch[0]) : null;

      const included = [];
      container.querySelectorAll("li").forEach(li => {
        const t = norm(li.innerText || "");
        if (t && t.length >= 3 && t.length <= 200) included.push(t);
      });

      if (!packages.some(p => p.title === title)) {
        packages.push({
          title,
          price,
          description: desc.slice(0, 500),
          included: included.slice(0, 30),
        });
      }
    });

    return {
      services: services.slice(0, 30),
      packages: packages.slice(0, 20),
      faq: faq.slice(0, 50),
      features: features.slice(0, 30),
    };
  });
}


// ═══════════════════════════════════════════════════════════════════════════
// EXISTING EXTRACTION FUNCTIONS (enhanced)
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

      const norm = (s = "") => s.replace(/\s+/g, " ").trim();

      const extractTableText = (table) => {
        const rows = [];
        table.querySelectorAll("tr").forEach((tr) => {
          const cells = Array.from(tr.querySelectorAll("th, td"))
            .map((cell) => norm(cell.innerText || cell.textContent || ""))
            .filter(Boolean);
          if (cells.length > 0) rows.push(cells.join(" | "));
        });
        return rows.join("\n");
      };

      const extractDetailsContent = (detailsEl) => {
        const parts = [];
        const summaryEl = detailsEl.querySelector(":scope > summary");
        const summaryText = norm(summaryEl?.innerText || summaryEl?.textContent || "");
        if (summaryText) parts.push(summaryText);

        Array.from(detailsEl.children).forEach((child) => {
          if (child.tagName?.toLowerCase() === "summary") return;
          child.querySelectorAll?.("table").forEach((table) => {
            const tableText = extractTableText(table);
            if (tableText) parts.push(tableText);
          });
          const textWithoutTables = norm(
            Array.from(child.childNodes)
              .filter((node) => !(node.nodeType === Node.ELEMENT_NODE && node.tagName?.toLowerCase() === "table"))
              .map((node) => node.textContent || "")
              .join(" ")
          );
          if (textWithoutTables) parts.push(textWithoutTables);
        });

        return parts.filter(Boolean).join("\n");
      };

      const detailsTexts = [];
      try {
        const detailsBlocks = Array.from(document.querySelectorAll("details.wp-block-details, details"));
        detailsBlocks.forEach((el) => {
          try {
            const summary = el.querySelector(":scope > summary");
            el.open = true;
            el.setAttribute("open", "");
            try { summary?.click(); } catch {}
            el.open = true;
            el.setAttribute("open", "");
            const blockText = extractDetailsContent(el);
            const uniqueText = addUniqueText(blockText, 5);
            if (uniqueText) detailsTexts.push(uniqueText);
          } catch {}
        });
      } catch {}

      const sections = [];
      let current = null;
      const processedElements = new Set();

      document.querySelectorAll("h1,h2,h3,p,li").forEach(el => {
        if (processedElements.has(el)) return;
        if (el.closest("details.wp-block-details, details")) return;
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

      const isVisible = (el) => {
        try {
          const rect = el.getBoundingClientRect();
          const style = window.getComputedStyle(el);
          return rect.width > 0 && rect.height > 0 && style.display !== "none" && style.visibility !== "hidden";
        } catch {
          return false;
        }
      };

      const topControlTexts = [];
      const seenControls = new Set();
      try {
        const controls = document.querySelectorAll('button, a, input, select, textarea, [role="button"], [role="combobox"], [aria-haspopup], summary');
        controls.forEach((el) => {
          if (!isVisible(el)) return;
          const rect = el.getBoundingClientRect();
          if (rect.top > Math.max(window.innerHeight + 250, 1100)) return;
          const parts = [
            el.textContent || "",
            el.getAttribute?.("aria-label") || "",
            el.getAttribute?.("placeholder") || "",
            el.getAttribute?.("value") || "",
            el.getAttribute?.("title") || "",
          ].join(" ").replace(/\s+/g, " ").trim();
          if (!parts || parts.length < 2 || parts.length > 80) return;
          const key = parts.toLowerCase();
          if (seenControls.has(key)) return;
          seenControls.add(key);
          topControlTexts.push(parts);
        });
      } catch {}

      // NEW: Also extract text from currently-visible accordion/tab panels
      const expandedPanelTexts = [];
      try {
        // Accordion panels that are now expanded/shown
        document.querySelectorAll(
          '.accordion-collapse.show, .collapse.show, ' +
          '[class*="accordion-body"]:not([style*="display: none"]), ' +
          '[class*="tab-pane"].active, [class*="tab-content"] > .active, ' +
          '[role="tabpanel"]:not([hidden]), ' +
          '[class*="panel"]:not([style*="display: none"]):not([hidden])'
        ).forEach(panel => {
          if (!isVisible(panel)) return;
          const text = norm(panel.innerText || panel.textContent || "");
          if (text && text.length > 10) {
            const uniqueText = addUniqueText(text, 10);
            if (uniqueText) expandedPanelTexts.push(uniqueText);
          }
        });
      } catch {}

      return {
        rawContent: [
          detailsTexts.length ? `DETAILS_CONTENT\n${detailsTexts.join("\n\n")}` : "",
          sections.map(s => `${s.heading}\n${s.text}`).join("\n\n"),
          mainContent,
          overlayTexts.join("\n"),
          pseudoTexts.join(" "),
          expandedPanelTexts.length ? `EXPANDED_PANELS\n${expandedPanelTexts.join("\n\n")}` : "",
          topControlTexts.length ? `TOP_CONTROLS\n${topControlTexts.join("\n")}` : "",
        ].filter(Boolean).join("\n\n"),
      };
    });
  } catch (e) {
    return { rawContent: "" };
  }
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

// ═══════════════════════════════════════════════════════════════════════════
// ██████████████████████████████████████████████████████████████████████████
// ███  PROCESS SINGLE PAGE - REWRITTEN WITH UI INTERACTION              ███
// ██████████████████████████████████████████████████████████████████████████
// ═══════════════════════════════════════════════════════════════════════════

async function processPage(page, url, base, stats, siteMaps, capabilitiesMaps) {
  const startTime = Date.now();

  try {
    console.log("[PAGE]", url);
    await page.goto(url, { timeout: 10000, waitUntil: "domcontentloaded" });

    // Step 1: Scroll for lazy load (unchanged)
    await page.evaluate(async ({ stepMs, maxSteps }) => {
      const scrollStep = window.innerHeight;
      const maxScroll = document.body.scrollHeight;
      const steps = Math.min(Math.ceil(maxScroll / scrollStep), maxSteps);
      for (let i = 0; i <= steps; i++) {
        window.scrollTo(0, i * scrollStep);
        await new Promise(r => setTimeout(r, stepMs));
      }
      window.scrollTo(0, maxScroll);
      document.querySelectorAll('img[loading="lazy"], img[data-src], img[data-lazy]').forEach(img => {
        img.loading = 'eager';
        if (img.dataset.src) img.src = img.dataset.src;
        if (img.dataset.lazy) img.src = img.dataset.lazy;
      });
    }, { stepMs: SCROLL_STEP_MS, maxSteps: MAX_SCROLL_STEPS });

    await page.waitForTimeout(150);
    try {
      await page.waitForLoadState('networkidle', { timeout: 1500 });
    } catch {}

    // ═══════════════════════════════════════════════
    // Step 2: ★ NEW — UI INTERACTION LAYER ★
    // Click tabs, accordions, dialogs, "show more"
    // BEFORE extracting content
    // ═══════════════════════════════════════════════
    let uiRevealResult = { revealedTexts: [], dialogTexts: [], clickCount: 0 };
    try {
      uiRevealResult = await revealHiddenContent(page);
    } catch (e) {
      console.error("[UI-REVEAL] Failed:", e.message);
    }

    // Scroll back to top so we don't miss anything
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(100);

    const title = clean(await page.title());
    const pageType = detectPageType(url, title);
    stats.byType[pageType] = (stats.byType[pageType] || 0) + 1;

    // Step 3: Extract structured content (now includes expanded panels from Step 2)
    const data = await extractStructured(page);

    // ═══════════════════════════════════════════════
    // Step 4: ★ NEW — STRUCTURED SECTIONS ★
    // Extract services, packages, FAQ, features
    // ═══════════════════════════════════════════════
    let structuredSections = { services: [], packages: [], faq: [], features: [] };
    try {
      structuredSections = await extractStructuredSections(page);
      const sectionCounts = [
        structuredSections.services.length && `${structuredSections.services.length} services`,
        structuredSections.packages.length && `${structuredSections.packages.length} packages`,
        structuredSections.faq.length && `${structuredSections.faq.length} faq`,
        structuredSections.features.length && `${structuredSections.features.length} features`,
      ].filter(Boolean);
      if (sectionCounts.length > 0) {
        console.log(`[STRUCTURED] ${sectionCounts.join(", ")}`);
      }
    } catch (e) {
      console.error("[STRUCTURED] Extract error:", e.message);
    }

    // Step 5: Pricing/package structured extraction (unchanged)
    let pricing = null;
    try {
      pricing = await extractPricingFromPage(page);
      if ((pricing?.pricing_cards?.length || 0) > 0 || (pricing?.installment_plans?.length || 0) > 0) {
        console.log(`[PRICING] Page: ${pricing.pricing_cards?.length || 0} cards, ${pricing.installment_plans?.length || 0} installment`);
      }
    } catch (e) {
      console.error("[PRICING] Extract error:", e.message);
    }

    // Step 6: SiteMap extraction (unchanged)
    try {
      const rawSiteMap = await extractSiteMapFromPage(page);
      if (rawSiteMap.buttons.length > 0 || rawSiteMap.forms.length > 0) {
        siteMaps.push(rawSiteMap);
        console.log(`[SITEMAP] Page: ${rawSiteMap.buttons.length} buttons, ${rawSiteMap.forms.length} forms`);
      }
    } catch (e) {
      console.error("[SITEMAP] Extract error:", e.message);
    }

    // Step 7: Capabilities extraction (unchanged)
    try {
      const caps = await extractCapabilitiesFromPage(page);
      if ((caps.forms?.length || 0) > 0 || (caps.wizards?.length || 0) > 0 || (caps.iframes?.length || 0) > 0 || (caps.availability?.length || 0) > 0) {
        capabilitiesMaps.push(caps);
        console.log(`[CAPS] Page: ${caps.forms?.length || 0} forms, ${caps.wizards?.length || 0} wizards, ${caps.iframes?.length || 0} iframes, ${caps.availability?.length || 0} availability`);
      }
    } catch (e) {
      console.error("[CAPS] Extract error:", e.message);
    }

    // Step 8: Build final content — now includes dialog content from UI interactions
    let htmlContent = normalizeNumbers(clean(data.rawContent));

    // Append dialog content that was captured by UI interaction
    if (uiRevealResult.dialogTexts.length > 0) {
      const dialogSection = "DIALOG_CONTENT\n" + uiRevealResult.dialogTexts.map(t => clean(t)).filter(Boolean).join("\n\n");
      htmlContent = htmlContent + "\n\n" + dialogSection;
    }

    const content = htmlContent;

    // Step 9: Contacts extraction (unchanged)
    const domContacts = await extractContactsFromPage(page);
    const textContacts = extractContactsFromText(`${htmlContent}\n\n${domContacts.textHints || ""}`);

    const mergedEmails = Array.from(new Set([...(domContacts.emails || []), ...(textContacts.emails || [])])).slice(0, 12);
    const mergedPhones = Array.from(new Set([...(domContacts.phones || []).map(normalizePhone).filter(Boolean), ...(textContacts.phones || [])])).slice(0, 12);

    const contacts = {
      emails: mergedEmails,
      phones: mergedPhones,
    };

    if (contacts.emails.length || contacts.phones.length) {
      console.log(`[CONTACTS] Page: ${contacts.phones.length} phones, ${contacts.emails.length} emails`);
    }

    const totalWords = countWordsExact(htmlContent);

    const elapsed = Date.now() - startTime;
    console.log(`[PAGE] ✓ ${totalWords}w ${elapsed}ms (${uiRevealResult.clickCount} UI clicks)`);

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
        // ★ ENHANCED structured output — now includes services, packages, faq, features
        structured: {
          pricing,
          contacts,
          services: structuredSections.services,
          packages: structuredSections.packages,
          faq: structuredSections.faq,
          features: structuredSections.features,
          // Track what UI interactions happened
          ui_interactions: uiRevealResult.clickCount > 0 ? {
            clicks: uiRevealResult.clickCount,
            dialogs_captured: uiRevealResult.dialogTexts.length,
          } : null,
        },
        wordCount: totalWords,
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
async function crawlSmart(startUrl, siteId = null, deadlineMs = null) {
  const effectiveMs = deadlineMs
    ? Math.min(deadlineMs, MAX_SECONDS * 1000)
    : MAX_SECONDS * 1000;
  const deadline = Date.now() + effectiveMs;
  console.log("\n[CRAWL START]", startUrl);
  console.log(`[CONFIG] ${PARALLEL_TABS} tabs, deadline in ${Math.round(effectiveMs / 1000)}s`);
  if (siteId) console.log(`[SITE ID] ${siteId}`);

  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--disable-software-rasterizer"],
  });

  const stats = {
    visited: 0,
    saved: 0,
    byType: {},
    errors: 0,
  };

  const pages = [];
  const queue = [];
  const siteMaps = [];
  const capabilitiesMaps = [];
  let base = "";

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
        config: { PARALLEL_TABS, MAX_SECONDS, MIN_WORDS }
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
        const rawDeadline = Number(parsed.deadline_ms) || 0;
        const deadlineMs = rawDeadline > 10000 ? rawDeadline - 5000 : null;
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
        if (deadlineMs) console.log(`[DEADLINE] ${Math.round(deadlineMs / 1000)}s (from caller)`);

        const result = await crawlSmart(parsed.url, siteId, deadlineMs);

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
    console.log(`Config: ${PARALLEL_TABS} tabs`);
    console.log(`Worker: ${WORKER_URL}`);
  });
