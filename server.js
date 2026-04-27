import http from "http";
import crypto from "crypto";
import { createRequire } from "module";

// playwright-extra не е ESM-native — зареждаме го чрез createRequire
const require = createRequire(import.meta.url);
const { chromium } = require("playwright-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
chromium.use(StealthPlugin());

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
const MAX_SECONDS = 120;             // ↓ was 180 — fits within scrape-website's 120s fetch timeout
const MIN_WORDS = 20;
const PARALLEL_TABS = 7;          // ↑ was 5
const SCROLL_STEP_MS = 30;           // ↓ was 100ms per scroll step
const MAX_SCROLL_STEPS = 12;
const HYDRATION_WAIT_MS = 4000;
const MUTATION_IDLE_MS = 900;          // NEW: cap scroll depth

const SKIP_URL_RE =
  /(wp-content\/uploads|media|gallery|video|photo|attachment|privacy|terms|cookies|gdpr)/i;

// ================= UTILS =================
const clean = (t = "") =>
  t.replace(/\r/g, "").replace(/[ \t]+/g, " ").replace(/\n{3,}/g, "\n\n").trim();

const countWordsExact = (t = "") => t.split(/\s+/).filter(Boolean).length;


async function waitForHydrationSettled(page){
try{

await page.evaluate(
(idleMs)=>new Promise(resolve=>{

let timer;

const finish=()=>{
obs.disconnect();
resolve();
};

const reset=()=>{
clearTimeout(timer);
timer=setTimeout(finish,idleMs);
};

const obs=new MutationObserver(reset);

obs.observe(document,{
subtree:true,
childList:true,
attributes:true,
characterData:true
});

reset();

}),
MUTATION_IDLE_MS
);

}catch{}
}



async function extractShadowAndPortalText(page){

return await page.evaluate(()=>{

const out=[];
const seen=new Set();

function push(v){
v=(v||'').replace(/\s+/g,' ').trim();
if(!v || v.length<3) return;
if(seen.has(v)) return;
seen.add(v);
out.push(v);
}

function walk(root){

if(!root) return;

const walker=document.createTreeWalker(
root,
NodeFilter.SHOW_ELEMENT|NodeFilter.SHOW_TEXT
);

let n;

while(n=walker.nextNode()){

if(n.nodeType===3){
push(n.textContent);
continue;
}

try{
if(n.shadowRoot){
walk(n.shadowRoot);
}

push(
n.innerText||n.textContent
);

}catch{}
}
}

walk(document);

document.querySelectorAll(
'[role="dialog"],[data-radix-portal],body > div'
).forEach(el=>{
push(el.innerText||el.textContent);
});

return out.join('\n');

});

}



async function attachNetworkMining(page){

const payloads=[];

page.on("response",async(res)=>{

try{

const ct=(res.headers()["content-type"]||"").toLowerCase();

if(
ct.includes("json") ||
ct.includes("graphql")
){

const txt=await res.text();

if(
/price|pricing|package|plan|amount|cost|лв|€|eur/i.test(txt)
){
payloads.push(
txt.slice(0,20000)
);
}

}

}catch{}

});

return ()=>payloads.join("\n");
}



async function forceRenderEverything(page){

await page.evaluate(async()=>{

for(let i=0;i<4;i++){

window.scrollTo(
0,
document.body.scrollHeight
);

document.querySelectorAll(
'button,[role=tab],summary,[aria-expanded="false"]'
).forEach(el=>{

const t=(el.innerText||'').toLowerCase();

if(
/цени|pricing|packages|plans|details/i.test(t)
){
try{el.click()}catch{}
}

});

await new Promise(
r=>setTimeout(r,180)
);

}

window.scrollTo(0,0);

});

}

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

// CRITICAL: НЕ дефинираме PHONE_CANDIDATE_RE като глобален /g обект.
// Глобален regex с /g пази lastIndex между извикванията → при второ
// извикване на extractContactsFromText started от грешна позиция и
// пропуска ВСИЧКИ номера. Използваме фабрична функция вместо това.
const DATE_DOT_RE = /\b\d{1,2}\.\d{1,2}\.\d{4}\b/;

function makePhoneRE() {
  // Хваща: +359 88 123 456, (052) 123-45-67, 0888123456, 00 359 88 ...
  // - Разрешаваме / като разделител (някои сайтове: 0888/123456)
  // - {4,} вместо {6,} → минимум 6 цифри total (покрива 6-цифрени местни)
  return /(\+?[\d][\d\s().\/\-]{4,}[\d])/g;
}

function normalizePhone(raw) {
  const s = String(raw || "")
    .replace(/\u00A0/g, " ")   // non-breaking space → space
    .replace(/\u2011/g, "-")   // non-breaking hyphen → hyphen
    .trim();
  if (!s) return "";
  if (DATE_DOT_RE.test(s)) return ""; // отхвърляме дати: 10.12.2025

  const hasPlus = s.startsWith("+");
  const digits = s.replace(/[^\d]/g, "");

  // 7–15 цифри: покрива кратки местни (7) до международни (15)
  if (digits.length < 7 || digits.length > 15) return "";

  // Отхвърляме очевидно не-телефонни числа (години, zip codes, цени)
  // Ако е чисто число без + и е точно 4 цифри → почти сигурно не е тел.
  if (!hasPlus && digits.length === 4) return "";

  return hasPlus ? `+${digits}` : digits;
}

function extractContactsFromText(text) {
  const out = { emails: [], phones: [] };
  if (!text) return out;

  // EMAIL_RE има /gi флаг — match() не пази state, безопасно е
  const emails = (text.match(EMAIL_RE) || [])
    .map(e => e.trim())
    .filter(Boolean);

  // КРИТИЧНО: Нов regex обект при всяко извикване — никакъв споделен lastIndex
  const re = makePhoneRE();
  const phonesRaw = [];
  let m;
  while ((m = re.exec(text)) !== null) {
    phonesRaw.push(m[1]);
  }
  const phones = phonesRaw.map(normalizePhone).filter(Boolean);

  out.emails = Array.from(new Set(emails)).slice(0, 20);
  out.phones = Array.from(new Set(phones)).slice(0, 20);
  return out;
}

async function extractContactsFromPage(page) {
  try {
    const dom = await page.evaluate(() => {
      const norm = (s) => (s || "").replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
      const emails = new Set();
      const phones = new Set();

      // 1. mailto: / tel: линкове
      document.querySelectorAll('a[href^="mailto:"]').forEach(a => {
        const v = (a.getAttribute("href") || "").replace(/^mailto:/i, "").split("?")[0];
        if (v) emails.add(norm(v));
      });
      document.querySelectorAll('a[href^="tel:"]').forEach(a => {
        const raw = (a.getAttribute("href") || "").replace(/^tel:/i, "").trim();
        if (raw) phones.add(raw);
        const visible = norm(a.innerText || a.textContent || "");
        if (visible && /[\d]/.test(visible)) phones.add(visible);
      });

      // 2. Контактни зони в DOM
      const candidates = [];
      const contactSelectors = [
        "footer", "header",
        "[id*='contact'],[class*='contact']",
        "[id*='kontakt'],[class*='kontakt']",
        "[id*='phone'],[class*='phone']",
        "[id*='tel'],[class*='tel']",
        "[id*='sidebar'],[class*='sidebar']",
        "[id*='info'],[class*='info']",
        ".widget",
        "[itemtype*='LocalBusiness']",
        "[itemtype*='Organization']",
      ];
      const seen = new Set();
      for (const sel of contactSelectors) {
        try {
          document.querySelectorAll(sel).forEach(el => {
            if (seen.has(el)) return;
            seen.add(el);
            const t = norm(el.innerText || el.textContent || "");
            if (t) candidates.push(t);
          });
        } catch {}
      }

      // 3. Schema.org
      document.querySelectorAll('[itemprop="telephone"]').forEach(el => {
        const t = norm(el.getAttribute("content") || el.innerText || el.textContent || "");
        if (t) phones.add(t);
      });
      document.querySelectorAll('[itemprop="email"]').forEach(el => {
        const t = norm(el.getAttribute("content") || el.innerText || el.textContent || "");
        if (t) emails.add(t);
      });

      return {
        emails: Array.from(emails).filter(Boolean).slice(0, 20),
        phones: Array.from(phones).filter(Boolean).slice(0, 20),
        textHints: candidates.filter(Boolean).join("\n"),
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
  // Product/vehicle/property detail pages — URL contains ID or product slug
  const PRODUCT_URL_RE = /prodajba-|\/proekt\/|\/id-\d|[?&]id=\d|(\/|[-_])(car|auto|vehicle|property|imot|apartament|hotel|offer|listing|detail|product|item)(\/|-\d|$)|\d{4,}-[a-z]/i;
  if (PRODUCT_URL_RE.test(url)) return "product_detail";
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
      try {
        if (el.id) out.push(`#${CSS.escape(el.id)}`);
      } catch {}
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

    // ═══════════════════════════════════════════════════════════
    // Helper: extract radio choices + button-group choices from
    // any container (used for both <form> and wizard roots)
    // ═══════════════════════════════════════════════════════════
    const extractChoices = (root) => {
      const choices = [];

      // ---- RADIO GROUPS ----
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

      // ---- BUTTON GROUPS (aria-pressed, role=radio, segmented) ----
      root.querySelectorAll('button[aria-pressed], [role="radio"], .segmented button').forEach(btn => {
        if (!isVisible(btn)) return;

        const text = (btn.textContent || "").trim();
        if (!text || text.length < 2) return;

        const parentLabel = getLabel(btn.parentElement) || "";
        const groupName = parentLabel || "button_group";

        let group = choices.find(c => c.name === groupName && c.type === "button_group");
        if (!group) {
          group = {
            name: groupName,
            label: parentLabel,
            required: false,
            type: "button_group",
            options: []
          };
          choices.push(group);
        }

        group.options.push({
          value: text,
          label: text,
          selector_candidates: selectorCandidates(btn)
        });
      });

      // ---- SIBLING BUTTON CHOICES ----
      // Detect containers that hold 2+ sibling buttons as option choices
      // (e.g. "Пол *" → [Мъж] [Жена])
      // Skip nav/submit buttons by filtering short-text, same-level buttons
      const seenBtnContainers = new Set();
      root.querySelectorAll("button").forEach(btn => {
        if (!isVisible(btn)) return;
        const parent = btn.parentElement;
        if (!parent || seenBtnContainers.has(parent)) return;

        // Skip if already captured by aria-pressed / role=radio
        if (btn.hasAttribute("aria-pressed") || btn.getAttribute("role") === "radio") return;

        // Get all sibling buttons in this container
        const siblingBtns = Array.from(parent.querySelectorAll(":scope > button, :scope > * > button"))
          .filter(b => isVisible(b));

        // Need at least 2 sibling buttons to form a choice group
        if (siblingBtns.length < 2) return;

        // Filter out nav/submit-like buttons
        const submitRe = /напред|назад|next|back|prev|submit|изпрати|запази|book|reserve|резерв|close|затвори|отказ|cancel/i;
        const optionBtns = siblingBtns.filter(b => {
          const t = (b.textContent || "").trim();
          // short text (1-30 chars), not a nav/submit button
          return t.length >= 1 && t.length <= 30 && !submitRe.test(t);
        });

        if (optionBtns.length < 2) return;

        seenBtnContainers.add(parent);

        // Find the label for this group — look for preceding label/text
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

        // Skip if already captured under same name
        if (choices.find(c => c.name === groupName)) return;

        const group = {
          name: groupName,
          label: cleanLabel,
          required,
          type: "button_group",
          options: []
        };

        optionBtns.forEach(b => {
          const text = (b.textContent || "").trim();
          group.options.push({
            value: text,
            label: text,
            selector_candidates: selectorCandidates(b)
          });
        });

        choices.push(group);
      });

      // ---- SELECT OPTIONS (capture <select> options as choices) ----
      root.querySelectorAll("select").forEach(sel => {
        if (!isVisible(sel)) return;
        const name = sel.getAttribute("name") || sel.id || "";
        const label = getLabel(sel);
        const required =
          sel.hasAttribute("required") ||
          sel.getAttribute("aria-required") === "true";

        const options = [];
        sel.querySelectorAll("option").forEach(opt => {
          const val = opt.value;
          const text = (opt.textContent || "").trim();
          // skip empty/placeholder options
          if (!val && !text) return;
          if (/^(--|изберете|избери|select|choose)/i.test(text) && !val) return;
          options.push({
            value: val,
            label: text,
            selector_candidates: [] // options don't need selectors, parent select does
          });
        });

        if (options.length > 0) {
          choices.push({
            name: name || label,
            label,
            required,
            type: "select",
            options,
            selector_candidates: selectorCandidates(sel)
          });
        }
      });

      return choices;
    };

    // ═══════════════════════════════════════════════════════════
    // FORMS EXTRACTION (original + enriched with choices)
    // ═══════════════════════════════════════════════════════════
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

          // Skip radios from fields array (they go into choices)
          if (type === "radio") return;

          fields.push({
            tag,
            type,
            name,
            label,
            placeholder,
            required,
            autocomplete,
            aria_label: ariaLabel,
            aria_describedby: ariaDesc,
            selector_candidates: selectorCandidates(input),
          });
        });

      if (fields.length === 0) return;

      // require at least 1 meaningful input (not just buttons/checkboxes)
      const meaningfulFields = fields.filter(f =>
        !['hidden','submit','button','reset','image'].includes(f.type)
      );
      if (meaningfulFields.length === 0) return;

      // ✅ Extract choices (radio groups, button groups, select options) from form
      const choices = extractChoices(form);

      const submitCandidates = [];
      form.querySelectorAll("button, input[type='submit'], [role='button']").forEach((btn) => {
        if (!isVisible(btn)) return;
        const text = (btn.textContent?.trim() || btn.getAttribute("value") || "").slice(0, 80);
        if (!text) return;
        submitCandidates.push({
          text,
          selector_candidates: selectorCandidates(btn),
        });
      });

      const bestSubmit =
        submitCandidates.find(b => /изпрати|send|submit|запази|резерв|book|reserve/i.test(b.text)) ||
        submitCandidates[0] ||
        null;

      let dom_snapshot = "";
      try {
        dom_snapshot = (form.outerHTML || "").slice(0, 4000);
      } catch {}

      forms.push({
        kind: "form",
        schema: {
          fields,
          choices,
          submit: bestSubmit,
          action: form.getAttribute("action") || "",
          method: (form.getAttribute("method") || "get").toLowerCase(),
        },
        dom_snapshot,
      });
    });

    // ═══════════════════════════════════════════════════════════
    // IFRAMES EXTRACTION (unchanged)
    // ═══════════════════════════════════════════════════════════
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
      try {
        if (fr.id) return `#${CSS.escape(fr.id)}`;
      } catch {}
      try {
        if (vendor && vendor !== "unknown") return `iframe[src*="${vendor}"]`;
      } catch {}
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
          src,
          title,
          name,
          vendor,
          visible,
          booking_like: bookingLike,
          selector_candidates: [selectorHint],
        },
      });

      if (bookingLike && visible && rect.width >= 220 && rect.height >= 40) {
        bookingIframes.push({
          vendor,
          src,
          title,
          name,
          selectorHint,
        });
      }
    });

    // ═══════════════════════════════════════════════════════════
    // AVAILABILITY EXTRACTION (unchanged)
    // ═══════════════════════════════════════════════════════════
    const availability = [];

    bookingIframes.forEach((widget) => {
      const vendorLabel = widget.vendor && widget.vendor !== "unknown"
        ? widget.vendor
        : "iframe_booking";
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
      .filter(isVisible)
      .slice(0, 10);

    if (dateInputs.length > 0) {
      availability.push({
        kind: "availability",
        schema: {
          date_inputs: dateInputs.map(inp => ({
            name: inp.getAttribute("name") || inp.id || "",
            label: getLabel(inp),
            selector_candidates: selectorCandidates(inp),
            required:
              inp.hasAttribute("required") || inp.getAttribute("aria-required") === "true",
          })),
        },
      });
    }

    const calendarLike = Array.from(document.querySelectorAll("[class*='calendar'],[class*='datepicker'],[id*='calendar'],[id*='datepicker']"))
      .filter(isVisible)
      .slice(0, 8);

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

    // ═══════════════════════════════════════════════════════════
    // INTERACTIVE BOOKING BAR DETECTION (additive, non-destructive)
    // ═══════════════════════════════════════════════════════════
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
      const genericWrapper = containerSelectors.some(sel => /^(div\.site|#page|mai|heade|div\.elemento)/i.test(sel));
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

      const dateInputs = dedupeSignals([...dedupedCheckIn, ...dedupedCheckOut], 6);

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
        ...dateInputs.map(x => x.text),
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
          date_inputs: dateInputs.slice(0, 6),
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

    // ═══════════════════════════════════════════════════════════
    // WIZARD / MULTI-STEP DETECTION (enriched with choices)
    // Catches div-based wizards not inside <form>
    // ═══════════════════════════════════════════════════════════
    const wizards = [];
    try {
      const stepSelectors = [
        '[class*="step"]',
        '[class*="wizard"]',
        '[data-step]',
        '[class*="multi-step"]',
        '[class*="multistep"]',
        '[class*="form-step"]',
        '[class*="stepper"]',
      ];

      const stepIndicatorSelectors = [
        '[class*="step-indicator"]',
        '[class*="progress-step"]',
        '[class*="step-nav"]',
        '[class*="stepper"]',
        '[class*="step-number"]',
        '[class*="form-progress"]',
      ];

      const wizardRoots = new Set();

      // Method 1: Find step containers with inputs
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

            const inputs = root.querySelectorAll(
              'input:not([type="hidden"]):not([type="submit"]), select, textarea'
            );
            const visibleInputs = Array.from(inputs).filter(isVisible);
            if (visibleInputs.length >= 1) {
              wizardRoots.add(root);
            }
          });
        } catch {}
      }

      // Method 2: Find step indicators near inputs (not in form)
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
              const inputs = container.querySelectorAll(
                'input:not([type="hidden"]):not([type="submit"]), select, textarea'
              );
              const visibleInputs = Array.from(inputs).filter(isVisible);
              if (visibleInputs.length >= 1) {
                wizardRoots.add(container);
                break;
              }
              container = container.parentElement;
            }
          });
        } catch {}
      }

      // Method 3: Navigation buttons (Напред/Назад, Next/Back) near inputs
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
            const inputs = container.querySelectorAll(
              'input:not([type="hidden"]):not([type="submit"]), select, textarea'
            );
            const visibleInputs = Array.from(inputs).filter(isVisible);
            if (visibleInputs.length >= 2) {
              wizardRoots.add(container);
              break;
            }
            container = container.parentElement;
          }
        } catch {}
      });

      // Extract fields + choices from each wizard root
      for (const root of wizardRoots) {
        const fields = [];
        root.querySelectorAll(
          'input:not([type="hidden"]):not([type="submit"]), select, textarea'
        ).forEach(input => {
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

          // Skip radios from fields (they go into choices)
          if (type === "radio") return;

          fields.push({
            tag,
            type,
            name,
            label,
            placeholder,
            required,
            autocomplete: input.getAttribute("autocomplete") || "",
            aria_label: input.getAttribute("aria-label") || "",
            aria_describedby: input.getAttribute("aria-describedby") || "",
            selector_candidates: selectorCandidates(input),
          });
        });

        // ✅ Extract choices (radio groups, button groups, select options) from wizard
        const choices = extractChoices(root);

        if (fields.length === 0 && choices.length === 0) continue;

        // Detect step indicators text
        const stepIndicators = [];
        root.querySelectorAll(
          '[class*="step"], [data-step], [class*="progress"]'
        ).forEach(el => {
          const t = (el.textContent || "").trim().slice(0, 80);
          if (t && t.length > 1 && t.length < 80) stepIndicators.push(t);
        });

        // Find submit/next buttons
        const submitCandidates = [];
        root.querySelectorAll('button, [role="button"], input[type="submit"]').forEach(btn => {
          if (!isVisible(btn)) return;
          const text = (btn.textContent?.trim() || btn.getAttribute("value") || "").slice(0, 80);
          if (!text) return;
          submitCandidates.push({
            text,
            selector_candidates: selectorCandidates(btn),
          });
        });

        const bestSubmit =
          submitCandidates.find(b => /изпрати|send|submit|запази|напред|next|резерв|book/i.test(b.text)) ||
          submitCandidates[0] ||
          null;

        // Detect total steps from text like "Стъпка 1 от 6" or "Step 1/6"
        const rootText = (root.textContent || "").slice(0, 500);
        const stepsMatch = rootText.match(/(?:стъпка|step)\s*\d+\s*(?:от|of|\/)\s*(\d+)/i);
        const totalSteps = stepsMatch ? parseInt(stepsMatch[1], 10) : null;

        let dom_snapshot = "";
        try {
          dom_snapshot = (root.outerHTML || "").slice(0, 4000);
        } catch {}

        wizards.push({
          kind: "wizard",
          schema: {
            fields,
            choices,
            submit: bestSubmit,
            is_multi_step: true,
            total_steps: totalSteps,
            step_indicators: [...new Set(stepIndicators)].slice(0, 10),
            action: "",
            method: "post",
          },
          dom_snapshot,
        });
      }
    } catch (e) {
      // wizard detection is best-effort, never crash
    }

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
      // ✅ FIX: fingerprint based ONLY on kind + schema (not url)
      // Same form on 15 pages → single fingerprint → single capability
      const normalized = { kind, schema };
      const fp = sha256Hex(stableStringify(normalized));
      const key = `${kind}|${fp}`;
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

    // ✅ NEW: Process wizard capabilities
    for (const w of p.wizards || []) pushCap("wizard", w.schema, w.dom_snapshot);

    for (const w of p.iframes || []) {
      const src = w.schema?.src || "";
      pushCap("booking_widget", { ...w.schema, vendor: guessVendorFromText(src) });
    }

    for (const a of p.availability || []) pushCap("availability", a.schema);
  }

  // ✅ FIX: Much tighter limits (was 40/30/30 → now 8/5/5/5)
  const forms = combined.filter(c => c.kind === "form").slice(0, 8);
  const wizards = combined.filter(c => c.kind === "wizard").slice(0, 5);
  const widgets = combined.filter(c => c.kind === "booking_widget").slice(0, 5);
  const avail = combined.filter(c => c.kind === "availability").slice(0, 10);
  const other = combined.filter(c => !["form","wizard","booking_widget","availability"].includes(c.kind)).slice(0, 10);

  return [...forms, ...wizards, ...widgets, ...avail, ...other];
}

// ═══════════════════════════════════════════════════════════════════════════
// UI-AWARE: EXPAND HIDDEN CONTENT (accordions, tabs, "Виж детайли" etc.)
// Връща string с текст от dialogs (Radix/React) — конкатенира се към content
// ═══════════════════════════════════════════════════════════════════════════

async function expandHiddenContent(page) {
  let dialogTexts = "";

  try {
    // ── Фаза 1: accordions, tabs, details (без dialog) ────────────────────
    await page.evaluate(async () => {
      const sleep = (ms) => new Promise(r => setTimeout(r, ms));

      const isVisible = (el) => {
        try {
          const rect = el.getBoundingClientRect();
          const style = window.getComputedStyle(el);
          return rect.width > 0 && rect.height > 0 &&
            style.display !== "none" &&
            style.visibility !== "hidden" &&
            style.opacity !== "0";
        } catch { return false; }
      };

      // 1. aria-expanded="false"
      const collapsed = Array.from(document.querySelectorAll('[aria-expanded="false"]'))
        .filter(isVisible).slice(0, 20);
      for (const el of collapsed) {
        try { el.click(); await sleep(80); } catch {}
      }

      // 2. Accordion headers
      const accordionSelectors = [
        '[class*="accordion"] [class*="header"]',
        '[class*="accordion"] [class*="title"]',
        '[class*="accordion"] > * > button',
        '[class*="accordion"] > button',
        '[class*="collapse-trigger"]',
        '[data-toggle="collapse"]',
        '[data-bs-toggle="collapse"]',
        '[class*="faq"] [class*="question"]',
        '[class*="faq"] button',
        '[class*="faq"] summary',
      ];
      const seenAccordion = new Set();
      for (const sel of accordionSelectors) {
        const els = Array.from(document.querySelectorAll(sel)).filter(isVisible).slice(0, 15);
        for (const el of els) {
          if (seenAccordion.has(el)) continue;
          seenAccordion.add(el);
          try { el.click(); await sleep(80); } catch {}
        }
      }

      // 3. Tabs
      const tabEls = Array.from(document.querySelectorAll('[role="tab"]')).filter(isVisible).slice(0, 12);
      for (const tab of tabEls) {
        try { tab.click(); await sleep(100); } catch {}
      }

      // 4. "Виж детайли" / текстови trigger бутони (НЕ-dialog)
      const textTriggerRe = /виж детайли|виж повече|повече информация|разгъни|покажи|show more|read more|expand|see more/i;
      const skipRe = /nav|menu|header|footer|cookie|gdpr/i;
      const clickable = Array.from(document.querySelectorAll(
        'button, [role="button"], a[href="#"], a[href="javascript:void(0)"], span[onclick], div[onclick]'
      )).filter(isVisible).slice(0, 30);
      for (const btn of clickable) {
        const text = (btn.textContent || btn.getAttribute('aria-label') || '').trim();
        if (!textTriggerRe.test(text)) continue;
        if (skipRe.test(btn.closest('[class]')?.className || '')) continue;
        // Пропусни ако отваря dialog — ще се handle-ва отделно
        if (btn.getAttribute('aria-haspopup') === 'dialog') continue;
        try { btn.click(); await sleep(120); } catch {}
      }

      // 5. <details> force open
      document.querySelectorAll('details:not([open])').forEach(d => {
        try { d.open = true; d.setAttribute('open', ''); } catch {}
      });

      await sleep(200);
    });

    await page.waitForTimeout(200);
  } catch {}

  // ── Фаза 2: Radix UI / React Dialogs — клик → извлечи текст → затвори ──
  try {
    // Намери всички dialog trigger бутони
    const triggerTexts = await page.evaluate(() => {
      const isVisible = (el) => {
        try {
          const rect = el.getBoundingClientRect();
          const style = window.getComputedStyle(el);
          return rect.width > 0 && rect.height > 0 &&
            style.display !== "none" && style.visibility !== "hidden";
        } catch { return false; }
      };

      const dialogTriggerRe = /пакет|basic|standard|premium|детайли|спецификация|виж|повече|цена|price|package|details|план|plan/i;
      const skipRe = /nav|menu|header|footer|cookie|gdpr/i;

      // Radix: aria-haspopup="dialog" или data-state="closed" на бутони
      const radixTriggers = Array.from(document.querySelectorAll(
        '[aria-haspopup="dialog"], button[data-state="closed"], [data-radix-collection-item]'
      )).filter(isVisible).slice(0, 15);

      // Текстови triggers за пакети
      const textTriggers = Array.from(document.querySelectorAll('button, [role="button"]'))
        .filter(isVisible)
        .filter(el => dialogTriggerRe.test(el.textContent || el.getAttribute('aria-label') || ''))
        .filter(el => !skipRe.test(el.closest('[class]')?.className || ''))
        .slice(0, 15);

      // Обедини и dedupe
      const all = new Set([...radixTriggers, ...textTriggers]);
      return Array.from(all).map((el, i) => {
        el.setAttribute('data-crawler-trigger', String(i));
        return { idx: i, text: (el.textContent || '').trim().slice(0, 60) };
      });
    });

    // За всеки trigger: клик → изчакай → вземи текст → затвори
    const collected = [];
    for (const { idx } of triggerTexts) {
      try {
        await page.evaluate((idx) => {
          const el = document.querySelector(`[data-crawler-trigger="${idx}"]`);
          if (el) el.click();
        }, idx);

        await page.waitForTimeout(450); // React re-render

        const text = await page.evaluate(() => {
          // Вземи текст от отворения dialog
          const dialog = document.querySelector(
            '[role="dialog"][data-state="open"], [role="dialog"]:not([data-state="closed"]), [role="dialog"]'
          );
          if (!dialog) return '';
          return (dialog.innerText || dialog.textContent || '').replace(/\s+/g, ' ').trim();
        });

        if (text && text.length > 20) {
          collected.push(text);
          console.log(`[DIALOG] Извлечен текст: ${text.slice(0, 80)}...`);
        }

        // Затвори dialog-а
        await page.evaluate(() => {
          // Опит 1: Escape key
          document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
          // Опит 2: Close бутон
          const closeBtn = document.querySelector(
            '[role="dialog"] button[aria-label*="close" i], [role="dialog"] button[aria-label*="затвори" i], ' +
            '[role="dialog"] [data-radix-focus-guard] ~ * button:first-child'
          );
          if (closeBtn) closeBtn.click();
          // Опит 3: data-state бутон
          const stateBtn = document.querySelector('[role="dialog"] button[data-state]');
          if (stateBtn) stateBtn.click();
        });

        await page.waitForTimeout(200);
      } catch {}
    }

    dialogTexts = collected.join('\n\n---DIALOG---\n\n');
  } catch (e) {
    console.error('[DIALOG] Error:', e.message);
  }

  await page.waitForTimeout(150);
  return dialogTexts;
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

      return {
        rawContent: [
          detailsTexts.length ? `DETAILS_CONTENT\n${detailsTexts.join("\n\n")}` : "",
          sections.map(s => `${s.heading}\n${s.text}`).join("\n\n"),
          mainContent,
          overlayTexts.join("\n"),
          pseudoTexts.join(" "),
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

// ================= BUTTON-TRIGGERED LINK DISCOVERY =================
// Универсален детектор: открива URLs зад бутони/карти на listing страници.
// Работи за авто сайтове, имоти, хотели, е-commerce — навсякъде където
// детайлите се зареждат след клик, а не директно от <a href>.
//
// Стратегия (от бързо към бавно):
//  Фаза 1: DOM mining — data-href, data-url, onclick, card <a> (0ms overhead)
//  Фаза 2: Mouseover на карти за lazy-set href (без navigation)
//  Фаза 3: Само ако фазите горе дадат <5 URL — реален клик макс 8 бутона
async function discoverLinksViaButtons(page, base) {
  const discovered = new Set();

  try {
    // ── Фаза 0: Стандартни <a href> ──────────────────────────────────────────
    const directLinks = await page.evaluate((base) => {
      const urls = new Set();
      const origin = new URL(base).origin;
      document.querySelectorAll("a[href]").forEach(a => {
        try {
          const u = new URL(a.href, base);
          if (u.origin === origin && u.pathname !== "/" && u.pathname.length > 1)
            urls.add(u.href.split("#")[0]);
        } catch {}
      });
      return Array.from(urls);
    }, base);
    directLinks.forEach(u => discovered.add(u));

    // ── Фаза 1: DOM mining — без кликване (бързо, 0ms overhead) ─────────────
    // Търси: data-href, data-url, onclick="location.href=...", <a> в карти
    const minedUrls = await page.evaluate((base) => {
      const urls = new Set();
      const origin = new URL(base).origin;

      const tryAdd = (raw) => {
        if (!raw || raw.startsWith("javascript:") || raw.startsWith("mailto:")) return;
        try {
          const u = new URL(raw, base);
          if (u.origin === origin && u.pathname.length > 2 && !u.pathname.endsWith("/"))
            urls.add(u.href.split("#")[0].split("?")[0]);
        } catch {}
      };

      // data-* атрибути с URL-и
      document.querySelectorAll("[data-href],[data-url],[data-link],[data-path],[data-navigate],[data-route]")
        .forEach(el => {
          ["data-href","data-url","data-link","data-path","data-navigate","data-route"].forEach(attr => {
            tryAdd(el.getAttribute(attr));
          });
        });

      // onclick с location.href
      document.querySelectorAll("[onclick]").forEach(el => {
        const oc = el.getAttribute("onclick") || "";
        const m = oc.match(/(?:location\.href|window\.location(?:\.href)?)\s*=\s*['"]([^'"]+)['"]/);
        if (m) tryAdd(m[1]);
      });

      // <a> с продуктов/детайл path pattern
      // Catches: /prodajba-na-.../mercedes-benz-..., /proekt/id-123, /car/ford etc.
      const productRe = /\/(prodaj|proekt|id-|car|auto|vehicle|propert|hotel|offer|listing|detail|item|product)/i;
      // Also: any URL with 2+ path segments where last segment has 5+ chars with hyphens
      const deepSlugRe = /\/[^/]+\/[a-z0-9][a-z0-9-]{5,}/i;
      document.querySelectorAll("a[href]").forEach(a => {
        try {
          const u = new URL(a.href, base);
          if (u.origin !== origin) return;
          const path = u.pathname;
          if (productRe.test(path) || deepSlugRe.test(path))
            urls.add(u.href.split("#")[0].split("?")[0]);
        } catch {}
      });

      // <a> вътре в card/item контейнери (React/Vue router links)
      const cardRoots = document.querySelectorAll(
        '[class*="card"],[class*="item-wrap"],[class*="product-item"],[class*="vehicle"],[class*="listing-item"],article'
      );
      cardRoots.forEach(card => {
        card.querySelectorAll("a[href]").forEach(a => {
          try {
            const u = new URL(a.href, base);
            if (u.origin === origin && u.pathname.length > 2)
              urls.add(u.href.split("#")[0].split("?")[0]);
          } catch {}
        });
      });

      return Array.from(urls);
    }, base);

    minedUrls.forEach(u => discovered.add(u));
    if (minedUrls.length > 0) {
      console.log(`[DISCOVER] DOM-mined ${minedUrls.length} detail URLs (0 clicks needed)`);
    }

    // Ако фаза 1 дава достатъчно резултати — спираме тук
    if (minedUrls.length >= 5) return Array.from(discovered);

    // ── Провери дали е listing страница изобщо ────────────────────────────────
    const isListing = await page.evaluate(() => {
      const sels = [
        '[class*="card"]','[class*="item"]','[class*="product"]',
        '[class*="listing"]','[class*="result"]','[class*="vehicle"]',
        '[class*="propert"]','[class*="hotel"]','[class*="offer"]','article',
        '[class*="avto"]','[class*="kola"]','[class*="auto"]','[class*="catalog"]',
        '[class*="grid"] > *','[class*="row"] > [class*="col"]',
      ];
      for (const sel of sels) {
        if (document.querySelectorAll(sel).length >= 3) return true;
      }
      // Fallback: if page has many same-domain links with long slugs → likely listing
      const origin = window.location.origin;
      const deepLinks = Array.from(document.querySelectorAll('a[href]')).filter(a => {
        try {
          const u = new URL(a.href);
          return u.origin === origin && u.pathname.split('/').length >= 3 && u.pathname.length > 20;
        } catch { return false; }
      });
      return deepLinks.length >= 5;
    });

    if (!isListing) return Array.from(discovered);

    // ── Фаза 2: Mouseover — lazy-set href (без navigation) ───────────────────
    const hoverLinks = await page.evaluate((base) => {
      const origin = new URL(base).origin;
      const urls = new Set();
      const cards = document.querySelectorAll(
        '[class*="card"],[class*="item"],article,[class*="vehicle"],[class*="product"]'
      );
      cards.forEach(card => {
        card.dispatchEvent(new MouseEvent("mouseover", { bubbles: true }));
        card.dispatchEvent(new MouseEvent("mouseenter", { bubbles: true }));
        card.querySelectorAll("a[href]").forEach(a => {
          try {
            const u = new URL(a.href, base);
            if (u.origin === origin && u.pathname.length > 2)
              urls.add(u.href.split("#")[0].split("?")[0]);
          } catch {}
        });
      });
      return Array.from(urls);
    }, base);
    hoverLinks.forEach(u => discovered.add(u));

    if (discovered.size >= 5) return Array.from(discovered);

    // ── Фаза 3: Реален клик — само ако горните не дадоха достатъчно ──────────
    // Лимит: макс 8 клика на бутони с текст за детайли
    const clickTargets = await page.evaluate(() => {
      const isVis = (el) => {
        try {
          const r = el.getBoundingClientRect();
          const s = window.getComputedStyle(el);
          return r.width > 5 && r.height > 5 &&
            s.display !== "none" && s.visibility !== "hidden" && s.opacity !== "0";
        } catch { return false; }
      };
      const detailRe = /детайли|виж|повече|details|view|more|открий|разгледай|покажи/i;
      const skipRe = /nav|menu|header|footer|cookie|gdpr|cart|wishlist|compare/i;
      const targets = [];

      document.querySelectorAll(
        'button,[role="button"],a[href="#"],a[href="javascript:void(0)"],a:not([href])'
      ).forEach(el => {
        if (!isVis(el)) return;
        const text = (el.textContent || el.getAttribute("aria-label") || "").trim();
        if (!detailRe.test(text)) return;
        const c = el.closest("[class]");
        if (c && skipRe.test(c.className || "")) return;
        const idx = targets.length;
        el.setAttribute("data-discover-btn", String(idx));
        targets.push({ idx, text: text.slice(0, 60) });
      });

      return targets.slice(0, 8);
    });

    if (clickTargets.length === 0) return Array.from(discovered);
    console.log(`[DISCOVER] Clicking ${clickTargets.length} detail buttons`);

    for (const target of clickTargets) {
      try {
        const beforeUrl = page.url();

        await page.evaluate((idx) => {
          const el = document.querySelector(`[data-discover-btn="${idx}"]`);
          if (el) el.click();
        }, target.idx);

        await page.waitForTimeout(700);
        const afterUrl = page.url();

        if (afterUrl !== beforeUrl && afterUrl.startsWith(base)) {
          // Навигирано до нов URL
          const cu = afterUrl.split("#")[0].split("?")[0];
          if (cu.length > base.length + 1) {
            discovered.add(cu);
            console.log(`[DISCOVER] Click nav → ${cu}`);
          }
          await page.goBack({ timeout: 6000, waitUntil: "domcontentloaded" }).catch(() => {});
          await page.waitForTimeout(500);
        } else {
          // Modal/panel — извлечи нови линкове от DOM
          const newLinks = await page.evaluate((base) => {
            const urls = new Set();
            const origin = new URL(base).origin;
            document.querySelectorAll("a[href]").forEach(a => {
              try {
                const u = new URL(a.href, base);
                if (u.origin === origin && u.pathname.length > 2)
                  urls.add(u.href.split("#")[0].split("?")[0]);
              } catch {}
            });
            return Array.from(urls);
          }, base);
          newLinks.forEach(u => discovered.add(u));

          // Затвори modal
          await page.evaluate(() => {
            document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
            const cb = document.querySelector(
              '[role="dialog"] button[aria-label*="close" i],[role="dialog"] button[aria-label*="затвори" i]'
            );
            if (cb) cb.click();
          });
          await page.waitForTimeout(250);
        }
      } catch { /* timeout или навигационна грешка — продължи */ }
    }

  } catch (e) {
    console.error("[DISCOVER] Error:", e.message);
  }

  return Array.from(discovered);
}

// ================= PRODUCT/VEHICLE SPEC EXTRACTOR =================
// Универсален extractor за product detail страници (коли, имоти, хотели, е-commerce).
// Чете structured spec rows: '► Година: 2013', 'Двигател: Бензинов', 'Price: 5000лв' и т.н.
async function extractProductSpecsFromPage(page) {
  try {
    return await page.evaluate(() => {
      const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
      const specs = [];
      const prices = [];
      let title = '';
      let description = '';

      // ── Заглавие ─────────────────────────────────────────────────────────
      const h1 = document.querySelector('h1');
      if (h1) title = norm(h1.innerText || h1.textContent || '');

      // ── Цена: широко търсене по CSS класове и text walker ─────────────────
      const priceRe = /[\d][\d\s.,]*\s*(лв\.?|лева|BGN|EUR|€|\$)/i;
      document.querySelectorAll('[class*="price"],[class*="cena"],[class*="cost"],[class*="amount"],[class*="suma"]').forEach(el => {
        const t = norm(el.innerText || el.textContent || '');
        if (priceRe.test(t) && t.length < 100) prices.push(t);
      });
      if (prices.length === 0) {
        const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
        let node;
        while ((node = walker.nextNode())) {
          const t = (node.textContent || '').trim();
          if (priceRe.test(t) && t.length < 80) {
            prices.push(t);
            if (prices.length >= 3) break;
          }
        }
      }

      // ── Spec rows: dl/dt/dd ───────────────────────────────────────────────
      document.querySelectorAll('dl').forEach(dl => {
        const dts = dl.querySelectorAll('dt');
        const dds = dl.querySelectorAll('dd');
        dts.forEach((dt, i) => {
          const key = norm(dt.innerText || dt.textContent || '');
          const val = norm(dds[i]?.innerText || dds[i]?.textContent || '');
          if (key && val && key.length < 60) specs.push({ key, value: val });
        });
      });

      // ── Spec rows: table 2 колони ─────────────────────────────────────────
      document.querySelectorAll('table tr').forEach(tr => {
        const cells = tr.querySelectorAll('td, th');
        if (cells.length === 2) {
          const key = norm(cells[0].innerText || cells[0].textContent || '');
          const val = norm(cells[1].innerText || cells[1].textContent || '');
          if (key && val && key.length < 60) specs.push({ key, value: val });
        }
      });

      // ── Spec rows: li/div с pattern '► Key: Value' ────────────────────────
      const specItemRe = /^[►▸•\-]?\s*(.{2,50}?)\s*[:\-–]\s*(.{1,200})$/;
      document.querySelectorAll(
        'li, [class*="spec"] *, [class*="detail"] *, [class*="feature"] *, [class*="param"] *, [class*="osobenost"] *'
      ).forEach(el => {
        if (el.children.length > 3) return;
        const t = norm(el.innerText || el.textContent || '');
        if (!t || t.length > 200) return;
        const m = t.match(specItemRe);
        if (m && m[1].length >= 2 && m[1].length < 50 && m[2].length > 0) {
          specs.push({ key: norm(m[1]), value: norm(m[2]) });
        }
      });

      // ── Spec rows: label + next sibling ──────────────────────────────────
      document.querySelectorAll('[class*="label"],[class*="key"],[class*="attr"],[class*="prop"]').forEach(lbl => {
        const key = norm(lbl.innerText || lbl.textContent || '');
        if (!key || key.length > 60) return;
        const next = lbl.nextElementSibling;
        if (next) {
          const val = norm(next.innerText || next.textContent || '');
          if (val && val.length < 200 && val !== key) specs.push({ key, value: val });
        }
      });

      // ── Fallback text walker: 'Key: Value' lines ─────────────────────────
      if (specs.length < 3) {
        const lineRe = /^(.{2,40})[:\-–]\s*(.{2,100})$/;
        const walker2 = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
        let node2;
        while ((node2 = walker2.nextNode())) {
          const lines = (node2.textContent || '').split('\n');
          for (const ln of lines) {
            const t = ln.trim().replace(/^[►▸•]\s*/, '');
            const m = t.match(lineRe);
            if (m && m[1].length < 40 && !/^https?/i.test(m[1])) {
              specs.push({ key: m[1].trim(), value: m[2].trim() });
              if (specs.length >= 50) break;
            }
          }
          if (specs.length >= 50) break;
        }
      }

      // ── Описание ─────────────────────────────────────────────────────────
      const descEl = document.querySelector(
        '[class*="desc"] p,[class*="about"] p,article p,.content p,[class*="text"] p'
      );
      if (descEl) description = norm(descEl.innerText || descEl.textContent || '').slice(0, 600);

      // Dedupe specs by key
      const seen = new Set();
      const deduped = specs.filter(s => {
        if (!s.key || !s.value) return false;
        const k = s.key.toLowerCase().replace(/\s+/g, ' ').trim();
        if (seen.has(k)) return false;
        seen.add(k);
        return true;
      }).slice(0, 50);

      return {
        title,
        description,
        specs: deduped,
        prices: [...new Set(prices.map(p => p.trim()))].filter(Boolean).slice(0, 5),
      };
    });
  } catch (e) {
    console.error('[SPECS] Extract error:', e.message);
    return { title: '', description: '', specs: [], prices: [] };
  }
}


// ================= PROCESS SINGLE PAGE =================
async function processPage(page, url, base, stats, siteMaps, capabilitiesMaps) {
  const startTime = Date.now();

  try {
    console.log("[PAGE]", url);
    await page.goto(url, { timeout: 15000, waitUntil: "domcontentloaded" });
const getNetworkPayloads =
await attachNetworkMining(page);

if(!/\/proekt\/|\/id-/i.test(url)){
 await waitForHydrationSettled(page);
}

await forceRenderEverything(page);

    // Cloudflare / bot-protection check — изчакваме до 15с ако е challenge страница
    const passedCf = await waitForRealContent(page, url);
    if (!passedCf) return { links: [], page: null };

    // Scroll for lazy load — fast version (30ms steps, capped at MAX_SCROLL_STEPS)
    await page.evaluate(async ({ stepMs, maxSteps }) => {
      const scrollStep = window.innerHeight;
      const maxScroll = document.body.scrollHeight;
      const steps = Math.min(Math.ceil(maxScroll / scrollStep), maxSteps);

      for (let i = 0; i <= steps; i++) {
        window.scrollTo(0, i * scrollStep);
        await new Promise(r => setTimeout(r, stepMs));
      }
      window.scrollTo(0, maxScroll);

      // Force-load lazy images without waiting for scroll events
      document.querySelectorAll('img[loading="lazy"], img[data-src], img[data-lazy]').forEach(img => {
        img.loading = 'eager';
        if (img.dataset.src) img.src = img.dataset.src;
        if (img.dataset.lazy) img.src = img.dataset.lazy;
      });
    }, { stepMs: SCROLL_STEP_MS, maxSteps: MAX_SCROLL_STEPS });

    await page.waitForTimeout(150); // ↓ was 500ms

    try {
      await page.waitForLoadState('networkidle', { timeout: 1500 }); // ↓ was 3000ms
    } catch {}

    // ── Detect page type FIRST (needed before expandHiddenContent) ──
    const title = clean(await page.title());
    const pageType = detectPageType(url, title);
    stats.byType[pageType] = (stats.byType[pageType] || 0) + 1;

    // ── UI-AWARE: кликай accordions, tabs, "Виж детайли" + Radix dialogs ──
    // Skip on product_detail pages — saves 2-4s per page (no useful accordions, only contact forms)
    const dialogTexts = (pageType !== 'product_detail') ? await expandHiddenContent(page) : '';
    if (dialogTexts) console.log(`[DIALOG] Collected ${dialogTexts.length} chars from dialogs`);

    // Extract structured content
    const data = await extractStructured(page);

const shadowText =
await extractShadowAndPortalText(page);

const apiPayloads =
getNetworkPayloads();

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

    // NEW: Product/vehicle spec extraction for detail pages
    let product_specs = null;
    if (pageType === 'product_detail') {
      try {
        product_specs = await extractProductSpecsFromPage(page);
        if (product_specs.specs.length > 0 || product_specs.prices.length > 0) {
          console.log(`[SPECS] Page: ${product_specs.specs.length} specs, prices: ${product_specs.prices.join(', ')}`);
        }
      } catch (e) {
        console.error('[SPECS] Extract error:', e.message);
      }
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
      if ((caps.forms?.length || 0) > 0 || (caps.wizards?.length || 0) > 0 || (caps.iframes?.length || 0) > 0 || (caps.availability?.length || 0) > 0) {
        capabilitiesMaps.push(caps);
        console.log(`[CAPS] Page: ${caps.forms?.length || 0} forms, ${caps.wizards?.length || 0} wizards, ${caps.iframes?.length || 0} iframes, ${caps.availability?.length || 0} availability`);
      }
    } catch (e) {
      console.error("[CAPS] Extract error:", e.message);
    }

    // Format content — включва и текст от Radix/React dialogs + product specs
    let specsText = '';
    if (product_specs && (product_specs.specs.length > 0 || product_specs.prices.length > 0)) {
      const specLines = [];
      if (product_specs.title) specLines.push(`Продукт: ${product_specs.title}`);
      if (product_specs.prices.length) specLines.push(`Цена: ${product_specs.prices.join(' | ')}`);
      product_specs.specs.forEach(s => specLines.push(`${s.key}: ${s.value}`));
      if (product_specs.description) specLines.push(`Описание: ${product_specs.description}`);
      specsText = `\n\nPRODUCT_SPECS\n${specLines.join('\n')}`;
    }

   const rawAll=[

data.rawContent,

shadowText
? `SHADOW_CONTENT\n${shadowText}`
:'',

apiPayloads
? `API_PAYLOADS\n${apiPayloads}`
:'',

dialogTexts
? `DIALOG_CONTENT\n${dialogTexts}`
:'',

specsText

].filter(Boolean).join('\n\n');

    // КРИТИЧНО: Извличаме контакти от СУРОВИЯ текст — ПРЕДИ normalizeNumbers
    // normalizeNumbers може да конвертира цифри в думи и да унищожи номерата
    const domContacts = await extractContactsFromPage(page);

    // Подаваме RAW текст + textHints от DOM (footer, contact секции и т.н.)
    const rawForContacts = `${rawAll}\n\n${domContacts.textHints || ""}`;
    const textContacts = extractContactsFromText(rawForContacts);

    // tel: href стойностите от DOM се нормализират тук в Node.js
    // (в browser evaluate не можем да викаме normalizePhone)
    const domPhones = (domContacts.phones || [])
      .map(p => normalizePhone(p))
      .filter(Boolean);

    const mergedEmails = Array.from(
      new Set([...(domContacts.emails || []), ...(textContacts.emails || [])])
    ).slice(0, 20);

    const mergedPhones = Array.from(
      new Set([...domPhones, ...(textContacts.phones || [])])
    ).slice(0, 20);

    const contacts = { emails: mergedEmails, phones: mergedPhones };

    if (contacts.emails.length || contacts.phones.length) {
      console.log(`[CONTACTS] Page: ${contacts.phones.length} phones, ${contacts.emails.length} emails`);
    }

    // Нормализираме числата СЛЕД като сме извлекли контактите
    const htmlContent = normalizeNumbers(clean(rawAll));
    const content = htmlContent;

    const totalWords = countWordsExact(htmlContent);

    const elapsed = Date.now() - startTime;
    console.log(`[PAGE] ✓ ${totalWords}w ${elapsed}ms`);

    // ── Link discovery: стандартни <a> + бутон-задвижени URL-и (universal) ──
    const standardLinks = await collectAllLinks(page, base);
    const buttonLinks = await discoverLinksViaButtons(page, base);
    const allLinks = Array.from(new Set([...standardLinks, ...buttonLinks]));
    const extraCount = allLinks.length - standardLinks.length;
    if (extraCount > 0) {
      console.log(`[DISCOVER] +${extraCount} button-discovered links (total ${allLinks.length})`);
    }

    if (pageType !== "services" && pageType !== "product_detail" && totalWords < MIN_WORDS) {
      return { links: allLinks, page: null };
    }

    return {
      links: allLinks,
      page: {
        url,
        title,
        pageType,
        content,
        // ✅ structured output: pricing + contacts + product specs
        structured: { pricing, contacts, product_specs },
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
// ================= STEALTH + CLOUDFLARE BYPASS =================
const STEALTH_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

async function makeStealthContext(browser) {
  return browser.newContext({
    viewport: { width: 1920, height: 1080 },
    userAgent: STEALTH_UA,
    locale: 'bg-BG',
    timezoneId: 'Europe/Sofia',
    extraHTTPHeaders: {
      'Accept-Language': 'bg-BG,bg;q=0.9,en-US;q=0.8,en;q=0.7',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
      'Accept-Encoding': 'gzip, deflate, br',
      'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"Windows"',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': 'none',
      'Sec-Fetch-User': '?1',
      'Upgrade-Insecure-Requests': '1',
    },
  });
}

async function applyStealthScripts(page) {
  await page.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, 'languages', { get: () => ['bg-BG', 'bg', 'en-US', 'en'] });
    if (!window.chrome) window.chrome = { runtime: {} };
    delete window.__playwright;
    delete window.__pw_manual;
  });
}

async function waitForRealContent(page, url) {
  const cfSignals = ['just a moment','checking your browser','performing security','please wait','enable javascript and cookies','ray id'];
  const isCfPage = async () => {
    try {
      const title = (await page.title()).toLowerCase();
      const body = await page.evaluate(() => ((document.body && document.body.innerText) || '').toLowerCase().slice(0, 600));
      return cfSignals.some(s => title.includes(s) || body.includes(s));
    } catch { return false; }
  };
  if (!(await isCfPage())) return true;
  console.log('[CF] Cloudflare detected — waiting up to 15s for ' + url);
  for (let i = 0; i < 15; i++) {
    await page.waitForTimeout(1000);
    if (!(await isCfPage())) {
      console.log('[CF] Passed after ' + (i + 1) + 's');
      await page.waitForTimeout(500);
      return true;
    }
  }
  console.log('[CF] Still blocked after 15s — skipping ' + url);
  return false;
}

async function crawlSmart(startUrl, siteId = null, deadlineMs = null) {
  // If caller passed a deadline (e.g. scrape-website knows its own timeout),
  // use that minus a 5s buffer for JSON serialization + response.
  // Otherwise fall back to MAX_SECONDS.
  const effectiveMs = deadlineMs
    ? Math.min(deadlineMs, MAX_SECONDS * 1000)
    : MAX_SECONDS * 1000;
  const deadline = Date.now() + effectiveMs;
  console.log("\n[CRAWL START]", startUrl);
  console.log(`[CONFIG] ${PARALLEL_TABS} tabs, deadline in ${Math.round(effectiveMs / 1000)}s`);
  if (siteId) console.log(`[SITE ID] ${siteId}`);

  const browser = await chromium.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
      "--disable-software-rasterizer",
      "--disable-blink-features=AutomationControlled",
      "--disable-features=IsolateOrigins,site-per-process",
      "--no-first-run",
      "--no-default-browser-check",
      "--disable-extensions",
      "--lang=bg-BG,bg;q=0.9,en-US;q=0.8,en;q=0.7",
    ],
  });

  const stats = {
    visited: 0,
    saved: 0,
    byType: {},
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
    const initContext = await makeStealthContext(browser);
    const initPage = await initContext.newPage();
    await applyStealthScripts(initPage);

    await initPage.goto(startUrl, { timeout: 10000, waitUntil: "domcontentloaded" });
    base = new URL(initPage.url()).origin;

    const initialLinks = await collectAllLinks(initPage, base);
    const initButtonLinks = await discoverLinksViaButtons(initPage, base);
    const allInitialLinks = Array.from(new Set([...initialLinks, ...initButtonLinks]));

    // Add homepage first
    const homeNorm = normalizeUrl(initPage.url());
    visited.add(homeNorm);
    queue.push(homeNorm);

    // Add all discovered links, marking visited immediately to prevent duplicates
    let initCount = 0;
    allInitialLinks.forEach(l => {
      const nl = normalizeUrl(l);
      if (!visited.has(nl) && !SKIP_URL_RE.test(nl)) {
        visited.add(nl);
        queue.push(nl);
        initCount++;
      }
    });
    console.log(`[INIT] Queued ${initCount} URLs from homepage (total: ${queue.length})`);

    await initPage.close();
    await initContext.close();

    console.log(`[CRAWL] Found ${queue.length} URLs`);

    const createWorker = async () => {
      const ctx = await makeStealthContext(browser);
      const pg = await ctx.newPage();
      await applyStealthScripts(pg);

      while (Date.now() < deadline) {
        let url = null;
        while (queue.length > 0) {
          url = queue.shift();
          break; // already deduplicated at push time via visited Set
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

        // Push discovered links into queue immediately (deduped, no queue.includes check — use visited Set)
        let newLinksAdded = 0;
        result.links.forEach(l => {
          const nl = normalizeUrl(l);
          if (!visited.has(nl) && !SKIP_URL_RE.test(nl)) {
            visited.add(nl); // reserve immediately to avoid duplicate queue entries
            queue.push(nl);
            newLinksAdded++;
          }
        });
        if (newLinksAdded > 0) {
          console.log(`[QUEUE] +${newLinksAdded} new URLs → queue: ${queue.length}`);
        }
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
        // Accept deadline from caller — scrape-website sends how many ms the crawler has
        // before the caller's own timeout fires. We subtract 5s for safety buffer.
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
