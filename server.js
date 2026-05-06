import http from "http";
import crypto from "crypto";
import { gzipSync } from "zlib";
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
const RESULT_TTL_MS = 120 * 1000;
const visited = new Set();

// ================= LIMITS =================
const MAX_SECONDS = 120;             // fits within scrape-website's 120s fetch timeout
const MIN_WORDS = 20;
const PARALLEL_TABS = 8;
const SCROLL_STEP_MS = 30;
const MAX_SCROLL_STEPS = 5;
const HYDRATION_WAIT_MS = 1800;
const MUTATION_IDLE_MS = 2200;
const MAX_PAGES = 40;                // max pages to crawl
const MAX_DEPTH = 5;                 // max link-following depth

const SKIP_URL_RE =
  /(wp-content\/uploads|wp-json|\/feed\/?$|\/rss\/?$|sitemap\.xml|\/attachment\/|\/author\/|\/tag\/|\/category\/|\/comment|\/trackback|\/xmlrpc|\/wp-admin|\/wp-login|privacy|terms|cookies|gdpr|impressum|datenschutz|disclaimer|legal|politica|politique|blog\/|news\/|article\/|archive\/|login|register|signup|sign-up|sign-in|cart|checkout|wishlist|basket|warenkorb|panier)/i;

// ================= UTILS =================
const clean = (t = "") =>
  t.replace(/\r/g, "").replace(/[ \t]+/g, " ").replace(/\n{3,}/g, "\n\n").trim();

const countWordsExact = (t = "") => t.split(/\s+/).filter(Boolean).length;

// Safe evaluate with hard timeout — prevents hanging Promises inside page.evaluate
async function safeEval(page, fn, arg, timeoutMs = 5000) {
  return Promise.race([
    typeof arg !== 'undefined' ? page.evaluate(fn, arg) : page.evaluate(fn),
    new Promise((_, reject) => setTimeout(() => reject(new Error('safeEval timeout')), timeoutMs))
  ]);
}

// Safe page operation with timeout — wraps any async page call
function withTimeout(promise, ms = 10000, label = 'operation') {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`${label} timeout after ${ms}ms`)), ms))
  ]);
}


async function waitForHydrationSettled(page){
try{

await safeEval(page,
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

// Hard safety: resolve after 4x idleMs regardless
setTimeout(finish, idleMs * 4);

reset();

}),
MUTATION_IDLE_MS,
10000  // 10s hard timeout for safeEval
);

}catch{}
}



async function extractShadowAndPortalText(page){

return await page.evaluate(()=>{

const out=[];
const seen=new Set();

function push(v){
v=(v||'').replace(/\s+/g,' ').trim();
if(!v || v.length<2) return;
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
/price|cost|amount|rate|tariff|subscription|monthly|annual|€|£|\$|¥|₹|currency|checkout|cart|product|service|plan|package|offer|catalog/i.test(txt)
){
payloads.push(
txt.slice(0,100000)
);
}

}

}catch{}

});

return ()=>payloads.join("\n");
}



async function forceRenderEverything(page){

await page.evaluate(async()=>{

for(let i=0;i<8;i++){

window.scrollTo(
0,
document.body.scrollHeight
);

document.querySelectorAll(
'button,[role=tab],summary,[aria-expanded="false"]'
).forEach(el=>{

const t=(el.innerText||'').toLowerCase();

// Universal: click any tab/accordion/expand button that might reveal content
// Detects pricing/services/plans/packages/details in any language by checking
// if the element is a tab, accordion trigger, or has aria-expanded
const isInteractive = el.tagName === 'SUMMARY' || 
  el.getAttribute('aria-expanded') === 'false' ||
  el.getAttribute('role') === 'tab';

// Also click buttons with short text (likely tab/toggle labels, not navigation)
const isShortLabel = t.length > 0 && t.length < 40;

if(isInteractive || (isShortLabel && el.closest('[class*="tab"],[class*="accordion"],[class*="toggle"],[class*="pricing"],[class*="plan"]'))){
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

// ================= NUMBER NORMALIZER =================
// Universal: only normalize numbers next to countable units (rooms, people, nights, sqm)
// Keeps all prices and standalone numbers intact
function normalizeNumbers(text = "") {
  // No-op: we keep all numbers as-is for universal compatibility
  // Downstream extractors handle number parsing per their own logic
  return text;
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
  // Universal patterns: check URL slugs and page titles in any language
  if (/\/(about|za-nas|uber-uns|a-propos|sobre|chi-siamo|o-nas|hakkimizda|om-oss)\b/i.test(s) || /\babout\b/i.test(title)) return "about";
  if (/\/(service|pricing|price|plans?|tariff|paket|packages?|rates?|uslugi|ceni|tseni|leistungen|preise|servicios|precios|servizi|prezzi)\b/i.test(s)) return "services";
  if (/\/(contact|kontakt|kontakti|contatto|contacto|kontakty|iletisim)\b/i.test(s) || /\bcontact\b/i.test(title)) return "contact";
  if (/\/(faq|questions|vuprosi|hilfe|ayuda|domande|pomoc)\b/i.test(s) || /\bfaq\b/i.test(title)) return "faq";
  if (/\/(blog|news|article|novini|actualites|noticias|notizie|aktuelles)\b/i.test(s)) return "blog";
  // Product/vehicle/property detail pages — URL contains ID pattern or detail slug
  if (/[?&]id=\d|\/id[-_]\d|\/detail\/|\/product\/|\/item\/|\d{4,}-[a-z]/i.test(url)) return "product_detail";
  return "general";
}

// ═══════════════════════════════════════════════════════════════════════════
// SITEMAP EXTRACTION - EXISTING
// ═══════════════════════════════════════════════════════════════════════════

// Keyword mappings for buttons and fields
const KEYWORD_MAP = {
  // Booking — universal patterns
  "book": ["book", "reserve", "booking", "reservation"],
  "reserve": ["book", "reserve", "reservation"],
  "резерв": ["book", "reserve", "booking"],
  "запази": ["book", "reserve"],
  "buchen": ["book", "reserve"],
  "prenota": ["book", "reserve"],
  "réserver": ["book", "reserve"],
  "reservar": ["book", "reserve"],
  // Search
  "search": ["search", "find", "lookup"],
  "find": ["search", "find"],
  "търси": ["search", "find"],
  "suchen": ["search", "find"],
  "buscar": ["search", "find"],
  "chercher": ["search", "find"],
  // Actions
  "submit": ["submit", "send"],
  "send": ["send", "submit"],
  "изпрати": ["send", "submit"],
  "senden": ["send", "submit"],
  "enviar": ["send", "submit"],
  "envoyer": ["send", "submit"],
  // Contact
  "contact": ["contact", "reach"],
  "контакт": ["contact", "reach"],
  "kontakt": ["contact"],
  // Dates
  "check-in": ["check-in", "checkin", "arrival"],
  "check-out": ["check-out", "checkout", "departure"],
  "настаняване": ["check-in", "arrival"],
  "напускане": ["check-out", "departure"],
  "arrival": ["check-in", "arrival"],
  "departure": ["check-out", "departure"],
  // Other
  "price": ["prices", "rates", "cost"],
  "service": ["services"],
  "цени": ["prices", "rates"],
  "услуги": ["services"],
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

  // Universal booking/reservation patterns
  if (/\b(book|reserve|reserv|buchen|prenota|réserver|reservar|запис|резерв|запази|foglal|boka)\b/i.test(lower)) return "booking";
  // Universal contact patterns
  if (/\b(contact|kontakt|contatto|contacto|связ|свърж|iletişim|kontakty)\b/i.test(lower)) return "contact";
  // Universal submit/search/action patterns
  if (/\b(search|find|submit|send|check|filter|apply|go|enter|изпрати|търси|suchen|buscar|chercher|cerca|szukaj|ara)\b/i.test(lower)) return "submit";
  // Universal navigation
  if (/\b(about|service|room|gallery|portfolio|menu|home|faq|help)\b/i.test(lower)) return "navigation";

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

  // Date-related
  if (/check-?in|checkin|arrival|from|start|anreise|arrivée|llegada|arrivo|пристигане|настаняване|приезд|giriş/i.test(searchText)) {
    ["check-in", "arrival", "from"].forEach(k => keywords.add(k));
  }
  if (/check-?out|checkout|departure|to\b|end|abreise|départ|salida|partenza|напускане|заминаване|отъезд|çıkış/i.test(searchText)) {
    ["check-out", "departure", "to"].forEach(k => keywords.add(k));
  }

  // People/guests
  if (/guest|adult|person|people|pax|room|гост|човек|деца|child|kinder|enfant|huésped|ospite|гостей|misafir/i.test(searchText)) {
    ["guests", "adults", "persons"].forEach(k => keywords.add(k));
  }

  // Name
  if (/\bname\b|име|nom\b|nombre|nome|имя|isim|naam/i.test(searchText)) {
    ["name"].forEach(k => keywords.add(k));
  }

  // Email
  if (/email|e-?mail|имейл|correo|courriel|почта|e-posta/i.test(searchText)) {
    ["email"].forEach(k => keywords.add(k));
  }

  // Phone
  if (/phone|tel|телефон|teléfono|téléphone|telefon|telefono|телефон/i.test(searchText)) {
    ["phone"].forEach(k => keywords.add(k));
  }

  // Message/notes
  if (/message|comment|note|съобщение|сообщение|nachricht|mensaje|messaggio|mesaj/i.test(searchText)) {
    ["message"].forEach(k => keywords.add(k));
  }

  // Add field name/id as keyword
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
    const priceRegex = /(?:(€|\$|£|¥|₹|₽|₺|zł|Kč|kr|лв\.?)\s*(\d+[\s,.]?\d*)|(\d+[\s,.]?\d*)\s*(лв\.?|BGN|EUR|€|\$|USD|GBP|£|¥|₹|CHF|PLN|zł|CZK|Kč|SEK|kr|NOK|DKK|RON|lei|RUB|₽|TRY|₺|лева|руб|грн|UAH))/gi;

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

    // Universal money regex: handles both "€350" and "350 EUR" patterns
    const currencySymbols = '(?:лв\\.?|лева|bgn|eur|€|\\$|usd|gbp|£|¥|₹|chf|pln|zł|czk|kč|sek|kr|nok|dkk|ron|lei|rub|₽|руб|try|₺|lv|грн|uah|brl|r\\$|aud|cad|huf|ft|hrk|kn|jpy|cny|元|inr|mxn)';
    const periodWords = '(?:месец|month|mo|jahr|año|mese|monat|rok|ay|luna|месяц)';
    const numPattern = '\\d{1,6}(?:[\\s,.]\\d{1,3})*(?:[.,]\\d{1,2})?';
    // Pattern 1: €350, $99, £1200 (symbol before number)
    // Pattern 2: 350 EUR, 99 лв, 1200€ (number before symbol)
    const moneyRe = new RegExp(
      '(?:' +
        '(' + currencySymbols + ')\\s*(' + numPattern + ')' +  // €350
        '|' +
        '(?:(?:from|от|ab|desde|da)\\s*)?' +
        '(' + numPattern + ')\\s*(' + currencySymbols + ')' +  // 350 EUR
      ')' +
      '(?:\\s*\\/?\\s*(' + periodWords + '))?',
      'i'
    );

    const getText = (el) => norm(el?.innerText || el?.textContent || "");
    const pickTitle = (root) => {
      const h = root.querySelector("h1,h2,h3,h4,[class*='title'],strong,b");
      const t = getText(h);
      if (t && t.length <= 80) return t;
      const lines = getText(root).split("\n").map(norm).filter(Boolean);
      return (lines.find(l => l.length >= 3 && l.length <= 80) || "");
    };

    const pickBadge = (root) => {
      const b = root.querySelector("[class*='badge'],[class*='label'],[class*='tag'],[class*='popular'],[class*='best'],[class*='recommend']");
      const t = getText(b);
      if (t && t.length <= 40) return t;
      const all = getText(root);
      if (/popular|best|recommended|most|top|special|featured|популярен|най-|empfohlen|beliebt|recomendado|consigliato|recommandé/i.test(all)) {
        const m = all.match(/(most popular|best value|recommended|популярен|най-популярен|special offer|best seller|top pick|featured)/i);
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
      if (/\/\s*(месец|month|mo|monat|mese|mes|mois)|per month|на месец|pro monat|al mese|por mes|par mois/i.test(t)) return "monthly";
      if (/\/\s*(year|год|jahr|anno|año|an\b)|per year|на година|pro jahr|all'anno|por año|par an/i.test(t)) return "yearly";
      if (/еднократно|one[-\s]?time|einmalig|una tantum|pago único|paiement unique/i.test(t)) return "one_time";
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

        if((looksCard||(moneyRe.test(txt)))&&(hasTitle||hasFeatures||moneyRe.test(txt))&&txt.length>=20)return el;
        el = el.parentElement;
      }
      return null;
    };

    const cards=[];
    const seen = new Set();

    // ── Phase 0: Extract from data-attributes (data-price, data-amount, data-cost) ──
    document.querySelectorAll('[data-price],[data-amount],[data-cost],[data-value]').forEach(el => {
      if (!isVisible(el)) return;
      const priceVal = el.getAttribute('data-price') || el.getAttribute('data-amount') || el.getAttribute('data-cost') || el.getAttribute('data-value') || '';
      const elText = getText(el);
      const displayPrice = elText || priceVal;
      if (!displayPrice) return;
      
      const root = findCardRoot(el);
      if (!root) return;
      const title = pickTitle(root);
      if (!title) return;
      
      const key = `${title}|${displayPrice}`;
      if (seen.has(key)) return;
      seen.add(key);
      
      cards.push({
        title,
        price_text: norm(displayPrice),
        period: pickPeriod(root),
        badge: pickBadge(root),
        features: pickFeatures(root),
      });
    });

    // ── Phase 0b: Extract from elements with price-related CSS classes ──
    document.querySelectorAll('[class*="price"],[class*="cost"],[class*="amount"],[class*="cena"],[class*="pricing"]').forEach(el => {
      if (!isVisible(el)) return;
      const t = getText(el);
      if (!t || t.length > 60) return;
      // Must contain at least one digit and a currency symbol
      if (!/\d/.test(t)) return;
      if (!/[€$£¥₹₽₺]|лв|eur|usd|bgn|gbp/i.test(t)) return;
      
      const root = findCardRoot(el);
      if (!root) return;
      const title = pickTitle(root);
      if (!title) return;
      
      const key = `${title}|${t}`;
      if (seen.has(key)) return;
      seen.add(key);
      
      cards.push({
        title,
        price_text: norm(t),
        period: pickPeriod(root),
        badge: pickBadge(root),
        features: pickFeatures(root),
      });
    });

document.querySelectorAll('script[type="application/ld+json"]').forEach(s=>{
try{
const j=JSON.parse(s.textContent||'{}');
const arr=Array.isArray(j)?j:[j];
arr.forEach(item=>{
const offers=item.offers||item.hasOfferCatalog?.itemListElement;
if(!offers) return;
const list=Array.isArray(offers)?offers:[offers];
list.forEach(o=>{
const p=o.price||o.offers?.price;
if(!p) return;
cards.push({
title:item.name||o.name||'Package',
price_text:String(p),
period:null,
badge:'',
features:[]
});
});
});
}catch{}
});

    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
    let node;
    while (node = walker.nextNode()) {
      const txt = norm(node.textContent || "");
      if (!txt) continue;
      if (!moneyRe.test(txt) && !/по договаряне|on request|auf anfrage|sur demande|a consultar|su richiesta|price on request|call for price|contact for price/i.test(txt)) continue;

      const parent = node.parentElement;
      if (!parent || !isVisible(parent)) continue;

      const root = findCardRoot(parent);
      if (!root || !isVisible(root)) continue;

      const title = pickTitle(root);
      if (!title) continue;

      const rootText = getText(root);
      const moneyMatch = rootText.match(moneyRe);
      const price_text = moneyMatch ? norm(moneyMatch[0]) : (/по договаряне|on request|auf anfrage|sur demande|a consultar|price on request|call for price/i.test(rootText) ? "On request" : "");

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

    const installment_plans = cards.filter(c => c.period === "monthly" || /месец|month|monat|mese|mes|mois|mo\b|\/mo|per month/i.test((c.title || "") + " " + (c.price_text || "")));
    const pricing_cards = cards.filter(c => !installment_plans.includes(c));

    installment_plans.forEach(p => {
      p.title = norm(p.title.replace(/\/\s*(месец|month|mo|monat)/i, "").trim());
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

// ================= CMS DETECTION (one-shot, on homepage) =================
// Reads <meta name="generator"> first (most authoritative), then falls back
// to a few rock-solid HTML/DOM markers. Returns short lowercase string or "unknown".
async function detectCms(page) {
  try {
    return await page.evaluate(() => {
      // 1) <meta name="generator"> — the canonical signal
      const gen = (document.querySelector('meta[name="generator"]')?.content || '').toLowerCase();
      if (gen) {
        if (gen.includes('wordpress')) return 'wordpress';
        if (gen.includes('shopify'))   return 'shopify';
        if (gen.includes('wix'))       return 'wix';
        if (gen.includes('squarespace')) return 'squarespace';
        if (gen.includes('webflow'))   return 'webflow';
        if (gen.includes('drupal'))    return 'drupal';
        if (gen.includes('joomla'))    return 'joomla';
        if (gen.includes('ghost'))     return 'ghost';
        if (gen.includes('hubspot'))   return 'hubspot';
        if (gen.includes('typo3'))     return 'typo3';
        if (gen.includes('prestashop')) return 'prestashop';
        if (gen.includes('magento'))   return 'magento';
        if (gen.includes('blogger'))   return 'blogger';
        if (gen.includes('weebly'))    return 'weebly';
        if (gen.includes('duda'))      return 'duda';
        if (gen.includes('framer'))    return 'framer';
        if (gen.includes('tilda'))     return 'tilda';
        if (gen.includes('bitrix'))    return 'bitrix';
        // unknown generator — return its first token rather than discard
        return gen.split(/[\s/]+/)[0].slice(0, 40) || 'unknown';
      }

      // 2) Hard DOM/HTML fallbacks (only the really reliable ones)
      const html = document.documentElement.outerHTML;
      if (/wp-content\/|wp-includes\/|\/wp-json\//.test(html)) return 'wordpress';
      if (window.Shopify || /cdn\.shopify\.com|myshopify\.com/.test(html)) return 'shopify';
      if (/static\.parastorage\.com|wix\.com/.test(html))     return 'wix';
      if (/squarespace\.com|static1\.squarespace/.test(html)) return 'squarespace';
      if (/assets\.website-files\.com/.test(html) || document.documentElement.getAttribute('data-wf-site')) return 'webflow';
      if (/sites\/default\/files\/|drupal-settings-json/.test(html)) return 'drupal';
      if (/\/media\/jui\/|\/templates\/system\//.test(html))  return 'joomla';
      if (/framerusercontent\.com/.test(html))                return 'framer';
      if (/static\.tildacdn\.com|tildacdn\.com/.test(html))   return 'tilda';
      if (window.__NEXT_DATA__ || /_next\/static\//.test(html)) return 'nextjs';
      if (window.__NUXT__ || /\/_nuxt\//.test(html))          return 'nuxt';

      return 'unknown';
    });
  } catch {
    return 'unknown';
  }
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
        const submitRe = /next|back|prev|forward|submit|send|cancel|close|continue|save|book|reserve|confirm|order|buy|напред|назад|изпрати|запази|резерв|затвори|отказ|weiter|zurück|siguiente|anterior|suivant|précédent|avanti|indietro|dalej|wstecz|ileri|geri/i;
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
        submitCandidates.find(b => /submit|send|book|reserve|save|go|apply|confirm|order|buy|purchase|изпрати|запази|резерв|senden|buchen|prenota|réserver|reservar|enviar|envoyer|bestellen|comprar|acheter|acquista/i.test(b.text)) ||
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
      checkIn: /(check\s*-?in|arrival|checkin|пристигане|настаняване|anreise|arrivée|llegada|arrivo|приезд|giriş)/i,
      checkOut: /(check\s*-?out|departure|checkout|напускане|заминаване|abreise|départ|salida|partenza|отъезд|çıkış)/i,
      guests: /(adults?|guests?|persons?|people|rooms?|children|kids|pax|гости|възрастни|деца|стаи?|gäste|invités|huéspedes|ospiti|гостей|misafir|promo\s*code|промо\s*код|coupon|gutschein)/i,
      action: /(book(?:\s*now)?|reserve|search|availability|check|find|резервирай|резервация|провери|търси|buchen|réserver|reservar|prenota|забронировать|ara|boka)/i,
      noise: /(jquery|document\.ready|swiper|slidesperview|pagination|navigation|autoplay|loop:)/i,
      menuNoise: /(^home$|^about$|^contact$|^gallery$|^faq$|^blog$|^news$|^menu$)/i,
      roomNoise: /(deluxe|double|studio|suite|standard|family|superior|junior|economy|single|twin)/i,
      genericActionNoise: /(learn more|read more|details|more info|see more|show more)/i,
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
      if (tag === 'a' && /(^#$|^javascript:|^\/$)/i.test(href)) return true;
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
      const navButtonRe = /next|back|prev|forward|continue|step|напред|назад|стъпка|weiter|zurück|siguiente|anterior|suivant|précédent|avanti|indietro|dalej|wstecz|ileri|geri/i;
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
          submitCandidates.find(b => /submit|send|next|forward|continue|book|reserve|save|confirm|go|apply|изпрати|напред|запази|резерв|weiter|siguiente|suivant|avanti|dalej|ileri/i.test(b.text)) ||
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
     // TEMPLATE FINGERPRINTING (ignore cosmetic field names / repeated wrappers)
const schemaStr = stableStringify(schema)
  .replace(/#\w+/g, "#ID")
  .replace(/:nth-of-type\(\d+\)/g, ":nth-of-type(N)")
  .replace(/\b(field|input)_\d+\b/g, "$1_N")
  .replace(/"selector_candidates":\[[^\]]*\]/g,'"selector_candidates":["GENERIC"]')
.replace(/"price_text":"[^"]*"/g,'"price_text":"PRICE"')


const templateFp = sha256Hex(kind + "|" + schemaStr);

const key = `${kind}|${templateFp}`;
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
    await withTimeout(page.evaluate(async () => {
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

      // 4. Content-revealing buttons — uses DOM semantics, not hardcoded text
      // Detects: aria-haspopup, data-toggle, buttons inside collapsed containers,
      // or short-text buttons near hidden content. Max 3 clicks.
      const skipRe = /nav|menu|header|footer|cookie|gdpr|consent/i;
      const clickable = Array.from(document.querySelectorAll(
        'button, [role="button"], a[href="#"], a[href="javascript:void(0)"], span[onclick], div[onclick]'
      )).filter(isVisible).slice(0, 30);
      let smartClicks = 0;
      const MAX_SMART_CLICKS = 5;
      for (const btn of clickable) {
        if (smartClicks >= MAX_SMART_CLICKS) break;
        const text = (btn.textContent || btn.getAttribute('aria-label') || '').trim();
        if (!text || text.length > 60) continue;
        if (skipRe.test(btn.closest('[class]')?.className || '')) continue;
        if (btn.getAttribute('aria-haspopup') === 'dialog') continue;
        
        // Smart detection: is this button likely to reveal hidden content?
        const looksLikeExpander = 
          // Has a sibling/child that is hidden
          btn.nextElementSibling?.style?.display === 'none' ||
          btn.getAttribute('data-toggle') ||
          btn.getAttribute('data-bs-toggle') ||
          // Short text with ellipsis-like patterns or "+" or "▸" symbols
          /[+▸▶►…]/.test(text) ||
          // Button is inside a section with collapsed content
          btn.closest('[class*="collaps"],[class*="expand"],[class*="toggle"],[class*="detail"],[class*="more"],[class*="pricing"],[class*="package"],[class*="plan"],[class*="card"]') ||
          // Text contains "detail/more/view/show" type words (universal)
          (text.split(/\s+/).length <= 4 && /detail|more|view|show|see|expand|reveal|open|info|подробн|детайл|повече|виж|покажи|разгъни|dettagli|detalles|détails|mehr|подробнее/i.test(text));
        
        if (looksLikeExpander) {
          try { btn.click(); await sleep(200); smartClicks++; } catch {}
        }
      }

      // 5. <details> force open
      document.querySelectorAll('details:not([open])').forEach(d => {
        try { d.open = true; d.setAttribute('open', ''); } catch {}
      });

      await sleep(200);
    }), 8000, 'expandHiddenContent-phase1');

    await page.waitForTimeout(200);
  } catch {}

  // ── Фаза 2: Dialog/Modal extraction — click buttons that reveal hidden content ──
  try {
    const triggerTexts = await page.evaluate(() => {
      const isVisible = (el) => {
        try {
          const rect = el.getBoundingClientRect();
          const style = window.getComputedStyle(el);
          return rect.width > 0 && rect.height > 0 &&
            style.display !== "none" && style.visibility !== "hidden";
        } catch { return false; }
      };

      const skipRe = /nav|menu|header|footer|cookie|gdpr|consent/i;
      const triggers = new Set();

      // Strategy 1: Any element with aria-haspopup="dialog" — guaranteed dialog trigger
      document.querySelectorAll('[aria-haspopup="dialog"]').forEach(el => {
        if (isVisible(el)) triggers.add(el);
      });

      // Strategy 2: Buttons with data-state (Radix UI pattern)
      document.querySelectorAll('button[data-state="closed"], [data-radix-collection-item]').forEach(el => {
        if (isVisible(el)) triggers.add(el);
      });

      // Strategy 3: Buttons inside card-like containers (pricing cards, product cards, etc.)
      // These often have "view details", "more info", etc. in any language
      const cardSelectors = [
        '[class*="card"]', '[class*="pricing"]', '[class*="package"]', '[class*="plan"]',
        '[class*="tier"]', '[class*="offer"]', '[class*="product"]', '[class*="item"]',
        '[class*="service"]', '[class*="feature"]',
      ];
      for (const sel of cardSelectors) {
        try {
          document.querySelectorAll(sel).forEach(card => {
            if (!isVisible(card)) return;
            // Find buttons inside this card that are NOT submit/form buttons
            card.querySelectorAll('button, [role="button"], a[href="#"], a[href="javascript:void(0)"]').forEach(btn => {
              if (!isVisible(btn)) return;
              const text = (btn.textContent || '').trim();
              if (!text || text.length > 60) return;
              // Skip navigation links and form submits
              if (/submit|send|buy|purchase|add to cart|изпрати|купи/i.test(text)) return;
              if (skipRe.test(btn.closest('[class]')?.className || '')) return;
              triggers.add(btn);
            });
          });
        } catch {}
      }

      // Strategy 4: Any button whose text suggests it reveals details (universal)
      const revealRe = /detail|more|info|view|show|see|expand|learn|подробн|детайл|повече|виж|покажи|разгъни|detalles|détails|dettagli|mehr|подробнее|посмотреть|göster|ver\b|voir/i;
      document.querySelectorAll('button, [role="button"]').forEach(btn => {
        if (!isVisible(btn)) return;
        const text = (btn.textContent || btn.getAttribute('aria-label') || '').trim();
        if (revealRe.test(text) && text.length <= 40) {
          if (!skipRe.test(btn.closest('[class]')?.className || '')) {
            triggers.add(btn);
          }
        }
      });

      // Dedupe and tag
      // ✅ FIX: Dedupe by text content and cap at 12
      const seenTriggerTexts = new Set();
      const deduped = [];
      for (const el of triggers) {
        const txt = (el.textContent || '').trim().slice(0, 60).toLowerCase();
        if (!txt || txt.length < 2 || seenTriggerTexts.has(txt)) continue;
        seenTriggerTexts.add(txt);
        deduped.push(el);
        if (deduped.length >= 12) break;
      }
      return deduped.map((el, i) => {
        el.setAttribute('data-crawler-trigger', String(i));
        return { idx: i, text: (el.textContent || '').trim().slice(0, 60) };
      });
    });

    if (triggerTexts.length > 0) {
      console.log(`[DIALOG] Found ${triggerTexts.length} potential triggers: ${triggerTexts.map(t => t.text.slice(0, 30)).join(', ')}`);
    }

    // Click each trigger, capture dialog/overlay content, close
    // ✅ FIX: Time-budget the dialog loop — max 10s total, reduced waits
    const DIALOG_BUDGET_MS = 10000;
    const dialogStartTime = Date.now();
    const collected = [];
    for (const { idx, text: triggerText } of triggerTexts) {
      if (Date.now() - dialogStartTime > DIALOG_BUDGET_MS) {
        console.log(`[DIALOG] Time budget exhausted (${DIALOG_BUDGET_MS}ms) — processed ${collected.length} dialogs, skipping remaining`);
        break;
      }
      try {
        const beforeDialogs = await page.evaluate(() => 
          document.querySelectorAll('[role="dialog"],[data-state="open"],[class*="modal"],[class*="overlay"]').length
        );

        await page.evaluate((idx) => {
          const el = document.querySelector(`[data-crawler-trigger="${idx}"]`);
          if (el) el.click();
        }, idx);

        await page.waitForTimeout(350);

        const dialogText = await page.evaluate((beforeCount) => {
          const dialogs = document.querySelectorAll(
            '[role="dialog"], [data-state="open"], [class*="modal"]:not([class*="modal-backdrop"]), ' +
            '.ReactModal__Content, [class*="dialog"], [class*="popup"]:not([class*="cookie"])'
          );
          const portals = document.querySelectorAll('[data-radix-portal], [data-radix-popper-content-wrapper]');
          const allOverlays = [...dialogs, ...portals];
          if (allOverlays.length <= beforeCount && allOverlays.length === 0) return '';
          const texts = [];
          allOverlays.forEach(el => {
            const t = (el.innerText || el.textContent || '').replace(/\s+/g, ' ').trim();
            if (t && t.length > 20) texts.push(t);
          });
          return texts.join('\n\n');
        }, beforeDialogs);

        if (dialogText && dialogText.length > 20) {
          collected.push(dialogText);
          console.log(`[DIALOG] Captured ${dialogText.length} chars from trigger "${triggerText.slice(0, 30)}"`);
        }

        await page.evaluate(() => {
          document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
          const closeSelectors = [
            '[role="dialog"] button[aria-label*="close" i]',
            '[role="dialog"] button[aria-label*="Close" i]',
            '[data-state="open"] button[aria-label*="close" i]',
            '.ReactModal__Content button[aria-label*="close" i]',
            '[class*="modal"] button[class*="close"]',
            '[role="dialog"] button:first-child',
            'button[data-dismiss="modal"]',
          ];
          for (const sel of closeSelectors) {
            try {
              const btn = document.querySelector(sel);
              if (btn) { btn.click(); return; }
            } catch {}
          }
          const backdrop = document.querySelector('[class*="backdrop"], [class*="overlay"][role="presentation"]');
          if (backdrop) backdrop.click();
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

      // Skip elements inside nav, footer, header, cookie bars — we capture those once from homepage
      const isInBoilerplate = (el) => {
        try {
          const bp = el.closest('nav, footer, header, [role="navigation"], [class*="cookie"], [class*="gdpr"], [class*="consent"], [class*="banner"], [id*="cookie"], [id*="gdpr"]');
          return !!bp;
        } catch { return false; }
      };

      document.querySelectorAll("h1,h2,h3,p,li").forEach(el => {
        if (processedElements.has(el)) return;
        if (el.closest("details.wp-block-details, details")) return;
        if (isInBoilerplate(el)) return;
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
// Collect ALL links from a page (used for inner pages)
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

// Collect only header/footer/nav links (used for homepage — selective crawling)
async function collectNavLinks(page, base) {
  try {
    return await page.evaluate(base => {
      const urls = new Set();
      const origin = new URL(base).origin;
      const navSelectors = [
        'header a[href]',
        'nav a[href]',
        'footer a[href]',
        '[role="navigation"] a[href]',
        '[class*="nav"] a[href]',
        '[class*="menu"] a[href]',
        '[id*="nav"] a[href]',
        '[id*="menu"] a[href]',
      ];
      const seen = new Set();
      for (const sel of navSelectors) {
        try {
          document.querySelectorAll(sel).forEach(a => {
            if (seen.has(a)) return;
            seen.add(a);
            try {
              const u = new URL(a.href, base);
              if (u.origin === origin) urls.add(u.href.split("#")[0]);
            } catch {}
          });
        } catch {}
      }
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
      const priceRe = /(?:(?:€|\$|£|¥|₹|₽|₺|zł|Kč|kr|лв\.?)\s*[\d][\d\s.,]*|[\d][\d\s.,]*\s*(?:лв\.?|лева|BGN|EUR|€|\$|USD|GBP|£|¥|₹|CHF|PLN|zł|CZK|Kč|SEK|kr|NOK|DKK|RON|lei|RUB|₽|TRY|₺|руб|грн|UAH))/i;
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
    
    // Wait for SPA content to render — try networkidle first, then check for dynamic content
    try {
      await page.waitForLoadState('networkidle', { timeout: 3000 });
    } catch {}

    // Smart wait: if the page has few visible elements, wait a bit more for SPA rendering
    try {
      await safeEval(page, () => new Promise(resolve => {
        let checks = 0;
        const maxChecks = 10;
        const check = () => {
          checks++;
          const elements = document.querySelectorAll('h1, h2, h3, [class*="card"], [class*="price"], [data-price], article, section');
          const visible = Array.from(elements).filter(el => {
            const rect = el.getBoundingClientRect();
            return rect.width > 0 && rect.height > 0;
          });
          if (visible.length >= 3 || checks >= maxChecks) {
            resolve();
          } else {
            setTimeout(check, 200);
          }
        };
        check();
      }), undefined, 4000);  // 4s hard timeout
    } catch {}
    
const getNetworkPayloads =
await attachNetworkMining(page);

if(!/[?&]id=\d|\/detail\/|\/product\//i.test(url)){
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
    // ✅ FIX: Wrap in withTimeout to prevent blowing the page budget
    let dialogTexts = '';
    try {
      dialogTexts = await withTimeout(
        expandHiddenContent(page),
        15000,
        'expandHiddenContent'
      );
    } catch (e) {
      console.log(`[DIALOG] expandHiddenContent timed out: ${e.message}`);
    }
    if (dialogTexts) console.log(`[DIALOG] Collected ${dialogTexts.length} chars from dialogs`);

    // Extract structured content
    const data = await extractStructured(page);

let shadowText = "";

if (/pricing|price|service|product|plan|package|tariff|shop|store|catalog/i.test(url)) {
 shadowText = await extractShadowAndPortalText(page);
}

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

   // SINGLE-PASS EXTRACTION PIPELINE (merge everything once, no second parsing passes)
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
const content = normalizeNumbers(
clean(
rawAll
.split(/\n+/)
.filter(Boolean)
.filter((v,i,a)=>a.indexOf(v)===i) // single-pass dedupe
.join("\n")
)
);

    const totalWords = countWordsExact(content);

    const elapsed = Date.now() - startTime;
    console.log(`[PAGE] ✓ ${totalWords}w ${elapsed}ms`);

    // ── Link discovery: run expensive button discovery ONLY on listing/general pages ──
const standardLinks = await collectAllLinks(page, base);

let buttonLinks = [];

if (pageType === "general") {
  try {
    buttonLinks = await withTimeout(
      discoverLinksViaButtons(page, base),
      8000,
      'discoverLinksViaButtons'
    );
  } catch (e) {
    console.error(`[DISCOVER] Timeout: ${e.message}`);
  }

  const extraCount = buttonLinks.length;
  if (extraCount > 0) {
    console.log(`[DISCOVER] +${extraCount} button-discovered links`);
  }
}

const allLinks = Array.from(
  new Set([
    ...standardLinks,
    ...buttonLinks
  ])
);

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
  const effectiveMs = deadlineMs
    ? Math.min(deadlineMs, MAX_SECONDS * 1000)
    : MAX_SECONDS * 1000;
  const deadline = Date.now() + effectiveMs;
  console.log("\n[CRAWL START]", startUrl);
  console.log(`[CONFIG] ${PARALLEL_TABS} tabs, deadline in ${Math.round(effectiveMs / 1000)}s, max ${MAX_PAGES} pages, depth ${MAX_DEPTH}`);
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
  const queue = [];       // [{url, depth}]
  const lowPriorityQueue = [];
  const siteMaps = [];
  const capabilitiesMaps = [];
  let base = "";
  let headerFooterText = ""; // captured once from homepage
  let cms = "unknown";        // detected once from homepage

  const contactAgg = { emails: new Set(), phones: new Set() };

  try {
    const initContext = await makeStealthContext(browser);
    const initPage = await initContext.newPage();
    await applyStealthScripts(initPage);

    await initPage.goto(startUrl, { timeout: 10000, waitUntil: "domcontentloaded" });
    base = new URL(initPage.url()).origin;

    // Capture header/footer text ONCE from homepage
    try {
      headerFooterText = await initPage.evaluate(() => {
        const parts = [];
        const seen = new Set();
        ['header', 'footer', 'nav', '[role="navigation"]'].forEach(sel => {
          document.querySelectorAll(sel).forEach(el => {
            if (seen.has(el)) return;
            seen.add(el);
            const t = (el.innerText || '').replace(/\s+/g, ' ').trim();
            if (t && t.length > 5) parts.push(t);
          });
        });
        return parts.join('\n');
      });
      console.log(`[INIT] Captured ${headerFooterText.length} chars of header/footer text (once)`);
    } catch {}

    // Detect CMS once on homepage (cheap, ~5–20ms)
    try {
      cms = await detectCms(initPage);
      console.log(`[CMS] Detected: ${cms}`);
    } catch (e) {
      console.log(`[CMS] Detection failed: ${e.message}`);
    }

    // Use SELECTIVE nav link discovery — only header/footer/nav links
    const navLinks = await collectNavLinks(initPage, base);
    // Also get button-discovered links for SPAs
    let initButtonLinks = [];
    try {
      initButtonLinks = await withTimeout(
        discoverLinksViaButtons(initPage, base),
        8000,
        'init discoverLinksViaButtons'
      );
    } catch (e) {
      console.error(`[INIT] Button discovery timeout: ${e.message}`);
    }
    const allInitialLinks = Array.from(new Set([...navLinks, ...initButtonLinks]));

    // Add homepage first
    const homeNorm = normalizeUrl(initPage.url());
    visited.add(homeNorm);
    queue.push({ url: homeNorm, depth: 0 });

    // Prioritize important pages from nav, skip blog/news/terms/etc.
    let initCount = 0;
    allInitialLinks.forEach(l => {
      const nl = normalizeUrl(l);
      if (visited.has(nl) || SKIP_URL_RE.test(nl)) return;

      visited.add(nl);

      // Priority: pages likely to have important business info (services, pricing, about, contact, FAQ)
      // Universal slugs covering EN, BG, DE, FR, ES, IT, TR, RU, and common patterns
      const PRIORITY_RE = /\/(about|service|pricing|price|plans?|tariff|contact|faq|product|portfolio|team|feature|offer|package|rate|shop|store|catalog|menu|za-nas|uslugi|ceni|tseni|kontakt|vuprosi|proekt|leistungen|preise|uber-uns|kontakt|servizio|prezzi|chi-siamo|servicios|precios|sobre|iletisim|hakkimizda|o-nas|cennik)\b/i;

      if (PRIORITY_RE.test(nl)) {
        queue.push({ url: nl, depth: 1 });
      } else {
        lowPriorityQueue.push({ url: nl, depth: 1 });
      }

      initCount++;
    });
    console.log(`[INIT] Queued ${initCount} nav URLs (priority: ${queue.length}, low: ${lowPriorityQueue.length})`);

    await initPage.close();
    await initContext.close();

    const createWorker = async () => {
      const ctx = await makeStealthContext(browser);
      const pg = await ctx.newPage();
      await applyStealthScripts(pg);

      while (Date.now() < deadline && stats.saved < MAX_PAGES) {
        let item = null;
        if (queue.length) {
          item = queue.shift();
        } else if (lowPriorityQueue.length) {
          item = lowPriorityQueue.shift();
        }

        if (!item) {
          await new Promise(r => setTimeout(r, 30));
          if (queue.length === 0 && lowPriorityQueue.length === 0) break;
          continue;
        }

        // Enforce depth limit
        if (item.depth > MAX_DEPTH) continue;
        // Enforce page limit
        if (stats.saved >= MAX_PAGES) break;

        // ✅ FIX: Check how much time is left. If less than 10s, stop crawling
        // so we have time for post-processing (sitemap build, Supabase save etc.)
        const timeLeft = deadline - Date.now();
        if (timeLeft < 10000) {
          console.log(`[PAGE] ✗ SKIP ${item.url}: only ${Math.round(timeLeft / 1000)}s left before deadline — stopping`);
          break;
        }
        // ✅ FIX: Dynamic page timeout — give this page at most (timeLeft - 8s),
        // capped at 30s, min 8s. This ensures we always have ~8s for the next page
        // or for post-processing.
        const pageTimeout = Math.min(Math.max(timeLeft - 8000, 8000), 30000);

        stats.visited++;

        let result;
        try {
          result = await withTimeout(
            processPage(pg, item.url, base, stats, siteMaps, capabilitiesMaps),
            pageTimeout,
            `processPage(${item.url})`
          );
        } catch (e) {
          console.error(`[PAGE] ✗ TIMEOUT ${item.url}: ${e.message}`);
          // Page timed out — skip it and continue with next
          continue;
        }

        if (result.page) {
          // Collect contacts
          const c = result.page?.structured?.contacts;
          if (c?.emails?.length) c.emails.forEach(e => contactAgg.emails.add(String(e).trim()));
          if (c?.phones?.length) c.phones.forEach(p => contactAgg.phones.add(String(p).trim()));

          pages.push(result.page);
          stats.saved++;
        }

        // Only follow links from pages within depth limit, and only relevant internal links
        if (item.depth < MAX_DEPTH) {
          let newLinksAdded = 0;
          result.links.forEach(l => {
            const nl = normalizeUrl(l);
            if (!visited.has(nl) && !SKIP_URL_RE.test(nl)) {
              visited.add(nl);
              // Inner page links go to low priority
              lowPriorityQueue.push({ url: nl, depth: item.depth + 1 });
              newLinksAdded++;
            }
          });
          if (newLinksAdded > 0) {
            console.log(`[QUEUE] +${newLinksAdded} new URLs (depth ${item.depth + 1}) → total: ${queue.length + lowPriorityQueue.length}`);
          }
        }
      }

      await pg.close();
      await ctx.close();
    };

    await Promise.all(Array(PARALLEL_TABS).fill(0).map(() => createWorker()));

  } finally {
    await browser.close();
    console.log(`\n[CRAWL DONE] ${stats.saved}/${stats.visited} pages (max ${MAX_PAGES})`);
  }

  const postCrawlTimeLeft = deadline - Date.now();
  console.log(`[POST-CRAWL] ${Math.round(postCrawlTimeLeft / 1000)}s left for post-processing`);

  let combinedSiteMap = null;
  if (siteMaps.length > 0 && siteId) {
    console.log(`\n[SITEMAP] Building combined map from ${siteMaps.length} pages...`);

    const enrichedMaps = siteMaps.map(raw => enrichSiteMap(raw, siteId, base));
    combinedSiteMap = buildCombinedSiteMap(enrichedMaps, siteId, base);

    // ✅ FIX: Only do network ops if we have enough time (>5s)
    if (deadline - Date.now() > 5000) {
      await saveSiteMapToSupabase(combinedSiteMap);
      await sendSiteMapToWorker(combinedSiteMap);
    } else {
      console.log(`[SITEMAP] Skipping Supabase/Worker save — only ${Math.round((deadline - Date.now()) / 1000)}s left`);
    }
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

  return { pages, stats, siteMap: combinedSiteMap, capabilities: combinedCapabilities, contacts, cms };
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

               const gz = gzipSync(
          JSON.stringify({ success: true, ...result })
        );

        res.writeHead(200, {
          "Content-Type": "application/json",
          "Content-Encoding": "gzip"
        });

        res.end(gz);
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
