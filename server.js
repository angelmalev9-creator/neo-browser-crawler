import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

// ------------------------------
// CONFIG LIMITS
// ------------------------------
const MAX_PAGES_DEFAULT = 30;
const MAX_PAGES_HARD = 120;

const MAX_CHARS_PER_PAGE = 18000;
const MAX_TOTAL_CHARS = 420000;
const MIN_PAGE_CHARS = 180;

// ------------------------------
// Selectors
// ------------------------------
const CLICK_SELECTORS = [
  "a[href]",
  "button",
  "[role='button']",
  "details > summary",

  "[aria-expanded='false']",
  "[data-bs-toggle]",
  ".accordion button",
  ".accordion-header",
  ".accordion-title",
  ".dropdown-toggle",
  ".menu-toggle",
  ".navbar-toggler",

  "[role='tab']",
  "[aria-controls]",
  ".tabs button",
  ".tab",
  "[data-tab]",
  "[data-state='closed']",
  "[data-radix-collection-item]",

  "button:has-text('Виж')",
  "button:has-text('Още')",
  "button:has-text('Прочети')",
  "button:has-text('Разгледай')",
  "button:has-text('Read more')",
  "button:has-text('More')",

  "a:has-text('Цени')",
  "a:has-text('Услуги')",
  "a:has-text('Стаи')",
  "a:has-text('Настаняване')",
  "a:has-text('Контакт')",
  "a:has-text('Резервация')",
];

const KEYWORD_IMPORTANCE =
  /rooms|accommodation|suite|deluxe|apart|стаи|настаняване|апартамент|резервац|booking|reservation|pricing|цени|price|tariff|contact|контакт|location|местоположение|how-to-get|spa|restaurant|menu|меню|services|услуги|faq|въпроси|packages|пакети|offers|оферти|conditions|условия|gallery|галерия|about|за-нас/i;

// ------------------------------
// Helpers
// ------------------------------
function cleanText(t = "") {
  return String(t)
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/\t/g, " ")
    .replace(/[ \u00A0]+/g, " ")
    .replace(/\s+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function stripBoilerplate(text = "") {
  if (!text) return "";

  const lines = text
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);

  const seen = new Set();
  const filtered = [];

  const badLine = (l) => {
    const s = l.toLowerCase();
    if (l.length <= 2) return true;

    const bad =
      s.includes("cookie") ||
      s.includes("cookies") ||
      s.includes("бисквит") ||
      s.includes("gdpr") ||
      s.includes("privacy") ||
      s.includes("terms") ||
      s.includes("policy") ||
      s.includes("лични данни") ||
      s.includes("условия") ||
      s.includes("политика") ||
      s.includes("consent") ||
      s.includes("preferences") ||
      s.includes("accept") ||
      s.includes("decline") ||
      s.includes("manage") ||
      s.includes("all rights reserved") ||
      s.includes("правата са запазени");

    if (bad) return true;

    if (s === "facebook" || s === "instagram" || s === "linkedin") return true;
    if (/^(ok|yes|no|close)$/i.test(s)) return true;

    return false;
  };

  for (const l of lines) {
    if (badLine(l)) continue;

    const key = l.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);

    filtered.push(l);
  }

  const joined = filtered.join("\n");
  if (joined.length < 200) return cleanText(text).slice(0, 8000);

  return cleanText(joined);
}

function clamp(str, max) {
  if (!str) return "";
  if (str.length <= max) return str;
  return str.slice(0, max);
}

function json(res, status, obj) {
  res.writeHead(status, { ...corsHeaders, "Content-Type": "application/json" });
  res.end(JSON.stringify(obj));
}

function normalizeUrl(u) {
  try {
    const url = new URL(u);
    url.hash = "";

    const killParams = [
      "utm_source",
      "utm_medium",
      "utm_campaign",
      "utm_term",
      "utm_content",
      "gclid",
      "fbclid",
      "yclid",
      "mc_cid",
      "mc_eid",
    ];
    killParams.forEach((p) => url.searchParams.delete(p));

    if (url.search && url.search.length > 120) url.search = "";

    return url.toString();
  } catch {
    return null;
  }
}

function isUselessUrl(u = "") {
  const s = String(u).toLowerCase();
  return (
    s.includes("privacy") ||
    s.includes("cookies") ||
    s.includes("cookie") ||
    s.includes("terms") ||
    s.includes("gdpr") ||
    s.includes("policy") ||
    s.includes("legal") ||
    s.includes("consent") ||
    s.includes("/cart") ||
    s.includes("/checkout") ||
    s.includes("/login") ||
    s.includes("/account") ||
    s.includes("wp-login") ||
    s.includes("/wp-admin") ||
    s.includes("/tag/") ||
    s.includes("/author/")
  );
}

function normalizeHost(host) {
  return String(host || "").replace(/^www\./i, "").toLowerCase().trim();
}

function pagePriorityScore(url, title = "", text = "") {
  const u = String(url || "").toLowerCase();
  const t = String(title || "").toLowerCase();
  const c = String(text || "").toLowerCase();

  let score = 0;

  if (KEYWORD_IMPORTANCE.test(u)) score += 70;
  if (KEYWORD_IMPORTANCE.test(t)) score += 50;

  if (u.includes("rooms") || u.includes("accommodation") || u.includes("настан") || u.includes("стаи")) score += 140;
  if (u.includes("pricing") || u.includes("цени") || u.includes("price")) score += 130;
  if (u.includes("booking") || u.includes("reservation") || u.includes("резервац")) score += 100;
  if (u.includes("contact") || u.includes("контакт")) score += 90;
  if (u.includes("location") || u.includes("местополож")) score += 80;

  if (c.includes("лв") || c.includes("лева") || c.includes("bgn") || c.includes("eur") || c.includes("евро"))
    score += 35;

  if (c.includes("тел") || c.includes("телефон") || c.includes("@")) score += 25;

  if (isUselessUrl(u)) score -= 999;

  score += Math.min(Math.floor((text || "").length / 1400) * 6, 60);

  return score;
}

async function safeClick(el) {
  try {
    await el.click({ timeout: 900, force: true });
    return true;
  } catch {
    return false;
  }
}

// ✅ no :has-text() inside evaluate
async function removeNoiseDom(page) {
  await page.evaluate(() => {
    const selectorsToRemove = [
      "#onetrust-banner-sdk",
      "#onetrust-consent-sdk",
      ".ot-sdk-container",
      ".ot-sdk-row",
      ".cookie",
      ".cookies",
      ".cookie-banner",
      ".cookie-consent",
      ".cookie-policy",
      ".consent",
      ".gdpr",
      ".privacy",
      ".terms",
      "[id*='cookie']",
      "[class*='cookie']",
      "[id*='consent']",
      "[class*='consent']",
      "[id*='gdpr']",
      "[class*='gdpr']",

      ".modal",
      ".popup",
      ".overlay",
      "[role='dialog']",
      "[aria-modal='true']",

      "iframe[src*='tawk']",
      "iframe[src*='intercom']",
      "iframe[src*='crisp']",
      "iframe[src*='zendesk']",
      "iframe[src*='livechat']",
    ];

    for (const sel of selectorsToRemove) {
      document.querySelectorAll(sel).forEach((el) => el.remove());
    }

    const closeWords = ["затвори", "приемам", "съгласен", "ок", "close", "accept", "agree", "×", "x"];
    const buttons = Array.from(document.querySelectorAll("button, [role='button'], a, div"));

    for (const el of buttons) {
      const txt = (el.textContent || "").trim().toLowerCase();
      if (!txt) continue;
      if (closeWords.some((w) => txt === w || txt.includes(w))) {
        try {
          el.click();
        } catch {}
      }
    }
  });
}

async function autoExpand(page) {
  for (let i = 0; i < 7; i++) {
    await page.mouse.wheel(0, 1600);
    await page.waitForTimeout(550);
  }

  for (const selector of CLICK_SELECTORS) {
    try {
      const nodes = await page.$$(selector);
      for (let i = 0; i < Math.min(nodes.length, 80); i++) {
        const ok = await safeClick(nodes[i]);
        if (ok) await page.waitForTimeout(110);
      }
    } catch {}
  }

  for (let i = 0; i < 5; i++) {
    await page.mouse.wheel(0, 1900);
    await page.waitForTimeout(520);
  }
}

async function extractMainText(page) {
  const raw = await page.evaluate(() => {
    const candidates = [
      document.querySelector("main"),
      document.querySelector("article"),
      document.querySelector("[role='main']"),
      document.querySelector("#content"),
      document.querySelector(".content"),
      document.querySelector(".main-content"),
      document.querySelector("#main"),
      document.querySelector(".container"),
    ].filter(Boolean);

    const el = candidates[0] || document.body;
    return el?.innerText || "";
  });

  const cleaned = cleanText(raw);
  const stripped = stripBoilerplate(cleaned);

  return clamp(stripped, MAX_CHARS_PER_PAGE);
}

// ✅ FIXED: only collect http/https links
async function collectLinks(page) {
  return await page.evaluate(() => {
    const out = new Set();

    const add = (u) => {
      try {
        const abs = new URL(u, location.href).toString();
        if (abs.startsWith("http://") || abs.startsWith("https://")) out.add(abs);
      } catch {}
    };

    document.querySelectorAll("a[href]").forEach((a) => {
      const href = a.getAttribute("href");
      if (!href) return;
      if (href.startsWith("#")) return;
      if (href.toLowerCase().startsWith("javascript:")) return;
      if (href.toLowerCase().startsWith("mailto:")) return;
      if (href.toLowerCase().startsWith("tel:")) return;
      add(href);
    });

    // data-href / custom routers
    document.querySelectorAll("[data-href]").forEach((el) => {
      const v = el.getAttribute("data-href");
      if (v) add(v);
    });

    // onclick router patterns
    document.querySelectorAll("[onclick]").forEach((el) => {
      const v = el.getAttribute("onclick") || "";
      const m1 = v.match(/location\.href\s*=\s*['"]([^'"]+)['"]/i);
      const m2 = v.match(/window\.location\s*=\s*['"]([^'"]+)['"]/i);
      const m = m1 || m2;
      if (m?.[1]) add(m[1]);
    });

    return Array.from(out);
  });
}

function pickInternalLinks(allLinks, rootUrl) {
  const base = new URL(rootUrl);
  const baseHost = normalizeHost(base.hostname);

  const internal = allLinks
    .map(normalizeUrl)
    .filter(Boolean)
    .filter((l) => {
      try {
        const u = new URL(l);

        // ✅ accept either same origin OR same hostname (www mismatch)
        const sameHost = normalizeHost(u.hostname) === baseHost;
        const sameOrigin = u.origin === base.origin;

        if (!sameHost && !sameOrigin) return false;
        if (!(u.protocol === "http:" || u.protocol === "https:")) return false;
        if (isUselessUrl(u.toString())) return false;

        return true;
      } catch {
        return false;
      }
    });

  const unique = Array.from(new Set(internal));

  const sorted = unique
    .map((l) => {
      const s = l.toLowerCase();
      const score =
        (KEYWORD_IMPORTANCE.test(s) ? 90 : 0) +
        (s.includes(baseHost) ? 10 : 0) +
        (s.length < 160 ? 8 : 0);
      return { url: l, score };
    })
    .sort((a, b) => b.score - a.score)
    .map((x) => x.url);

  return sorted;
}

// ------------------------------
// MAIN CRAWL (BFS)
// ------------------------------
async function crawlSite(url, maxPages = MAX_PAGES_DEFAULT) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext({
    viewport: { width: 1280, height: 720 },
    locale: "bg-BG",
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
  });

  const pages = [];
  const visited = new Set();
  let totalChars = 0;

  try {
    const queue = [url];

    while (queue.length && pages.length < maxPages && totalChars < MAX_TOTAL_CHARS) {
      const link = queue.shift();
      const norm = normalizeUrl(link);
      if (!norm) continue;
      if (visited.has(norm)) continue;
      if (isUselessUrl(norm)) continue;

      visited.add(norm);

      let page;
      try {
        page = await context.newPage();
        page.setDefaultTimeout(60000);

        await page.goto(norm, { waitUntil: "domcontentloaded", timeout: 60000 });
        await page.waitForTimeout(1400);

        await autoExpand(page);

        // collect links first
        const links = await collectLinks(page);

        // DEBUG: show what links we got
        console.log("   rawLinksSample:", links.slice(0, 20));

        await removeNoiseDom(page);

        const title = cleanText(await page.title());
        const text = await extractMainText(page);

        console.log("[PAGE]", norm);
        console.log("   title:", (title || "").slice(0, 70));
        console.log("   textLen:", (text || "").length);
        console.log("   linksFound:", links.length);

        if (!text || text.length < MIN_PAGE_CHARS) {
          console.log("   SKIP page (too short):", norm, "len=", (text || "").length);
        } else {
          const remaining = MAX_TOTAL_CHARS - totalChars;
          const finalText = clamp(text, Math.max(0, remaining));

          pages.push({ url: norm, title, text: finalText });
          totalChars += finalText.length;

          console.log("   ✅ ADDED. pages:", pages.length, "totalChars:", totalChars);
        }

        const internalLinks = pickInternalLinks(links, url);
        console.log("   internalLinks:", internalLinks.length);
        console.log("   visited:", visited.size, "queue:", queue.length);

        for (const l of internalLinks) {
          const ln = normalizeUrl(l);
          if (!ln) continue;
          if (visited.has(ln)) continue;
          if (queue.length > 900) break;
          queue.push(ln);
        }
      } catch (e) {
        console.log("   ❌ ERROR page:", norm, e?.message || String(e));
      } finally {
        try {
          if (page) await page.close();
        } catch {}
      }
    }

    pages.sort((a, b) => pagePriorityScore(b.url, b.title, b.text) - pagePriorityScore(a.url, a.title, a.text));
    return pages.slice(0, maxPages);
  } finally {
    await browser.close();
  }
}

// ------------------------------
// HTTP SERVER
// ------------------------------
const server = http.createServer(async (req, res) => {
  if (req.method === "OPTIONS") {
    res.writeHead(200, corsHeaders);
    return res.end();
  }

  if (req.url !== "/crawl") {
    return json(res, 404, { success: false, error: "Not found" });
  }

  if (req.method !== "POST") {
    return json(res, 405, { success: false, error: "Method not allowed" });
  }

  try {
    const rawBody = await new Promise((resolve, reject) => {
      let data = "";
      req.on("data", (chunk) => (data += chunk));
      req.on("end", () => resolve(data));
      req.on("error", reject);
    });

    const body = rawBody ? JSON.parse(rawBody) : {};
    const { url, token, maxPages } = body;

    if (!url) return json(res, 400, { success: false, error: "Missing url" });

    const expectedToken = process.env.CRAWLER_TOKEN;
    if (expectedToken && token !== expectedToken) {
      return json(res, 401, { success: false, error: "Unauthorized" });
    }

    const resolvedMaxPages = Math.min(Math.max(Number(maxPages || MAX_PAGES_DEFAULT), 6), MAX_PAGES_HARD);

    console.log("==================================================");
    console.log("[CRAWL] url:", url);
    console.log("[CRAWL] maxPages:", resolvedMaxPages);
    console.log("[LIMITS] MIN_PAGE_CHARS:", MIN_PAGE_CHARS);
    console.log("[LIMITS] MAX_CHARS_PER_PAGE:", MAX_CHARS_PER_PAGE);
    console.log("[LIMITS] MAX_TOTAL_CHARS:", MAX_TOTAL_CHARS);
    console.log("==================================================");

    const pages = await crawlSite(url, resolvedMaxPages);

    return json(res, 200, {
      success: true,
      root: url,
      pagesCount: pages.length,
      totalChars: pages.reduce((a, p) => a + (p.text?.length || 0), 0),
      pages,
    });
  } catch (e) {
    console.error("[CRAWL ERROR]", e);
    return json(res, 500, {
      success: false,
      error: e?.message || "Crawler error",
    });
  }
});

server.listen(PORT, () => {
  console.log("Crawler listening on :" + PORT);
});
