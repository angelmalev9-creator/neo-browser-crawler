import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

// ------------------------------
// LIMITS (ALLOW 100 PAGES)
// ------------------------------
const MAX_PAGES_DEFAULT = 60;
const MAX_PAGES_HARD = 120;

// allow more pages without exploding context later
const MAX_CHARS_PER_PAGE = 16000;
const MAX_TOTAL_CHARS = 420000;

// IMPORTANT: lowered threshold (was 450)
const MIN_PAGE_CHARS = 220;

// ------------------------------
// Selectors & scoring
// ------------------------------
const CLICK_SELECTORS = [
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
];

const KEYWORD_IMPORTANCE =
  /rooms|accommodation|suite|deluxe|apart|стаи|настаняване|апартамент|резервац|booking|reservation|pricing|цени|price|tariff|contact|контакт|location|местоположение|how-to-get|spa|restaurant|menu|меню|services|услуги|faq|въпроси|packages|пакети|offers|оферти|conditions|условия/i;

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
  if (joined.length < 200) return cleanText(text).slice(0, 7000);

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
      "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
      "gclid", "fbclid", "yclid", "mc_cid", "mc_eid"
    ];
    killParams.forEach((p) => url.searchParams.delete(p));

    if (url.search && url.search.length > 80) url.search = "";

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
    s.includes("wp-login")
  );
}

function pagePriorityScore(url, title = "", text = "") {
  const u = String(url || "").toLowerCase();
  const t = String(title || "").toLowerCase();
  const c = String(text || "").toLowerCase();
  let score = 0;

  if (KEYWORD_IMPORTANCE.test(u)) score += 70;
  if (KEYWORD_IMPORTANCE.test(t)) score += 50;

  if (u.includes("rooms") || u.includes("accommodation") || u.includes("настан") || u.includes("стаи")) score += 120;
  if (u.includes("pricing") || u.includes("цени") || u.includes("price")) score += 110;
  if (u.includes("booking") || u.includes("reservation") || u.includes("резервац")) score += 90;
  if (u.includes("contact") || u.includes("контакт")) score += 80;
  if (u.includes("location") || u.includes("местополож")) score += 70;

  if (c.includes("лв") || c.includes("лева") || c.includes("bgn") || c.includes("eur") || c.includes("евро")) score += 30;
  if (c.includes("тел") || c.includes("телефон") || c.includes("@")) score += 20;

  if (isUselessUrl(u)) score -= 999;

  score += Math.min(Math.floor((text || "").length / 1500) * 6, 36);
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

async function removeNoiseDom(page) {
  await page.evaluate(() => {
    const selectorsToRemove = [
      "header",
      "footer",
      "nav",
      "aside",

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
  });
}

async function autoExpand(page) {
  for (let i = 0; i < 5; i++) {
    await page.mouse.wheel(0, 1400);
    await page.waitForTimeout(500);
  }

  for (const selector of CLICK_SELECTORS) {
    try {
      const nodes = await page.$$(selector);
      for (let i = 0; i < Math.min(nodes.length, 70); i++) {
        const ok = await safeClick(nodes[i]);
        if (ok) await page.waitForTimeout(120);
      }
    } catch {}
  }

  for (let i = 0; i < 4; i++) {
    await page.mouse.wheel(0, 1600);
    await page.waitForTimeout(450);
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

  const best = stripped.length >= 120 ? stripped : cleaned;
  return clamp(best, MAX_CHARS_PER_PAGE);
}

async function collectLinks(page) {
  return await page.evaluate(() => {
    return Array.from(document.querySelectorAll("a[href]"))
      .map((a) => a.href)
      .filter((h) => typeof h === "string");
  });
}

function pickInternalLinks(allLinks, rootUrl) {
  const base = new URL(rootUrl);

  const sameOrigin = allLinks
    .map(normalizeUrl)
    .filter(Boolean)
    .filter((l) => {
      try {
        const u = new URL(l);
        return u.origin === base.origin;
      } catch {
        return false;
      }
    })
    .filter((l) => !isUselessUrl(l));

  const unique = Array.from(new Set(sameOrigin));

  const sorted = unique
    .map((l) => ({
      url: l,
      score: (KEYWORD_IMPORTANCE.test(l) ? 80 : 0) + (l.length < 140 ? 10 : 0),
    }))
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

        // networkidle дава повече шанс да се зареди реалното съдържание
        await page.goto(norm, { waitUntil: "networkidle", timeout: 60000 });
        await page.waitForTimeout(900);

        await removeNoiseDom(page);
        await autoExpand(page);

        const title = cleanText(await page.title());
        const text = await extractMainText(page);

        // ✅ keep even smaller pages now
        if (text && text.length >= MIN_PAGE_CHARS) {
          const remaining = MAX_TOTAL_CHARS - totalChars;
          const finalText = clamp(text, Math.max(0, remaining));
          pages.push({ url: norm, title, text: finalText });
          totalChars += finalText.length;
        }

        // ✅ always collect links even if text is small
        const links = await collectLinks(page);
        const internalLinks = pickInternalLinks(links, url);

        for (const l of internalLinks) {
          const ln = normalizeUrl(l);
          if (!ln) continue;
          if (visited.has(ln)) continue;
          if (queue.length > 900) break;
          queue.push(ln);
        }
      } catch (err) {
        // ✅ DO NOT ignore anymore
        console.log("[CRAWL PAGE ERROR]", norm, String(err?.message || err));
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
    console.log("[LIMITS] MAX_CHARS_PER_PAGE:", MAX_CHARS_PER_PAGE);
    console.log("[LIMITS] MAX_TOTAL_CHARS:", MAX_TOTAL_CHARS);
    console.log("[LIMITS] MIN_PAGE_CHARS:", MIN_PAGE_CHARS);
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
