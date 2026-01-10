import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

// ✅ expanded + safer selectors
const CLICK_SELECTORS = [
  // generic
  "button",
  "[role='button']",
  "details > summary",

  // accordion/dropdowns
  "[aria-expanded='false']",
  "[aria-expanded='true']",
  "[data-bs-toggle]",
  ".accordion button",
  ".accordion-header",
  ".accordion-title",
  ".dropdown-toggle",
  ".menu-toggle",
  ".navbar-toggler",

  // tabs
  "[role='tab']",
  ".tabs button",
  ".tab",
  "[data-tab]",
  "[data-state='closed']",

  // common “read more”
  "button:has-text('Виж')",
  "button:has-text('Още')",
  "button:has-text('Прочети')",
  "button:has-text('Read more')",
  "button:has-text('More')",
];

const KEYWORD_IMPORTANCE =
  /rooms|accommodation|suite|deluxe|apart|стаи|настаняване|апартамент|резервац|booking|reservation|pricing|цени|price|tariff|contact|контакт|location|местоположение|how-to-get|spa|restaurant|menu|меню|services|услуги|faq|въпроси/i;

// ---------- helpers ----------
function cleanText(t = "") {
  return String(t)
    .replace(/[\u200B-\u200D\uFEFF]/g, "") // zero-width
    .replace(/\t/g, " ")
    .replace(/\s+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \u00A0]+/g, " ")
    .trim();
}

function stripBoilerplate(text = "") {
  if (!text) return "";

  const lines = text
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);

  const badLine = (l) => {
    const s = l.toLowerCase();

    // too short = menu items
    if (l.length <= 2) return true;

    // cookie/GDPR/terms
    const bad =
      s.includes("cookies") ||
      s.includes("cookie") ||
      s.includes("бисквит") ||
      s.includes("gdpr") ||
      s.includes("privacy") ||
      s.includes("terms") ||
      s.includes("лични данни") ||
      s.includes("политика") ||
      s.includes("условия") ||
      s.includes("consent") ||
      s.includes("preferences") ||
      s.includes("accept") ||
      s.includes("decline") ||
      s.includes("manage") ||
      s.includes("правата са запазени") ||
      s.includes("all rights reserved");

    if (bad) return true;

    // footer noise
    if (s === "facebook" || s === "instagram" || s === "linkedin") return true;

    return false;
  };

  const filtered = lines.filter((l) => !badLine(l));
  const joined = filtered.join("\n");

  // fallback safety
  if (joined.length < 250) return cleanText(text).slice(0, 5000);

  return cleanText(joined);
}

function json(res, status, obj) {
  res.writeHead(status, { ...corsHeaders, "Content-Type": "application/json" });
  res.end(JSON.stringify(obj));
}

function normalizeUrl(u) {
  try {
    const url = new URL(u);
    url.hash = "";
    return url.toString();
  } catch {
    return null;
  }
}

function isUselessUrl(u = "") {
  const s = u.toLowerCase();
  return (
    s.includes("privacy") ||
    s.includes("cookies") ||
    s.includes("terms") ||
    s.includes("gdpr") ||
    s.includes("policy") ||
    s.includes("legal") ||
    s.includes("consent")
  );
}

function pagePriorityScore(url, title = "", text = "") {
  const u = String(url || "").toLowerCase();
  const t = String(title || "").toLowerCase();
  const c = String(text || "").toLowerCase();
  let score = 0;

  if (KEYWORD_IMPORTANCE.test(u)) score += 60;
  if (KEYWORD_IMPORTANCE.test(t)) score += 40;

  // extra hotel boosts
  if (u.includes("rooms") || u.includes("sta") || u.includes("accommodation") || u.includes("настан")) score += 80;
  if (u.includes("booking") || u.includes("reservation") || u.includes("резервац")) score += 70;
  if (u.includes("pricing") || u.includes("цени") || u.includes("price")) score += 70;
  if (u.includes("contact") || u.includes("контакт")) score += 60;
  if (u.includes("location") || u.includes("местополож")) score += 55;

  // content hints
  if (c.includes("лв") || c.includes("лева") || c.includes("eur") || c.includes("евро")) score += 25;
  if (c.includes("тел") || c.includes("телефон") || c.includes("@")) score += 20;

  // penalize legal pages
  if (isUselessUrl(u)) score -= 999;

  // prefer long-ish useful pages
  score += Math.min(Math.floor((text || "").length / 2000) * 5, 30);

  return score;
}

async function safeClick(el) {
  try {
    await el.click({ timeout: 800, force: true });
    return true;
  } catch {
    return false;
  }
}

// ✅ remove popups/cookie banners + nav/footer before extracting text
async function removeNoiseDom(page) {
  await page.evaluate(() => {
    const selectorsToRemove = [
      "header",
      "footer",
      "nav",
      "aside",

      // cookie banners / GDPR
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

      // modal/popup overlays
      ".modal",
      ".popup",
      ".overlay",
      "[role='dialog']",
      "[aria-modal='true']",
    ];

    for (const sel of selectorsToRemove) {
      document.querySelectorAll(sel).forEach((el) => el.remove());
    }
  });
}

// ✅ click-expand + scroll for dynamic content
async function autoExpand(page) {
  // initial scroll (lazy-load)
  for (let i = 0; i < 6; i++) {
    await page.mouse.wheel(0, 1400);
    await page.waitForTimeout(650);
  }

  // click common UI elements
  for (const selector of CLICK_SELECTORS) {
    try {
      const nodes = await page.$$(selector);
      for (let i = 0; i < Math.min(nodes.length, 40); i++) {
        const ok = await safeClick(nodes[i]);
        if (ok) await page.waitForTimeout(160);
      }
    } catch {
      // ignore
    }
  }

  // final scroll
  for (let i = 0; i < 4; i++) {
    await page.mouse.wheel(0, 1600);
    await page.waitForTimeout(600);
  }
}

// ✅ extract only meaningful content (main/article)
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
    ].filter(Boolean);

    const el = candidates[0] || document.body;
    return el?.innerText || "";
  });

  const cleaned = cleanText(raw);
  return stripBoilerplate(cleaned);
}

// ✅ link collection
async function collectLinks(page) {
  const links = await page.evaluate(() => {
    return Array.from(document.querySelectorAll("a[href]"))
      .map((a) => a.href)
      .filter((h) => typeof h === "string");
  });

  return links;
}

function pickInternalLinks(allLinks, rootUrl, maxPages = 30) {
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

  // unique
  const unique = Array.from(new Set(sameOrigin));

  // prioritize by keywords in URL first
  const important = unique
    .map((l) => ({
      url: l,
      score: KEYWORD_IMPORTANCE.test(l) ? 50 : 0,
    }))
    .sort((a, b) => b.score - a.score)
    .map((x) => x.url);

  // take more pages (not 12 only!)
  return important.slice(0, maxPages);
}

// ✅ MAIN crawl
async function crawlSite(url, maxPages = 30) {
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

  try {
    // 1) root page
    const page = await context.newPage();
    page.setDefaultTimeout(45000);

    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45000 });
    await page.waitForTimeout(1500);

    await autoExpand(page);
    await removeNoiseDom(page);

    const title = cleanText(await page.title());
    const text = await extractMainText(page);

    pages.push({ url, title, text });
    visited.add(normalizeUrl(url));

    // 2) collect links
    const links = await collectLinks(page);
    const internalLinks = pickInternalLinks(links, url, maxPages + 8);

    // 3) crawl internal pages
    for (const link of internalLinks) {
      const norm = normalizeUrl(link);
      if (!norm) continue;
      if (visited.has(norm)) continue;
      if (normalizeUrl(link) === normalizeUrl(url)) continue;

      visited.add(norm);

      try {
        const p2 = await context.newPage();
        p2.setDefaultTimeout(45000);

        await p2.goto(link, { waitUntil: "domcontentloaded", timeout: 45000 });
        await p2.waitForTimeout(1200);

        await autoExpand(p2);
        await removeNoiseDom(p2);

        const t2 = cleanText(await p2.title());
        const tx2 = await extractMainText(p2);

        // skip useless content
        if (tx2 && tx2.length > 450 && !isUselessUrl(link)) {
          pages.push({ url: link, title: t2, text: tx2 });
        }

        await p2.close();
      } catch {
        // ignore page errors
      }

      if (pages.length >= maxPages) break;
    }

    // 4) final sort by importance (so knowledge is better)
    pages.sort((a, b) => pagePriorityScore(b.url, b.title, b.text) - pagePriorityScore(a.url, a.title, a.text));

    return pages.slice(0, maxPages);
  } finally {
    await browser.close();
  }
}

// ---------- HTTP server ----------
const server = http.createServer(async (req, res) => {
  // CORS preflight
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

    // token auth
    const expectedToken = process.env.CRAWLER_TOKEN;
    if (expectedToken && token !== expectedToken) {
      return json(res, 401, { success: false, error: "Unauthorized" });
    }

    const resolvedMaxPages = Math.min(Math.max(Number(maxPages || 30), 6), 60);

    console.log("==================================================");
    console.log("[CRAWL] url:", url);
    console.log("[CRAWL] maxPages:", resolvedMaxPages);
    console.log("[ENV] NODE_ENV =", process.env.NODE_ENV);
    console.log("==================================================");

    const pages = await crawlSite(url, resolvedMaxPages);

    return json(res, 200, {
      success: true,
      root: url,
      pagesCount: pages.length,
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
