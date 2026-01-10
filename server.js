import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const CLICK_SELECTORS = [
  "button",
  "[role='button']",
  "details > summary",
  "[aria-expanded='false']",
  "[data-bs-toggle]",

  // common UI libs
  ".accordion button",
  ".accordion-header",
  ".accordion-title",
  ".tabs button",
  ".tab",
  ".dropdown-toggle",
  ".menu-toggle",
  ".navbar-toggler",

  // tailwind-ish
  "[data-state='closed']",
];

function cleanText(t = "") {
  return String(t)
    .replace(/[\u200B-\u200D\uFEFF]/g, "") // zero-width
    .replace(/\s+/g, " ")
    .trim();
}

function json(res, status, obj) {
  res.writeHead(status, { ...corsHeaders, "Content-Type": "application/json" });
  res.end(JSON.stringify(obj));
}

async function safeClick(el) {
  try {
    await el.click({ timeout: 600 });
    return true;
  } catch {
    return false;
  }
}

async function autoExpand(page) {
  // initial scroll (lazy-load)
  for (let i = 0; i < 6; i++) {
    await page.mouse.wheel(0, 1500);
    await page.waitForTimeout(700);
  }

  // click common UI elements
  for (const selector of CLICK_SELECTORS) {
    try {
      const nodes = await page.$$(selector);
      for (let i = 0; i < Math.min(nodes.length, 30); i++) {
        const ok = await safeClick(nodes[i]);
        if (ok) await page.waitForTimeout(220);
      }
    } catch {
      // ignore
    }
  }

  // final scroll to reveal content
  for (let i = 0; i < 3; i++) {
    await page.mouse.wheel(0, 1500);
    await page.waitForTimeout(650);
  }
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

function pickImportantLinks(allLinks, rootUrl) {
  const base = new URL(rootUrl);

  const keywords =
    /about|за-нас|services|услуги|pricing|цени|price|contact|контакти|menu|меню|booking|reservation|appointment|запази|schedule|calendar|hours|работно-време|time/i;

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
    });

  // prioritize important pages
  const important = sameOrigin.filter((l) => keywords.test(l));

  // unique
  const unique = Array.from(new Set(important));

  // if we have too few, take some extra internal pages
  if (unique.length < 6) {
    const extras = Array.from(new Set(sameOrigin)).slice(0, 20);
    for (const ex of extras) {
      if (!unique.includes(ex)) unique.push(ex);
      if (unique.length >= 12) break;
    }
  }

  return unique;
}

async function crawlSite(url, maxPages = 12) {
  const browser = await chromium.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
    ],
  });

  const context = await browser.newContext({
    viewport: { width: 1280, height: 720 },
    locale: "bg-BG",
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
  });

  const pages = [];

  try {
    // 1) root page
    const page = await context.newPage();
    page.setDefaultTimeout(45000);

    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45000 });
    await page.waitForTimeout(2000);

    await autoExpand(page);

    const title = cleanText(await page.title());
    const text = cleanText(
      await page.evaluate(() => document.body?.innerText || "")
    );

    pages.push({ url, title, text });

    // 2) collect links
    const links = await page.evaluate(() => {
      return Array.from(document.querySelectorAll("a[href]"))
        .map((a) => a.href)
        .filter((h) => typeof h === "string");
    });

    const importantLinks = pickImportantLinks(links, url).slice(0, maxPages);

    // 3) crawl important internal pages
    for (const link of importantLinks) {
      // skip root if duplicated
      if (normalizeUrl(link) === normalizeUrl(url)) continue;

      try {
        const p2 = await context.newPage();
        p2.setDefaultTimeout(45000);

        await p2.goto(link, { waitUntil: "domcontentloaded", timeout: 45000 });
        await p2.waitForTimeout(1500);

        await autoExpand(p2);

        const t2 = cleanText(await p2.title());
        const tx2 = cleanText(
          await p2.evaluate(() => document.body?.innerText || "")
        );

        if (tx2.length > 400) {
          pages.push({ url: link, title: t2, text: tx2 });
        }

        await p2.close();
      } catch {
        // ignore single-page errors
      }

      if (pages.length >= maxPages) break;
    }

    return pages;
  } finally {
    await browser.close();
  }
}

const server = http.createServer(async (req, res) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    res.writeHead(200, corsHeaders);
    return res.end();
  }

  // routing
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

    if (!url) {
      return json(res, 400, { success: false, error: "Missing url" });
    }

    // token auth
    const expectedToken = process.env.CRAWLER_TOKEN;
    if (expectedToken && token !== expectedToken) {
      return json(res, 401, { success: false, error: "Unauthorized" });
    }

    // debug important envs
    console.log("==================================================");
    console.log("[CRAWL] url:", url);
    console.log("[CRAWL] maxPages:", maxPages || 12);
    console.log("[ENV] PLAYWRIGHT_BROWSERS_PATH =", process.env.PLAYWRIGHT_BROWSERS_PATH);
    console.log("[ENV] NODE_ENV =", process.env.NODE_ENV);
    console.log("==================================================");

    const pages = await crawlSite(url, Number(maxPages || 12));

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
