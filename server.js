import http from "http";
import { chromium } from "playwright";
import { parseStringPromise } from "xml2js";

const PORT = Number(process.env.PORT || 10000);
const MAX_PAGES_DEFAULT = 80;
const MAX_PAGES_HARD = 200;

const IGNORE_RE = /privacy|terms|cookies|policy|login|register|cart|checkout/i;
const IMPORTANT_RE = /uslug|service|price|ceni|tseni|about|za-nas|contact|kontakti|booking|reservation/i;

const clean = (t = "") =>
  t.replace(/[\u200B-\u200D\uFEFF]/g, "").replace(/\s+/g, " ").trim();

/* =========================
   SITEMAP DISCOVERY
========================= */
async function discoverFromSitemap(baseUrl) {
  const candidates = [
    "/sitemap.xml",
    "/wp-sitemap.xml",
    "/sitemap_index.xml",
  ];

  for (const path of candidates) {
    try {
      const res = await fetch(baseUrl + path);
      if (!res.ok) continue;

      const xml = await res.text();
      const parsed = await parseStringPromise(xml);
      const urls =
        parsed?.urlset?.url?.map((u) => u.loc[0]) ||
        parsed?.sitemapindex?.sitemap?.map((s) => s.loc[0]) ||
        [];

      if (urls.length) {
        console.log(`[SITEMAP] Found ${urls.length} urls`);
        return urls;
      }
    } catch {}
  }

  return [];
}

/* =========================
   MAIN CRAWLER
========================= */
async function crawl(startUrl, maxPages) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();
  const visited = new Set();
  const queue = [];
  const pages = [];

  const base = new URL(startUrl).origin;

  // 1️⃣ Sitemap first
  const sitemapUrls = await discoverFromSitemap(base);
  sitemapUrls.forEach((u) => queue.push(u));

  // fallback if no sitemap
  if (!queue.length) queue.push(startUrl);

  while (queue.length && pages.length < maxPages) {
    const url = queue.shift();
    if (!url || visited.has(url)) continue;
    visited.add(url);

    let page;
    try {
      page = await context.newPage();
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      await page.waitForTimeout(1500);

      // scroll for SPA / lazy
      for (let i = 0; i < 5; i++) {
        await page.evaluate(() => window.scrollBy(0, 1600));
        await page.waitForTimeout(400);
      }

      const title = clean(await page.title());
      const content = clean(await page.evaluate(() => document.body?.innerText || ""));

      if (content.length > 400 && !IGNORE_RE.test(url)) {
        pages.push({ url, title, content });
        console.log(`[CRAWL] ✔ ${pages.length}: ${url}`);
      }

      // discover links from DOM
      const links = await page.evaluate(() =>
        Array.from(document.querySelectorAll("a[href]"))
          .map((a) => a.href)
          .filter(Boolean)
      );

      for (const l of links) {
        try {
          const u = new URL(l);
          if (
            u.origin === base &&
            !visited.has(u.href) &&
            !IGNORE_RE.test(u.href)
          ) {
            if (IMPORTANT_RE.test(u.href)) {
              queue.unshift(u.href); // priority
            } else {
              queue.push(u.href);
            }
          }
        } catch {}
      }
    } catch {}
    finally {
      if (page) await page.close();
    }
  }

  await browser.close();
  return pages;
}

/* =========================
   SERVER
========================= */
http.createServer((req, res) => {
  if (req.method !== "POST") {
    res.writeHead(405);
    return res.end();
  }

  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", async () => {
    try {
      const { url, maxPages } = JSON.parse(body || {});
      if (!url) throw new Error("Missing url");

      const limit = Math.min(Number(maxPages) || MAX_PAGES_DEFAULT, MAX_PAGES_HARD);
      const pages = await crawl(url, limit);

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        success: true,
        pagesCount: pages.length,
        pages,
      }));
    } catch (e) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: e.message }));
    }
  });
}).listen(PORT, () => {
  console.log("Crawler running on", PORT);
});
