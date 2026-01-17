import http from "http";
import { chromium } from "playwright";
import xml2js from "xml2js";

const PORT = Number(process.env.PORT || 10000);
const MAX_PAGES_DEFAULT = 40;
const MAX_PAGES_HARD = 120;

const BLOCK_RE = /privacy|cookie|terms|policy|login|register|gdpr/i;

const clean = (t = "") =>
  t
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/\s+/g, " ")
    .trim();

/* =========================
   SITEMAP PARSER
========================= */
async function extractUrlsFromSitemap(sitemapUrl) {
  try {
    const res = await fetch(sitemapUrl);
    if (!res.ok) return [];
    const xml = await res.text();
    const parsed = await xml2js.parseStringPromise(xml);

    const urls = [];

    // normal sitemap
    if (parsed.urlset?.url) {
      for (const u of parsed.urlset.url) {
        if (u.loc?.[0]) urls.push(u.loc[0]);
      }
    }

    // sitemap index (nested)
    if (parsed.sitemapindex?.sitemap) {
      for (const sm of parsed.sitemapindex.sitemap) {
        if (sm.loc?.[0]) {
          const nested = await extractUrlsFromSitemap(sm.loc[0]);
          urls.push(...nested);
        }
      }
    }

    return urls;
  } catch {
    return [];
  }
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

  const origin = new URL(startUrl).origin;

  console.log("[CRAWL] Start:", startUrl);

  // 1️⃣ Load sitemap URLs first
  const sitemapUrls = await extractUrlsFromSitemap(`${origin}/sitemap.xml`);
  console.log("[SITEMAP] Found:", sitemapUrls.length);

  for (const u of sitemapUrls) {
    if (u.startsWith(origin) && !BLOCK_RE.test(u)) {
      queue.push(u);
    }
  }

  // fallback: homepage
  if (!queue.length) queue.push(startUrl);

  // 2️⃣ Crawl queue
  while (queue.length && pages.length < maxPages) {
    const url = queue.shift();
    if (!url || visited.has(url) || BLOCK_RE.test(url)) continue;
    visited.add(url);

    let page;
    try {
      page = await context.newPage();
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });

      // scroll for SPA / lazy content
      for (let i = 0; i < 5; i++) {
        await page.evaluate(() => window.scrollBy(0, 1600));
        await page.waitForTimeout(400);
      }

      const title = clean(await page.title());
      const content = clean(
        await page.evaluate(() => document.body?.innerText || "")
      );

      if (content.length > 300) {
        pages.push({ url, title, content });
        console.log("[CRAWL] Added:", pages.length, url);
      }

      // discover internal links dynamically
      const links = await page.evaluate(() =>
        Array.from(document.querySelectorAll("a[href]"))
          .map((a) => a.href)
          .filter(Boolean)
      );

      for (const l of links) {
        try {
          const u = new URL(l);
          if (
            u.origin === origin &&
            !visited.has(u.href) &&
            !BLOCK_RE.test(u.href) &&
            queue.length < maxPages * 2
          ) {
            queue.push(u.href);
          }
        } catch {}
      }
    } catch {
      // skip broken pages
    } finally {
      if (page) await page.close();
    }
  }

  await browser.close();
  return pages;
}

/* =========================
   HTTP SERVER
========================= */
http
  .createServer((req, res) => {
    if (req.method !== "POST") {
      res.writeHead(405);
      return res.end();
    }

    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", async () => {
      try {
        const { url, maxPages } = JSON.parse(body || "{}");
        if (!url) throw new Error("Missing url");

        const limit = Math.min(
          Number(maxPages) || MAX_PAGES_DEFAULT,
          MAX_PAGES_HARD
        );

        const pages = await crawl(url, limit);

        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            success: true,
            pagesCount: pages.length,
            pages,
          })
        );
      } catch (e) {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            success: false,
            error: e.message || "Crawler error",
          })
        );
      }
    });
  })
  .listen(PORT, () => {
    console.log("Crawler running on", PORT);
  });
