import http from "http";
import { chromium } from "playwright";
import { parseStringPromise } from "xml2js";

const PORT = Number(process.env.PORT || 10000);

const MAX_PAGES = 70;
const MIN_TEXT_CHARS = 300;

const SKIP_RE =
  /gallery|portfolio|projects|images|media|photo|video|work|album|wp-content|uploads/i;

const clean = (t = "") =>
  t.replace(/[\u200B-\u200D\uFEFF]/g, "").replace(/\s+/g, " ").trim();

/* =========================
   URL NORMALIZATION (KEY FIX)
========================= */
function normalizeUrl(url) {
  try {
    const u = new URL(url);
    u.hash = "";
    u.search = "";
    return u.toString().replace(/\/$/, "");
  } catch {
    return null;
  }
}

function sameDomain(a, b) {
  return (
    a.replace(/^https?:\/\/(www\.)?/, "") ===
    b.replace(/^https?:\/\/(www\.)?/, "")
  );
}

/* =========================
   SITEMAP
========================= */
async function getSitemapUrls(origin) {
  try {
    const res = await fetch(`${origin}/sitemap.xml`, { timeout: 15000 });
    if (!res.ok) return [];

    const xml = await res.text();
    const parsed = await parseStringPromise(xml);

    const urls =
      parsed?.urlset?.url?.map((u) => u.loc?.[0]).filter(Boolean) || [];

    console.log(`[SITEMAP] Found ${urls.length} urls`);
    return urls.map(normalizeUrl).filter(Boolean);
  } catch {
    console.log("[SITEMAP] No sitemap");
    return [];
  }
}

/* =========================
   PAGE SCRAPE
========================= */
async function scrapePage(context, url) {
  const page = await context.newPage();
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });

    for (let i = 0; i < 3; i++) {
      await page.evaluate(() => window.scrollBy(0, 1500));
      await page.waitForTimeout(300);
    }

    const title = clean(await page.title());
    const text = clean(
      await page.evaluate(() => document.body?.innerText || "")
    );

    if (text.length < MIN_TEXT_CHARS) return null;

    return { url, title, content: text };
  } catch {
    return null;
  } finally {
    await page.close();
  }
}

/* =========================
   MAIN CRAWL (FIXED)
========================= */
async function crawlSite(startUrl) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();

  const start = normalizeUrl(startUrl);
  const visited = new Set();
  const pages = [];

  const sitemapUrls = await getSitemapUrls(start);
  const queue = [...new Set([start, ...sitemapUrls])];

  console.log(`[CRAWL] Queue size: ${queue.length}`);

  for (const rawUrl of queue) {
    if (pages.length >= MAX_PAGES) break;

    const url = normalizeUrl(rawUrl);
    if (!url) continue;
    if (visited.has(url)) continue;
    if (SKIP_RE.test(url)) continue;
    if (!sameDomain(url, start)) continue;

    visited.add(url);

    const data = await scrapePage(context, url);
    if (!data) continue;

    pages.push(data);
    console.log(`[CRAWL] +${pages.length}: ${url}`);
  }

  /* BFS fallback – реални линкове */
  if (pages.length < 10) {
    console.log("[CRAWL] BFS fallback");

    const page = await context.newPage();
    await page.goto(start, { waitUntil: "domcontentloaded", timeout: 30000 });

    const links = await page.evaluate(() =>
      Array.from(document.querySelectorAll("a[href]"))
        .map((a) => a.href)
        .filter(Boolean)
    );
    await page.close();

    for (const l of links) {
      if (pages.length >= MAX_PAGES) break;

      const url = normalizeUrl(l);
      if (!url) continue;
      if (visited.has(url)) continue;
      if (!sameDomain(url, start)) continue;
      if (SKIP_RE.test(url)) continue;

      visited.add(url);

      const data = await scrapePage(context, url);
      if (!data) continue;

      pages.push(data);
      console.log(`[CRAWL] +${pages.length}: ${url}`);
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
        const { url } = JSON.parse(body || "{}");
        if (!url) throw new Error("Missing url");

        console.log(`[CRAWL] Start ${url}`);

        const pages = await crawlSite(url);

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
        res.end(JSON.stringify({ success: false, error: e.message }));
      }
    });
  })
  .listen(PORT, () => {
    console.log("Crawler running on", PORT);
  });
