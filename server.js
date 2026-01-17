import http from "http";
import { chromium } from "playwright";
import { parseStringPromise } from "xml2js";

const PORT = Number(process.env.PORT || 10000);

const MAX_PAGES = 70;
const MIN_TEXT_CHARS = 300;
const MAX_SITEMAP_DEPTH = 3;

const SKIP_PATH_RE =
  /gallery|portfolio|projects|images|media|photo|video|album|wp-content|uploads/i;

const clean = (t = "") =>
  t.replace(/[\u200B-\u200D\uFEFF]/g, "").replace(/\s+/g, " ").trim();

/* =========================
   FETCH
========================= */
async function fetchText(url, timeout = 15000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  try {
    const res = await fetch(url, { signal: controller.signal });
    if (!res.ok) return null;
    return await res.text();
  } catch {
    return null;
  } finally {
    clearTimeout(id);
  }
}

/* =========================
   ROBOTS → SITEMAPS
========================= */
async function getSitemapsFromRobots(base) {
  const txt = await fetchText(`${base}/robots.txt`);
  if (!txt) return [];

  const sitemaps = txt
    .split("\n")
    .map(l => l.trim())
    .filter(l => l.toLowerCase().startsWith("sitemap:"))
    .map(l => l.split(":").slice(1).join(":").trim());

  console.log(`[ROBOTS] Found ${sitemaps.length} sitemaps`);
  return sitemaps;
}

/* =========================
   SITEMAP PARSER (recursive)
========================= */
async function extractUrlsFromSitemap(url, depth = 0, seen = new Set()) {
  if (depth > MAX_SITEMAP_DEPTH) return [];
  if (seen.has(url)) return [];
  seen.add(url);

  const xml = await fetchText(url);
  if (!xml) return [];

  try {
    const parsed = await parseStringPromise(xml);

    // sitemapindex → recurse
    if (parsed?.sitemapindex?.sitemap) {
      const nested = parsed.sitemapindex.sitemap
        .map(s => s.loc?.[0])
        .filter(Boolean);

      let all = [];
      for (const sm of nested) {
        const urls = await extractUrlsFromSitemap(sm, depth + 1, seen);
        all.push(...urls);
      }
      return all;
    }

    // urlset → actual pages
    if (parsed?.urlset?.url) {
      return parsed.urlset.url
        .map(u => u.loc?.[0])
        .filter(Boolean);
    }

    return [];
  } catch {
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
      await page.waitForTimeout(400);
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
   MAIN CRAWLER
========================= */
async function crawlSite(startUrl) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();

  const tmp = await context.newPage();
  await tmp.goto(startUrl, { waitUntil: "domcontentloaded", timeout: 30000 });
  const base = new URL(tmp.url()).origin;
  await tmp.close();

  console.log(`[CRAWL] Canonical base: ${base}`);

  const baseOrigin = new URL(base).origin;

  const visited = new Set();
  const pages = [];

  const sitemapIndexes = await getSitemapsFromRobots(base);

  let sitemapUrls = [];
  for (const sm of sitemapIndexes) {
    const urls = await extractUrlsFromSitemap(sm);
    sitemapUrls.push(...urls);
  }

  sitemapUrls = sitemapUrls.filter(u => {
    try {
      const o = new URL(u).origin;
      return (
        o === baseOrigin &&
        !SKIP_PATH_RE.test(u)
      );
    } catch {
      return false;
    }
  });

  console.log(`[SITEMAP] URLs after filter: ${sitemapUrls.length}`);

  for (const url of sitemapUrls) {
    if (pages.length >= MAX_PAGES) break;
    if (visited.has(url)) continue;

    visited.add(url);
    const data = await scrapePage(context, url);
    if (!data) continue;

    pages.push(data);
    console.log(`[CRAWL] +${pages.length}: ${url}`);
  }

  await browser.close();

  console.log(`[DONE] Total pages scraped: ${pages.length}`);
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
    req.on("data", c => (body += c));
    req.on("end", async () => {
      try {
        const { url } = JSON.parse(body || "{}");
        if (!url) throw new Error("Missing url");

        console.log(`[CRAWL] Start ${url}`);
        const pages = await crawlSite(url);

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
  })
  .listen(PORT, () => {
    console.log("Crawler running on", PORT);
  });
