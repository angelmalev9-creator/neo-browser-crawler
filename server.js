import http from "http";
import { chromium } from "playwright";
import { parseStringPromise } from "xml2js";

const PORT = Number(process.env.PORT || 10000);

const MAX_PAGES = 70;
const PAGE_TIMEOUT = 15000;
const MIN_WORDS = 10;
const MAX_SITEMAP_DEPTH = 3;

const SKIP_PATH_RE =
  /gallery|portfolio|projects|images|media|photo|video|album|attachment|wp-content|uploads/i;

const clean = (t = "") =>
  t.replace(/[\u200B-\u200D\uFEFF]/g, "").replace(/\s+/g, " ").trim();

const countWords = (text) =>
  text.split(/\s+/).filter(w => w.length > 2).length;

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
   SITEMAPS (robots + fallback)
========================= */
async function getSitemaps(base) {
  const robots = await fetchText(`${base}/robots.txt`);
  let found = [];

  if (robots) {
    found = robots
      .split("\n")
      .map(l => l.trim())
      .filter(l => l.toLowerCase().startsWith("sitemap:"))
      .map(l => l.split(":").slice(1).join(":").trim());
  }

  if (found.length) {
    console.log(`[ROBOTS] Found ${found.length} sitemaps`);
    return found;
  }

  console.log("[SITEMAP] Robots empty → trying fallbacks");

  const candidates = [
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/wp-sitemap.xml",
  ].map(p => `${base}${p}`);

  const valid = [];
  for (const url of candidates) {
    const xml = await fetchText(url);
    if (!xml) continue;
    try {
      await parseStringPromise(xml);
      console.log(`[SITEMAP] Found via fallback: ${url}`);
      valid.push(url);
    } catch {}
  }

  return valid;
}

/* =========================
   SITEMAP PARSER
========================= */
async function extractUrlsFromSitemap(url, depth = 0, seen = new Set()) {
  if (depth > MAX_SITEMAP_DEPTH) return [];
  if (seen.has(url)) return [];
  seen.add(url);

  const xml = await fetchText(url);
  if (!xml) return [];

  try {
    const parsed = await parseStringPromise(xml);

    if (parsed?.sitemapindex?.sitemap) {
      let all = [];
      for (const sm of parsed.sitemapindex.sitemap) {
        const loc = sm.loc?.[0];
        if (!loc) continue;
        all.push(...await extractUrlsFromSitemap(loc, depth + 1, seen));
      }
      return all;
    }

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
   PAGE SCRAPE (TEXT-ONLY)
========================= */
async function scrapePage(context, url) {
  const page = await context.newPage();
  try {
    await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: PAGE_TIMEOUT,
    });

    // взимаме текст БЕЗ scroll
    const text = clean(
      await page.evaluate(() => document.body?.innerText || "")
    );

    if (countWords(text) < MIN_WORDS) {
      return null;
    }

    const title = clean(await page.title());
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

  // 🚫 BLOCK IMAGES / MEDIA / FONTS
  await context.route("**/*", route => {
    const type = route.request().resourceType();
    if (type === "image" || type === "media" || type === "font") {
      return route.abort();
    }
    route.continue();
  });

  const tmp = await context.newPage();
  await tmp.goto(startUrl, { waitUntil: "domcontentloaded", timeout: 30000 });
  const base = new URL(tmp.url()).origin;
  await tmp.close();

  console.log(`[CRAWL] Canonical base: ${base}`);

  const visited = new Set();
  const pages = [];

  const sitemaps = await getSitemaps(base);
  if (!sitemaps.length) {
    await browser.close();
    return [];
  }

  let urls = [];
  for (const sm of sitemaps) {
    urls.push(...await extractUrlsFromSitemap(sm));
  }

  urls = urls.filter(u => {
    try {
      return (
        new URL(u).origin === base &&
        !SKIP_PATH_RE.test(u)
      );
    } catch {
      return false;
    }
  });

  console.log(`[SITEMAP] URLs after filter: ${urls.length}`);

  for (const url of urls) {
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
http.createServer((req, res) => {
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
}).listen(PORT, () => {
  console.log("Crawler running on", PORT);
});
