import http from "http";
import { chromium } from "playwright";
import { parseStringPromise } from "xml2js";

const PORT = Number(process.env.PORT || 10000);

const MAX_PAGES = 70;
const MIN_TEXT_CHARS = 400;

const ALLOWED_RE =
  /(about|za-nas|services|uslugi|pricing|prices|ceni|tseni|contact|kontakti)/i;

const BLOCK_RE =
  /(gallery|project|portfolio|image|img|photo|media|video)/i;

const clean = (t = "") =>
  t.replace(/[\u200B-\u200D\uFEFF]/g, "").replace(/\s+/g, " ").trim();

/* =========================
   SITEMAP
========================= */
async function loadSitemap(browser, baseUrl) {
  const urls = [];
  const page = await browser.newPage();

  try {
    await page.goto(`${baseUrl}/sitemap.xml`, { timeout: 15000 });
    const xml = await page.content();

    const parsed = await parseStringPromise(xml);
    const locs = parsed?.urlset?.url || [];

    for (const u of locs) {
      const loc = u.loc?.[0];
      if (
        loc &&
        ALLOWED_RE.test(loc) &&
        !BLOCK_RE.test(loc)
      ) {
        urls.push(loc);
      }
    }
  } catch {
    // sitemap missing → fallback later
  } finally {
    await page.close();
  }

  return urls.slice(0, MAX_PAGES);
}

/* =========================
   SCRAPE PAGE
========================= */
async function scrapePage(context, url) {
  const page = await context.newPage();
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });

    // scroll for lazy content
    for (let i = 0; i < 3; i++) {
      await page.evaluate(() => window.scrollBy(0, 1200));
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
   MAIN CRAWL
========================= */
async function crawlSite(startUrl) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox"],
  });

  const context = await browser.newContext();
  const base = new URL(startUrl).origin;

  const pages = [];
  const visited = new Set();

  // 1️⃣ sitemap
  let targets = await loadSitemap(browser, base);

  // 2️⃣ fallback – home links
  if (!targets.length) {
    const home = await scrapePage(context, startUrl);
    if (home) pages.push(home);

    const page = await context.newPage();
    await page.goto(startUrl);
    const links = await page.evaluate(() =>
      Array.from(document.querySelectorAll("a[href]"))
        .map(a => a.href)
        .filter(Boolean)
    );
    await page.close();

    targets = links.filter(
      l =>
        l.startsWith(base) &&
        ALLOWED_RE.test(l) &&
        !BLOCK_RE.test(l)
    );
  }

  for (const url of targets) {
    if (pages.length >= MAX_PAGES) break;
    if (visited.has(url)) continue;

    visited.add(url);

    const data = await scrapePage(context, url);
    if (data) pages.push(data);
  }

  await browser.close();
  return pages;
}

/* =========================
   HTTP SERVER
========================= */
http.createServer(async (req, res) => {
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
}).listen(PORT, () => {
  console.log("Crawler running on", PORT);
});
