import http from "http";
import { chromium } from "playwright";
import { parseStringPromise } from "xml2js";

const PORT = Number(process.env.PORT || 10000);

const MAX_PAGES = 70;
const MIN_TEXT_CHARS = 300;

// режем галерии, медии, wp боклуци
const SKIP_RE =
  /gallery|portfolio|projects|images|media|photo|video|work|album|wp-content|uploads/i;

const clean = (t = "") =>
  t.replace(/[\u200B-\u200D\uFEFF]/g, "").replace(/\s+/g, " ").trim();

/* =========================
   SITEMAP
========================= */
async function getSitemapUrls(origin) {
  try {
    const controller = new AbortController();
    setTimeout(() => controller.abort(), 15000);

    const res = await fetch(`${origin}/sitemap.xml`, {
      signal: controller.signal,
    });

    if (!res.ok) return [];

    const xml = await res.text();
    const parsed = await parseStringPromise(xml);

    return (
      parsed?.urlset?.url
        ?.map((u) => u.loc?.[0])
        .filter(Boolean) || []
    );
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

    // scroll за lazy text
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
   MAIN CRAWL
========================= */
async function crawlSite(startUrl) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();
  const origin = new URL(startUrl).origin;

  const visited = new Set();
  const pages = [];

  // 1️⃣ sitemap first
  let queue = await getSitemapUrls(origin);
  if (!queue.length) queue = [startUrl];

  for (const url of queue) {
    if (pages.length >= MAX_PAGES) break;
    if (!url.startsWith(origin)) continue;
    if (visited.has(url)) continue;
    if (SKIP_RE.test(url)) continue;

    visited.add(url);

    const data = await scrapePage(context, url);
    if (!data) continue;

    pages.push(data);
    console.log(`[CRAWL] +${pages.length}: ${url}`);
  }

  // 2️⃣ BFS fallback ако sitemap е слаб
  if (pages.length < 10) {
    const page = await context.newPage();
    await page.goto(startUrl, { waitUntil: "domcontentloaded", timeout: 30000 });

    const links = await page.evaluate(() =>
      Array.from(document.querySelectorAll("a[href]"))
        .map((a) => a.href)
        .filter(Boolean)
    );
    await page.close();

    for (const link of links) {
      if (pages.length >= MAX_PAGES) break;
      if (!link.startsWith(origin)) continue;
      if (visited.has(link)) continue;
      if (SKIP_RE.test(link)) continue;

      visited.add(link);

      const data = await scrapePage(context, link);
      if (!data) continue;

      pages.push(data);
      console.log(`[CRAWL] +${pages.length}: ${link}`);
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
