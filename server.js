import http from "http";
import { chromium } from "playwright";
import { parseStringPromise } from "xml2js";
import fetch from "node-fetch";

const PORT = Number(process.env.PORT || 10000);

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE envs");
}

const MAX_PAGES = 70;
const MIN_TEXT_CHARS = 300;

const SKIP_RE = /gallery|portfolio|projects|images|media|photo|video|work|album/i;

const clean = (t = "") =>
  t
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/\s+/g, " ")
    .trim();

/* =========================
   SUPABASE HELPERS
========================= */
async function updateSession(sessionId, data) {
  await fetch(`${SUPABASE_URL}/rest/v1/demo_sessions?id=eq.${sessionId}`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      Prefer: "return=minimal",
    },
    body: JSON.stringify(data),
  });
}

/* =========================
   SITEMAP
========================= */
async function getSitemapUrls(baseUrl) {
  try {
    const res = await fetch(`${baseUrl}/sitemap.xml`, { timeout: 15000 });
    if (!res.ok) return [];

    const xml = await res.text();
    const parsed = await parseStringPromise(xml);

    const urls =
      parsed?.urlset?.url?.map((u) => u.loc?.[0]).filter(Boolean) || [];

    console.log(`[SITEMAP] Found ${urls.length} urls`);
    return urls;
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

    // scroll for SPA / lazy text
    for (let i = 0; i < 4; i++) {
      await page.evaluate(() => window.scrollBy(0, 1600));
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
async function crawlSite(startUrl, sessionId) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();
  const baseOrigin = new URL(startUrl).origin;

  const visited = new Set();
  const pages = [];

  /* 1️⃣ sitemap first */
  let queue = await getSitemapUrls(baseOrigin);

  /* 2️⃣ fallback */
  if (!queue.length) queue = [startUrl];

  for (const url of queue) {
    if (pages.length >= MAX_PAGES) break;
    if (visited.has(url)) continue;
    if (SKIP_RE.test(url)) continue;

    visited.add(url);

    const pageData = await scrapePage(context, url);
    if (!pageData) continue;

    pages.push(pageData);
    console.log(`[CRAWL] Added ${pages.length}: ${url}`);
  }

  /* 3️⃣ BFS fallback if sitemap was weak */
  if (pages.length < 10) {
    console.log("[CRAWL] Sitemap weak → BFS fallback");

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
      if (!link.startsWith(baseOrigin)) continue;
      if (visited.has(link)) continue;
      if (SKIP_RE.test(link)) continue;

      visited.add(link);

      const pageData = await scrapePage(context, link);
      if (!pageData) continue;

      pages.push(pageData);
      console.log(`[CRAWL] Added ${pages.length}: ${link}`);
    }
  }

  await browser.close();

  await updateSession(sessionId, {
    status: "ready",
    scraped_content: pages,
    error_message: null,
    updated_at: new Date().toISOString(),
  });

  console.log(`[DONE] Total pages: ${pages.length}`);
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
        const { url, sessionId } = JSON.parse(body || "{}");
        if (!url || !sessionId) throw new Error("Missing data");

        console.log(`[CRAWL] Start: ${url}`);

        crawlSite(url, sessionId).catch(async (e) => {
          await updateSession(sessionId, {
            status: "error",
            error_message: e.message || "Crawler failed",
          });
        });

        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: true }));
      } catch (e) {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: false, error: e.message }));
      }
    });
  })
  .listen(PORT, () => {
    console.log("Crawler running on", PORT);
  });
