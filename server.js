import http from "http";
import { chromium } from "playwright";
import { parseStringPromise } from "xml2js";

const PORT = Number(process.env.PORT || 10000);

/* =========================
   ENV (ЗАДЪЛЖИТЕЛНИ)
========================= */
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("❌ Missing SUPABASE envs in Render");
  process.exit(1);
}

/* =========================
   LIMITS / FILTERS
========================= */
const MAX_PAGES = 70;
const MIN_TEXT_CHARS = 300;

const SKIP_RE =
  /gallery|portfolio|projects|images|media|photo|video|work|album|wp-content|uploads|\.jpg|\.png|\.webp|\.svg|\.mp4|\.pdf/i;

/* =========================
   HELPERS
========================= */
const clean = (t = "") =>
  t.replace(/[\u200B-\u200D\uFEFF]/g, "").replace(/\s+/g, " ").trim();

/* =========================
   SUPABASE UPDATE (REST)
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
async function getSitemapUrls(origin) {
  try {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), 15000);

    const res = await fetch(`${origin}/sitemap.xml`, {
      signal: controller.signal,
    });

    clearTimeout(t);
    if (!res.ok) return [];

    const xml = await res.text();
    const parsed = await parseStringPromise(xml);

    const urls =
      parsed?.urlset?.url?.map((u) => u.loc?.[0]).filter(Boolean) || [];

    console.log(`[SITEMAP] Found ${urls.length} urls`);
    return urls;
  } catch {
    console.log("[SITEMAP] Missing or failed");
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
   CRAWLER CORE
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

  /* 1️⃣ Sitemap first */
  let queue = await getSitemapUrls(baseOrigin);
  if (!queue.length) queue = [startUrl];

  for (const url of queue) {
    if (pages.length >= MAX_PAGES) break;
    if (!url.startsWith(baseOrigin)) continue;
    if (visited.has(url)) continue;
    if (SKIP_RE.test(url)) continue;

    visited.add(url);

    const data = await scrapePage(context, url);
    if (!data) continue;

    pages.push(data);
    console.log(`[CRAWL] +${pages.length} ${url}`);
  }

  /* 2️⃣ BFS fallback */
  if (pages.length < 8) {
    console.log("[CRAWL] Sitemap weak → BFS");

    const page = await context.newPage();
    await page.goto(startUrl, { waitUntil: "domcontentloaded", timeout: 30000 });

    const links = await page.evaluate(() =>
      Array.from(document.querySelectorAll("a[href]"))
        .map((a) => a.href)
        .filter(
          (h) =>
            h.startsWith(location.origin) &&
            !h.includes("#") &&
            !h.match(/\.(jpg|png|webp|svg|mp4|pdf)$/i)
        )
    );

    await page.close();

    for (const link of links) {
      if (pages.length >= MAX_PAGES) break;
      if (visited.has(link)) continue;
      if (SKIP_RE.test(link)) continue;

      visited.add(link);

      const data = await scrapePage(context, link);
      if (!data) continue;

      pages.push(data);
      console.log(`[CRAWL] +${pages.length} ${link}`);
    }
  }

  await browser.close();

  await updateSession(sessionId, {
    status: "ready",
    scraped_content: pages,
    error_message: null,
    updated_at: new Date().toISOString(),
  });

  console.log(`[DONE] Pages scraped: ${pages.length}`);
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
        if (!url || !sessionId) throw new Error("Missing url or sessionId");

        console.log(`[CRAWL] Start ${url}`);

        // FIRE & FORGET
        crawlSite(url, sessionId).catch(async (e) => {
          console.error("[CRAWL ERROR]", e.message);
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
