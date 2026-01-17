import http from "http";
import { chromium } from "playwright";
import { parseStringPromise } from "xml2js";

const PORT = process.env.PORT || 10000;

const MAX_PAGES = 70;
const MIN_TEXT_CHARS = 300;

const SKIP_RE =
  /gallery|portfolio|projects|images|media|photo|video|work|album|wp-content|uploads/i;

const clean = (t = "") =>
  t.replace(/[\u200B-\u200D\uFEFF]/g, "").replace(/\s+/g, " ").trim();

/* ================= SITEMAP ================= */
async function getSitemapUrls(origin) {
  try {
    const res = await fetch(`${origin}/sitemap.xml`, { timeout: 15000 });
    if (!res.ok) return [];

    const xml = await res.text();
    const parsed = await parseStringPromise(xml);

    return (
      parsed?.urlset?.url?.map((u) => u.loc?.[0]).filter(Boolean) || []
    );
  } catch {
    return [];
  }
}

/* ================= PAGE ================= */
async function scrapePage(context, url) {
  const page = await context.newPage();
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });

    for (let i = 0; i < 3; i++) {
      await page.evaluate(() => window.scrollBy(0, 1500));
      await page.waitForTimeout(300);
    }

    const title = clean(await page.title());
    const text = clean(await page.evaluate(() => document.body?.innerText || ""));

    if (text.length < MIN_TEXT_CHARS) return null;

    return { url, title, content: text };
  } catch {
    return null;
  } finally {
    await page.close();
  }
}

/* ================= CRAWL ================= */
async function crawl({ url, sessionId }) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();
  const origin = new URL(url).origin;

  const visited = new Set();
  const pages = [];

  let queue = await getSitemapUrls(origin);
  if (!queue.length) queue = [url];

  for (const link of queue) {
    if (pages.length >= MAX_PAGES) break;
    if (!link.startsWith(origin)) continue;
    if (visited.has(link)) continue;
    if (SKIP_RE.test(link)) continue;

    visited.add(link);

    const data = await scrapePage(context, link);
    if (!data) continue;

    pages.push(data);
    console.log(`[CRAWL] ${pages.length} ${link}`);
  }

  await browser.close();

  // 3️⃣ връщаме резултата към Edge
  await fetch("https://lvcxmcdbopxxfuoaqgze.supabase.co/functions/v1/store-scrape-results", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sessionId, pages }),
  });
}

/* ================= SERVER ================= */
http
  .createServer((req, res) => {
    if (req.method !== "POST") return res.end();

    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      try {
        const payload = JSON.parse(body);
        crawl(payload).catch((e) =>
          fetch("https://lvcxmcdbopxxfuoaqgze.supabase.co/functions/v1/store-scrape-results", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              sessionId: payload.sessionId,
              error: e.message,
            }),
          })
        );

        res.end(JSON.stringify({ success: true }));
      } catch {
        res.statusCode = 400;
        res.end();
      }
    });
  })
  .listen(PORT, () => {
    console.log("Crawler running on", PORT);
  });
