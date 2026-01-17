import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);
const MAX_PAGES_DEFAULT = 40;
const MAX_PAGES_HARD = 120;

/* =========================
   URL FILTERS
========================= */
const BLOCK_RE =
  /privacy|policy|cookies|cookie|terms|gdpr|consent|login|register|account|cart|checkout|wishlist|compare|admin|wp-admin/i;

const PRIORITY_RE =
  /price|pricing|цени|services|услуги|products|product|shop|menu|меню|booking|reservation|appointment|contact|about|за-нас/i;

/* =========================
   TEXT CLEAN
========================= */
const cleanText = (t = "") =>
  t
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/\s+/g, " ")
    .trim();

/* =========================
   NORMALIZE URL
========================= */
function normalizeUrl(u) {
  try {
    const url = new URL(u);
    url.hash = "";
    if (url.pathname.endsWith("/")) {
      url.pathname = url.pathname.slice(0, -1);
    }
    return url.toString();
  } catch {
    return null;
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
  const baseOrigin = new URL(startUrl).origin;

  const visited = new Set();
  const queue = [];
  const pages = [];

  queue.push({ url: startUrl, priority: true });

  while (queue.length && pages.length < maxPages) {
    const { url } = queue.shift();
    const normalized = normalizeUrl(url);
    if (!normalized || visited.has(normalized)) continue;

    visited.add(normalized);

    let page;
    try {
      page = await context.newPage();
      await page.goto(normalized, {
        waitUntil: "domcontentloaded",
        timeout: 30000,
      });

      // SPA / lazy load
      for (let i = 0; i < 4; i++) {
        await page.evaluate(() => window.scrollBy(0, 1600));
        await page.waitForTimeout(400);
      }

      const title = cleanText(await page.title());
      const rawText = cleanText(
        await page.evaluate(() => document.body?.innerText || "")
      );

      // skip junk pages
      if (rawText.length > 400) {
        pages.push({
          url: normalized,
          title,
          content: rawText.slice(0, 16000),
        });
      }

      // extract links
      const links = await page.evaluate(() =>
        Array.from(document.querySelectorAll("a[href]"))
          .map((a) => a.href)
          .filter(Boolean)
      );

      for (const l of links) {
        const n = normalizeUrl(l);
        if (!n) continue;

        try {
          const u = new URL(n);

          if (u.origin !== baseOrigin) continue;
          if (BLOCK_RE.test(n)) continue;
          if (visited.has(n)) continue;

          // priority links first
          if (PRIORITY_RE.test(n)) {
            queue.unshift({ url: n, priority: true });
          } else if (queue.length < maxPages * 3) {
            queue.push({ url: n, priority: false });
          }
        } catch {}
      }
    } catch {
      // skip page errors
    } finally {
      if (page) await page.close();
    }
  }

  await browser.close();
  return pages;
}

/* =========================
   SERVER
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
        const { url, maxPages } = JSON.parse(body || {});
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
