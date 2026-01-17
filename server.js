import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);
const MAX_PAGES_DEFAULT = 40;
const MAX_PAGES_HARD = 120;

const BLOCK_RE =
  /privacy|policy|cookies|cookie|terms|gdpr|consent|login|register|account|cart|checkout|wishlist|compare|admin|wp-admin/i;

const PRIORITY_RE =
  /price|pricing|цени|services|услуги|products|product|shop|menu|меню|booking|reservation|appointment|contact|about|за-нас/i;

const clean = (t = "") =>
  t.replace(/[\u200B-\u200D\uFEFF]/g, "").replace(/\s+/g, " ").trim();

function normalizeUrl(u) {
  try {
    const url = new URL(u);
    url.hash = "";
    return url.toString().replace(/\/$/, "");
  } catch {
    return null;
  }
}

async function crawl(startUrl, maxPages) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();
  const baseOrigin = new URL(startUrl).origin;

  const visited = new Set();
  const queue = [startUrl];
  const pages = [];

  console.log("[CRAWLER] start:", startUrl);

  while (queue.length && pages.length < maxPages) {
    const rawUrl = queue.shift();
    const url = normalizeUrl(rawUrl);
    if (!url || visited.has(url)) continue;
    visited.add(url);

    let page;
    try {
      page = await context.newPage();

      await page.goto(url, {
        waitUntil: "networkidle",
        timeout: 45000,
      });

      // force SPA render
      for (let i = 0; i < 5; i++) {
        await page.evaluate(() => window.scrollBy(0, window.innerHeight));
        await page.waitForTimeout(600);
      }

      const title = clean(await page.title());
      const text = clean(
        await page.evaluate(() => document.body?.innerText || "")
      );

      if (text.length > 400) {
        pages.push({ url, title, content: text.slice(0, 16000) });
        console.log("[CRAWLED]", pages.length, url);
      }

      // extract links AFTER render
      const links = await page.evaluate(() =>
        Array.from(document.links).map((a) => a.href)
      );

      console.log("[LINKS FOUND]", links.length, "on", url);

      for (const l of links) {
        const n = normalizeUrl(l);
        if (!n) continue;

        try {
          const u = new URL(n);
          if (u.origin !== baseOrigin) continue;
          if (BLOCK_RE.test(n)) continue;
          if (visited.has(n)) continue;

          if (PRIORITY_RE.test(n)) {
            queue.unshift(n);
          } else if (queue.length < maxPages * 3) {
            queue.push(n);
          }
        } catch {}
      }
    } catch (e) {
      console.log("[SKIP]", url);
    } finally {
      if (page) await page.close();
    }
  }

  await browser.close();
  console.log("[DONE] pages:", pages.length);
  return pages;
}

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
        res.end(JSON.stringify({ success: true, pagesCount: pages.length, pages }));
      } catch (e) {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: false, error: e.message }));
      }
    });
  })
  .listen(PORT, () => {
    console.log("Crawler running on", PORT);
  });
