import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);
const MAX_PAGES_DEFAULT = 40;
const MAX_PAGES_HARD = 120;

const SKIP_RE =
  /cookie|privacy|policy|terms|login|register|cart|checkout|account|wishlist|#|javascript:/i;

const IMPORTANT_RE =
  /about|за-нас|services|услуги|pricing|цени|price|contact|контакти|menu|меню|booking|reservation|appointment|запази/i;

const clean = (t = "") =>
  t
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/\s+/g, " ")
    .trim();

async function preparePage(page) {
  await page.waitForLoadState("networkidle");

  // aggressive scroll for SPA
  for (let i = 0; i < 8; i++) {
    await page.evaluate(() => window.scrollBy(0, window.innerHeight));
    await page.waitForTimeout(500);
  }

  // click expandable elements
  await page.evaluate(() => {
    document.querySelectorAll("button, summary, [role='button']").forEach((el) => {
      try {
        const txt = el.innerText?.toLowerCase() || "";
        if (txt.match(/menu|услуг|цени|about|contact|о нас/)) {
          el.click();
        }
      } catch {}
    });
  });

  await page.waitForTimeout(1200);
}

async function extractLinks(page, baseOrigin) {
  return await page.evaluate((baseOrigin) => {
    return Array.from(document.querySelectorAll("a[href]"))
      .map((a) => a.href)
      .filter((h) => {
        try {
          const u = new URL(h);
          return u.origin === baseOrigin;
        } catch {
          return false;
        }
      });
  }, baseOrigin);
}

async function crawl(startUrl, maxPages) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();
  const visited = new Set();
  const queue = [startUrl];
  const pages = [];

  const baseOrigin = new URL(startUrl).origin;

  while (queue.length && pages.length < maxPages) {
    const url = queue.shift();
    if (!url || visited.has(url)) continue;
    if (SKIP_RE.test(url)) continue;

    visited.add(url);
    console.log("[CRAWL] Visiting:", url);

    let page;
    try {
      page = await context.newPage();
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45000 });

      await preparePage(page);

      const title = clean(await page.title());
      const content = clean(
        await page.evaluate(() => document.body?.innerText || "")
      );

      if (content.length > 400) {
        pages.push({ url, title, content });
        console.log("[CRAWL] ✔ Added page:", pages.length);
      }

      const links = await extractLinks(page, baseOrigin);

      for (const l of links) {
        if (
          !visited.has(l) &&
          !SKIP_RE.test(l) &&
          (IMPORTANT_RE.test(l) || pages.length < 5)
        ) {
          queue.push(l);
        }
      }

      console.log(
        "[CRAWL] Queue:",
        queue.length,
        "| Visited:",
        visited.size
      );
    } catch (e) {
      console.log("[CRAWL] ❌ Failed:", url);
    } finally {
      if (page) await page.close();
    }
  }

  await browser.close();
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
        const { url, maxPages } = JSON.parse(body || "{}");
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
    console.log("🚀 Smart crawler running on", PORT);
  });
