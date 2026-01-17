import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

const MAX_PAGES_SOFT = 40;
const MAX_PAGES_HARD = 120;

const SKIP_RE = /privacy|cookies|terms|gdpr|policy/i;
const IMPORTANT_RE = /about|за-нас|services|услуги|pricing|цени|price|contact|контакти|process|процес|faq|въпроси/i;

const CLICK_SELECTORS = [
  "button",
  "[role='button']",
  "[aria-expanded='false']",
  "details summary",
  ".accordion button",
  ".accordion-header",
  ".tabs button",
  ".tab",
  ".dropdown-toggle",
  ".menu-toggle",
  ".expand",
  ".show-more",
];

const clean = (t = "") =>
  t
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/\s+/g, " ")
    .trim();

async function autoExpand(page) {
  // scroll (SPA + lazy)
  for (let i = 0; i < 6; i++) {
    await page.evaluate(() => window.scrollBy(0, 1600));
    await page.waitForTimeout(500);
  }

  // click expandable UI
  for (const sel of CLICK_SELECTORS) {
    try {
      const els = await page.$$(sel);
      for (let i = 0; i < Math.min(els.length, 30); i++) {
        try {
          await els[i].click({ delay: 20 });
          await page.waitForTimeout(200);
        } catch {}
      }
    } catch {}
  }

  // final scroll
  for (let i = 0; i < 3; i++) {
    await page.evaluate(() => window.scrollBy(0, 1600));
    await page.waitForTimeout(400);
  }
}

function extractFacts(text) {
  const facts = [];

  const lines = text.split(". ").map((l) => l.trim());

  for (const l of lines) {
    if (l.length < 40) continue;

    if (/лв|€|\$|price|цена/i.test(l)) {
      facts.push({ type: "price", value: l });
    } else if (/процес|стъпк|how|как/i.test(l)) {
      facts.push({ type: "process", value: l });
    } else if (/услуг|service|предлаг/i.test(l)) {
      facts.push({ type: "service", value: l });
    } else if (/не включва|услов|огранич/i.test(l)) {
      facts.push({ type: "condition", value: l });
    }
  }

  return facts;
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
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      await page.waitForTimeout(1200);

      await autoExpand(page);

      const title = clean(await page.title());
      const text = clean(
        await page.evaluate(() => document.body?.innerText || "")
      );

      if (text.length > 300) {
        const facts = extractFacts(text);
        pages.push({ url, title, content: text, facts });
        console.log("[CRAWL] ✔ Added page:", pages.length);
      }

      // discover internal links
      const links = await page.evaluate(() =>
        Array.from(document.querySelectorAll("a[href]"))
          .map((a) => a.href)
          .filter(Boolean)
      );

      for (const l of links) {
        try {
          const u = new URL(l);
          if (
            u.origin === baseOrigin &&
            !visited.has(u.href) &&
            !SKIP_RE.test(u.href)
          ) {
            if (IMPORTANT_RE.test(u.href)) {
              queue.unshift(u.href); // приоритет
            } else {
              queue.push(u.href);
            }
          }
        } catch {}
      }
    } catch (e) {
      console.log("[CRAWL] ✖ Error:", url);
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
          Number(maxPages) || MAX_PAGES_SOFT,
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
