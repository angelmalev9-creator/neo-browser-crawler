import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);
const MAX_PAGES_DEFAULT = 40;
const MAX_PAGES_HARD = 120;

const IMPORTANT_RE =
  /about|за-нас|services|услуги|pricing|цени|price|contact|контакти|menu|меню|booking|reservation|appointment|запази|faq|въпроси/i;

const CLICK_SELECTORS = [
  "button",
  "[role='button']",
  "details summary",
  ".accordion button",
  ".accordion-header",
  ".tabs button",
  ".tab",
  ".dropdown-toggle",
  ".menu-toggle",
];

const clean = (t = "") =>
  t
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/\s+/g, " ")
    .trim();

/* ================= CORE CRAWL ================= */
async function crawl(url, maxPages) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();
  const page = await context.newPage();
  page.setDefaultTimeout(45000);

  await page.goto(url, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(2000);

  /* ---------- SPA EXPAND ---------- */

  // scroll
  for (let i = 0; i < 8; i++) {
    await page.evaluate(() => window.scrollBy(0, 1800));
    await page.waitForTimeout(600);
  }

  // click expandable elements
  for (const sel of CLICK_SELECTORS) {
    try {
      const els = await page.$$(sel);
      for (let i = 0; i < Math.min(els.length, 30); i++) {
        try {
          await els[i].click({ delay: 20 });
          await page.waitForTimeout(150);
        } catch {}
      }
    } catch {}
  }

  // final scroll
  for (let i = 0; i < 4; i++) {
    await page.evaluate(() => window.scrollBy(0, 1800));
    await page.waitForTimeout(600);
  }

  const pages = [];

  const rootTitle = clean(await page.title());
  const rootText = clean(
    await page.evaluate(() => document.body?.innerText || "")
  );

  pages.push({ url, title: rootTitle, content: rootText });

  /* ---------- LINK DISCOVERY ---------- */

  const base = new URL(url).origin;

  const links = await page.evaluate(() =>
    Array.from(document.querySelectorAll("a[href]"))
      .map((a) => a.href)
      .filter(Boolean)
  );

  const importantLinks = Array.from(
    new Set(
      links.filter(
        (l) =>
          l.startsWith(base) &&
          IMPORTANT_RE.test(l)
      )
    )
  ).slice(0, maxPages - 1);

  /* ---------- CRAWL SUBPAGES ---------- */

  for (const link of importantLinks) {
    if (pages.length >= maxPages) break;

    try {
      const p = await context.newPage();
      await p.goto(link, { waitUntil: "domcontentloaded", timeout: 30000 });
      await p.waitForTimeout(1500);

      // light expand
      for (let i = 0; i < 3; i++) {
        await p.evaluate(() => window.scrollBy(0, 1600));
        await p.waitForTimeout(500);
      }

      const t = clean(await p.title());
      const c = clean(
        await p.evaluate(() => document.body?.innerText || "")
      );

      if (c.length > 300) {
        pages.push({ url: link, title: t, content: c });
      }

      await p.close();
    } catch {}
  }

  await browser.close();
  return pages;
}

/* ================= SERVER ================= */
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
    console.log("Crawler running on", PORT);
  });
