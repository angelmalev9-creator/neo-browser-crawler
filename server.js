import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

const MAX_PAGES = 70;
const MAX_VISITS = 120;
const MIN_TEXT_CHARS = 300;

// 🔴 Блокирани пътища (галерии, проекти, медии)
const BLOCK_PATH_RE =
  /(gallery|portfolio|projects?|media|images?|video|slider|wp-content)/i;

// 🔴 Блокирани файлове
const BLOCK_EXT_RE =
  /\.(jpg|jpeg|png|webp|gif|svg|mp4|pdf|zip)$/i;

// 🟢 Важни бизнес страници – приоритет
const IMPORTANT_RE =
  /(about|за-нас|services|услуги|pricing|цени|price|contact|контакти|booking|reservation|appointment)/i;

const clean = (t = "") =>
  t.replace(/[\u200B-\u200D\uFEFF]/g, "").replace(/\s+/g, " ").trim();

/* =========================
   SITEMAP (без xml2js)
========================= */
async function getSitemapUrls(origin) {
  try {
    const res = await fetch(`${origin}/sitemap.xml`, { timeout: 8000 });
    if (!res.ok) return [];

    const xml = await res.text();

    // прост, бърз regex parser
    const urls = Array.from(
      xml.matchAll(/<loc>(.*?)<\/loc>/g)
    ).map((m) => m[1]);

    return urls;
  } catch {
    return [];
  }
}

/* =========================
   CRAWLER
========================= */
async function crawl(startUrl) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();
  const visited = new Set();
  const queue = [];
  const pages = [];

  const origin = new URL(startUrl).origin;

  console.log("[CRAWL] Start:", startUrl);

  // 1️⃣ Sitemap first
  const sitemapUrls = await getSitemapUrls(origin);
  sitemapUrls.forEach((u) => queue.push(u));

  // fallback
  if (!queue.length) queue.push(startUrl);

  while (
    queue.length &&
    pages.length < MAX_PAGES &&
    visited.size < MAX_VISITS
  ) {
    const url = queue.shift();
    if (!url || visited.has(url)) continue;
    visited.add(url);

    try {
      const u = new URL(url);
      if (u.origin !== origin) continue;
      if (BLOCK_PATH_RE.test(u.pathname)) continue;
      if (BLOCK_EXT_RE.test(u.pathname)) continue;
    } catch {
      continue;
    }

    let page;
    try {
      page = await context.newPage();
      await page.goto(url, {
        waitUntil: "domcontentloaded",
        timeout: 15000,
      });

      await page.evaluate(() => window.scrollBy(0, 1200));
      await page.waitForTimeout(600);

      const title = clean(await page.title());
      const text = clean(
        await page.evaluate(() => document.body?.innerText || "")
      );

      // ❗ АКО НЯМА ТЕКСТ → ИЗХВЪРЛЯМЕ
      if (text.length < MIN_TEXT_CHARS) continue;

      pages.push({ url, title, content: text });
      console.log("[CRAWL] ✔ Added:", pages.length, url);

      // откриване на нови линкове
      const links = await page.evaluate(() =>
        Array.from(document.querySelectorAll("a[href]"))
          .map((a) => a.href)
          .filter(Boolean)
      );

      for (const l of links) {
        try {
          const lu = new URL(l);
          if (
            lu.origin === origin &&
            !visited.has(lu.href) &&
            !BLOCK_PATH_RE.test(lu.pathname) &&
            !BLOCK_EXT_RE.test(lu.pathname)
          ) {
            IMPORTANT_RE.test(lu.pathname)
              ? queue.unshift(lu.href)
              : queue.push(lu.href);
          }
        } catch {}
      }
    } catch {
      // skip
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
  .createServer(async (req, res) => {
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

        const pages = await crawl(url);

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
