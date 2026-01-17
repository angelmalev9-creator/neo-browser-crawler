import http from "http";
import { chromium } from "playwright";
import xml2js from "xml2js";

const PORT = Number(process.env.PORT || 10000);
const MAX_PAGES = 80;
const MIN_TEXT_CHARS = 600;

const BUSINESS_RE =
  /service|услуг|price|цен|about|за нас|contact|контакт|booking|резервац|appointment/i;

const BLOCK_RE =
  /privacy|cookie|terms|policy|login|register|gdpr|gallery|image|img/i;

const PRIORITY_RE =
  /service|услуг|price|цен|about|за-нас|contact|контакт/i;

const clean = (t = "") =>
  t.replace(/[\u200B-\u200D\uFEFF]/g, "").replace(/\s+/g, " ").trim();

const normalizeHost = (url) => {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
};

/* =====================
   SITEMAP
===================== */
async function getSitemapUrls(base) {
  const urls = [];

  async function parse(url) {
    try {
      const r = await fetch(url);
      if (!r.ok) return;
      const xml = await r.text();
      const parsed = await xml2js.parseStringPromise(xml);

      if (parsed.urlset?.url) {
        for (const u of parsed.urlset.url) {
          if (u.loc?.[0]) urls.push(u.loc[0]);
        }
      }

      if (parsed.sitemapindex?.sitemap) {
        for (const sm of parsed.sitemapindex.sitemap) {
          if (sm.loc?.[0]) await parse(sm.loc[0]);
        }
      }
    } catch {}
  }

  await parse(`${base}/sitemap.xml`);
  return urls;
}

/* =====================
   CRAWLER
===================== */
async function crawl(startUrl) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const ctx = await browser.newContext();
  const pages = [];
  const visited = new Set();

  const host = normalizeHost(startUrl);
  const sitemap = await getSitemapUrls(new URL(startUrl).origin);

  const queue = sitemap
    .filter(
      (u) =>
        normalizeHost(u) === host &&
        !BLOCK_RE.test(u)
    )
    .sort((a, b) => (PRIORITY_RE.test(b) ? 1 : -1));

  if (!queue.length) queue.push(startUrl);

  for (const url of queue) {
    if (pages.length >= MAX_PAGES) break;
    if (visited.has(url)) continue;
    visited.add(url);

    let page;
    try {
      page = await ctx.newPage();
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 25000 });

      const text = clean(
        await page.evaluate(() => document.body?.innerText || "")
      );

      if (
        text.length < MIN_TEXT_CHARS ||
        !BUSINESS_RE.test(text)
      ) {
        continue;
      }

      const title = clean(await page.title());

      pages.push({ url, title, content: text });
      console.log("[CRAWL] ✔", pages.length, url);
    } catch {
    } finally {
      if (page) await page.close();
    }
  }

  await browser.close();
  return pages;
}

/* =====================
   SERVER
===================== */
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
        res.end(JSON.stringify({ success: false, error: e.message }));
      }
    });
  })
  .listen(PORT, () => {
    console.log("Crawler running on", PORT);
  });
