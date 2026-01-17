import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);
const MAX_PAGES = 120;

const IMPORTANT_PATHS = [
  "/about",
  "/za-nas",
  "/services",
  "/uslugi",
  "/pricing",
  "/prices",
  "/ceni",
  "/contact",
  "/kontakti",
  "/menu",
  "/booking",
];

const clean = (t = "") =>
  t.replace(/[\u200B-\u200D\uFEFF]/g, "").replace(/\s+/g, " ").trim();

async function crawl(startUrl) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();
  const base = new URL(startUrl).origin;

  const visited = new Set();
  const queue = [];
  const pages = [];

  // 1️⃣ forced important urls
  for (const p of IMPORTANT_PATHS) {
    queue.push(base + p);
  }
  queue.push(startUrl);

  while (queue.length && pages.length < MAX_PAGES) {
    const url = queue.shift();
    if (!url || visited.has(url)) continue;
    visited.add(url);

    let page;
    try {
      page = await context.newPage();
      await page.goto(url, { waitUntil: "networkidle", timeout: 45000 });

      // scroll
      for (let i = 0; i < 5; i++) {
        await page.evaluate(() => window.scrollBy(0, 1600));
        await page.waitForTimeout(400);
      }

      // click common buttons / menus
      const clickables = await page.$$(
        "button, [role='button'], .menu-toggle, .hamburger"
      );
      for (const el of clickables.slice(0, 5)) {
        try {
          await el.click({ delay: 50 });
          await page.waitForTimeout(300);
        } catch {}
      }

      const title = clean(await page.title());
      const content = clean(
        await page.evaluate(() => document.body?.innerText || "")
      );

      if (content.length > 300) {
        pages.push({ url, title, content });
        console.log("[CRAWL] Added:", pages.length, url);
      }

      // extract links (advanced)
      const links = await page.evaluate(() => {
        const urls = new Set();

        document.querySelectorAll("a[href]").forEach(a => urls.add(a.href));
        document.querySelectorAll("[data-href]").forEach(el => urls.add(el.dataset.href));
        document.querySelectorAll("[onclick]").forEach(el => {
          const m = el.getAttribute("onclick")?.match(/location\.href='([^']+)'/);
          if (m) urls.add(m[1]);
        });

        return Array.from(urls);
      });

      for (const l of links) {
        try {
          const u = new URL(l, base);
          if (u.origin === base && !visited.has(u.href)) {
            queue.push(u.href);
          }
        } catch {}
      }
    } catch (e) {
      console.log("[CRAWL] Skip", url);
    } finally {
      if (page) await page.close();
    }
  }

  await browser.close();
  return pages;
}

http.createServer(async (req, res) => {
  if (req.method !== "POST") {
    res.writeHead(405);
    return res.end();
  }

  let body = "";
  req.on("data", c => body += c);
  req.on("end", async () => {
    try {
      const { url } = JSON.parse(body || "{}");
      if (!url) throw new Error("Missing url");

      const pages = await crawl(url);

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        success: true,
        pagesCount: pages.length,
        pages,
      }));
    } catch (e) {
      res.writeHead(500);
      res.end(JSON.stringify({ success: false, error: e.message }));
    }
  });
}).listen(PORT, () => {
  console.log("Crawler running on", PORT);
});
