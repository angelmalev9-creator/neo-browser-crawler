import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);
const MAX_PAGES_DEFAULT = 40;

const IMPORTANT_RE =
  /about|за-нас|services|услуги|pricing|цени|price|contact|контакти|menu|меню|booking|reservation|appointment|запази/i;

const clean = (t = "") =>
  t.replace(/[\u200B-\u200D\uFEFF]/g, "").replace(/\s+/g, " ").trim();

async function crawl(url, maxPages) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const page = await browser.newPage();
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
  await page.waitForTimeout(1500);

  const base = new URL(url).origin;

  const pages = [];

  const title = clean(await page.title());
  const text = clean(await page.evaluate(() => document.body?.innerText || ""));
  pages.push({ url, title, content: text });

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

  for (const link of importantLinks) {
    try {
      const p = await browser.newPage();
      await p.goto(link, { waitUntil: "domcontentloaded", timeout: 25000 });
      await p.waitForTimeout(1200);

      const t = clean(await p.title());
      const c = clean(await p.evaluate(() => document.body?.innerText || ""));

      if (c.length > 200) {
        pages.push({ url: link, title: t, content: c });
      }

      await p.close();
    } catch {}
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
        const { url, maxPages } = JSON.parse(body);
        if (!url) throw new Error("Missing url");

        const pages = await crawl(
          url,
          Math.min(Number(maxPages) || MAX_PAGES_DEFAULT, 120)
        );

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
