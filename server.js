import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);
const MAX_PAGES = 40;
const MIN_TEXT_LEN = 180;

/* ================= UTILS ================= */
const cleanText = (t = "") =>
  String(t)
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/\s+/g, " ")
    .trim();

const sameOrigin = (base, link) => {
  try {
    return new URL(link).origin === new URL(base).origin;
  } catch {
    return false;
  }
};

/* ================= CRAWLER ================= */
async function crawlSite(startUrl) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();
  const visited = new Set();
  const pages = [];

  async function crawlPage(url) {
    if (visited.has(url)) return;
    if (pages.length >= MAX_PAGES) return;

    visited.add(url);

    const page = await context.newPage();
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      await page.waitForTimeout(800);

      const title = cleanText(await page.title());
      const text = cleanText(
        await page.evaluate(() => document.body?.innerText || "")
      );

      if (text.length >= MIN_TEXT_LEN) {
        pages.push({ url, title, content: text });
      }

      const links = await page.evaluate(() =>
        Array.from(document.querySelectorAll("a[href]"))
          .map((a) => a.href)
          .filter(Boolean)
      );

      for (const link of links) {
        if (pages.length >= MAX_PAGES) break;
        if (sameOrigin(startUrl, link)) {
          await crawlPage(link);
        }
      }
    } catch {}
    finally {
      await page.close();
    }
  }

  await crawlPage(startUrl);
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
        const { url, maxPages } = JSON.parse(body);
        if (!url) throw new Error("Missing url");

        if (typeof maxPages === "number" && maxPages > 0) {
          globalThis.MAX_PAGES = Math.min(maxPages, 120);
        }

        const pages = await crawlSite(url);

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
    console.log("Crawler running on port", PORT);
  });
