import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

const MAX_PAGES = 50;
const PAGE_TIMEOUT = 15000;
const MIN_WORDS = 50; // ⬅️ ВАЖНО: минимум 50 думи

const SKIP_URL_RE =
  /\/(wp-content|uploads|media|images|gallery|video|photo|attachment)/i;

const clean = (t = "") => t.replace(/\s+/g, " ").trim();

function countWords(text) {
  return text.split(" ").filter(w => w.length > 2).length;
}

/* =========================
   PAGE CONTENT EXTRACTOR
========================= */
async function extractVisibleText(page) {
  return clean(
    await page.evaluate(() => {
      const selectors = [
        "main p", "main li", "main h1", "main h2", "main h3", "main h4",
        "article p", "article li", "article h1", "article h2", "article h3"
      ];

      const nodes = document.querySelectorAll(selectors.join(","));
      return Array.from(nodes)
        .filter(el => el.offsetParent !== null)
        .map(el => el.innerText)
        .join(" ");
    })
  );
}

/* =========================
   MAIN CRAWLER (CLICK-BASED)
========================= */
async function crawlSite(startUrl) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();

  // 🚫 BLOCK IMAGES / MEDIA / FONTS
  await context.route("**/*", route => {
    const type = route.request().resourceType();
    if (["image", "media", "font"].includes(type)) {
      return route.abort();
    }
    route.continue();
  });

  const page = await context.newPage();
  await page.goto(startUrl, { timeout: PAGE_TIMEOUT });

  const base = new URL(page.url()).origin;

  const visited = new Set();
  const queue = [page.url()];
  const pages = [];

  console.log(`[CRAWL] Start from ${page.url()}`);

  while (queue.length && pages.length < MAX_PAGES) {
    const url = queue.shift();
    if (!url || visited.has(url)) continue;

    visited.add(url);

    try {
      await page.goto(url, { timeout: PAGE_TIMEOUT });

      const text = await extractVisibleText(page);
      const words = countWords(text);

      if (words >= MIN_WORDS) {
        pages.push({
          url,
          title: clean(await page.title()),
          content: text,
        });
        console.log(`[SAVE] ${pages.length}: ${url} (${words} words)`);
      } else {
        console.log(`[SKIP] ${url} (${words} words)`);
      }

      // collect clickable targets
      const links = await page.evaluate(() => {
        const els = [
          ...document.querySelectorAll("a[href]"),
          ...document.querySelectorAll("button"),
          ...document.querySelectorAll("[role='button']")
        ];

        return els
          .map(el => el.href || el.getAttribute("data-href"))
          .filter(Boolean);
      });

      for (const link of links) {
        try {
          const u = new URL(link, base).href;
          if (
            u.startsWith(base) &&
            !visited.has(u) &&
            !SKIP_URL_RE.test(u)
          ) {
            queue.push(u);
          }
        } catch {}
      }

    } catch {
      console.log(`[ERROR] ${url}`);
      continue;
    }
  }

  await browser.close();
  console.log(`[DONE] Pages saved: ${pages.length}`);
  return pages;
}

/* =========================
   HTTP SERVER
========================= */
http.createServer((req, res) => {
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

      const pages = await crawlSite(url);

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        success: true,
        pagesCount: pages.length,
        pages,
      }));
    } catch (e) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: e.message }));
    }
  });
}).listen(PORT, () => {
  console.log("Crawler running on", PORT);
});
