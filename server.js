import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

// SPEED-FIRST LIMITS
const MAX_SECONDS = 25;
const MIN_WORDS = 30;

const SKIP_URL_RE =
  /(wp-content|uploads|media|images|gallery|video|photo|attachment|category|tag|page\/)/i;

const clean = (t = "") => t.replace(/\s+/g, " ").trim();
const countWords = (t) => t.split(" ").filter(w => w.length > 2).length;

/* =========================
   TEXT EXTRACTOR (FAST + CLEAN)
========================= */
async function extractText(page) {
  return clean(
    await page.evaluate(() => {
      const bad = ["header", "footer", "nav", "aside"];

      const nodes = document.querySelectorAll(
        ".elementor-text-editor, " +
        ".elementor-widget-text-editor, " +
        "[data-widget_type='text-editor.default'], " +
        "main p, main h1, main h2, main h3, main li, " +
        "article p, article h1, article h2, article h3, article li"
      );

      return Array.from(nodes)
        .filter(el =>
          el.offsetParent !== null &&
          !el.closest(bad.join(","))
        )
        .map(el => el.innerText)
        .join(" ");
    })
  );
}

/* =========================
   COLLECT NAV LINKS
========================= */
async function collectNavLinks(page, base) {
  return await page.evaluate((base) => {
    const urls = new Set();

    // HEADER NAV
    document.querySelectorAll("header a[href], nav a[href]").forEach(a => {
      try {
        const u = new URL(a.href, base).href;
        urls.add(u);
      } catch {}
    });

    // FOOTER (LIMITED)
    document.querySelectorAll("footer a[href]").forEach(a => {
      const text = a.innerText?.toLowerCase() || "";
      if (
        text.includes("about") ||
        text.includes("services") ||
        text.includes("contact") ||
        text.includes("projects") ||
        text.includes("за нас") ||
        text.includes("контакти") ||
        text.includes("услуги") ||
        text.includes("проекти")
      ) {
        try {
          const u = new URL(a.href, base).href;
          urls.add(u);
        } catch {}
      }
    });

    return Array.from(urls);
  }, base);
}

/* =========================
   FAST NAV CRAWLER
========================= */
async function crawlFastNav(startUrl) {
  const deadline = Date.now() + MAX_SECONDS * 1000;

  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();

  // 🚫 BLOCK HEAVY RESOURCES
  await context.route("**/*", route => {
    const type = route.request().resourceType();
    if (["image", "media", "font"].includes(type)) {
      return route.abort();
    }
    route.continue();
  });

  const page = await context.newPage();
  await page.goto(startUrl, { timeout: 15000 });

  const base = new URL(page.url()).origin;
  const visited = new Set();
  const pages = [];

  console.log(`[CRAWL] Start from ${page.url()}`);

  // 1️⃣ Collect important links
  let links = await collectNavLinks(page, base);

  links = links.filter(u =>
    u.startsWith(base) &&
    !SKIP_URL_RE.test(u)
  );

  // always include homepage
  links.unshift(page.url());

  // 2️⃣ Visit them fast
  for (const url of links) {
    if (Date.now() > deadline) break;
    if (visited.has(url)) continue;

    visited.add(url);

    try {
      await page.goto(url, { timeout: 12000 });

      const text = await extractText(page);
      const words = countWords(text);

      if (words >= MIN_WORDS) {
        pages.push({
          url,
          title: clean(await page.title()),
          content: text,
        });
        console.log(`[SAVE] ${url} (${words} words)`);
      } else {
        console.log(`[SKIP] ${url} (${words} words)`);
      }

    } catch {
      console.log(`[ERROR] ${url}`);
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

      const pages = await crawlFastNav(url);

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
