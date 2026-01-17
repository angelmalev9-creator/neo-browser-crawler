import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

// DETAIL-FIRST LIMITS
const MAX_SECONDS = 45;            // ⬅️ увеличен hard deadline
const MIN_WORDS = 30;
const MAX_CHILD_PER_PAGE = 4;      // ⬅️ повече детайли на страница

const SKIP_URL_RE =
  /(wp-content|uploads|media|images|gallery|video|photo|attachment|category|tag|page\/)/i;

const clean = (t = "") => t.replace(/\s+/g, " ").trim();
const countWords = (t = "") => t.split(" ").filter(w => w.length > 2).length;

/* =========================
   TEXT EXTRACTOR (ROBUST)
========================= */
async function extractText(page) {
  try {
    await page.waitForSelector(
      ".elementor-text-editor, p, h1",
      { timeout: 3000 }
    );
  } catch {}

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

      let text = Array.from(nodes)
        .filter(el =>
          el.offsetParent !== null &&
          !el.closest(bad.join(","))
        )
        .map(el => el.innerText)
        .join(" ");

      // fallback ако Elementor още не е хидратирал
      if (text.length < 120) {
        const clone = document.body.cloneNode(true);
        clone
          .querySelectorAll("header, footer, nav, aside")
          .forEach(n => n.remove());
        text = clone.innerText || "";
      }

      return text;
    })
  );
}

/* =========================
   COLLECT NAV LINKS
========================= */
async function collectNavLinks(page, base) {
  return await page.evaluate((base) => {
    const urls = new Set();
    document
      .querySelectorAll("header a[href], nav a[href], footer a[href]")
      .forEach(a => {
        try {
          urls.add(new URL(a.href, base).href);
        } catch {}
      });
    return Array.from(urls);
  }, base);
}

/* =========================
   COLLECT CONTENT LINKS (DETAIL)
========================= */
async function collectContentLinks(page, base) {
  return await page.evaluate((base) => {
    const urls = new Set();
    document
      .querySelectorAll("main a[href], article a[href]")
      .forEach(a => {
        try {
          urls.add(new URL(a.href, base).href);
        } catch {}
      });
    return Array.from(urls);
  }, base);
}

/* =========================
   SMART DETAIL CRAWLER
========================= */
async function crawlSmart(startUrl) {
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

  // STEP 1: NAV PAGES
  let targets = await collectNavLinks(page, base);
  targets.unshift(page.url());

  targets = targets.filter(
    u => u.startsWith(base) && !SKIP_URL_RE.test(u)
  );

  for (const url of targets) {
    if (Date.now() > deadline) break;
    if (visited.has(url)) continue;

    visited.add(url);

    try {
      await page.goto(url, { timeout: 12000 });

      const text = await extractText(page);
      const words = countWords(text);

      if (words < MIN_WORDS) continue;

      pages.push({
        url,
        title: clean(await page.title()),
        content: text,
      });
      console.log(`[SAVE] ${url} (${words} words)`);

      // STEP 2: DETAIL PAGES (DEPTH = 2)
      const children = await collectContentLinks(page, base);
      let taken = 0;

      for (const child of children) {
        if (Date.now() > deadline) break;
        if (taken >= MAX_CHILD_PER_PAGE) break;
        if (visited.has(child)) continue;
        if (!child.startsWith(base) || SKIP_URL_RE.test(child)) continue;

        visited.add(child);
        taken++;

        try {
          await page.goto(child, { timeout: 10000 });

          const childText = await extractText(page);
          const childWords = countWords(childText);

          if (childWords < MIN_WORDS) continue;

          pages.push({
            url: child,
            title: clean(await page.title()),
            content: childText,
          });

          console.log(
            `[DETAIL] ${child} (${childWords} words)`
          );
        } catch {}
      }

    } catch {}
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

      const pages = await crawlSmart(url);

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
