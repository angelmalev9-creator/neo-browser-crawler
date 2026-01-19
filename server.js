import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

// DETAIL-FIRST LIMITS
const MAX_SECONDS = 45;
const MIN_WORDS = 25;              // ↓ по-малко, но по-умно
const MIN_VALUE_SCORE = 3;         // ⭐ нов quality gate
const MAX_CHILD_PER_PAGE = 4;

const SKIP_URL_RE =
  /(wp-content|uploads|media|images|gallery|video|photo|attachment|category|tag|page\/)/i;

const clean = (t = "") => t.replace(/\s+/g, " ").trim();
const countWords = (t = "") => t.split(/\s+/).filter(w => w.length > 2).length;

/* =========================
   SAFE GOTO
========================= */
async function safeGoto(page, url, timeout = 12000) {
  try {
    await page.goto(url, { timeout, waitUntil: "domcontentloaded" });
    return true;
  } catch (e) {
    console.error("[GOTO FAIL]", url, e.message);
    return false;
  }
}

/* =========================
   PAGE TYPE DETECTOR
========================= */
function detectPageType(url = "", title = "") {
  const s = (url + " " + title).toLowerCase();
  if (/za-nas|about/.test(s)) return "about";
  if (/uslugi|services/.test(s)) return "services";
  if (/kontakti|contact/.test(s)) return "contact";
  if (/blog|news|article/.test(s)) return "blog";
  return "general";
}

/* =========================
   STRUCTURED EXTRACTOR (DETAIL-FIRST)
========================= */
async function extractStructured(page) {
  try {
    await page.waitForSelector("body", { timeout: 3000 });
  } catch {}

  return await page.evaluate(() => {
    ["header", "footer", "nav", "aside"].forEach(sel => {
      document.querySelectorAll(sel).forEach(n => n.remove());
    });

    const grabText = el =>
      el?.textContent?.replace(/\s+/g, " ").trim() || "";

    const headings = Array.from(document.querySelectorAll("h1,h2,h3"))
      .map(h => grabText(h))
      .filter(Boolean);

    const sections = [];
    let current = null;

    document.querySelectorAll("h1,h2,h3,p,li").forEach(el => {
      if (el.tagName.startsWith("H")) {
        current = { heading: grabText(el), text: "" };
        sections.push(current);
      } else if (current) {
        current.text += " " + grabText(el);
      }
    });

    // Lists
    const lists = Array.from(document.querySelectorAll("ul,ol"))
      .map(l =>
        Array.from(l.querySelectorAll("li"))
          .map(li => grabText(li))
          .filter(Boolean)
      )
      .filter(l => l.length > 1);

    // Tables
    const tables = Array.from(document.querySelectorAll("table"))
      .map(t =>
        Array.from(t.querySelectorAll("tr"))
          .map(tr =>
            Array.from(tr.querySelectorAll("th,td"))
              .map(td => grabText(td))
              .filter(Boolean)
          )
          .filter(r => r.length)
      )
      .filter(t => t.length);

    // Definition lists (label:value)
    const definitions = Array.from(document.querySelectorAll("dt"))
      .map(dt => {
        const dd = dt.nextElementSibling;
        return dd ? `${grabText(dt)}: ${grabText(dd)}` : null;
      })
      .filter(Boolean);

    const main =
      document.querySelector("main") ||
      document.querySelector("article") ||
      document.body;

    const content = grabText(main);

    const valueScore =
      (headings.length > 0 ? 1 : 0) +
      (lists.length > 0 ? 1 : 0) +
      (tables.length > 0 ? 1 : 0) +
      (definitions.length > 0 ? 1 : 0);

    return {
      headings,
      sections: sections.map(s => ({
        heading: s.heading,
        text: s.text.trim(),
      })),
      lists,
      tables,
      definitions,
      content,
      valueScore,
    };
  });
}

/* =========================
   LINK COLLECTORS
========================= */
async function collectNavLinks(page, base) {
  return await page.evaluate(base => {
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

async function collectContentLinks(page, base) {
  return await page.evaluate(base => {
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
   SMART CRAWLER
========================= */
async function crawlSmart(startUrl) {
  const deadline = Date.now() + MAX_SECONDS * 1000;

  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();
  await context.route("**/*", route => {
    if (["image", "media", "font"].includes(route.request().resourceType())) {
      return route.abort();
    }
    route.continue();
  });

  const page = await context.newPage();
  if (!(await safeGoto(page, startUrl, 15000))) {
    await browser.close();
    throw new Error("Failed to load start URL");
  }

  const base = new URL(page.url()).origin;
  const visited = new Set();
  const pages = [];

  let targets = await collectNavLinks(page, base);
  targets.unshift(page.url());
  targets = targets.filter(u => u.startsWith(base) && !SKIP_URL_RE.test(u));

  for (const url of targets) {
    if (Date.now() > deadline) break;
    if (visited.has(url)) continue;
    visited.add(url);

    if (!(await safeGoto(page, url))) continue;

    const title = clean(await page.title());
    const data = await extractStructured(page);
    const words = countWords(data.content);

    if (words < MIN_WORDS && data.valueScore < MIN_VALUE_SCORE) {
      console.log("[SKIP] Low value", url);
      continue;
    }

    pages.push({
      url,
      title,
      pageType: detectPageType(url, title),
      ...data,
      wordCount: words,
      status: "ok",
    });

    const children = await collectContentLinks(page, base);
    let taken = 0;

    for (const child of children) {
      if (Date.now() > deadline || taken >= MAX_CHILD_PER_PAGE) break;
      if (visited.has(child) || !child.startsWith(base) || SKIP_URL_RE.test(child)) continue;

      visited.add(child);
      taken++;

      if (!(await safeGoto(page, child, 10000))) continue;

      const cTitle = clean(await page.title());
      const cData = await extractStructured(page);
      const cWords = countWords(cData.content);

      if (cWords < MIN_WORDS && cData.valueScore < MIN_VALUE_SCORE) continue;

      pages.push({
        url: child,
        title: cTitle,
        pageType: detectPageType(child, cTitle),
        ...cData,
        wordCount: cWords,
        status: "ok",
      });
    }
  }

  await browser.close();
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
      res.end(JSON.stringify({ success: true, pagesCount: pages.length, pages }));
    } catch (e) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: e.message }));
    }
  });
}).listen(PORT, () => {
  console.log("Crawler running on", PORT);
});
