import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

// DETAIL-FIRST LIMITS
const MAX_SECONDS = 45;
const MIN_WORDS = 30;
const MAX_CHILD_PER_PAGE = 4;

const SKIP_URL_RE =
  /(wp-content|uploads|media|images|gallery|video|photo|attachment|category|tag|page\/)/i;

const clean = (t = "") => t.replace(/\s+/g, " ").trim();
const countWords = (t = "") => t.split(" ").filter(w => w.length > 2).length;

/* =========================
   PAGE TYPE DETECTOR (SAFE)
========================= */
function detectPageType(url = "", title = "") {
  const u = url.toLowerCase();
  const t = title.toLowerCase();

  if (/za-nas|about/.test(u + t)) return "about";
  if (/uslugi|services/.test(u + t)) return "services";
  if (/kontakti|contact/.test(u + t)) return "contact";
  if (/blog|news|articles/.test(u + t)) return "blog";
  return "general";
}

/* =========================
   STRUCTURED EXTRACTOR
========================= */
async function extractStructured(page) {
  try {
    await page.waitForSelector("main, article, p", { timeout: 3000 });
  } catch {}

  return await page.evaluate(() => {
    const bad = ["header", "footer", "nav", "aside"];

    bad.forEach(sel => {
      document.querySelectorAll(sel).forEach(n => n.remove());
    });

    const headings = Array.from(
      document.querySelectorAll("h1, h2, h3")
    )
      .filter(h => h.offsetParent !== null)
      .map(h => h.innerText.trim());

    const sections = [];
    let current = null;

    document
      .querySelectorAll("h1, h2, h3, p, li")
      .forEach(el => {
        if (el.tagName.startsWith("H")) {
          current = { heading: el.innerText.trim(), text: "" };
          sections.push(current);
        } else if (current) {
          current.text += " " + el.innerText;
        }
      });

    const content = document.body.innerText || "";

    const summary = [];
    document.querySelectorAll("p, li").forEach(el => {
      const t = el.innerText.trim();
      if (t.length > 60 && summary.length < 5) {
        summary.push(t);
      }
    });

    return {
      headings,
      sections: sections.map(s => ({
        heading: s.heading,
        text: s.text.trim(),
      })),
      summary,
      content,
    };
  });
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
   COLLECT CONTENT LINKS
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

      const title = clean(await page.title());
      const data = await extractStructured(page);
      const words = countWords(data.content);

      if (words < MIN_WORDS) continue;

      pages.push({
        url,
        title,
        pageType: detectPageType(url, title),
        headings: data.headings,
        sections: data.sections,
        summary: data.summary,
        content: clean(data.content),
        wordCount: words,
      });

      console.log(`[SAVE] ${url} (${words} words)`);

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

          const cTitle = clean(await page.title());
          const cData = await extractStructured(page);
          const cWords = countWords(cData.content);

          if (cWords < MIN_WORDS) continue;

          pages.push({
            url: child,
            title: cTitle,
            pageType: detectPageType(child, cTitle),
            headings: cData.headings,
            sections: cData.sections,
            summary: cData.summary,
            content: clean(cData.content),
            wordCount: cWords,
          });

          console.log(`[DETAIL] ${child} (${cWords} words)`);
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
