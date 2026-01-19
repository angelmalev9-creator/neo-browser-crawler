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
   SAFE GOTO (NO SILENT FAIL)
========================= */
async function safeGoto(page, url, timeout = 12000) {
  try {
    await page.goto(url, {
      timeout,
      waitUntil: "domcontentloaded",
    });
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
   IMAGE TEXT EXTRACTOR
========================= */
async function extractImageText(page) {
  return await page.evaluate(() => {
    const texts = [];

    document.querySelectorAll("img").forEach(img => {
      if (img.alt) texts.push(img.alt);
      if (img.title) texts.push(img.title);
      if (img.getAttribute("aria-label"))
        texts.push(img.getAttribute("aria-label"));
    });

    // OCR HOOK (placeholder)
    // тук по-късно се връзва Tesseract / external OCR API

    return texts.join(" ");
  });
}

/* =========================
   STRUCTURED EXTRACTOR (ROBUST)
========================= */
async function extractStructured(page) {
  try {
    await page.waitForSelector("body", { timeout: 3000 });
  } catch {}

  return await page.evaluate(() => {
    ["header", "footer", "nav", "aside"].forEach(sel => {
      document.querySelectorAll(sel).forEach(n => n.remove());
    });

    document.querySelectorAll("button").forEach(b => {
      const t = (b.innerText || "").toLowerCase();
      if (
        t.includes("accept") ||
        t.includes("agree") ||
        t.includes("allow") ||
        t.includes("прием")
      ) {
        b.click();
      }
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

    let content = "";
    if (document.querySelector("main")) {
      content = document.querySelector("main").innerText;
    } else if (document.querySelector("article")) {
      content = document.querySelector("article").innerText;
    } else {
      content = document.body.innerText || "";
    }

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
   LINK COLLECTORS
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
    const type = route.request().resourceType();
    if (["media", "font"].includes(type)) {
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

  console.log("[CRAWL] Start", page.url());

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
      if (!(await safeGoto(page, url))) continue;

      const title = clean(await page.title());
      const data = await extractStructured(page);
      const imageText = await extractImageText(page);

      const mergedContent = clean(
        data.content + " " + imageText
      );

      const words = countWords(mergedContent);

      if (words < MIN_WORDS) {
        console.log("[SKIP] Thin page", url);
        continue;
      }

      pages.push({
        url,
        title,
        pageType: detectPageType(url, title),
        headings: data.headings,
        sections: data.sections,
        summary: data.summary,
        content: mergedContent,
        imageText,
        wordCount: words,
        status: "ok",
      });

      console.log("[SAVE]", url, words);

    } catch (e) {
      console.error("[PAGE FAIL]", url, e.message);
      pages.push({ url, status: "failed" });
    }
  }

  await browser.close();
  console.log("[DONE] Pages:", pages.length);
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
      console.error("[CRAWL ERROR]", e.message);
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: e.message }));
    }
  });
}).listen(PORT, () => {
  console.log("Crawler running on", PORT);
});
