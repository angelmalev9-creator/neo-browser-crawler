import http from "http";
import { chromium } from "playwright";
import Tesseract from "tesseract.js";

const PORT = Number(process.env.PORT || 10000);

// DETAIL-FIRST LIMITS
const MAX_SECONDS = 45;
const MIN_WORDS = 30;
const MAX_CHILD_PER_PAGE = 4;

// OCR LIMITS
const MAX_OCR_ELEMENTS = 6;
const MIN_OCR_AREA = 2500; // px²

const SKIP_URL_RE =
  /(wp-content|uploads|media|images|gallery|video|photo|attachment|category|tag|page\/)/i;

const clean = (t = "") => t.replace(/\s+/g, " ").trim();
const countWords = (t = "") => t.split(" ").filter(w => w.length > 2).length;

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
   IMAGE OCR EXTRACTOR (REAL)
========================= */
async function extractImageText(page) {
  const elements = await page.$$(
    "img, button img, a img, button, a"
  );

  let extracted = [];
  let taken = 0;

  for (const el of elements) {
    if (taken >= MAX_OCR_ELEMENTS) break;

    try {
      const box = await el.boundingBox();
      if (!box) continue;

      const area = box.width * box.height;
      if (area < MIN_OCR_AREA) continue;

      const buffer = await el.screenshot({ type: "png" });

      const result = await Tesseract.recognize(buffer, "eng+bul", {
        tessedit_pageseg_mode: 6,
      });

      const text = clean(result.data.text || "");
      if (text.length > 3) {
        extracted.push(text);
        taken++;
      }
    } catch {
      // silent skip
    }
  }

  return extracted.join(" ");
}

/* =========================
   STRUCTURED EXTRACTOR
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

    const headings = [...document.querySelectorAll("h1,h2,h3")]
      .filter(h => h.offsetParent !== null)
      .map(h => h.innerText.trim());

    let content =
      document.querySelector("main")?.innerText ||
      document.querySelector("article")?.innerText ||
      document.body.innerText ||
      "";

    return { headings, content };
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
    if (["media", "font"].includes(type)) return route.abort();
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

    try {
      if (!(await safeGoto(page, url))) continue;

      const title = clean(await page.title());
      const data = await extractStructured(page);
      const imageText = await extractImageText(page);

      const mergedContent = clean(
        data.content + " " + imageText
      );

      const words = countWords(mergedContent);
      if (words < MIN_WORDS) continue;

      pages.push({
        url,
        title,
        pageType: detectPageType(url, title),
        content: mergedContent,
        imageText,
        wordCount: words,
        status: "ok",
      });

    } catch (e) {
      pages.push({ url, status: "failed" });
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
      const pages = await crawlSmart(url);

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: true, pages }));
    } catch (e) {
      res.writeHead(500);
      res.end(JSON.stringify({ success: false, error: e.message }));
    }
  });
}).listen(PORT);
