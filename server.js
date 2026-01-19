import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

// ================= LIMITS =================
const MAX_SECONDS = 45;
const MIN_WORDS = 30;
const MAX_CHILD_PER_PAGE = 4;

// OCR limits
const MAX_OCR_ELEMENTS = 4;
const MIN_OCR_AREA = 2500; // px²
const OCR_TIMEOUT_MS = 3000;

// ================= UTILS =================
const SKIP_URL_RE =
  /(wp-content|uploads|media|images|gallery|video|photo|attachment|category|tag|page\/)/i;

const clean = (t = "") => t.replace(/\s+/g, " ").trim();
const countWords = (t = "") => t.split(" ").filter(w => w.length > 2).length;

function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error("timeout")), ms)
    ),
  ]);
}

// ================= SAFE GOTO =================
async function safeGoto(page, url, timeout = 12000) {
  try {
    await page.goto(url, { timeout, waitUntil: "domcontentloaded" });
    return true;
  } catch (e) {
    console.error("[GOTO FAIL]", url);
    return false;
  }
}

// ================= PAGE TYPE =================
function detectPageType(url = "", title = "") {
  const s = (url + " " + title).toLowerCase();
  if (/za-nas|about/.test(s)) return "about";
  if (/uslugi|services|pricing|price/.test(s)) return "services";
  if (/kontakti|contact/.test(s)) return "contact";
  if (/blog|news|article/.test(s)) return "blog";
  return "general";
}

// ================= OCR =================
async function extractImageText(page, pageType) {
  // OCR само когато има смисъл
  if (!["services", "general"].includes(pageType)) return "";

  const images = await page.$$("img");
  if (!images.length) return "";

  let Tesseract;
  try {
    ({ default: Tesseract } = await import("tesseract.js"));
  } catch {
    return "";
  }

  const extracted = [];
  let taken = 0;

  for (const img of images) {
    if (taken >= MAX_OCR_ELEMENTS) break;

    try {
      const box = await img.boundingBox();
      if (!box) continue;

      if (box.width * box.height < MIN_OCR_AREA) continue;

      const buffer = await img.screenshot({ type: "png" });

      const result = await withTimeout(
        Tesseract.recognize(buffer, "eng+bul", {
          tessedit_pageseg_mode: 6,
        }),
        OCR_TIMEOUT_MS
      );

      const text = clean(result?.data?.text || "");
      if (text.length > 3) {
        extracted.push(text);
        taken++;
      }
    } catch {
      // OCR fail → skip
    }
  }

  return extracted.join(" ");
}

// ================= CONTENT =================
async function extractStructured(page) {
  try {
    await page.waitForSelector("body", { timeout: 3000 });
  } catch {}

  return await page.evaluate(() => {
    ["header", "footer", "nav", "aside"].forEach(sel => {
      document.querySelectorAll(sel).forEach(n => n.remove());
    });

    const headings = [...document.querySelectorAll("h1,h2,h3")]
      .filter(h => h.offsetParent !== null)
      .map(h => h.innerText.trim());

    const content =
      document.querySelector("main")?.innerText ||
      document.querySelector("article")?.innerText ||
      document.body.innerText ||
      "";

    return { headings, content };
  });
}

// ================= LINKS =================
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

// ================= CRAWLER =================
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
    throw new Error("Start URL failed");
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

    try {
      const title = clean(await page.title());
      const pageType = detectPageType(url, title);

      const data = await extractStructured(page);
      const imageText = await extractImageText(page, pageType);

      const mergedContent = clean(data.content + " " + imageText);
      const words = countWords(mergedContent);
      if (words < MIN_WORDS) continue;

      pages.push({
        url,
        title,
        pageType,
        content: mergedContent,
        imageText,
        wordCount: words,
        status: "ok",
      });

    } catch {
      pages.push({ url, status: "failed" });
    }
  }

  await browser.close();
  return pages;
}

// ================= SERVER =================
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
      res.end(JSON.stringify({ success: true, pages }));
    } catch (e) {
      res.writeHead(500);
      res.end(JSON.stringify({ success: false, error: e.message }));
    }
  });
}).listen(PORT, () => {
  console.log("Crawler running on", PORT);
});
