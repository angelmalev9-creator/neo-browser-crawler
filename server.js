import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

// LIMITS
const MAX_SECONDS = 45;
const MIN_WORDS = 30;
const MAX_CHILD_PER_PAGE = 4;

// OCR FLAG
const ENABLE_OCR = true;

const SKIP_URL_RE =
  /(wp-content|uploads|media|gallery|video|photo|attachment|category|tag|page\/)/i;

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
   PAGE TYPE
========================= */
function detectPageType(url = "", title = "") {
  const s = (url + " " + title).toLowerCase();
  if (/za-nas|about/.test(s)) return "about";
  if (/uslugi|services/.test(s)) return "services";
  if (/kontakt|contact/.test(s)) return "contact";
  if (/blog|news|article/.test(s)) return "blog";
  return "general";
}

/* =========================
   ADDRESS + CONTACT EXTRACTOR
========================= */
async function extractBusinessInfo(page) {
  return await page.evaluate(() => {
    const text = document.body.innerText || "";

    const emails = [...new Set(
      text.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}/g) || []
    )];

    const phones = [...new Set(
      text.match(/(\+?\d[\d\s\-()]{7,})/g) || []
    )];

    const addresses = [];

    document.querySelectorAll("address").forEach(a => {
      addresses.push(a.innerText.trim());
    });

    document
      .querySelectorAll('[itemtype*="PostalAddress"]')
      .forEach(el => addresses.push(el.innerText.trim()));

    const addressRegex =
      /(бул\.|ул\.|улица|ж\.к\.|кв\.|pl\.|street|st\.|road|rd\.|blvd|avenue).{5,80}/gi;

    (text.match(addressRegex) || []).forEach(a => addresses.push(a));

    return {
      emails,
      phones,
      addresses: [...new Set(addresses)].filter(a => a.length > 10),
    };
  });
}

/* =========================
   OCR (IMAGE TEXT)
========================= */
async function extractImageText(page) {
  if (!ENABLE_OCR) return "";

  const images = await page.$$eval("img", imgs =>
    imgs
      .map(i => i.src)
      .filter(s => s && !s.startsWith("data:"))
      .slice(0, 5)
  );

  let ocrText = "";

  for (let i = 0; i < images.length; i++) {
    try {
      const img = await page.goto(images[i]);
      const buffer = await img.body();
      // ⚠️ placeholder – OCR hook
      // тук реално се вика Tesseract / API
      ocrText += " ";
    } catch {}
  }

  return ocrText.trim();
}

/* =========================
   CONTENT EXTRACTOR
========================= */
async function extractStructured(page) {
  return await page.evaluate(() => {
    ["header", "footer", "nav", "aside"].forEach(sel => {
      document.querySelectorAll(sel).forEach(n => n.remove());
    });

    const headings = [...document.querySelectorAll("h1,h2,h3")]
      .map(h => h.innerText.trim());

    const content =
      document.querySelector("main")?.innerText ||
      document.querySelector("article")?.innerText ||
      document.body.innerText ||
      "";

    return { headings, content };
  });
}

/* =========================
   CRAWLER
========================= */
async function crawlSmart(startUrl) {
  const deadline = Date.now() + MAX_SECONDS * 1000;

  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox"],
  });

  const context = await browser.newContext();

  await context.route("**/*", route => {
    const t = route.request().resourceType();
    if (t === "media" || t === "font") return route.abort();
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

  const targets = [page.url()];

  for (const url of targets) {
    if (Date.now() > deadline) break;
    if (visited.has(url)) continue;
    visited.add(url);

    if (!(await safeGoto(page, url))) continue;

    const title = clean(await page.title());
    const structured = await extractStructured(page);
    const business = await extractBusinessInfo(page);
    const imageText = await extractImageText(page);

    const fullContent = clean(
      structured.content + " " + imageText
    );

    const words = countWords(fullContent);
    if (words < MIN_WORDS) continue;

    pages.push({
      url,
      title,
      pageType: detectPageType(url, title),
      headings: structured.headings,
      content: fullContent,
      wordCount: words,
      business,
      status: "ok",
    });
  }

  await browser.close();
  return pages;
}

/* =========================
   SERVER
========================= */
http.createServer((req, res) => {
  if (req.method !== "POST") return res.end();

  let body = "";
  req.on("data", c => body += c);
  req.on("end", async () => {
    try {
      const { url } = JSON.parse(body || "{}");
      const pages = await crawlSmart(url);
      res.end(JSON.stringify({ success: true, pages }));
    } catch (e) {
      res.end(JSON.stringify({
        success: false,
        error: e instanceof Error ? e.message : String(e)
      }));
    }
  });
}).listen(PORT, () => {
  console.log("Crawler running on", PORT);
});
