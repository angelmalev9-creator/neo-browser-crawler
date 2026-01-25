import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

// ================= LIMITS =================
const MAX_SECONDS = 180;
const MIN_WORDS = 20;
const MAX_OCR_BLOCKS = 5;

// режем САМО реален шум
const SKIP_URL_RE =
  /(wp-content|uploads|media|gallery|video|photo|attachment|privacy|terms|cookies|gdpr)/i;

// ================= UTILS =================
const clean = (t = "") =>
  t
    .replace(/\r/g, "")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

const countWordsExact = (t = "") =>
  t.split(/\s+/).filter(Boolean).length;

// ================= BG NUMBER NORMALIZER =================
const BG_0_19 = [
  "нула","едно","две","три","четири","пет","шест","седем","осем","девет",
  "десет","единадесет","дванадесет","тринадесет","четиринадесет",
  "петнадесет","шестнадесет","седемнадесет","осемнадесет","деветнадесет"
];

const BG_TENS = [
  "", "", "двадесет","тридесет","четиридесет",
  "петдесет","шестдесет","седемдесет","осемдесет","деветдесет"
];

function numberToBgWords(n) {
  n = Number(n);
  if (Number.isNaN(n)) return n;

  if (n < 20) return BG_0_19[n];

  if (n < 100) {
    const t = Math.floor(n / 10);
    const r = n % 10;
    return BG_TENS[t] + (r ? " и " + BG_0_19[r] : "");
  }

  // НЕ пипаме по-големи числа (телефони, години, ID)
  return String(n);
}

function normalizeNumbers(text = "") {
  return text.replace(
    /(\d+)\s?(лв|лева|€|eur|bgn|стая|стаи|човек|човека|нощувка|нощувки|кв\.?|sqm)/gi,
    (_, num, unit) => `${numberToBgWords(num)} ${unit}`
  );
}

function extractPricing(text = "") {
  const results = [];

  const patterns = [
    /(basic|standart|standard|premium)[^\d]{0,40}(\d{2,4})\s?(€|eur|лв|leva|bgn)[^\n]{0,20}(кв\.?\s?м|sqm)?/gi,
    /(\d{2,4})\s?(€|eur|лв|leva|bgn)[^\n]{0,20}(кв\.?\s?м|sqm)[^\n]{0,40}(basic|standart|standard|premium)/gi
  ];

  for (const re of patterns) {
    let match;
    while ((match = re.exec(text)) !== null) {
      const pkg = (match[1] || match[4] || "").toLowerCase();
      const price = Number(match[2] || match[1]);
      const currency = (match[3] || match[2] || "").toUpperCase();

      if (!Number.isNaN(price)) {
        results.push({
          package: pkg,
          price_per_sqm: price,
          currency
        });
      }
    }
  }

  return results;
}


// ================= SAFE GOTO =================
async function safeGoto(page, url, timeout = 20000) {
  try {
    console.log("[GOTO]", url);
    await page.goto(url, { timeout, waitUntil: "domcontentloaded" });
    return true;
  } catch (e) {
    console.error("[GOTO FAIL]", url, e.message);
    return false;
  }
}

// ================= PAGE TYPE =================
function detectPageType(url = "", title = "") {
  const s = (url + " " + title).toLowerCase();
  if (/za-nas|about/.test(s)) return "about";
  if (/uslugi|services|pricing|price|ceni/.test(s)) return "services";
  if (/kontakti|contact/.test(s)) return "contact";
  if (/faq|vuprosi|questions/.test(s)) return "faq";
  if (/blog|news|article/.test(s)) return "blog";
  return "general";
}

// ================= STRUCTURED EXTRACTOR =================
async function extractStructured(page) {
  try {
    await page.waitForSelector("body", { timeout: 5000 });
  } catch {}

  return await page.evaluate(() => {
    function isVisible(el) {
      const style = window.getComputedStyle(el);
      return (
        style &&
        style.display !== "none" &&
        style.visibility !== "hidden" &&
        style.opacity !== "0"
      );
    }

    const textBlocks = [];

    // 1️⃣ Visible text
    document.querySelectorAll(
      "h1,h2,h3,h4,h5,h6,p,li,span,strong,b,button,a,div"
    ).forEach(el => {
      if (!isVisible(el)) return;
      const text = el.innerText?.trim();
      if (text && text.length > 2) {
        textBlocks.push(text);
      }
    });

    // 2️⃣ IMG alt / aria
    document.querySelectorAll("img").forEach(img => {
      if (img.alt) textBlocks.push(img.alt);
      const aria = img.getAttribute("aria-label");
      if (aria) textBlocks.push(aria);
    });

    // 3️⃣ SVG text
    document.querySelectorAll("svg").forEach(svg => {
      svg.querySelectorAll("text,title,desc").forEach(node => {
        const t = node.textContent?.trim();
        if (t && t.length > 2) textBlocks.push(t);
      });
    });

    // 4️⃣ data-* attrs
    document.querySelectorAll("*").forEach(el => {
      Array.from(el.attributes || []).forEach(attr => {
        if (
          attr.name.startsWith("data-") &&
          typeof attr.value === "string" &&
          attr.value.length > 2
        ) {
          textBlocks.push(attr.value);
        }
      });
    });

    const mainContent =
      document.querySelector("main")?.innerText ||
      document.querySelector("article")?.innerText ||
      "";

    return {
      rawContent: [
        textBlocks.join("\n"),
        mainContent,
      ].join("\n\n"),
    };
  });
}




// ================= GOOGLE VISION OCR (SCREENSHOT) =================
async function ocrElementScreenshot(page, elementHandle) {
  const apiKey = process.env.GOOGLE_VISION_API_KEY;
  if (!apiKey) return "";

  try {
    const buffer = await elementHandle.screenshot();
    const base64 = buffer.toString("base64");

    const res = await fetch(
      `https://vision.googleapis.com/v1/images:annotate?key=${apiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          requests: [
            {
              image: { content: base64 },
              features: [{ type: "TEXT_DETECTION" }],
            },
          ],
        }),
      }
    );

    const json = await res.json();
    return json.responses?.[0]?.fullTextAnnotation?.text?.trim() || "";
  } catch {
    return "";
  }
}

async function ocrFullPage(page) {
  const apiKey = process.env.GOOGLE_VISION_API_KEY;
  if (!apiKey) return "";

  try {
    const buffer = await page.screenshot({ fullPage: true });
    const base64 = buffer.toString("base64");

    const res = await fetch(
      `https://vision.googleapis.com/v1/images:annotate?key=${apiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          requests: [
            {
              image: { content: base64 },
              features: [{ type: "TEXT_DETECTION" }],
            },
          ],
        }),
      }
    );

    const json = await res.json();
    return json.responses?.[0]?.fullTextAnnotation?.text?.trim() || "";
  } catch {
    return "";
  }
}



// ================= LINK DISCOVERY =================
async function collectAllLinks(page, base) {
  return await page.evaluate(base => {
    const urls = new Set();
    document.querySelectorAll("a[href]").forEach(a => {
      try {
        const u = new URL(a.href, base);
        if (u.origin === base) urls.add(u.href.split("#")[0]);
      } catch {}
    });
    return Array.from(urls);
  }, base);
}

// ================= CRAWLER =================
async function crawlSmart(startUrl) {
  const deadline = Date.now() + MAX_SECONDS * 1000;
  console.log("\n[CRAWL START]", startUrl);

  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();
  const page = await context.newPage();

  if (!(await safeGoto(page, startUrl))) {
    await browser.close();
    throw new Error("Failed to load start URL");
  }

  const base = new URL(page.url()).origin;
  const visited = new Set();
  const queue = [page.url()];
  const pages = [];

  const stats = {
    visited: 0,
    saved: 0,
    byType: {},
    ocrBlocksUsed: 0,
  };

  while (queue.length && Date.now() < deadline) {
    const url = queue.shift();
    if (!url || visited.has(url) || SKIP_URL_RE.test(url)) continue;

    visited.add(url);
    stats.visited++;

   if (!(await safeGoto(page, url))) continue;

// === PATCH: trigger JS-rendered / scroll-based content ===
for (let i = 0; i < 4; i++) {
  await page.evaluate(() => window.scrollBy(0, window.innerHeight));
  await page.waitForTimeout(500);
}
// === END PATCH ===

const title = clean(await page.title());
const pageType = detectPageType(url, title);
stats.byType[pageType] = (stats.byType[pageType] || 0) + 1;

const data = await extractStructured(page);

// ===== OCR =====
let ocrText = "";

if (
  pageType === "services" ||
  pageType === "general" ||
  /tseni|pricing|price|ceni/.test(url)
) {
  // 1️⃣ OCR images
  const images = await page.$$("img");
  for (const img of images) {
    if (stats.ocrBlocksUsed >= MAX_OCR_BLOCKS) break;

    const box = await img.boundingBox();
    if (!box || box.width < 200 || box.height < 200) continue;

    const text = await ocrElementScreenshot(page, img);
    if (text) {
      ocrText += "\n" + text;
      stats.ocrBlocksUsed++;
    }
  }

  // 2️⃣ OCR large sections (pricing tables, materials, packages)
  const sections = await page.$$(
    "section, article, div[style*='background'], div[class*='price'], div[class*='card']"
  );

  for (const sec of sections) {
    if (stats.ocrBlocksUsed >= MAX_OCR_BLOCKS) break;

    const box = await sec.boundingBox();
    if (!box || box.width < 400 || box.height < 250) continue;

    const text = await ocrElementScreenshot(page, sec);
    if (text && text.length > 30) {
      ocrText += "\n" + text;
      stats.ocrBlocksUsed++;
    }
  }

  // 3️⃣ OCR embedded PDFs / iframes
  const embeds = await page.$$("iframe, embed, object");
  for (const emb of embeds) {
    if (stats.ocrBlocksUsed >= MAX_OCR_BLOCKS) break;

    const box = await emb.boundingBox();
    if (!box || box.width < 400 || box.height < 300) continue;

    const text = await ocrElementScreenshot(page, emb);
    if (text && text.length > 30) {
      ocrText += "\n" + text;
      stats.ocrBlocksUsed++;
    }
  }

  // 🔥 FALLBACK: OCR whole page
  if (!ocrText || ocrText.length < 50) {
    const fullPageText = await ocrFullPage(page);
    if (fullPageText) {
      ocrText += "\n" + fullPageText;
    }
  }
}


// ================= HTTP SERVER =================
http
  .createServer((req, res) => {
    if (req.method === "GET") {
      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ status: "ok" }));
    }

    if (req.method !== "POST") {
      res.writeHead(405);
      return res.end();
    }

    let body = "";
    req.on("data", c => (body += c));
    req.on("end", async () => {
      try {
        const { url } = JSON.parse(body || "{}");
        const result = await crawlSmart(url);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: true, ...result }));
      } catch (e) {
        res.writeHead(500);
        res.end(
          JSON.stringify({
            success: false,
            error: e instanceof Error ? e.message : String(e),
          })
        );
      }
    });
  })
  .listen(PORT, () => {
    console.log("Crawler running on", PORT);
  });
