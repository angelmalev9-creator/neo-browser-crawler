import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

// ================= LIMITS =================
const MAX_SECONDS = 180;
const MIN_WORDS = 20;
const MAX_OCR_BLOCKS = 3;

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
    const sections = [];
    let current = null;

    document.querySelectorAll("h1,h2,h3,p,li").forEach(el => {
      if (el.tagName.startsWith("H")) {
        current = { heading: el.innerText.trim(), text: "" };
        sections.push(current);
      } else if (current) {
        current.text += " " + el.innerText;
      }
    });

    const mainContent =
      document.querySelector("main")?.innerText ||
      document.querySelector("article")?.innerText ||
      document.body.innerText ||
      "";

    return {
      rawContent: [
        sections.map(s => `${s.heading}\n${s.text}`).join("\n\n"),
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
    console.log("[OCR] screenshot sent to Vision API");

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

    console.log(
      "[OCR] Vision response chars:",
      json.responses?.[0]?.fullTextAnnotation?.text?.length || 0
    );

    return json.responses?.[0]?.fullTextAnnotation?.text?.trim() || "";
  } catch (e) {
    console.error("[OCR FAIL]", e.message);
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

    const title = clean(await page.title());
    const pageType = detectPageType(url, title);
    stats.byType[pageType] = (stats.byType[pageType] || 0) + 1;

    const data = await extractStructured(page);
    
 // <<< ADDED: trigger JS-rendered / scroll-based content
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(1200);
    // <<< END ADDED

    // const title = clean(await page.title());
    // const pageType = detectPageType(url, title);
    stats.byType[pageType] = (stats.byType[pageType] || 0) + 1;

    // const data = await extractStructured(page);
    // ===== OCR =====
    let ocrText = "";

    if (pageType === "services" || pageType === "general") {
      console.log("[OCR] checking images on page:", url);

      const images = await page.$$("img");

      for (const img of images) {
        if (stats.ocrBlocksUsed >= MAX_OCR_BLOCKS) break;

        const box = await img.boundingBox();
        if (!box || box.width < 200 || box.height < 200) continue;

        const text = await ocrElementScreenshot(page, img);
        if (text && /\d+\s?(€|лв|eur|bgn|кв\.?|sqm)/i.test(text)) {
          console.log("[OCR HIT]", text.slice(0, 120));
          ocrText += "\n" + text;
          stats.ocrBlocksUsed++;
        }
      }
    }

   const htmlContent = normalizeNumbers(clean(data.rawContent));
const ocrContent = normalizeNumbers(clean(ocrText));


    const content = `
=== HTML_CONTENT_START ===
${htmlContent}
=== HTML_CONTENT_END ===

=== OCR_CONTENT_START ===
${ocrContent}
=== OCR_CONTENT_END ===
`.trim();

    const htmlWords = countWordsExact(htmlContent);
    const ocrWords = countWordsExact(ocrContent);
    const totalWords = countWordsExact(content);

    console.log(`
[CONTENT STATS]
url: ${url}
type: ${pageType}
htmlWords: ${htmlWords}
ocrWords: ${ocrWords}
totalWords: ${totalWords}
`);

    if (pageType !== "services" && totalWords < MIN_WORDS) {
      console.log("[SKIP] too few words:", totalWords);
      continue;
    }

    pages.push({
      url,
      title,
      pageType,
      content,
      wordCount: totalWords,
      breakdown: {
        htmlWords,
        ocrWords,
      },
      status: "ok",
    });

    stats.saved++;

    const links = await collectAllLinks(page, base);
    links.forEach(l => {
      if (!visited.has(l) && !SKIP_URL_RE.test(l)) queue.push(l);
    });
  }

  await browser.close();
  return { pages, stats };
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
