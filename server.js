import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

// ================= LIMITS =================
const MAX_SECONDS = 180;
const MIN_WORDS = 20;

// режем САМО реален шум
const SKIP_URL_RE =
  /(wp-content\/uploads|media|gallery|video|photo|attachment|privacy|terms|cookies|gdpr)/i;

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

  return String(n);
}

function normalizeNumbers(text = "") {
  try {
    return text.replace(
      /(\d+)\s?(лв|лева|€|eur|bgn|стая|стаи|човек|човека|нощувка|нощувки|кв\.?|sqm)/gi,
      (_, num, unit) => `${numberToBgWords(num)} ${unit}`
    );
  } catch (e) {
    console.error("[NORMALIZE NUMBERS ERROR]", e.message);
    return text;
  }
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
  try {
    const s = (url + " " + title).toLowerCase();
    if (/za-nas|about/.test(s)) return "about";
    if (/uslugi|services|pricing|price|ceni|tseni/.test(s)) return "services";
    if (/kontakti|contact/.test(s)) return "contact";
    if (/faq|vuprosi|questions/.test(s)) return "faq";
    if (/blog|news|article/.test(s)) return "blog";
    return "general";
  } catch (e) {
    console.error("[PAGE TYPE ERROR]", e.message);
    return "general";
  }
}

// ================= STRUCTURED EXTRACTOR WITH CSS OVERLAYS =================
async function extractStructured(page) {
  try {
    await page.waitForSelector("body", { timeout: 5000 });
  } catch (e) {
    console.warn("[WAIT BODY TIMEOUT]", e.message);
  }

  try {
    return await page.evaluate(() => {
      const seenTexts = new Set();
      
      function addUniqueText(text, minLength = 10) {
        const normalized = text.trim().replace(/\s+/g, ' ');
        if (normalized.length < minLength) return "";
        if (seenTexts.has(normalized)) return "";
        seenTexts.add(normalized);
        return normalized;
      }

      const sections = [];
      let current = null;
      const processedElements = new Set();

      document.querySelectorAll("h1,h2,h3,p,li").forEach(el => {
        if (processedElements.has(el)) return;
        
        let parent = el.parentElement;
        while (parent) {
          if (processedElements.has(parent)) return;
          parent = parent.parentElement;
        }
        
        const text = el.innerText?.trim();
        if (!text) return;

        const uniqueText = addUniqueText(text, 5);
        if (!uniqueText) return;

        if (el.tagName.startsWith("H")) {
          current = { heading: uniqueText, text: "" };
          sections.push(current);
        } else if (current) {
          current.text += " " + uniqueText;
        }
        
        processedElements.add(el);
      });

      const overlaySelectors = [
        '[class*="overlay"]',
        '[class*="modal"]',
        '[class*="popup"]',
        '[class*="tooltip"]',
        '[class*="banner"]',
        '[class*="notification"]',
        '[class*="alert"]',
        '[style*="position: fixed"]',
        '[style*="position: absolute"]',
        '[role="dialog"]',
        '[role="alertdialog"]',
      ];

      const overlayTexts = [];
      overlaySelectors.forEach(selector => {
        try {
          document.querySelectorAll(selector).forEach(el => {
            if (processedElements.has(el)) return;
            
            const text = el.innerText?.trim();
            if (!text) return;
            
            const uniqueText = addUniqueText(text);
            if (uniqueText) {
              overlayTexts.push(uniqueText);
              processedElements.add(el);
            }
          });
        } catch (e) {}
      });

      const pseudoTexts = [];
      try {
        document.querySelectorAll("*").forEach(el => {
          const before = window.getComputedStyle(el, "::before").content;
          const after = window.getComputedStyle(el, "::after").content;
          
          if (before && before !== "none" && before !== '""') {
            const cleaned = before.replace(/^["']|["']$/g, "");
            const uniqueText = addUniqueText(cleaned, 3);
            if (uniqueText) pseudoTexts.push(uniqueText);
          }
          
          if (after && after !== "none" && after !== '""') {
            const cleaned = after.replace(/^["']|["']$/g, "");
            const uniqueText = addUniqueText(cleaned, 3);
            if (uniqueText) pseudoTexts.push(uniqueText);
          }
        });
      } catch (e) {}

      let mainContent = "";
      const mainEl = document.querySelector("main") || document.querySelector("article");
      if (mainEl && !processedElements.has(mainEl)) {
        const text = mainEl.innerText?.trim();
        if (text) {
          mainContent = addUniqueText(text) || "";
        }
      }

      return {
        rawContent: [
          sections.map(s => `${s.heading}\n${s.text}`).join("\n\n"),
          mainContent,
          overlayTexts.join("\n"),
          pseudoTexts.join(" "),
        ].filter(Boolean).join("\n\n"),
      };
    });
  } catch (e) {
    console.error("[EXTRACT STRUCTURED ERROR]", e.message);
    return { rawContent: "" };
  }
}

// ================= GOOGLE VISION OCR =================
async function ocrElement(page, element, context = "") {
  const apiKey = "AIzaSyCoai4BCKJtnnryHbhsPKxJN35UMcMAKrk";

  try {
    if (page.isClosed()) {
      console.log(`[OCR] ${context}: page closed`);
      return "";
    }

    const box = await element.boundingBox().catch(() => null);
    if (!box) {
      console.log(`[OCR] ${context}: no box`);
      return "";
    }

    if (box.width < 50 || box.height < 30) {
      return "";
    }

    console.log(`[OCR] ${context}: ${Math.round(box.width)}x${Math.round(box.height)}px`);

    const buffer = await element.screenshot({ 
      type: 'png',
      timeout: 10000
    }).catch(e => {
      console.log(`[OCR] ${context}: screenshot failed`);
      return null;
    });

    if (!buffer) return "";

    const base64 = buffer.toString("base64");

    const res = await fetch(
      `https://vision.googleapis.com/v1/images:annotate?key=${apiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          requests: [{
            image: { content: base64 },
            features: [
              { type: "TEXT_DETECTION" },
              { type: "DOCUMENT_TEXT_DETECTION" }
            ],
            imageContext: {
              languageHints: ["bg", "en", "ru"]
            }
          }],
        }),
      }
    );

    if (!res.ok) {
      console.error(`[OCR] ${context}: API ${res.status}`);
      return "";
    }

    const json = await res.json();
    const text = json.responses?.[0]?.fullTextAnnotation?.text?.trim() || "";
    
    if (text) {
      console.log(`[OCR] ✓ ${context}: ${text.length} chars - "${text.slice(0, 80)}"`);
    }

    return text;
  } catch (e) {
    console.error(`[OCR] ${context}: ERROR -`, e.message);
    return "";
  }
}
// ================= OCR QUEUE =================
let ocrRunning = false;
const ocrQueue = [];

async function enqueueOCR(task) {
  ocrQueue.push(task);
  if (ocrRunning) return;

  ocrRunning = true;
  while (ocrQueue.length) {
    const t = ocrQueue.shift();
    await t();
  }
  ocrRunning = false;
}

// ================= LINK DISCOVERY =================
async function collectAllLinks(page, base) {
  try {
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
  } catch (e) {
    console.error("[COLLECT LINKS ERROR]", e.message);
    return [];
  }
}

// ================= CRAWLER =================
async function crawlSmart(startUrl) {
  const deadline = Date.now() + MAX_SECONDS * 1000;
  console.log("\n[CRAWL START]", startUrl);

  let browser;
  try {
    browser = await chromium.launch({
      headless: true,
      args: ["--no-sandbox", "--disable-dev-shm-usage"],
    });
  } catch (e) {
    console.error("[BROWSER LAUNCH ERROR]", e.message);
    throw new Error("Failed to launch browser: " + e.message);
  }

  let context;
  let page;
  try {
    context = await browser.newContext({
      viewport: { width: 1920, height: 1080 },
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    });
    page = await context.newPage();
  } catch (e) {
    await browser.close().catch(() => {});
    throw new Error("Failed to create page context: " + e.message);
  }

  try {
    if (!(await safeGoto(page, startUrl))) {
      throw new Error("Failed to load start URL");
    }

    const base = new URL(page.url()).origin;
    const visited = new Set();

const normalizeUrl = (u) => {
  try {
    const url = new URL(u);
    url.hash = "";
    url.search = "";
    url.hostname = url.hostname.replace(/^www\./, "");
    if (url.pathname.endsWith("/") && url.pathname !== "/") {
      url.pathname = url.pathname.slice(0, -1);
    }
    return url.toString();
  } catch {
    return u;
  }
};

const queue = [normalizeUrl(page.url())];

    const pages = [];
    const ocrImageCache = new Set();
    const stats = {
      visited: 0,
      saved: 0,
      byType: {},
      ocrElementsProcessed: 0,
      ocrCharsExtracted: 0,
      errors: 0,
    };

    while (queue.length && Date.now() < deadline) {
      const rawUrl = queue.shift();
const url = normalizeUrl(rawUrl);

if (!url || visited.has(url) || SKIP_URL_RE.test(url)) continue;

visited.add(url);

      stats.visited++;

      if (!(await safeGoto(page, url))) {
        stats.errors++;
        continue;
      }

      try {
        console.log("[PAGE] Loading content...");
        
        for (let i = 0; i < 3; i++) {
          await page.evaluate(() => window.scrollBy(0, window.innerHeight));
          await page.waitForTimeout(600);
        }

        await page.evaluate(() => {
          document.querySelectorAll('img[loading="lazy"]').forEach(img => {
            img.loading = 'eager';
          });
          window.dispatchEvent(new Event('scroll'));
        });

        await page.waitForTimeout(1000);

        const title = clean(await page.title());
        const pageType = detectPageType(url, title);
        stats.byType[pageType] = (stats.byType[pageType] || 0) + 1;

        const data = await extractStructured(page);
const htmlWordCount = countWordsExact(data.rawContent || "");


        // ===== OCR =====
        let ocrText = "";
        if (htmlWordCount < 300) {
        const ocrTexts = new Set();

        if (htmlWordCount < 300) {
  console.log(`[OCR] === START on ${url} ===`);
} else {
  console.log(`[OCR] SKIP (HTML sufficient): ${htmlWordCount} words`);
}


        try {
          await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {});

          let imgs;
          try {
            imgs = await page.$$("img");
          } catch (e) {
            console.log("[OCR] ERROR getting images:", e.message);
            imgs = [];
          }

          console.log(`[OCR] Found ${imgs.length} images`);

          for (let i = 0; i < imgs.length; i++) {
            if (page.isClosed()) break;

            try {
              const info = await imgs[i].evaluate(el => ({
                src: el.src || "",
                w: Math.round(el.getBoundingClientRect().width),
                h: Math.round(el.getBoundingClientRect().height)
              })).catch(() => null);

              if (!info) continue;

              console.log(`[OCR] Img ${i+1}: ${info.src.slice(-50)} (${info.w}x${info.h})`);

              if (info.w < 50 || info.h < 30) continue;
if (/logo|icon/i.test(info.src)) continue;

const ocrKey = `${info.src}|${info.w}x${info.h}`;
if (ocrImageCache.has(ocrKey)) continue;
ocrImageCache.add(ocrKey);


              let text = "";
await enqueueOCR(async () => {
  text = await ocrElement(page, imgs[i], `img-${i+1}`);
});

              if (text && text.length > 3 && !ocrTexts.has(text)) {
                ocrText += "\n" + text;
                ocrTexts.add(text);
                stats.ocrElementsProcessed++;
                stats.ocrCharsExtracted += text.length;
              }
            } catch (e) {
              console.error(`[OCR] Img ${i+1} error:`, e.message);
            }
          }

          console.log(`[OCR] === DONE: ${stats.ocrElementsProcessed} elements, ${stats.ocrCharsExtracted} chars ===`);
        } catch (e) {
          console.error("[OCR ERROR]", e.message);
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
  const nl = normalizeUrl(l);
  if (!visited.has(nl) && !SKIP_URL_RE.test(nl)) queue.push(nl);
});

      } catch (e) {
        console.error("[PAGE PROCESSING ERROR]", url, e.message);
        stats.errors++;
      }
    }

    return { pages, stats };
  } finally {
    try {
      if (page && !page.isClosed()) await page.close();
      if (context) await context.close();
      if (browser) await browser.close();
      console.log("[CLEANUP] Browser closed");
    } catch (e) {
      console.error("[CLEANUP ERROR]", e.message);
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
      res.writeHead(405, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ error: "Method not allowed" }));
    }

    let body = "";
    req.on("data", c => (body += c));
    req.on("error", err => {
      console.error("[REQUEST ERROR]", err.message);
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: "Request error" }));
    });

    req.on("end", async () => {
      try {
        const parsed = JSON.parse(body || "{}");
        
        if (!parsed.url) {
          res.writeHead(400, { "Content-Type": "application/json" });
          return res.end(JSON.stringify({ 
            success: false, 
            error: "Missing 'url' parameter" 
          }));
        }

        const result = await crawlSmart(parsed.url);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: true, ...result }));
      } catch (e) {
        console.error("[CRAWL ERROR]", e.message);
        res.writeHead(500, { "Content-Type": "application/json" });
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
