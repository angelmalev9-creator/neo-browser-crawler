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
    // Проверка дали page е затворен
    if (page.isClosed()) {
      console.log(`[OCR] ${context}: page closed, skipping`);
      return "";
    }

    const box = await element.boundingBox().catch(() => null);
    if (!box) {
      console.log(`[OCR] ${context}: no bounding box`);
      return "";
    }

    if (box.width < 50 || box.height < 30) {
      return "";
    }

    console.log(`[OCR] ${context}: ${Math.round(box.width)}x${Math.round(box.height)}px - taking screenshot...`);

    // Screenshot с по-кратък timeout
    const buffer = await element.screenshot({ 
      type: 'png',
      timeout: 10000  // 10s вместо 30s
    }).catch(e => {
      console.log(`[OCR] ${context}: screenshot failed - ${e.message}`);
      return null;
    });

    if (!buffer) return "";

    console.log(`[OCR] ${context}: screenshot OK, sending to Vision API...`);

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
      console.log(`[OCR] ✓ ${context}: ${text.length} chars - "${text.slice(0, 100)}"`);
    } else {
      console.log(`[OCR] ${context}: no text found in image`);
    }

    return text;
  } catch (e) {
    console.error(`[OCR] ${context}: ERROR -`, e.message);
    return "";
  }
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
    const queue = [page.url()];
    const pages = [];

    const stats = {
      visited: 0,
      saved: 0,
      byType: {},
      ocrElementsProcessed: 0,
      ocrCharsExtracted: 0,
      errors: 0,
    };

    while (queue.length && Date.now() < deadline) {
      const url = queue.shift();
      if (!url || visited.has(url) || SKIP_URL_RE.test(url)) continue;

      visited.add(url);
      stats.visited++;

      if (!(await safeGoto(page, url))) {
        stats.errors++;
        continue;
      }

      try {
        // Scroll + load images
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

        // ===== OCR =====
        let ocrText = "";
        const ocrTexts = new Set();

        console.log(`[OCR] Starting OCR on: ${url}`);

        try {
          // OCR на всички изображения
          const imageElements = await page.$("img");
          const imageCount = imageElements ? imageElements.length : 0;
          console.log(`[OCR] Found ${imageCount} images`);
          
          const finalImageCount = imageElements ? imageElements.length : 0;
          console.log(`[OCR] Processing ${finalImageCount} images...`);

          if (imageElements && finalImageCount > 0) {
            for (let i = 0; i < finalImageCount; i++) {
              console.log(`[OCR] --- Processing image ${i+1}/${finalImageCount} ---`);
              try {
                const img = imageElements[i];
                
                // Проверяваме дали изображението изглежда че има текст
                const imageInfo = await img.evaluate(el => {
                  const src = el.src || "";
                  const alt = el.alt || "";
                  const className = el.className || "";
                  const parent = el.parentElement;
                  const parentClass = parent ? parent.className : "";
                  
                  // Индикатори че има текст
                  const hasTextIndicators = 
                    /price|ceni|tseni|card|banner|plan|package|offer|promo|badge|label/i.test(src + alt + className + parentClass);
                  
                  // Пропускаме decorative изображения
                  const isDecorative = 
                    /logo|icon|arrow|bullet|social|facebook|instagram|decoration|background|bg-|hero/i.test(src + alt + className);
                  
                  // Пропускаме много малки изображения (икони)
                  const rect = el.getBoundingClientRect();
                  const tooSmall = rect.width < 80 || rect.height < 50;
                  
                  return {
                    src,
                    alt,
                    className,
                    hasTextIndicators,
                    isDecorative,
                    tooSmall,
                    width: Math.round(rect.width),
                    height: Math.round(rect.height)
                  };
                });

                console.log(`[OCR] Image ${i+1}/${imageCount}: ${imageInfo.src.slice(-60)}`);
                console.log(`      Size: ${imageInfo.width}x${imageInfo.height}, Text indicators: ${imageInfo.hasTextIndicators}, Decorative: ${imageInfo.isDecorative}`);

                // Решаваме дали да правим OCR
                const shouldOCR = 
                  !imageInfo.tooSmall && 
                  !imageInfo.isDecorative && 
                  (imageInfo.hasTextIndicators || imageInfo.width > 200);

                if (!shouldOCR) {
                  console.log(`      → Skipping (likely no text)`);
                  continue;
                }

                console.log(`      → Processing with OCR...`);

                const text = await ocrElement(page, img, `img-${i+1}`);
                if (text && text.length > 5 && !ocrTexts.has(text)) {
                  ocrText += "\n" + text;
                  ocrTexts.add(text);
                  stats.ocrElementsProcessed++;
                  stats.ocrCharsExtracted += text.length;
                }
              } catch (e) {
                console.error(`[OCR] Image ${i+1} error:`, e.message);
              }
            }
          }

          // OCR на pricing cards/divs
          const cardElements = await page.$('[class*="card"], [class*="price"], [class*="ceni"]');
          const cardCount = cardElements ? cardElements.length : 0;
          console.log(`[OCR] Found ${cardCount} cards`);
          
          if (cardElements && cardCount > 0) {
            for (let i = 0; i < Math.min(cardCount, 10); i++) {
              try {
                // Проверка дали page е все още отворен
                if (page.isClosed()) {
                  console.log(`[OCR] Page closed, stopping card OCR at ${i+1}`);
                  break;
                }

                const text = await ocrElement(page, cardElements[i], `card-${i+1}`);
                if (text && text.length > 5 && !ocrTexts.has(text)) {
                  ocrText += "\n" + text;
                  ocrTexts.add(text);
                  stats.ocrElementsProcessed++;
                  stats.ocrCharsExtracted += text.length;
                }
              } catch (e) {
                console.error(`[OCR] Card ${i+1} error:`, e.message);
              }
            }
          }

          console.log(`[OCR] ✓ Done: ${stats.ocrElementsProcessed} elements, ${stats.ocrCharsExtracted} chars`);

        } catch (e) {
          console.error("[OCR ERROR]", e.message);
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
