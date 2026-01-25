import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

// ================= LIMITS =================
const MAX_SECONDS = 180;
const MIN_WORDS = 20;
const MAX_OCR_ELEMENTS = 5;

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
    if (/uslugi|services|pricing|price|ceni/.test(s)) return "services";
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
      const sections = [];
      let current = null;

      // Събираме от основни елементи
      document.querySelectorAll("h1,h2,h3,p,li,span,div").forEach(el => {
        const text = el.innerText?.trim();
        if (!text) return;

        if (el.tagName.startsWith("H")) {
          current = { heading: text, text: "" };
          sections.push(current);
        } else if (current && text.length > 5) {
          current.text += " " + text;
        }
      });

      // Вземаме текст от CSS overlays, modals, popups
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

      let overlayText = "";
      overlaySelectors.forEach(selector => {
        try {
          document.querySelectorAll(selector).forEach(el => {
            const computed = window.getComputedStyle(el);
            // Взимаме дори скритите overlay-и (може да се покажат с JS)
            const text = el.innerText?.trim();
            if (text && text.length > 10) {
              overlayText += "\n" + text;
            }
          });
        } catch (e) {
          console.error("Overlay extraction error:", e);
        }
      });

      // Вземаме текст от ::before и ::after псевдоелементи
      let pseudoText = "";
      try {
        document.querySelectorAll("*").forEach(el => {
          const before = window.getComputedStyle(el, "::before").content;
          const after = window.getComputedStyle(el, "::after").content;
          
          if (before && before !== "none" && before !== '""') {
            const cleaned = before.replace(/^["']|["']$/g, "");
            if (cleaned.length > 3) pseudoText += " " + cleaned;
          }
          
          if (after && after !== "none" && after !== '""') {
            const cleaned = after.replace(/^["']|["']$/g, "");
            if (cleaned.length > 3) pseudoText += " " + cleaned;
          }
        });
      } catch (e) {
        console.error("Pseudo element extraction error:", e);
      }

      const mainContent =
        document.querySelector("main")?.innerText ||
        document.querySelector("article")?.innerText ||
        document.body.innerText ||
        "";

      return {
        rawContent: [
          sections.map(s => `${s.heading}\n${s.text}`).join("\n\n"),
          mainContent,
          overlayText,
          pseudoText,
        ].filter(Boolean).join("\n\n"),
      };
    });
  } catch (e) {
    console.error("[EXTRACT STRUCTURED ERROR]", e.message);
    return { rawContent: "" };
  }
}

// ================= GOOGLE VISION OCR (IMPROVED) =================
async function ocrElement(page, element, context = "") {
  const apiKey = process.env.GOOGLE_VISION_API_KEY || "AIzaSyB1g-JZCwk2AuhoGtroF0zurDV9PVHEZq0";
  if (!apiKey) {
    console.warn("[OCR] No API key found - skipping");
    return "";
  }

  try {
    const box = await element.boundingBox();
    if (!box) {
      console.log("[OCR] Element has no bounding box - skipping");
      return "";
    }

    // Минимални размери за OCR
    if (box.width < 100 || box.height < 50) {
      console.log(`[OCR] Element too small (${box.width}x${box.height}) - skipping`);
      return "";
    }

    console.log(`[OCR] Processing ${context}: ${box.width}x${box.height}px`);

    const buffer = await element.screenshot({ type: 'png' });
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
              features: [
                { type: "TEXT_DETECTION", maxResults: 50 },
                { type: "DOCUMENT_TEXT_DETECTION", maxResults: 50 }
              ],
              imageContext: {
                languageHints: ["bg", "en"]
              }
            },
          ],
        }),
      }
    );

    if (!res.ok) {
      const errorText = await res.text();
      console.error(`[OCR] Vision API error: ${res.status} - ${errorText}`);
      return "";
    }

    const json = await res.json();
    
    if (json.responses?.[0]?.error) {
      console.error("[OCR] API returned error:", json.responses[0].error);
      return "";
    }

    const text = json.responses?.[0]?.fullTextAnnotation?.text?.trim() || "";
    
    console.log(`[OCR] Extracted ${text.length} chars from ${context}`);
    if (text) {
      console.log(`[OCR] Preview: ${text.slice(0, 150)}...`);
    }

    return text;
  } catch (e) {
    console.error(`[OCR FAIL] ${context}:`, e.message);
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
    await browser.close();
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
        // Trigger JS-rendered content
        for (let i = 0; i < 4; i++) {
          await page.evaluate(() => window.scrollBy(0, window.innerHeight));
          await page.waitForTimeout(500);
        }

        // Trigger hover states and modals
        try {
          await page.mouse.move(100, 100);
          await page.waitForTimeout(300);
        } catch (e) {
          console.log("[MOUSE MOVE ERROR]", e.message);
        }

        const title = clean(await page.title());
        const pageType = detectPageType(url, title);
        stats.byType[pageType] = (stats.byType[pageType] || 0) + 1;

        const data = await extractStructured(page);

        // ===== OCR НА РАЗЛИЧНИ ЕЛЕМЕНТИ =====
        let ocrText = "";

        if (stats.ocrElementsProcessed < MAX_OCR_ELEMENTS) {
          console.log(`[OCR] Scanning page for visual elements: ${url}`);

          try {
            // 1. OCR на изображения с текст
            const images = await page.$$("img");
            for (const img of images) {
              if (stats.ocrElementsProcessed >= MAX_OCR_ELEMENTS) break;
              
              const text = await ocrElement(page, img, "image");
              if (text) {
                ocrText += "\n" + text;
                stats.ocrElementsProcessed++;
                stats.ocrCharsExtracted += text.length;
              }
            }

            // 2. OCR на canvas елементи
            const canvases = await page.$$("canvas");
            for (const canvas of canvases) {
              if (stats.ocrElementsProcessed >= MAX_OCR_ELEMENTS) break;
              
              const text = await ocrElement(page, canvas, "canvas");
              if (text) {
                ocrText += "\n" + text;
                stats.ocrElementsProcessed++;
                stats.ocrCharsExtracted += text.length;
              }
            }

            // 3. OCR на div/section с background images
            const bgElements = await page.$$('[style*="background-image"], [class*="bg-"], [class*="banner"]');
            for (const el of bgElements) {
              if (stats.ocrElementsProcessed >= MAX_OCR_ELEMENTS) break;
              
              const text = await ocrElement(page, el, "bg-element");
              if (text) {
                ocrText += "\n" + text;
                stats.ocrElementsProcessed++;
                stats.ocrCharsExtracted += text.length;
              }
            }

            // 4. OCR на SVG елементи
            const svgs = await page.$$("svg");
            for (const svg of svgs) {
              if (stats.ocrElementsProcessed >= MAX_OCR_ELEMENTS) break;
              
              const text = await ocrElement(page, svg, "svg");
              if (text) {
                ocrText += "\n" + text;
                stats.ocrElementsProcessed++;
                stats.ocrCharsExtracted += text.length;
              }
            }

          } catch (e) {
            console.error("[OCR EXTRACTION ERROR]", e.message);
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
      } catch (e) {
        console.error("[PAGE PROCESSING ERROR]", url, e.message);
        stats.errors++;
      }
    }

    return { pages, stats };
  } finally {
    try {
      await browser.close();
    } catch (e) {
      console.error("[BROWSER CLOSE ERROR]", e.message);
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
