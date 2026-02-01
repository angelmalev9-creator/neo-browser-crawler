import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);
let crawlInProgress = false;
let crawlFinished = false;
let lastResult = null;
let lastCrawlUrl = null;
let lastCrawlTime = 0;
const RESULT_TTL_MS = 5 * 60 * 1000;
const visited = new Set();

// ================= LIMITS =================
const MAX_SECONDS = 180;
const MIN_WORDS = 20;
const PARALLEL_TABS = 5;
const PARALLEL_OCR = 10;
const OCR_TIMEOUT_MS = 6000;

const SKIP_URL_RE =
  /(wp-content\/uploads|media|gallery|video|photo|attachment|privacy|terms|cookies|gdpr)/i;

// Skip OCR САМО за наистина безполезни изображения (много специфични patterns)
const SKIP_OCR_RE = /\/(logo|favicon|spinner|avatar|pixel|spacer|blank|transparent)\.|\/icons?\//i;

// ================= UTILS =================
const clean = (t = "") =>
  t.replace(/\r/g, "").replace(/[ \t]+/g, " ").replace(/\n{3,}/g, "\n\n").trim();

const countWordsExact = (t = "") => t.split(/\s+/).filter(Boolean).length;

// ================= BG NUMBER NORMALIZER =================
const BG_0_19 = [
  "нула","едно","две","три","четири","пет","шест","седем","осем","девет",
  "десет","единадесет","дванадесет","тринадесет","четиринадесет",
  "петнадесет","шестнадесет","седемнадесет","осемнадесет","деветнадесет"
];
const BG_TENS = ["", "", "двадесет","тридесет","четиридесет","петдесет","шестдесет","седемдесет","осемдесет","деветдесет"];

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
  } catch { return text; }
}

// ================= URL NORMALIZER =================
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
  } catch { return u; }
};

// ================= PAGE TYPE =================
function detectPageType(url = "", title = "") {
  const s = (url + " " + title).toLowerCase();
  if (/za-nas|about/.test(s)) return "about";
  if (/uslugi|services|pricing|price|ceni|tseni/.test(s)) return "services";
  if (/kontakti|contact/.test(s)) return "contact";
  if (/faq|vuprosi|questions/.test(s)) return "faq";
  if (/blog|news|article/.test(s)) return "blog";
  return "general";
}

// ================= STRUCTURED EXTRACTOR =================
async function extractStructured(page) {
  try {
    await page.waitForSelector("body", { timeout: 1500 });
  } catch {}

  try {
    return await page.evaluate(() => {
      const seenTexts = new Set();
      
      function addUniqueText(text, minLength = 10) {
        const normalized = text.trim().replace(/\s+/g, ' ');
        if (normalized.length < minLength || seenTexts.has(normalized)) return "";
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
        '[class*="overlay"]', '[class*="modal"]', '[class*="popup"]',
        '[class*="tooltip"]', '[class*="banner"]', '[class*="notification"]',
        '[class*="alert"]', '[style*="position: fixed"]',
        '[style*="position: absolute"]', '[role="dialog"]', '[role="alertdialog"]',
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
        } catch {}
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
      } catch {}

      let mainContent = "";
      const mainEl = document.querySelector("main") || document.querySelector("article");
      if (mainEl && !processedElements.has(mainEl)) {
        const text = mainEl.innerText?.trim();
        if (text) mainContent = addUniqueText(text) || "";
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
    return { rawContent: "" };
  }
}

// ================= GLOBAL OCR CACHE (между страници, по ТОЧЕН src URL) =================
const globalOcrCache = new Map();
const API_KEY = "AIzaSyCoai4BCKJtnnryHbhsPKxJN35UMcMAKrk";

// Бърз OCR
async function fastOCR(buffer) {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), OCR_TIMEOUT_MS);

    const res = await fetch(
      `https://vision.googleapis.com/v1/images:annotate?key=${API_KEY}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          requests: [{
            image: { content: buffer.toString("base64") },
            features: [{ type: "TEXT_DETECTION" }],
            imageContext: { languageHints: ["bg", "en", "tr", "ru"] }
          }]
        }),
        signal: controller.signal
      }
    );

    clearTimeout(timeout);
    if (!res.ok) return "";

    const json = await res.json();
    return json.responses?.[0]?.fullTextAnnotation?.text?.trim() || "";
  } catch { return ""; }
}

// OCR за ВСИЧКИ изображения на страницата
// Кешира между страници по ТОЧЕН src URL (за header/footer)
// НЕ skip-ва подобни изображения на СЪЩАТА страница
async function ocrAllImages(page, stats) {
  const results = [];
  
  try {
    const imgElements = await page.$$("img");
    if (imgElements.length === 0) return results;

    // Вземаме info за всички изображения
    const imgInfos = await Promise.all(
      imgElements.map(async (img, i) => {
        try {
          const info = await img.evaluate(el => ({
            src: el.src || "",
            alt: el.alt || "",
            w: Math.round(el.getBoundingClientRect().width),
            h: Math.round(el.getBoundingClientRect().height),
            visible: el.getBoundingClientRect().width > 0
          }));
          return { ...info, element: img, index: i };
        } catch { return null; }
      })
    );

    // Филтрираме валидни изображения (но НЕ по подобие на src)
    const validImages = imgInfos.filter(info => {
      if (!info || !info.visible) return false;
      if (info.w < 50 || info.h < 25) return false;  // Намалени минимални размери
      if (info.w > 1800 && info.h > 700) return false;
      if (SKIP_OCR_RE.test(info.src)) {
        console.log(`[OCR] Skip (pattern): ${info.src.slice(-50)}`);
        return false;
      }
      return true;
    });

    console.log(`[OCR] ${validImages.length}/${imgElements.length} images to process`);
    validImages.forEach((img, i) => console.log(`[OCR] Image ${i+1}: ${img.src.slice(-60)} (${img.w}x${img.h})`));
    if (validImages.length === 0) return results;

    // Screenshots паралелно
    const screenshots = await Promise.all(
      validImages.map(async (img) => {
        try {
          // Проверка в глобален кеш (по ТОЧЕН src URL)
          if (globalOcrCache.has(img.src)) {
            const cachedText = globalOcrCache.get(img.src);
            return { ...img, buffer: null, cached: true, text: cachedText };
          }
          
          if (page.isClosed()) return null;
          const buffer = await img.element.screenshot({ type: 'png', timeout: 2500 });
          return { ...img, buffer, cached: false };
        } catch { return null; }
      })
    );

    const validScreenshots = screenshots.filter(s => s !== null);

    // OCR паралелно на batch-ове
    for (let i = 0; i < validScreenshots.length; i += PARALLEL_OCR) {
      const batch = validScreenshots.slice(i, i + PARALLEL_OCR);
      
      const batchResults = await Promise.all(
        batch.map(async (img) => {
          // Ако е от кеша
          if (img.cached) {
            if (img.text && img.text.length > 2) {
              return { text: img.text, src: img.src, alt: img.alt };
            }
            return null;
          }
          
          if (!img.buffer) return null;
          
          const text = await fastOCR(img.buffer);
          
          // Кеширай в глобален кеш по ТОЧЕН src URL
          globalOcrCache.set(img.src, text);
          
          if (text && text.length > 2) {
            stats.ocrElementsProcessed++;
            stats.ocrCharsExtracted += text.length;
            console.log(`[OCR] ✓ Text: "${text.slice(0, 60).replace(/\n/g, ' ')}..."`);
            return { text, src: img.src, alt: img.alt };
          }
          return null;
        })
      );

      results.push(...batchResults.filter(r => r !== null));
    }

    console.log(`[OCR] ✓ Got text from ${results.length} images`);
  } catch (e) {
    console.error("[OCR ERROR]", e.message);
  }

  return results;
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
  } catch { return []; }
}

// ================= PROCESS SINGLE PAGE =================
async function processPage(page, url, base, stats) {
  const startTime = Date.now();
  
  try {
    console.log("[PAGE]", url);
    await page.goto(url, { timeout: 10000, waitUntil: "domcontentloaded" });

    // Пълно скролиране за lazy load - важно за всички изображения!
    await page.evaluate(async () => {
      // Scroll до края на страницата на стъпки
      const scrollStep = window.innerHeight;
      const maxScroll = document.body.scrollHeight;
      
      for (let pos = 0; pos < maxScroll; pos += scrollStep) {
        window.scrollTo(0, pos);
        await new Promise(r => setTimeout(r, 100));
      }
      
      // Scroll до края
      window.scrollTo(0, maxScroll);
      
      // Trigger lazy load за всички изображения
      document.querySelectorAll('img[loading="lazy"], img[data-src], img[data-lazy]').forEach(img => {
        img.loading = 'eager';
        if (img.dataset.src) img.src = img.dataset.src;
        if (img.dataset.lazy) img.src = img.dataset.lazy;
      });
    });
    
    await page.waitForTimeout(500);
    
    // Изчакваме изображенията да се заредят
    try {
      await page.waitForLoadState('networkidle', { timeout: 3000 });
    } catch {}

    const title = clean(await page.title());
    const pageType = detectPageType(url, title);
    stats.byType[pageType] = (stats.byType[pageType] || 0) + 1;

    // Извличане на HTML content
    const data = await extractStructured(page);

    // OCR ВСИЧКИ изображения на страницата
    const ocrResults = await ocrAllImages(page, stats);

    // Форматиране
    const htmlContent = normalizeNumbers(clean(data.rawContent));
    const ocrTexts = ocrResults.map(r => r.text);
    const ocrContent = normalizeNumbers(clean(ocrTexts.join("\n\n")));

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
    const totalWords = htmlWords + ocrWords;

    const elapsed = Date.now() - startTime;
    console.log(`[PAGE] ✓ ${totalWords}w (${htmlWords}+${ocrWords}ocr, ${ocrResults.length} imgs) ${elapsed}ms`);

    if (pageType !== "services" && totalWords < MIN_WORDS) {
      return { links: await collectAllLinks(page, base), page: null };
    }

    return {
      links: await collectAllLinks(page, base),
      page: {
        url,
        title,
        pageType,
        content,
        wordCount: totalWords,
        breakdown: { htmlWords, ocrWords, images: ocrResults.length },
        status: "ok",
      }
    };
  } catch (e) {
    console.error("[PAGE ERROR]", url, e.message);
    stats.errors++;
    return { links: [], page: null };
  }
}

// ================= PARALLEL CRAWLER =================
async function crawlSmart(startUrl) {
  const deadline = Date.now() + MAX_SECONDS * 1000;
  console.log("\n[CRAWL START]", startUrl);
  console.log(`[CONFIG] ${PARALLEL_TABS} tabs, ${PARALLEL_OCR} parallel OCR`);

  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--disable-software-rasterizer"],
  });

  const stats = {
    visited: 0,
    saved: 0,
    byType: {},
    ocrElementsProcessed: 0,
    ocrCharsExtracted: 0,
    errors: 0,
  };

  const pages = [];
  const queue = [];
  let base = "";

  try {
    // Първоначално зареждане
    const initContext = await browser.newContext({
      viewport: { width: 1920, height: 1080 },
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    });
    const initPage = await initContext.newPage();
    
    await initPage.goto(startUrl, { timeout: 10000, waitUntil: "domcontentloaded" });
    base = new URL(initPage.url()).origin;
    
    const initialLinks = await collectAllLinks(initPage, base);
    queue.push(normalizeUrl(initPage.url()));
    initialLinks.forEach(l => {
      const nl = normalizeUrl(l);
      if (!visited.has(nl) && !SKIP_URL_RE.test(nl) && !queue.includes(nl)) {
        queue.push(nl);
      }
    });
    
    await initPage.close();
    await initContext.close();

    console.log(`[CRAWL] Found ${queue.length} URLs`);

    // Worker функция
    const createWorker = async () => {
      const ctx = await browser.newContext({
        viewport: { width: 1920, height: 1080 },
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      });
      const pg = await ctx.newPage();

      while (Date.now() < deadline) {
        let url = null;
        while (queue.length > 0) {
          const candidate = queue.shift();
          const normalized = normalizeUrl(candidate);
          if (!visited.has(normalized) && !SKIP_URL_RE.test(normalized)) {
            visited.add(normalized);
            url = normalized;
            break;
          }
        }

        if (!url) {
          await new Promise(r => setTimeout(r, 30));
          if (queue.length === 0) break;
          continue;
        }

        stats.visited++;
        
        const result = await processPage(pg, url, base, stats);
        
        if (result.page) {
          pages.push(result.page);
          stats.saved++;
        }

        result.links.forEach(l => {
          const nl = normalizeUrl(l);
          if (!visited.has(nl) && !SKIP_URL_RE.test(nl) && !queue.includes(nl)) {
            queue.push(nl);
          }
        });
      }

      await pg.close();
      await ctx.close();
    };

    // Стартирай workers паралелно
    await Promise.all(Array(PARALLEL_TABS).fill(0).map(() => createWorker()));

  } finally {
    await browser.close();
    console.log(`\n[CRAWL DONE] ${stats.saved}/${stats.visited} pages`);
    console.log(`[OCR STATS] ${stats.ocrElementsProcessed} images → ${stats.ocrCharsExtracted} chars`);
  }

  return { pages, stats };
}

// ================= HTTP SERVER =================
http
  .createServer((req, res) => {
    if (req.method === "GET") {
      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ 
        status: crawlInProgress ? "crawling" : (crawlFinished ? "ready" : "idle"),
        crawlInProgress,
        crawlFinished,
        lastCrawlUrl,
        lastCrawlTime: lastCrawlTime ? new Date(lastCrawlTime).toISOString() : null,
        resultAvailable: !!lastResult,
        pagesCount: lastResult?.pages?.length || 0,
        config: { PARALLEL_TABS, MAX_SECONDS, MIN_WORDS, PARALLEL_OCR }
      }));
    }

    if (req.method !== "POST") {
      res.writeHead(405, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ error: "Method not allowed" }));
    }

    let body = "";
    req.on("data", c => (body += c));
    req.on("error", err => {
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

        const requestedUrl = normalizeUrl(parsed.url);
        const now = Date.now();

        // Ако имаме готов резултат за същия URL → върни го
        if (crawlFinished && lastResult && lastCrawlUrl === requestedUrl) {
          if (now - lastCrawlTime < RESULT_TTL_MS) {
            console.log("[CACHE HIT] Returning cached result for:", requestedUrl);
            res.writeHead(200, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({ success: true, cached: true, ...lastResult }));
          }
        }

        // Ако crawl е в прогрес
        if (crawlInProgress) {
          if (lastCrawlUrl === requestedUrl) {
            res.writeHead(202, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({
              success: false,
              status: "in_progress",
              message: "Crawl in progress for this URL"
            }));
          } else {
            res.writeHead(429, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({
              success: false,
              error: "Crawler busy with different URL"
            }));
          }
        }

        // Стартирай нов crawl
        crawlInProgress = true;
        crawlFinished = false;
        lastCrawlUrl = requestedUrl;
        visited.clear();
        globalOcrCache.clear();

        console.log("[CRAWL START] New crawl for:", requestedUrl);

        const result = await crawlSmart(parsed.url);

        crawlInProgress = false;
        crawlFinished = true;
        lastResult = result;
        lastCrawlTime = Date.now();

        console.log("[CRAWL DONE] Result ready for:", requestedUrl);

        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: true, ...result }));
      } catch (e) {
        crawlInProgress = false;
        console.error("[CRAWL ERROR]", e.message);
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({
          success: false,
          error: e instanceof Error ? e.message : String(e),
        }));
      }
    });
  })
  .listen(PORT, () => {
    console.log("Crawler running on", PORT);
    console.log(`Config: ${PARALLEL_TABS} tabs, ${PARALLEL_OCR} parallel OCR`);
  });
