import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);
let crawlInProgress = false;
let crawlFinished = false;
let lastResult = null;
const visited = new Set();

// ================= LIMITS =================
const MAX_SECONDS = 180;
const MIN_WORDS = 20;
const PARALLEL_TABS = 3; // паралелни табове

const SKIP_URL_RE =
  /(wp-content\/uploads|media|gallery|video|photo|attachment|privacy|terms|cookies|gdpr)/i;

// Skip OCR за тези изображения
const SKIP_OCR_RE = /logo|icon|sprite|placeholder|loading|spinner|avatar|profile|thumb|social|facebook|twitter|instagram|linkedin|youtube|pinterest/i;

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
    return text;
  }
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
  } catch {
    return u;
  }
};

// Нормализира image src за кеширане (премахва hash-ове)
const normalizeImageSrc = (src) => {
  try {
    return src.replace(/-[a-zA-Z0-9]{6,12}\.(png|jpg|jpeg|gif|webp)$/i, '.$1');
  } catch {
    return src;
  }
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
    await page.waitForSelector("body", { timeout: 3000 });
  } catch {}

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

// ================= SMART OCR - САМО ЗА ИЗОБРАЖЕНИЯ С ТЕКСТ =================
const ocrCache = new Map();

async function smartOCR(page, imgElement, context) {
  const apiKey = "AIzaSyCoai4BCKJtnnryHbhsPKxJN35UMcMAKrk";

  try {
    if (page.isClosed()) return "";

    const info = await imgElement.evaluate(el => ({
      src: el.src || "",
      alt: el.alt || "",
      w: Math.round(el.getBoundingClientRect().width),
      h: Math.round(el.getBoundingClientRect().height),
      isVisible: el.getBoundingClientRect().width > 0
    })).catch(() => null);

    if (!info || !info.isVisible) return "";

    // Skip малки изображения
    if (info.w < 100 || info.h < 50) return "";
    
    // Skip много големи (hero/background)
    if (info.w > 1400 && info.h > 500) return "";
    
    // Skip по pattern в src
    if (SKIP_OCR_RE.test(info.src)) return "";

    // Нормализиран ключ за кеша
    const cacheKey = normalizeImageSrc(info.src);
    
    // Проверка в кеша
    if (ocrCache.has(cacheKey)) {
      const cached = ocrCache.get(cacheKey);
      if (cached) console.log(`[OCR] Cache hit: ${context}`);
      return cached;
    }

    const buffer = await imgElement.screenshot({ type: 'png', timeout: 5000 }).catch(() => null);
    if (!buffer) return "";

    // LABEL_DETECTION първо - бързо проверява дали има текст
    const labelRes = await fetch(
      `https://vision.googleapis.com/v1/images:annotate?key=${apiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          requests: [{
            image: { content: buffer.toString("base64") },
            features: [{ type: "LABEL_DETECTION", maxResults: 5 }]
          }]
        })
      }
    );

    const labelJson = await labelRes.json();
    const labels = labelJson.responses?.[0]?.labelAnnotations || [];
    const labelDescriptions = labels.map(l => l.description.toLowerCase());
    
    // Проверка дали има индикация за текст
    const hasTextIndicator = labelDescriptions.some(l => 
      /text|document|paper|sign|poster|banner|certificate|menu|card|letter|font|writing|receipt|screenshot|presentation/i.test(l)
    );

    // Ако няма текст индикатор, skip
    if (!hasTextIndicator) {
      ocrCache.set(cacheKey, "");
      return "";
    }

    // Има текст - правим пълен OCR
    const res = await fetch(
      `https://vision.googleapis.com/v1/images:annotate?key=${apiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          requests: [{
            image: { content: buffer.toString("base64") },
            features: [
              { type: "TEXT_DETECTION" },
              { type: "DOCUMENT_TEXT_DETECTION" }
            ],
            imageContext: { languageHints: ["bg", "en", "ru"] }
          }]
        })
      }
    );

    const json = await res.json();
    const text = json.responses?.[0]?.fullTextAnnotation?.text?.trim() || "";
    
    // Кеширане
    ocrCache.set(cacheKey, text);
    
    if (text) {
      console.log(`[OCR] ✓ ${context}: ${text.length} chars - "${text.slice(0, 50)}..."`);
    }

    return text;
  } catch (e) {
    console.error(`[OCR] ${context}: ${e.message}`);
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
  } catch {
    return [];
  }
}

// ================= PROCESS SINGLE PAGE =================
async function processPage(page, url, base, stats, ocrImageCache) {
  const startTime = Date.now();
  
  try {
    console.log("[GOTO]", url);
    await page.goto(url, { timeout: 15000, waitUntil: "domcontentloaded" });

    // Бързо скролиране (2 пъти вместо 3)
    for (let i = 0; i < 2; i++) {
      await page.evaluate(() => window.scrollBy(0, window.innerHeight));
      await page.waitForTimeout(400);
    }

    // Trigger lazy load
    await page.evaluate(() => {
      document.querySelectorAll('img[loading="lazy"]').forEach(img => {
        img.loading = 'eager';
      });
    });

    await page.waitForTimeout(500);

    const title = clean(await page.title());
    const pageType = detectPageType(url, title);
    stats.byType[pageType] = (stats.byType[pageType] || 0) + 1;

    const data = await extractStructured(page);

    // ===== SMART OCR =====
    let ocrText = "";
    const ocrTexts = new Set();

    try {
      const imgs = await page.$$("img");
      console.log(`[OCR] Page has ${imgs.length} images`);

      let ocrCount = 0;
      const MAX_OCR_PER_PAGE = 4;

      for (let i = 0; i < imgs.length && ocrCount < MAX_OCR_PER_PAGE; i++) {
        if (page.isClosed()) break;

        try {
          const src = await imgs[i].evaluate(el => el.src).catch(() => "");
          const cacheKey = normalizeImageSrc(src);
          
          // Skip вече обработени
          if (ocrImageCache.has(cacheKey)) continue;
          ocrImageCache.add(cacheKey);

          const text = await smartOCR(page, imgs[i], `img-${i + 1}`);
          
          if (text && text.length > 5 && !ocrTexts.has(text)) {
            ocrText += "\n" + text;
            ocrTexts.add(text);
            stats.ocrElementsProcessed++;
            stats.ocrCharsExtracted += text.length;
            ocrCount++;
          }
        } catch {}
      }

      console.log(`[OCR] Extracted from ${ocrTexts.size} images`);
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
    const totalWords = htmlWords + ocrWords;

    const elapsed = Date.now() - startTime;
    console.log(`[PAGE] ✓ ${url} - ${totalWords} words (${htmlWords} HTML + ${ocrWords} OCR) in ${elapsed}ms`);

    if (pageType !== "services" && totalWords < MIN_WORDS) {
      console.log("[SKIP] too few words:", totalWords);
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
        breakdown: { htmlWords, ocrWords },
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
  console.log(`[CONFIG] Parallel tabs: ${PARALLEL_TABS}, Max time: ${MAX_SECONDS}s`);

  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
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
  const ocrImageCache = new Set();
  let base = "";

  try {
    // Първоначално зареждане
    const initContext = await browser.newContext({
      viewport: { width: 1920, height: 1080 },
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    });
    const initPage = await initContext.newPage();
    
    await initPage.goto(startUrl, { timeout: 15000, waitUntil: "domcontentloaded" });
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
    const createWorker = async (workerId) => {
      const ctx = await browser.newContext({
        viewport: { width: 1920, height: 1080 },
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      });
      const pg = await ctx.newPage();

      while (Date.now() < deadline) {
        // Вземи URL от опашката
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
          // Изчакай малко и пробвай пак
          await new Promise(r => setTimeout(r, 100));
          if (queue.length === 0) break;
          continue;
        }

        stats.visited++;
        
        const result = await processPage(pg, url, base, stats, ocrImageCache);
        
        if (result.page) {
          pages.push(result.page);
          stats.saved++;
        }

        // Добави нови линкове
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
    const workers = [];
    for (let i = 0; i < PARALLEL_TABS; i++) {
      workers.push(createWorker(i + 1));
    }

    await Promise.all(workers);

  } finally {
    await browser.close();
    console.log(`\n[CRAWL DONE] ${stats.saved}/${stats.visited} pages saved`);
    console.log(`[STATS] OCR: ${stats.ocrElementsProcessed} images, ${stats.ocrCharsExtracted} chars`);
    console.log("[CLEANUP] Browser closed");
  }

  return { pages, stats };
}

// ================= HTTP SERVER =================
http
  .createServer((req, res) => {
    if (req.method === "GET") {
      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ 
        status: "ok",
        crawlInProgress,
        config: { PARALLEL_TABS, MAX_SECONDS, MIN_WORDS }
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

        if (crawlInProgress) {
          res.writeHead(429, { "Content-Type": "application/json" });
          return res.end(JSON.stringify({
            success: false,
            error: "Crawler already running"
          }));
        }

        crawlInProgress = true;
        crawlFinished = false;
        visited.clear();
        ocrCache.clear();

        const result = await crawlSmart(parsed.url);

        crawlInProgress = false;
        crawlFinished = true;
        lastResult = result;

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
    console.log(`Config: ${PARALLEL_TABS} parallel tabs, ${MAX_SECONDS}s max`);
  });
