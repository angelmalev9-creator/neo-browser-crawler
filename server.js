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
const MIN_WORDS = 15;
const PARALLEL_TABS = 6;
const PARALLEL_OCR = 10;
const OCR_TIMEOUT_MS = 6000;

const SKIP_URL_RE = /(wp-content\/uploads|media|gallery|video|photo|attachment|privacy|terms|cookies|gdpr)/i;

// Skip САМО наистина безполезни (не skip-ваме нищо което може да е съдържание)
const SKIP_OCR_RE = /^(logo|icon|sprite|pixel|spacer|blank|transparent|favicon)/i;

// ================= UTILS =================
const clean = (t = "") => t.replace(/\r/g, "").replace(/[ \t]+/g, " ").replace(/\n{3,}/g, "\n\n").trim();
const countWords = (t = "") => t.split(/\s+/).filter(Boolean).length;

// ================= STRUCTURED DATA EXTRACTORS =================
const PATTERNS = {
  price: /(\d+[\s.,]?\d*)\s*(лв|лева|€|EUR|BGN|USD|\$|евро)/gi,
  phone: /(\+?359|0)[\s.-]?(\d{2,3})[\s.-]?(\d{2,3})[\s.-]?(\d{2,4})/g,
  email: /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g,
  time: /(\d{1,2})[:.:](\d{2})\s*(ч\.?|часа?)?/g,
  address: /(ул\.|бул\.|пл\.|ж\.к\.|кв\.|гр\.|с\.)[\s\w\d.,№-]+/gi,
  percentage: /(\d+)\s*%/g,
  date: /(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})/g,
};

function extractStructuredData(text) {
  const data = {};
  
  // Цени
  const prices = [];
  let match;
  while ((match = PATTERNS.price.exec(text)) !== null) {
    prices.push({ value: match[1].replace(/\s/g, ''), currency: match[2] });
  }
  if (prices.length) data.prices = prices;
  
  // Телефони
  const phones = text.match(PATTERNS.phone);
  if (phones?.length) data.phones = [...new Set(phones)];
  
  // Имейли
  const emails = text.match(PATTERNS.email);
  if (emails?.length) data.emails = [...new Set(emails)];
  
  // Адреси
  const addresses = text.match(PATTERNS.address);
  if (addresses?.length) data.addresses = [...new Set(addresses)];
  
  // Работно време
  const times = text.match(PATTERNS.time);
  if (times?.length) data.workingHours = [...new Set(times)];
  
  return Object.keys(data).length ? data : null;
}

// ================= BG NUMBER NORMALIZER =================
const BG_0_19 = ["нула","едно","две","три","четири","пет","шест","седем","осем","девет","десет","единадесет","дванадесет","тринадесет","четиринадесет","петнадесет","шестнадесет","седемнадесет","осемнадесет","деветнадесет"];
const BG_TENS = ["","","двадесет","тридесет","четиридесет","петдесет","шестдесет","седемдесет","осемдесет","деветдесет"];

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
  if (/za-nas|about|за.?нас/.test(s)) return "about";
  if (/uslugi|services|услуги|pricing|price|ceni|tseni|цени/.test(s)) return "services";
  if (/kontakti|contact|контакт/.test(s)) return "contact";
  if (/faq|vuprosi|questions|въпрос/.test(s)) return "faq";
  if (/blog|news|article|новин|статия/.test(s)) return "blog";
  if (/gallery|galeria|галерия|portfolio|портфолио/.test(s)) return "gallery";
  if (/team|ekip|екип/.test(s)) return "team";
  return "general";
}

// ================= DEEP CONTENT EXTRACTOR =================
async function extractDeepContent(page) {
  try {
    await page.waitForSelector("body", { timeout: 1500 });
  } catch {}

  try {
    return await page.evaluate(() => {
      const result = {
        sections: [],
        lists: [],
        tables: [],
        links: [],
        meta: {}
      };

      // Meta информация
      const metaDesc = document.querySelector('meta[name="description"]');
      if (metaDesc) result.meta.description = metaDesc.content;
      
      const h1 = document.querySelector('h1');
      if (h1) result.meta.mainHeading = h1.innerText?.trim();

      // Секции с headings
      const seenTexts = new Set();
      let currentSection = null;

      document.querySelectorAll("h1,h2,h3,h4,p,li,td,th,span,div,a").forEach(el => {
        const text = el.innerText?.trim();
        if (!text || text.length < 3 || seenTexts.has(text)) return;
        
        // Skip navigation и footer елементи
        const parent = el.closest('nav,footer,header');
        if (parent && !el.closest('main,article,.content,#content')) return;

        seenTexts.add(text);

        if (/^H[1-4]$/.test(el.tagName)) {
          currentSection = { heading: text, level: parseInt(el.tagName[1]), content: [] };
          result.sections.push(currentSection);
        } else if (currentSection && text.length > 10) {
          currentSection.content.push(text);
        }
      });

      // Списъци (важни за услуги, характеристики)
      document.querySelectorAll("ul,ol").forEach(list => {
        const items = [];
        list.querySelectorAll("li").forEach(li => {
          const text = li.innerText?.trim();
          if (text && text.length > 5 && !seenTexts.has(text)) {
            items.push(text);
            seenTexts.add(text);
          }
        });
        if (items.length > 0) result.lists.push(items);
      });

      // Таблици (ценови листи, спецификации)
      document.querySelectorAll("table").forEach(table => {
        const rows = [];
        table.querySelectorAll("tr").forEach(tr => {
          const cells = [];
          tr.querySelectorAll("td,th").forEach(cell => {
            cells.push(cell.innerText?.trim() || "");
          });
          if (cells.some(c => c.length > 0)) rows.push(cells);
        });
        if (rows.length > 0) result.tables.push(rows);
      });

      // Важни линкове
      document.querySelectorAll("a[href]").forEach(a => {
        const text = a.innerText?.trim();
        const href = a.href;
        if (text && text.length > 3 && href && !href.startsWith('javascript:')) {
          result.links.push({ text, href });
        }
      });

      // Пълен текст (backup)
      const mainEl = document.querySelector("main,article,.content,#content,[role='main']") || document.body;
      result.fullText = mainEl.innerText?.trim() || "";

      return result;
    });
  } catch (e) {
    return { sections: [], lists: [], tables: [], links: [], meta: {}, fullText: "" };
  }
}

// ================= ULTRA FAST PARALLEL OCR =================
const globalOcrCache = new Map(); // Кеш между страниците (по URL на изображение)
const API_KEY = "AIzaSyCoai4BCKJtnnryHbhsPKxJN35UMcMAKrk";

async function ultraFastOCR(buffer) {
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
            imageContext: { languageHints: ["bg", "en", "tr", "ru", "de"] }
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

// OCR за ВСИЧКИ изображения на страницата (без skip на подобни)
async function ocrAllPageImages(page, stats) {
  const ocrResults = [];
  
  try {
    const imgElements = await page.$$("img");
    if (imgElements.length === 0) return ocrResults;

    // Вземаме info за всички
    const imgInfos = await Promise.all(
      imgElements.map(async (img, i) => {
        try {
          const info = await img.evaluate(el => ({
            src: el.src || "",
            alt: el.alt || "",
            w: Math.round(el.getBoundingClientRect().width),
            h: Math.round(el.getBoundingClientRect().height),
            visible: el.getBoundingClientRect().width > 0,
            // Позиция за контекст
            top: Math.round(el.getBoundingClientRect().top),
          }));
          return { ...info, element: img, index: i };
        } catch { return null; }
      })
    );

    // Филтрираме валидни изображения
    const validImages = imgInfos.filter(info => {
      if (!info || !info.visible) return false;
      if (info.w < 60 || info.h < 30) return false; // Малки
      if (info.w > 1800 && info.h > 700) return false; // Hero backgrounds
      if (SKIP_OCR_RE.test(info.src)) return false;
      return true;
    });

    console.log(`[OCR] ${validImages.length}/${imgElements.length} images valid`);

    if (validImages.length === 0) return ocrResults;

    // Screenshots паралелно
    const screenshots = await Promise.all(
      validImages.map(async (img) => {
        try {
          // Проверка в глобален кеш (само за идентични URL-и)
          if (globalOcrCache.has(img.src)) {
            return { ...img, buffer: null, cached: true, text: globalOcrCache.get(img.src) };
          }
          
          if (page.isClosed()) return null;
          const buffer = await img.element.screenshot({ type: 'png', timeout: 2500 });
          return { ...img, buffer, cached: false };
        } catch { return null; }
      })
    );

    const validScreenshots = screenshots.filter(s => s !== null);

    // OCR паралелно (batch по PARALLEL_OCR)
    for (let i = 0; i < validScreenshots.length; i += PARALLEL_OCR) {
      const batch = validScreenshots.slice(i, i + PARALLEL_OCR);
      
      const batchResults = await Promise.all(
        batch.map(async (img) => {
          if (img.cached) return { text: img.text, src: img.src, alt: img.alt };
          if (!img.buffer) return null;
          
          const text = await ultraFastOCR(img.buffer);
          
          // Кеширай в глобален кеш
          if (img.src) globalOcrCache.set(img.src, text);
          
          if (text && text.length > 2) {
            stats.ocrElementsProcessed++;
            stats.ocrCharsExtracted += text.length;
            return { text, src: img.src, alt: img.alt };
          }
          return null;
        })
      );

      ocrResults.push(...batchResults.filter(r => r !== null));
    }

    console.log(`[OCR] ✓ Got text from ${ocrResults.length} images`);
  } catch (e) {
    console.error("[OCR ERROR]", e.message);
  }

  return ocrResults;
}

// ================= LINK DISCOVERY =================
async function collectLinks(page, base) {
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

    // Бързо скролиране за lazy load
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight / 2);
      document.querySelectorAll('img[loading="lazy"]').forEach(img => img.loading = 'eager');
    });
    await page.waitForTimeout(350);
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(250);

    const title = clean(await page.title());
    const pageType = detectPageType(url, title);
    stats.byType[pageType] = (stats.byType[pageType] || 0) + 1;

    // Deep content extraction
    const deepContent = await extractDeepContent(page);

    // OCR ВСИЧКИ изображения на страницата
    const ocrResults = await ocrAllPageImages(page, stats);

    // Структуриране на данните
    const htmlContent = normalizeNumbers(clean(deepContent.fullText));
    const ocrContent = normalizeNumbers(clean(ocrResults.map(r => r.text).join("\n\n")));
    const allText = htmlContent + "\n" + ocrContent;

    // Извличане на структурирани данни
    const structuredData = extractStructuredData(allText);

    // Форматиране на секции
    const sectionsText = deepContent.sections
      .map(s => `[${s.heading}]\n${s.content.join("\n")}`)
      .join("\n\n");

    // Форматиране на таблици
    const tablesText = deepContent.tables
      .map(t => t.map(row => row.join(" | ")).join("\n"))
      .join("\n\n");

    // Форматиране на OCR (с контекст)
    const ocrText = ocrResults
      .map(r => r.alt ? `[${r.alt}]: ${r.text}` : r.text)
      .join("\n\n");

    const content = `
=== PAGE: ${url} ===
=== TITLE: ${title} ===
=== TYPE: ${pageType} ===

=== MAIN_CONTENT ===
${sectionsText || htmlContent}

=== LISTS ===
${deepContent.lists.map(l => "• " + l.join("\n• ")).join("\n\n")}

=== TABLES ===
${tablesText}

=== IMAGE_TEXT (OCR) ===
${ocrText}

=== STRUCTURED_DATA ===
${structuredData ? JSON.stringify(structuredData, null, 2) : "none"}

=== META ===
${deepContent.meta.description || ""}
`.trim();

    const htmlWords = countWords(htmlContent);
    const ocrWords = countWords(ocrContent);
    const totalWords = htmlWords + ocrWords;

    const elapsed = Date.now() - startTime;
    console.log(`[PAGE] ✓ ${totalWords}w (${ocrResults.length} imgs) ${elapsed}ms`);

    if (pageType !== "services" && pageType !== "gallery" && totalWords < MIN_WORDS) {
      return { links: await collectLinks(page, base), page: null };
    }

    return {
      links: await collectLinks(page, base),
      page: {
        url,
        title,
        pageType,
        content,
        wordCount: totalWords,
        breakdown: { htmlWords, ocrWords, images: ocrResults.length },
        structured: structuredData,
        meta: deepContent.meta,
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
  console.log(`[CONFIG] ${PARALLEL_TABS} tabs, ${PARALLEL_OCR} OCR parallel`);

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
    // Initial load
    const initCtx = await browser.newContext({
      viewport: { width: 1920, height: 1080 },
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    });
    const initPage = await initCtx.newPage();
    
    await initPage.goto(startUrl, { timeout: 10000, waitUntil: "domcontentloaded" });
    base = new URL(initPage.url()).origin;
    
    const initialLinks = await collectLinks(initPage, base);
    queue.push(normalizeUrl(initPage.url()));
    initialLinks.forEach(l => {
      const nl = normalizeUrl(l);
      if (!visited.has(nl) && !SKIP_URL_RE.test(nl) && !queue.includes(nl)) {
        queue.push(nl);
      }
    });
    
    await initPage.close();
    await initCtx.close();

    console.log(`[CRAWL] Found ${queue.length} URLs`);

    // Workers
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

    await Promise.all(Array(PARALLEL_TABS).fill(0).map(() => createWorker()));

  } finally {
    await browser.close();
    console.log(`\n[CRAWL DONE] ${stats.saved}/${stats.visited} pages`);
    console.log(`[OCR] ${stats.ocrElementsProcessed} images → ${stats.ocrCharsExtracted} chars`);
  }

  return { pages, stats };
}

// ================= HTTP SERVER =================
http.createServer((req, res) => {
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
      config: { PARALLEL_TABS, MAX_SECONDS, PARALLEL_OCR }
    }));
  }

  if (req.method !== "POST") {
    res.writeHead(405, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ error: "Method not allowed" }));
  }

  let body = "";
  req.on("data", c => (body += c));
  req.on("error", () => {
    res.writeHead(400, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ success: false, error: "Request error" }));
  });

  req.on("end", async () => {
    try {
      const parsed = JSON.parse(body || "{}");
      
      if (!parsed.url) {
        res.writeHead(400, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ success: false, error: "Missing 'url'" }));
      }

      const requestedUrl = normalizeUrl(parsed.url);
      const now = Date.now();

      // Cache hit
      if (crawlFinished && lastResult && lastCrawlUrl === requestedUrl && now - lastCrawlTime < RESULT_TTL_MS) {
        console.log("[CACHE HIT]", requestedUrl);
        res.writeHead(200, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ success: true, cached: true, ...lastResult }));
      }

      // In progress
      if (crawlInProgress) {
        const status = lastCrawlUrl === requestedUrl ? 202 : 429;
        res.writeHead(status, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({
          success: false,
          status: status === 202 ? "in_progress" : "busy",
          message: status === 202 ? "Crawl in progress" : "Busy with different URL"
        }));
      }

      // New crawl
      crawlInProgress = true;
      crawlFinished = false;
      lastCrawlUrl = requestedUrl;
      visited.clear();
      globalOcrCache.clear();

      console.log("[CRAWL NEW]", requestedUrl);

      const result = await crawlSmart(parsed.url);

      crawlInProgress = false;
      crawlFinished = true;
      lastResult = result;
      lastCrawlTime = Date.now();

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: true, ...result }));
    } catch (e) {
      crawlInProgress = false;
      console.error("[ERROR]", e.message);
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: e.message }));
    }
  });
}).listen(PORT, () => {
  console.log(`Crawler on ${PORT} | ${PARALLEL_TABS} tabs | ${PARALLEL_OCR} OCR`);
});
