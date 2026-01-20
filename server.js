import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

// ================= LIMITS =================
const MAX_SECONDS = 60;
const MIN_WORDS = 25;

const SKIP_URL_RE =
  /(wp-content|uploads|media|images|gallery|video|photo|attachment|category|tag|page\/|privacy|terms|cookies|gdpr)/i;

// ================= UTILS =================
const clean = (t = "") => t.replace(/\s+/g, " ").trim();
const countWords = (t = "") =>
  t.split(/\s+/).filter(w => w.length > 2).length;

// ================= SAFE GOTO =================
async function safeGoto(page, url, timeout = 12000) {
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
  if (/uslugi|services|pricing|price/.test(s)) return "services";
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

  return await page.evaluate(() => {
    ["header", "footer", "nav", "aside"].forEach(sel => {
      document.querySelectorAll(sel).forEach(n => n.remove());
    });

    const faqBlocks = [];
    document.querySelectorAll(
      '[class*="faq"], [class*="accordion"], [class*="question"], [class*="answer"], [aria-expanded]'
    ).forEach(el => {
      const t = el.innerText?.trim();
      if (t && t.length > 40) faqBlocks.push(t);
    });

    const headings = [...document.querySelectorAll("h1,h2,h3")]
      .filter(h => h.offsetParent !== null)
      .map(h => h.innerText.trim());

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

    const metaDescription =
      document.querySelector('meta[name="description"]')?.content || "";

    const content = [
      faqBlocks.join("\n\n"),
      sections.map(s => `${s.heading}: ${s.text}`).join("\n\n"),
      metaDescription,
      mainContent,
    ].join("\n\n");

    return {
      faqCount: faqBlocks.length,
      sectionCount: sections.length,
      headings,
      sections,
      summary: faqBlocks.slice(0, 10),
      content,
      breakdown: {
        faqWords: countWords(faqBlocks.join(" ")),
        sectionWords: countWords(sections.map(s => s.text).join(" ")),
        metaWords: countWords(metaDescription),
        mainWords: countWords(mainContent),
      },
    };
  });
}

// ================= LINK DISCOVERY =================
async function collectAllLinks(page, base) {
  return await page.evaluate(base => {
    const urls = new Set();
    document.querySelectorAll("a[href]").forEach(a => {
      try {
        const u = new URL(a.href, base);
        if (u.origin === base) urls.add(u.href);
      } catch {}
    });
    return Array.from(urls);
  }, base);
}

// ================= CRAWLER =================
async function crawlSmart(startUrl) {
  const deadline = Date.now() + MAX_SECONDS * 1000;
  console.log("[CRAWL START]", startUrl);

  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();

  await context.route("**/*", route => {
    const type = route.request().resourceType();
    if (["image", "media", "font"].includes(type)) return route.abort();
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
  const queue = [page.url()];

  while (queue.length && Date.now() < deadline) {
    const url = queue.shift();
    if (!url || visited.has(url) || SKIP_URL_RE.test(url)) continue;
    visited.add(url);

    if (!(await safeGoto(page, url))) continue;

    const title = clean(await page.title());
    const pageType = detectPageType(url, title);
    const data = await extractStructured(page);

    const totalWords = countWords(data.content);

    console.log("────────────────────────────");
    console.log("[PAGE]", url);
    console.log(" type:", pageType);
    console.log(" faq blocks:", data.faqCount, "| words:", data.breakdown.faqWords);
    console.log(" sections:", data.sectionCount, "| words:", data.breakdown.sectionWords);
    console.log(" main content words:", data.breakdown.mainWords);
    console.log(" TOTAL WORDS:", totalWords);

    if (totalWords >= MIN_WORDS) {
      pages.push({
        url,
        title,
        pageType,
        headings: data.headings,
        sections: data.sections,
        summary: data.summary,
        content: clean(data.content),
        wordCount: totalWords,
        breakdown: data.breakdown,
        status: "ok",
      });
      console.log("[SAVE]");
    }

    const links = await collectAllLinks(page, base);
    links.forEach(l => {
      if (!visited.has(l) && !SKIP_URL_RE.test(l)) queue.push(l);
    });
  }

  await browser.close();
  console.log("[CRAWL DONE] Pages saved:", pages.length);
  return pages;
}

// ================= HTTP SERVER =================
http.createServer((req, res) => {
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
      const pages = await crawlSmart(url);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: true, pagesCount: pages.length, pages }));
    } catch (e) {
      res.writeHead(500);
      res.end(JSON.stringify({ success: false, error: e.message }));
    }
  });
}).listen(PORT, () => {
  console.log("Crawler running on", PORT);
});
