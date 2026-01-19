import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

// ================= LIMITS =================
const MAX_SECONDS = 45;
const MIN_WORDS = 30;
const MAX_PAGES = 40;

const SKIP_URL_RE =
  /(wp-content|uploads|media|images|gallery|video|photo|attachment|category|tag|page\/)/i;

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
  if (/faq|въпроси|questions/.test(s)) return "faq";
  if (/price|pricing|цени/.test(s)) return "pricing";
  if (/uslugi|services/.test(s)) return "services";
  if (/kontakti|contact/.test(s)) return "contact";
  if (/za-nas|about/.test(s)) return "about";
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

    const faq = [];
    document.querySelectorAll(
      '[class*="faq"],[class*="accordion"],[aria-expanded]'
    ).forEach(el => {
      const t = el.innerText?.trim();
      if (t && t.length > 40) faq.push(t);
    });

    const headings = [...document.querySelectorAll("h1,h2,h3")]
      .map(h => h.innerText.trim())
      .filter(Boolean);

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

    const main =
      document.querySelector("main")?.innerText ||
      document.querySelector("article")?.innerText ||
      document.body.innerText ||
      "";

    return {
      faq,
      headings,
      sections,
      content: clean(
        [
          faq.join("\n\n"),
          sections.map(s => `${s.heading}: ${s.text}`).join("\n\n"),
          main,
        ].join("\n\n")
      ),
    };
  });
}

// ================= LINK COLLECTOR (GLOBAL) =================
async function collectAllLinks(page, base) {
  return await page.evaluate(base => {
    const urls = new Set();
    document.querySelectorAll("a[href]").forEach(a => {
      try {
        const u = new URL(a.href, base).href;
        urls.add(u);
      } catch {}
    });
    return Array.from(urls);
  }, base);
}

// ================= CRAWLER =================
async function crawlSmart(startUrl) {
  const deadline = Date.now() + MAX_SECONDS * 1000;

  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext();
  await context.route("**/*", route => {
    if (["image", "media", "font"].includes(route.request().resourceType()))
      return route.abort();
    route.continue();
  });

  const page = await context.newPage();
  if (!(await safeGoto(page, startUrl, 15000))) {
    await browser.close();
    throw new Error("Failed to load start URL");
  }

  const base = new URL(page.url()).origin;

  const queue = [page.url()];
  const visited = new Set();
  const pages = [];

  while (
    queue.length &&
    pages.length < MAX_PAGES &&
    Date.now() < deadline
  ) {
    const url = queue.shift();
    if (!url || visited.has(url) || SKIP_URL_RE.test(url)) continue;

    visited.add(url);

    if (!(await safeGoto(page, url))) continue;

    const title = clean(await page.title());
    const pageType = detectPageType(url, title);
    const data = await extractStructured(page);

    const words = countWords(data.content);
    console.log("[PAGE]", url, pageType, words);

    if (words >= MIN_WORDS) {
      pages.push({
        url,
        title,
        pageType,
        headings: data.headings,
        sections: data.sections,
        faq: data.faq,
        content: data.content,
        wordCount: words,
        status: "ok",
      });
    }

    const links = await collectAllLinks(page, base);
    for (const l of links) {
      if (
        l.startsWith(base) &&
        !visited.has(l) &&
        !queue.includes(l)
      ) {
        queue.push(l);
      }
    }
  }

  await browser.close();
  console.log("[DONE] Pages:", pages.length);
  return pages;
}

// ================= HTTP SERVER =================
http.createServer((req, res) => {
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
