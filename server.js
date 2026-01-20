import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

// ================= LIMITS =================
const MAX_SECONDS = 70;
const MIN_WORDS = 40;
const MAX_CHILD_PER_PAGE = 6;

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
  if (/za-nas|about/.test(s)) return "about";
  if (/uslugi|services|pricing|price/.test(s)) return "services";
  if (/kontakti|contact/.test(s)) return "contact";
  if (/faq|vuprosi|questions/.test(s)) return "faq";
  if (/blog|news|article/.test(s)) return "blog";
  return "general";
}

// ================= BUSINESS SIGNALS =================
function extractBusinessSignals(content = "") {
  const lc = content.toLowerCase();

  const signals = {
    requires_booking: false,
    lead_method: "unknown", // booking | email | phone | form
    primary_cta: null,
    business_intent: "information", // booking | consultation | sell
  };

  if (
    lc.includes("запази час") ||
    lc.includes("резервира") ||
    lc.includes("booking") ||
    lc.includes("appointment")
  ) {
    signals.requires_booking = true;
    signals.lead_method = "booking";
    signals.business_intent = "booking";
    signals.primary_cta = "Запазване на час";
  }

  if (
    lc.includes("оферта") ||
    lc.includes("запитване") ||
    lc.includes("форма") ||
    lc.includes("свържете се")
  ) {
    signals.lead_method = "form";
    signals.business_intent = "consultation";
    signals.primary_cta = "Запитване / оферта";
  }

  if (lc.includes("@") && lc.includes("имейл")) {
    signals.lead_method = "email";
  }

  if (
    lc.includes("тел") ||
    lc.includes("обадете") ||
    lc.includes("телефон")
  ) {
    if (signals.lead_method === "unknown") {
      signals.lead_method = "phone";
    }
  }

  return signals;
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

    document.querySelectorAll("button").forEach(b => {
      const t = (b.innerText || "").toLowerCase();
      if (
        t.includes("accept") ||
        t.includes("agree") ||
        t.includes("allow") ||
        t.includes("прием")
      ) {
        b.click();
      }
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

    const finalContent = [
      faqBlocks.join("\n\n"),
      sections.map(s => `${s.heading}: ${s.text}`).join("\n\n"),
      metaDescription,
      mainContent,
    ].join("\n\n");

    return {
      headings,
      sections,
      summary: faqBlocks.slice(0, 5),
      content: finalContent,
    };
  });
}

// ================= LINK COLLECTOR =================
async function collectNavLinks(page, base) {
  return await page.evaluate(base => {
    const urls = new Set();
    document
      .querySelectorAll("header a[href], nav a[href], footer a[href]")
      .forEach(a => {
        try {
          urls.add(new URL(a.href, base).href);
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

  let targets = await collectNavLinks(page, base);
  targets.unshift(page.url());

  targets = targets
    .filter(u => u.startsWith(base) && !SKIP_URL_RE.test(u))
    .sort((a, b) => {
      const score = (u: string) => {
        let s = 0;
        if (/uslugi|services|pricing|price/.test(u)) s += 50;
        if (/kontakt|contact/.test(u)) s += 40;
        if (/faq|vuprosi/.test(u)) s += 30;
        if (/za-nas|about/.test(u)) s += 10;
        if (/blog|news/.test(u)) s -= 20;
        return s;
      };
      return score(b) - score(a);
    });

  console.log("[TARGETS]", targets.length);

  for (const url of targets) {
    if (Date.now() > deadline) break;
    if (visited.has(url)) continue;

    visited.add(url);

    try {
      if (!(await safeGoto(page, url))) continue;

      const title = clean(await page.title());
      const pageType = detectPageType(url, title);
      const data = await extractStructured(page);
      const words = countWords(data.content);

      if (words < MIN_WORDS) continue;

      const businessSignals = extractBusinessSignals(data.content);

      pages.push({
        url,
        title,
        pageType,
        headings: data.headings,
        sections: data.sections,
        summary: data.summary,
        content: clean(data.content),
        wordCount: words,
        businessSignals,
        status: "ok",
      });

    } catch {
      pages.push({ url, status: "failed" });
    }
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
  req.on("data", c => body += c);
  req.on("end", async () => {
    try {
      const { url } = JSON.parse(body || "{}");
      const pages = await crawlSmart(url);

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: true, pagesCount: pages.length, pages }));
    } catch (e) {
      res.writeHead(500);
      res.end(JSON.stringify({ success: false, error: String(e) }));
    }
  });
}).listen(PORT, () => {
  console.log("Crawler running on", PORT);
});
