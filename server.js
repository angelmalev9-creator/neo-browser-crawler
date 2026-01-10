import http from "http";
import { chromium } from "playwright";

const PORT = process.env.PORT || 10000;

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const CLICK_SELECTORS = [
  "button",
  "[role='button']",
  "[aria-expanded='false']",
  "details summary",
  ".accordion button",
  ".accordion-header",
  ".tabs button",
  ".tab",
  ".dropdown-toggle",
  ".menu-toggle",
];

function cleanText(t = "") {
  return String(t)
    .replace(/\s+/g, " ")
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .trim();
}

async function autoExpand(page) {
  for (let i = 0; i < 6; i++) {
    await page.mouse.wheel(0, 1600);
    await page.waitForTimeout(700);
  }

  for (const selector of CLICK_SELECTORS) {
    try {
      const nodes = await page.$$(selector);
      for (let i = 0; i < Math.min(nodes.length, 25); i++) {
        try {
          await nodes[i].click({ timeout: 500 });
          await page.waitForTimeout(250);
        } catch {}
      }
    } catch {}
  }

  for (let i = 0; i < 3; i++) {
    await page.mouse.wheel(0, 1600);
    await page.waitForTimeout(600);
  }
}

function json(res, status, obj) {
  res.writeHead(status, { ...corsHeaders, "Content-Type": "application/json" });
  res.end(JSON.stringify(obj));
}

const server = http.createServer(async (req, res) => {
  if (req.method === "OPTIONS") {
    res.writeHead(200, corsHeaders);
    return res.end();
  }

  if (req.method !== "POST" || req.url !== "/crawl") {
    return json(res, 404, { success: false, error: "Not found" });
  }

  try {
    const body = await new Promise((resolve, reject) => {
      let data = "";
      req.on("data", (chunk) => (data += chunk));
      req.on("end", () => resolve(data));
      req.on("error", reject);
    });

    const { url, maxPages = 12, token } = JSON.parse(body || "{}");

    if (!url) return json(res, 400, { success: false, error: "Missing url" });

    if (process.env.CRAWLER_TOKEN && token !== process.env.CRAWLER_TOKEN) {
      return json(res, 401, { success: false, error: "Unauthorized" });
    }

    const browser = await chromium.launch({
      headless: true,
      args: ["--no-sandbox"],
    });

    const context = await browser.newContext({
      viewport: { width: 1280, height: 720 },
      locale: "bg-BG",
    });

    const page = await context.newPage();
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45000 });
    await page.waitForTimeout(1500);
    await autoExpand(page);

    const title = cleanText(await page.title());
    const bodyText = cleanText(await page.evaluate(() => document.body?.innerText || ""));

    const base = new URL(url);
    const links = await page.evaluate(() => {
      return Array.from(document.querySelectorAll("a[href]"))
        .map((a) => a.href)
        .filter((h) => typeof h === "string");
    });

    const important = links
      .filter((l) => {
        try {
          const u = new URL(l);
          return u.origin === base.origin;
        } catch {
          return false;
        }
      })
      .filter((l) =>
        /about|за-нас|services|услуги|pricing|цени|price|contact|контакти|menu|меню|booking|reservation|appointment|запази/i.test(l)
      );

    const uniqueImportant = Array.from(new Set(important)).slice(0, maxPages);

    const pages = [{ url, title, text: bodyText }];

    for (const link of uniqueImportant) {
      try {
        const p2 = await context.newPage();
        await p2.goto(link, { waitUntil: "domcontentloaded", timeout: 45000 });
        await p2.waitForTimeout(1200);
        await autoExpand(p2);

        const t2 = cleanText(await p2.title());
        const tx2 = cleanText(await p2.evaluate(() => document.body?.innerText || ""));

        if (tx2.length > 500) pages.push({ url: link, title: t2, text: tx2 });

        await p2.close();
      } catch {}
    }

    await browser.close();

    return json(res, 200, {
      success: true,
      root: url,
      pagesCount: pages.length,
      pages,
    });
  } catch (e) {
    return json(res, 500, { success: false, error: e?.message || "Crawler error" });
  }
});

server.listen(PORT, () => console.log(`Crawler listening on :${PORT}`));
