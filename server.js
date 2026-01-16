import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

/* ================= CORS ================= */
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

/* ================= LIMITS ================= */
const MAX_PAGES_DEFAULT = 30;
const MAX_PAGES_HARD = 100;

const MAX_CHARS_PER_PAGE = 16000;
const MAX_TOTAL_CHARS = 360000;
const MIN_PAGE_CHARS = 180;
const MAX_QUEUE = 900;

/* ================= BLOCKED RESOURCES ================= */
const BLOCK_RESOURCE_TYPES = new Set([
  "image",
  "media",
  "font",
  "stylesheet",
]);

/* ================= HELPERS ================= */
function json(res, status, obj) {
  res.writeHead(status, { ...corsHeaders, "Content-Type": "application/json" });
  res.end(JSON.stringify(obj));
}

function cleanText(t = "") {
  return String(t)
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/[ \u00A0]+/g, " ")
    .replace(/\s+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function clamp(str, max) {
  if (!str) return "";
  return str.length <= max ? str : str.slice(0, max);
}

function normalizeUrl(u) {
  try {
    const url = new URL(u);
    url.hash = "";
    return url.toString();
  } catch {
    return null;
  }
}

/* ================= MAIN CRAWL ================= */
async function crawlSite(url, maxPages) {
  const pages = [];
  const visited = new Set();
  let totalChars = 0;
  const queue = [url];

  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const context = await browser.newContext({
    viewport: { width: 1280, height: 720 },
    locale: "bg-BG",
  });

  // 🔴 BLOCK HEAVY RESOURCES
  await context.route("**/*", (route) => {
    if (BLOCK_RESOURCE_TYPES.has(route.request().resourceType())) {
      return route.abort();
    }
    route.continue();
  });

  try {
    while (queue.length && pages.length < maxPages && totalChars < MAX_TOTAL_CHARS) {
      const next = queue.shift();
      const norm = normalizeUrl(next);
      if (!norm || visited.has(norm)) continue;
      visited.add(norm);

      const page = await context.newPage();
      page.setDefaultTimeout(25000);

      try {
        // ✅ CRITICAL FIX HERE
        await page.goto(norm, {
          waitUntil: "commit",
          timeout: 25000,
        });

        await page.waitForTimeout(800);

        const title = cleanText(await page.title());
        const text = cleanText(
          await page.evaluate(() => document.body?.innerText || "")
        );

        if (text.length >= MIN_PAGE_CHARS) {
          const remaining = MAX_TOTAL_CHARS - totalChars;
          const finalText = clamp(text, remaining);
          pages.push({ url: norm, title, text: finalText });
          totalChars += finalText.length;
        }

        const links = await page.evaluate(() =>
          Array.from(document.querySelectorAll("a[href]"))
            .map((a) => a.href)
            .filter((h) => h.startsWith("http"))
        );

        for (const l of links) {
          const ln = normalizeUrl(l);
          if (!ln || visited.has(ln)) continue;
          if (queue.length < MAX_QUEUE) queue.push(ln);
        }
      } catch (e) {
        console.log("⏱️ SKIP timeout:", norm);
      } finally {
        await page.close();
      }
    }

    return pages;
  } finally {
    await context.close();
    await browser.close();
  }
}

/* ================= HTTP SERVER ================= */
const server = http.createServer(async (req, res) => {
  if (req.method === "OPTIONS") {
    res.writeHead(200, corsHeaders);
    return res.end();
  }

  if (req.url !== "/crawl" || req.method !== "POST") {
    return json(res, 404, { success: false, error: "Not found" });
  }

  try {
    let body = "";
    for await (const chunk of req) body += chunk;
    const { url, maxPages } = JSON.parse(body || "{}");

    if (!url) return json(res, 400, { success: false, error: "Missing url" });

    const pages = await crawlSite(
      url,
      Math.min(Math.max(maxPages || MAX_PAGES_DEFAULT, 6), MAX_PAGES_HARD)
    );

    return json(res, 200, {
      success: true,
      pagesCount: pages.length,
      totalChars: pages.reduce((a, p) => a + p.text.length, 0),
      pages,
    });
  } catch (e) {
    return json(res, 500, {
      success: false,
      error: e?.message || "Crawler error",
    });
  }
});

server.listen(PORT, () => {
  console.log("Crawler listening on :" + PORT);
});
