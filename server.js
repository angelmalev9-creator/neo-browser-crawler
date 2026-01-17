import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}

/* ================= LIMITS ================= */
const MAX_CHARS_PER_PAGE = 16000;
const MIN_PAGE_CHARS = 180;

/* ================= UTILS ================= */
const cleanText = (t = "") =>
  String(t)
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/[ \u00A0]+/g, " ")
    .replace(/\s+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

const clamp = (s, m) => (s.length <= m ? s : s.slice(0, m));

async function updateSession(sessionId, payload) {
  const resp = await fetch(
    `${SUPABASE_URL}/rest/v1/demo_sessions?id=eq.${sessionId}`,
    {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
        apikey: SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
        Prefer: "return=minimal",
      },
      body: JSON.stringify(payload),
    }
  );

  if (!resp.ok) {
    const t = await resp.text();
    throw new Error("Supabase update failed: " + t.slice(0, 200));
  }
}

/* ================= CRAWLER ================= */
async function crawlSite(url) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const page = await browser.newPage();
  page.setDefaultTimeout(30000);

  try {
    await page.goto(url, { waitUntil: "commit" });
    await page.waitForTimeout(1200);

    const text = cleanText(
      await page.evaluate(() => document.body?.innerText || "")
    );

    if (text.length < MIN_PAGE_CHARS) {
      throw new Error("Content too short");
    }

    return [
      {
        url,
        title: cleanText(await page.title()),
        content: clamp(text, MAX_CHARS_PER_PAGE),
        category: "general",
      },
    ];
  } finally {
    await browser.close();
  }
}

/* ================= HTTP API ================= */
http
  .createServer(async (req, res) => {
    if (req.method !== "POST" || req.url !== "/crawl") {
      res.writeHead(404);
      return res.end("Not found");
    }

    let body = "";
    for await (const chunk of req) body += chunk;

    try {
      const { url, sessionId } = JSON.parse(body || "{}");
      if (!url || !sessionId) {
        res.writeHead(400);
        return res.end("Missing url or sessionId");
      }

      await updateSession(sessionId, {
        status: "scraping",
        error_message: null,
      });

      try {
        const pages = await crawlSite(url);

        await updateSession(sessionId, {
          status: "ready",
          scraped_content: pages,
          language: "bg",
          error_message: null,
        });
      } catch (e) {
        await updateSession(sessionId, {
          status: "error",
          error_message: e.message || "Crawler error",
        });
      }

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: true }));
    } catch (e) {
      res.writeHead(500);
      res.end("Server error");
    }
  })
  .listen(PORT, () => {
    console.log("Crawler running on :" + PORT);
  });
