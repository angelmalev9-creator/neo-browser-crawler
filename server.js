import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);
const POLL_INTERVAL_MS = 8000;

// 🔴 EDGE FUNCTION ENDPOINT
const EDGE_URL = process.env.EDGE_FUNCTION_URL;
// пример:
// https://lvcxmcdbopxxfuoaqgze.supabase.co/functions/v1/scrape-website

if (!EDGE_URL) {
  throw new Error("EDGE_FUNCTION_URL env var is required");
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

/* ================= CRAWLER ================= */
async function crawlSite(url) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const page = await browser.newPage();
  page.setDefaultTimeout(25000);

  try {
    await page.goto(url, { waitUntil: "commit", timeout: 25000 });
    await page.waitForTimeout(800);

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

/* ================= JOB LOOP ================= */
async function pollJobs() {
  try {
    // 1️⃣ ask edge for next job
    const jobResp = await fetch(EDGE_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "next-job" }),
    });

    const jobData = await jobResp.json();
    if (!jobData?.job) return;

    const { id, url } = jobData.job;
    console.log("▶️ Processing job:", id);

    try {
      const pages = await crawlSite(url);

      // 2️⃣ send result back
      await fetch(EDGE_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action: "complete-job",
          id,
          pages,
          language: "bg",
        }),
      });

      console.log("✅ Done:", id);
    } catch (e) {
      await fetch(EDGE_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action: "fail-job",
          id,
          error: e.message || "Crawler error",
        }),
      });
    }
  } catch (e) {
    console.error("pollJobs error:", e);
  }
}

setInterval(pollJobs, POLL_INTERVAL_MS);

/* ================= HEALTH SERVER ================= */
http
  .createServer((_, res) => {
    res.writeHead(200);
    res.end("OK");
  })
  .listen(PORT, () => {
    console.log("Crawler worker running on :" + PORT);
  });
