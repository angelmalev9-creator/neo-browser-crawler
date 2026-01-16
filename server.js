import http from "http";
import { chromium } from "playwright";
import { createClient } from "@supabase/supabase-js";

const PORT = Number(process.env.PORT || 10000);
const POLL_INTERVAL_MS = 8000;

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

/* ================= LIMITS ================= */
const MAX_PAGES = 40;
const MAX_CHARS_PER_PAGE = 16000;
const MIN_PAGE_CHARS = 180;
const MAX_TOTAL_CHARS = 360000;

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
  const pages = [];
  let totalChars = 0;

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

    if (text.length >= MIN_PAGE_CHARS) {
      pages.push({
        url,
        title: cleanText(await page.title()),
        content: clamp(text, MAX_CHARS_PER_PAGE),
        category: "general",
      });
    }

    return pages;
  } finally {
    await browser.close();
  }
}

/* ================= JOB LOOP ================= */
async function pollJobs() {
  const { data: job } = await supabase
    .from("demo_sessions")
    .select("*")
    .eq("status", "queued")
    .order("created_at", { ascending: true })
    .limit(1)
    .maybeSingle();

  if (!job) return;

  console.log("▶️ Processing job:", job.id);

  await supabase
    .from("demo_sessions")
    .update({ status: "scraping" })
    .eq("id", job.id);

  try {
    const pages = await crawlSite(job.url);

    if (!pages.length) throw new Error("No usable content");

    await supabase
      .from("demo_sessions")
      .update({
        status: "ready",
        scraped_content: pages,
        language: "bg",
        error_message: null,
      })
      .eq("id", job.id);

    console.log("✅ Done:", job.id);
  } catch (e) {
    await supabase
      .from("demo_sessions")
      .update({
        status: "error",
        error_message: e.message || "Crawler error",
      })
      .eq("id", job.id);
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
