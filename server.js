import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

const clean = (t = "") =>
  t.replace(/[\u200B-\u200D\uFEFF]/g, "").replace(/\s+/g, " ").trim();

async function scrapeSections(url) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const page = await browser.newPage();
  await page.goto(url, { waitUntil: "networkidle", timeout: 45000 });

  // aggressive scroll
  for (let i = 0; i < 10; i++) {
    await page.evaluate(() => window.scrollBy(0, window.innerHeight));
    await page.waitForTimeout(400);
  }

  // expand menus / accordions
  await page.evaluate(() => {
    document.querySelectorAll("button, summary, [role='button']").forEach((el) => {
      try {
        el.click();
      } catch {}
    });
  });

  await page.waitForTimeout(1200);

  const sections = await page.evaluate(() => {
    const blocks = [];
    const candidates = document.querySelectorAll(
      "section, article, main > div, div[class*='section'], div[class*='block']"
    );

    candidates.forEach((el) => {
      const text = el.innerText?.trim();
      if (!text || text.length < 300) return;

      const title =
        el.querySelector("h1,h2,h3")?.innerText ||
        el.getAttribute("aria-label") ||
        "Секция";

      blocks.push({
        title,
        content: text,
      });
    });

    return blocks;
  });

  await browser.close();

  return sections.map((s, i) => ({
    url: `${url}#section-${i + 1}`,
    title: clean(s.title),
    content: clean(s.content),
  }));
}

http
  .createServer((req, res) => {
    if (req.method !== "POST") {
      res.writeHead(405);
      return res.end();
    }

    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", async () => {
      try {
        const { url } = JSON.parse(body || "{}");
        if (!url) throw new Error("Missing url");

        const pages = await scrapeSections(url);

        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            success: true,
            pagesCount: pages.length,
            pages,
          })
        );
      } catch (e) {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            success: false,
            error: e.message || "Crawler error",
          })
        );
      }
    });
  })
  .listen(PORT, () => {
    console.log("🚀 Section crawler running on", PORT);
  });
