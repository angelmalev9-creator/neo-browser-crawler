import http from "http";
import { chromium } from "playwright";

const PORT = Number(process.env.PORT || 10000);

async function crawl(url) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });

  const page = await browser.newPage();
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });

  const title = await page.title();
  const text = await page.evaluate(() => document.body?.innerText || "");

  await browser.close();

  return [{ url, title, content: text }];
}

http.createServer(async (req, res) => {
  if (req.method !== "POST") {
    res.writeHead(405);
    return res.end();
  }

  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", async () => {
    try {
      const { url } = JSON.parse(body);
      const pages = await crawl(url);

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: true, pages }));
    } catch (e) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: e.message }));
    }
  });
}).listen(PORT, () => {
  console.log("Crawler running on", PORT);
});
