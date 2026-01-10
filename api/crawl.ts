import type { VercelRequest, VercelResponse } from "@vercel/node";
import chromium from "@sparticuz/chromium";
import puppeteer from "puppeteer-core";

const cors = (res: VercelResponse) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
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

function cleanText(t: string) {
  return t
    .replace(/\s+/g, " ")
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .trim();
}

async function autoExpand(page: any) {
  // scroll to load lazy sections
  for (let i = 0; i < 6; i++) {
    await page.evaluate(() => window.scrollBy(0, 1600));
    await page.waitForTimeout(700);
  }

  // click buttons/tabs/accordions
  for (const sel of CLICK_SELECTORS) {
    try {
      const els = await page.$$(sel);
      for (let i = 0; i < Math.min(els.length, 25); i++) {
        try {
          await els[i].click({ delay: 20 });
          await page.waitForTimeout(200);
        } catch {}
      }
    } catch {}
  }

  // final scroll
  for (let i = 0; i < 3; i++) {
    await page.evaluate(() => window.scrollBy(0, 1600));
    await page.waitForTimeout(600);
  }
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  cors(res);

  if (req.method === "OPTIONS") {
    return res.status(200).json({ ok: true });
  }

  try {
    const { url, maxPages = 12, token } = req.body || {};

    if (!url) return res.status(400).json({ success: false, error: "Missing url" });

    if (process.env.CRAWLER_TOKEN && token !== process.env.CRAWLER_TOKEN) {
      return res.status(401).json({ success: false, error: "Unauthorized" });
    }

    const browser = await puppeteer.launch({
      executablePath: await chromium.executablePath(),
      headless: chromium.headless,
      args: chromium.args,
      defaultViewport: { width: 1280, height: 720 },
    });

    const page = await browser.newPage();
    page.setDefaultTimeout(45000);

    await page.goto(url, { waitUntil: "domcontentloaded" });
    await page.waitForTimeout(2000);

    await autoExpand(page);

    const title = cleanText((await page.title()) || "");
    const bodyText = cleanText(
      await page.evaluate(() => document.body?.innerText || "")
    );

    const base = new URL(url);
    const links: string[] = await page.evaluate(() => {
      return Array.from(document.querySelectorAll("a[href]"))
        .map((a: any) => a.href)
        .filter((h: string) => typeof h === "string");
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
        /about|за-нас|services|услуги|pricing|цени|price|contact|контакти|menu|меню|booking|reservation|appointment|запази/i.test(
          l
        )
      );

    const uniqueImportant = Array.from(new Set(important)).slice(0, maxPages);

    const pages: any[] = [{ url, title, text: bodyText }];

    // crawl important pages
    for (const link of uniqueImportant) {
      try {
        const p2 = await browser.newPage();
        await p2.goto(link, { waitUntil: "domcontentloaded" });
        await p2.waitForTimeout(1500);
        await autoExpand(p2);

        const t2 = cleanText((await p2.title()) || "");
        const tx2 = cleanText(
          await p2.evaluate(() => document.body?.innerText || "")
        );

        if (tx2.length > 500) pages.push({ url: link, title: t2, text: tx2 });

        await p2.close();
      } catch {}
    }

    await browser.close();

    return res.status(200).json({
      success: true,
      root: url,
      pagesCount: pages.length,
      pages,
    });
  } catch (e: any) {
    return res.status(500).json({
      success: false,
      error: e?.message || "Crawler error",
    });
  }
}
