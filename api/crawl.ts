import type { VercelRequest, VercelResponse } from "@vercel/node";
import { chromium } from "playwright-core";

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
  // scroll a few times to load lazy content
  for (let i = 0; i < 5; i++) {
    await page.mouse.wheel(0, 1500);
    await page.waitForTimeout(700);
  }

  // click expandable items
  for (const selector of CLICK_SELECTORS) {
    const nodes = await page.$$(selector);
    for (let i = 0; i < Math.min(nodes.length, 25); i++) {
      try {
        await nodes[i].click({ timeout: 300 });
        await page.waitForTimeout(250);
      } catch {}
    }
  }

  // scroll again after expanding
  for (let i = 0; i < 3; i++) {
    await page.mouse.wheel(0, 1500);
    await page.waitForTimeout(600);
  }
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const { url, maxPages = 12, token } = req.body || {};

    if (!url) {
      return res.status(400).json({ success: false, error: "Missing url" });
    }

    // optional token security
    if (process.env.CRAWLER_TOKEN && token !== process.env.CRAWLER_TOKEN) {
      return res.status(401).json({ success: false, error: "Unauthorized" });
    }

    const browser = await chromium.launch({
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
      ],
    });

    const context = await browser.newContext({
      userAgent:
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari/537.36",
      viewport: { width: 1280, height: 720 },
      locale: "bg-BG",
    });

    const page = await context.newPage();
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
      const as = Array.from(document.querySelectorAll("a[href]"));
      return as
        .map((a: any) => a.href)
        .filter((h: string) => typeof h === "string");
    });

    // prioritise important pages
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

    // crawl important internal pages
    for (const link of uniqueImportant) {
      try {
        const p2 = await context.newPage();
        await p2.goto(link, { waitUntil: "domcontentloaded" });
        await p2.waitForTimeout(1500);
        await autoExpand(p2);

        const t2 = cleanText((await p2.title()) || "");
        const tx2 = cleanText(
          await p2.evaluate(() => document.body?.innerText || "")
        );

        if (tx2.length > 500) {
          pages.push({ url: link, title: t2, text: tx2 });
        }

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
