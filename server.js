import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

type ScrapedPage = {
  url: string;
  title?: string;
  text?: string;
};

const safeJson = async (resp: Response) => {
  try {
    return await resp.json();
  } catch {
    return null;
  }
};

// ------------------------------
// LIMITS (DO NOT TOUCH)
// ------------------------------
const MAX_PAGES = 60;
const MIN_PAGE_CHARS = 450;

const MAX_CHARS_PER_PAGE = 18000;
const MAX_TOTAL_CHARS = 280000;
const MAX_AI_CONTEXT_CHARS = 220000;

// ✅ NEW: AI selection limit (we scrape many pages, but summarize only top N)
const MAX_AI_PAGES = 22;

// ------------------------------
// Text utilities
// ------------------------------
const normalizeText = (t: string) =>
  String(t || "")
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/[ \u00A0]+/g, " ")
    .replace(/\s+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

const clamp = (t: string, max: number) => {
  if (!t) return "";
  if (t.length <= max) return t;
  return t.slice(0, max);
};

function detectLanguage(text: string) {
  const sample = text.slice(0, 6000);
  const cyr = (sample.match(/[а-яА-ЯЁё]/g) || []).length;
  const lat = (sample.match(/[a-zA-Z]/g) || []).length;
  return cyr > lat * 0.3 ? "bg" : "en";
}

function categorizeUrl(url: string) {
  const u = url.toLowerCase();

  if (u.includes("/pricing") || u.includes("/price") || u.includes("/цени") || u.includes("tariff")) return "pricing";
  if (u.includes("/packages") || u.includes("/offers") || u.includes("/пакети") || u.includes("/оферти")) return "packages";
  if (u.includes("/services") || u.includes("/service") || u.includes("/услуги")) return "services";
  if (u.includes("/rooms") || u.includes("/accommodation") || u.includes("/настан")) return "rooms";
  if (u.includes("/booking") || u.includes("/reservation") || u.includes("/резервац")) return "booking";
  if (u.includes("/product") || u.includes("/shop") || u.includes("/catalog")) return "products";
  if (u.includes("/faq") || u.includes("/help") || u.includes("/въпроси")) return "faq";
  if (u.includes("/contact") || u.includes("/контакт") || u.includes("/контакти")) return "contact";
  if (u.includes("/about") || u.includes("/за-нас")) return "about";
  if (u.includes("/blog") || u.includes("/news") || u.includes("/article")) return "blog";

  return "general";
}

function isUselessUrl(url: string) {
  const u = (url || "").toLowerCase();
  return (
    u.includes("privacy") ||
    u.includes("cookies") ||
    u.includes("cookie") ||
    u.includes("terms") ||
    u.includes("policy") ||
    u.includes("gdpr") ||
    u.includes("consent") ||
    u.includes("legal")
  );
}

// aggressive boilerplate stripper + dedupe
function stripBoilerplate(text: string) {
  if (!text) return "";

  const lines = text
    .split(/\n+/g)
    .map((l) => l.trim())
    .filter(Boolean);

  const bad = (l: string) => {
    const s = l.toLowerCase();
    if (l.length <= 2) return true;

    return (
      s.includes("cookie") ||
      s.includes("cookies") ||
      s.includes("gdpr") ||
      s.includes("consent") ||
      s.includes("privacy") ||
      s.includes("terms") ||
      s.includes("policy") ||
      s.includes("политика") ||
      s.includes("лични данни") ||
      s.includes("правата са запазени") ||
      s.includes("all rights reserved") ||
      s.includes("accept") ||
      s.includes("decline") ||
      s.includes("preferences") ||
      s.includes("настройки на бисквитките")
    );
  };

  const seen = new Set<string>();
  const cleanedLines: string[] = [];

  for (const l of lines) {
    if (bad(l)) continue;
    const key = l.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    cleanedLines.push(l);
  }

  return normalizeText(cleanedLines.join("\n"));
}

// ------------------------------
// ✅ Pricing extractor (IMPROVED)
// ------------------------------
function extractPricingFromPage(content: string, url: string, title: string) {
  const text = content || "";
  const lower = text.toLowerCase();

  const looksLikePricing =
    lower.includes("цена") ||
    lower.includes("цени") ||
    lower.includes("пакет") ||
    lower.includes("оферта") ||
    lower.includes("price") ||
    lower.includes("pricing") ||
    lower.includes("tariff") ||
    lower.includes("лв") ||
    lower.includes("лева") ||
    lower.includes("eur") ||
    lower.includes("евро") ||
    lower.includes("night") ||
    lower.includes("per person") ||
    lower.includes("на нощ") ||
    lower.includes("на човек");

  if (!looksLikePricing) return [];

  const results: any[] = [];

  // 1) structured currency formats
  const priceRegex =
    /(?:^|\s)(.{0,55}?)(\d{1,7}(?:[.,]\d{1,2})?)\s?(лв\.?|лева|bgn|евро|eur|€)(.{0,55}?)(?=\s|$)/gi;

  let match;
  while ((match = priceRegex.exec(text)) !== null) {
    const before = (match[1] || "").trim();
    const amountRaw = (match[2] || "").trim();
    const currencyRaw = (match[3] || "").trim().toLowerCase();
    const after = (match[4] || "").trim();

    const amount = amountRaw.replace(".", ",");
    const currency =
      currencyRaw.includes("лв") || currencyRaw.includes("лева") || currencyRaw.includes("bgn")
        ? "BGN"
        : "EUR";

    const label = normalizeText(`${before} ${after}`)
      .replace(/\s+/g, " ")
      .slice(0, 140)
      .trim();

    results.push({
      label: label || "Цена",
      amount,
      currency,
      source_url: url,
      source_title: title || "",
    });

    if (results.length >= 60) break;
  }

  // 2) “от … лв” style
  const fromRegex =
    /(пакет|оферта|room|стая|настаняване|нощувка|престой).{0,60}?(от)\s+(\d{1,7}(?:[.,]\d{1,2})?)\s?(лв|лева|евро|eur|€)/gi;

  while ((match = fromRegex.exec(text)) !== null) {
    const label = normalizeText(match[0]).slice(0, 140);
    const amount = String(match[3]).replace(".", ",");
    const c = String(match[4]).toLowerCase();
    const currency = c.includes("лв") || c.includes("лева") ? "BGN" : "EUR";

    results.push({
      label,
      amount,
      currency,
      source_url: url,
      source_title: title || "",
    });

    if (results.length >= 80) break;
  }

  return results;
}

// ------------------------------
// Crawler fetch
// ------------------------------
async function scrapeWithBrowserCrawler(url: string) {
  const crawlerUrl = Deno.env.get("BROWSER_CRAWLER_URL");
  const token = Deno.env.get("CRAWLER_TOKEN");

  if (!crawlerUrl) throw new Error("BROWSER_CRAWLER_URL not configured");
  if (!token) throw new Error("CRAWLER_TOKEN not configured");

  const resp = await fetch(crawlerUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      url,
      token,
      maxPages: MAX_PAGES,
    }),
  });

  if (!resp.ok) {
    const t = await resp.text();
    throw new Error(`Browser crawler error (${resp.status}): ${t}`);
  }

  const data = await safeJson(resp);
  if (!data?.success) throw new Error(data?.error || "Browser crawler failed");

  const pages: ScrapedPage[] = Array.isArray(data.pages) ? data.pages : [];

  const combinedContent = pages
    .map((p: ScrapedPage) => {
      const pageUrl = p.url || url;
      const raw = normalizeText(p.text || "");
      const cleaned = stripBoilerplate(raw);
      const finalContent = clamp(cleaned, MAX_CHARS_PER_PAGE);

      return {
        url: pageUrl,
        title: normalizeText(p.title || ""),
        description: "",
        content: finalContent,
        category: categorizeUrl(pageUrl),
      };
    })
    .filter((p) => !isUselessUrl(p.url))
    .filter((p) => (p.content || "").length >= MIN_PAGE_CHARS);

  // ✅ better weights (pricing/packages/rooms first)
  const categoryWeight: Record<string, number> = {
    pricing: 150,
    packages: 145,
    rooms: 135,
    booking: 120,
    services: 110,
    products: 100,
    contact: 80,
    faq: 70,
    about: 25,
    blog: 5,
    general: 10,
  };

  combinedContent.sort((a, b) => {
    const sa = (categoryWeight[a.category] || 10) + Math.min(a.content.length / 2000, 35);
    const sb = (categoryWeight[b.category] || 10) + Math.min(b.content.length / 2000, 35);
    return sb - sa;
  });

  // HARD cap
  const final: any[] = [];
  let acc = 0;

  for (const p of combinedContent) {
    if (final.length >= MAX_PAGES) break;
    if (acc >= MAX_TOTAL_CHARS) break;

    const remaining = MAX_TOTAL_CHARS - acc;
    const content = clamp(p.content || "", Math.max(0, remaining));
    if (content.length < MIN_PAGE_CHARS) continue;

    final.push({ ...p, content });
    acc += content.length;
  }

  return {
    pagesCount: final.length,
    combinedContent: final,
    totalChars: acc,
  };
}

// ------------------------------
// Summary generation (TOP PAGES ONLY)
// ------------------------------
async function generateSummaryWithGemini(
  combinedContent: any[],
  detectedLanguage: "bg" | "en",
  pagesCount: number,
  pricingItems: any[],
) {
  const GEMINI_API_KEY = Deno.env.get("GEMINI_API_KEY");
  if (!GEMINI_API_KEY) return "";

  const pricingBlock = pricingItems?.length
    ? `\n\n【PRICING_ITEMS (EXTRACTED)】\n${pricingItems
        .slice(0, 120)
        .map((x) => `- ${x.label}: ${x.amount} ${x.currency}`)
        .join("\n")}`
    : "\n\n【PRICING_ITEMS (EXTRACTED)】\n- None found";

  // ✅ only top pages sent to Gemini (CRITICAL!)
  const topPages = combinedContent.slice(0, MAX_AI_PAGES);

  const contentForAI = (
    pricingBlock +
    "\n\n" +
    topPages
      .map((p: any) => {
        return `【${String(p.category).toUpperCase()}: ${p.title || "Страница"}】
URL: ${p.url}
CONTENT:
${p.content}`;
      })
      .join("\n\n════════════════════════════════\n\n")
  ).slice(0, MAX_AI_CONTEXT_CHARS);

  const summaryPrompt =
    detectedLanguage === "bg"
      ? `Ти създаваш ПРОФЕСИОНАЛНА база знания за виртуален рецепционист.

⚠️ ПЪРВИ РЕД ЗАДЪЛЖИТЕЛНО:
COMPANY_NAME: [Име на компанията]

СТРОГИ ПРАВИЛА:
- Не измисляй. Само реални данни.
- Приоритет: цени, пакети, стаи/настаняване, резервации.
- Ако има PRICING_ITEMS — използвай ги.

ФОРМАТ:
1) Кратко описание (2-3 изречения)
2) Услуги/продукти
3) Пакети/оферти/стаи
4) Цени (списък)
5) Работно време
6) Адрес/локация
7) Контакти (тел/имейл)
8) Резервации / правила
9) FAQ

САЙТ СЪДЪРЖАНИЕ:
${contentForAI}`
      : `Build a receptionist knowledge base.

FIRST LINE MUST BE:
COMPANY_NAME: [Company Name]

Focus: pricing/packages/rooms/booking.

WEBSITE CONTENT:
${contentForAI}`;

  const aiResponse = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${GEMINI_API_KEY}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [{ role: "user", parts: [{ text: summaryPrompt }] }],
        generationConfig: {
          maxOutputTokens: 8000,
          temperature: 0.1,
        },
      }),
    },
  );

  if (!aiResponse.ok) return "";

  const aiData = await safeJson(aiResponse);
  return aiData?.candidates?.[0]?.content?.parts?.[0]?.text || "";
}

// ------------------------------
// HTTP handler
// ------------------------------
serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const { url, sessionId, sessionToken, action } = await req.json();

    if (action === "check-status") {
      return new Response(JSON.stringify({ success: true, status: "ready" }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    if (!url) throw new Error("URL is required");

    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const supabase = createClient(supabaseUrl, supabaseKey);

    if (sessionId && sessionToken) {
      const { data: sessionData, error: sessionError } = await supabase
        .from("demo_sessions")
        .select("id, session_token")
        .eq("id", sessionId)
        .eq("session_token", sessionToken)
        .single();

      if (sessionError || !sessionData) throw new Error("Invalid session token");
    }

    if (sessionId) {
      await supabase.from("demo_sessions").update({ status: "scraping" }).eq("id", sessionId);
    }

    // 1) scrape
    const { combinedContent, pagesCount, totalChars } = await scrapeWithBrowserCrawler(url);

    if (!pagesCount || pagesCount < 2) {
      throw new Error("Crawler returned too few pages (site may block scraping or JS not loaded)");
    }

    // 2) detect language
    const allText = combinedContent.map((p: any) => p.content).join(" ");
    const detectedLanguage = detectLanguage(allText) as "bg" | "en";

    // 3) pricing extraction
    const pricingItems: any[] = [];
    for (const p of combinedContent) {
      const items = extractPricingFromPage(p.content, p.url, p.title);
      for (const it of items) pricingItems.push(it);
      if (pricingItems.length >= 200) break;
    }

    const pricingDedup = new Map<string, any>();
    for (const it of pricingItems) {
      const key = `${(it.label || "").toLowerCase()}|${it.amount}|${it.currency}`;
      if (!pricingDedup.has(key)) pricingDedup.set(key, it);
    }
    const finalPricing = Array.from(pricingDedup.values()).slice(0, 160);

    const pricingText = finalPricing
      .slice(0, 80)
      .map((x) => `- ${x.label}: ${x.amount} ${x.currency}`)
      .join("\n");

    // 4) summary
    const summary = await generateSummaryWithGemini(combinedContent, detectedLanguage, pagesCount, finalPricing);

    // 5) company name extraction
    let companyName: string | null = null;
    if (summary) {
      const m = summary.match(/COMPANY_NAME:\s*(.+?)(?:\n|$)/i);
      if (m) companyName = m[1].trim();
    }

    // 6) save
    if (sessionId) {
      await supabase
        .from("demo_sessions")
        .update({
          status: "ready",
          url,
          scraped_content: combinedContent,
          summary,
          language: detectedLanguage,
          company_name: companyName,
          pricing_items: finalPricing,
          pricing_text: pricingText,
        })
        .eq("id", sessionId);
    }

    return new Response(
      JSON.stringify({
        success: true,
        status: "ready",
        pagesScraped: pagesCount,
        totalChars,
        language: detectedLanguage,
        summary,
        companyName,
        pricingItemsFound: finalPricing.length,
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } },
    );
  } catch (error) {
    console.error("❌ scrape-website error:", error);
    return new Response(
      JSON.stringify({
        success: false,
        error: error instanceof Error ? error.message : "Unknown error",
      }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } },
    );
  }
});
