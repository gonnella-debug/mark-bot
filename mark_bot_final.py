"""
Mark — AI Marketing Bot (Final)
Powered by Claude API | Playwright/Canva | Meta Graph API | LinkedIn API
Google Drive PDF Reader | Telegram | FastAPI (Alex integration)

Brands:
  - Nucassa Real Estate  → Instagram + Facebook + LinkedIn (@nucassadubai)
  - Nucassa Holdings     → Instagram + LinkedIn (@nucassaholdings_ltd)
  - ListR.ae             → Instagram only (@listr.ae)

Architecture:
  - GG never talks to Mark directly
  - Alex orchestrates Mark via internal FastAPI endpoints
  - Mark sends Telegram previews to GG for approval
  - GG approves/rejects via Telegram inline buttons only
"""

import os
import json
import logging
import asyncio
import httpx
import base64
import re
import uuid
import hashlib
import time
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse, RedirectResponse
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ── ENV ───────────────────────────────────────────────────────────────────────
CLAUDE_API_KEY      = os.getenv("CLAUDE_API_KEY")
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID")

# Meta
META_APP_ID         = os.getenv("META_APP_ID", "212164228622395")
META_APP_SECRET     = os.getenv("META_APP_SECRET", "c9592e63b3355f9e7e512223464681c0")
META_SYSTEM_TOKEN   = os.getenv("META_SYSTEM_TOKEN")

# LinkedIn
LI_CLIENT_ID        = os.getenv("LI_CLIENT_ID", "771btfsjdaf16f")
LI_CLIENT_SECRET    = os.getenv("LI_CLIENT_SECRET", "WPL_AP1.RbvoWqxW1b8AJXwr.8tkOrQ==")
LI_NUCASSA_RE_PAGE  = os.getenv("LI_NUCASSA_RE_PAGE", "90919312")
LI_HOLDINGS_PAGE    = os.getenv("LI_HOLDINGS_PAGE", "109941216")
LI_ACCESS_TOKEN     = os.getenv("LI_ACCESS_TOKEN", "")       # set after first OAuth
LI_REFRESH_TOKEN    = os.getenv("LI_REFRESH_TOKEN", "")
LI_TOKEN_EXPIRY     = int(os.getenv("LI_TOKEN_EXPIRY", "0"))

# Canva
CANVA_EMAIL         = os.getenv("CANVA_EMAIL", "gonnella@nu-propertygroup.com")
CANVA_PASSWORD      = os.getenv("CANVA_PASSWORD", "EmaarEmaar21?!?")

# Google Drive
GDRIVE_API_KEY        = os.getenv("GDRIVE_API_KEY")
GDRIVE_FOLDER_ID        = os.getenv("GDRIVE_FOLDER_ID", "1QoloKwEVPojBMfkTcSkbRL1ryo0a8jif")
GDRIVE_LISTR_REF_ID     = os.getenv("GDRIVE_LISTR_REF_ID", "1C7axRZjVVxP9TjCzGVxdRHdfICYieT1L")
GDRIVE_NUCASSA_REF_ID   = os.getenv("GDRIVE_NUCASSA_REF_ID", "10REvNFPKlF42_6HuuSRf7iBHf8CKAfWU")

# Google Drive — Mark Marketing folder (approved content saved here)
GDRIVE_MARKETING_FOLDER_ID = "1CJQsPFZqDuOTNkMWLx5C191uyo9bT_fj"
GDRIVE_MARKETING_BRAND_FOLDERS = {
    "nucassa_re": "1h9-rHbwy_u781I5JK9AfC9Ig4UGgJ9lV",
    "nucassa_holdings": "1jZXVpp4zxKD4my1CU2NBEerlH81iNpJ2",
    "listr": "1DTSCTvgN_nMR9Wn71vzGHphF8QKXfZbM",
}
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

# Instagram Account IDs (from Meta Business Manager)
IG_NUCASSA_RE       = "17841457839005074"   # @nucassadubai
IG_HOLDINGS         = "17841406888818689"   # @nucassaholdings_ltd
IG_LISTR            = "17841475489496432"   # @listr.ae

# Facebook Page IDs
FB_NUCASSA_RE       = "106173405736149"     # Nucassa Real Estate Dubai (only FB page Mark posts to)

# Railway public URL for LinkedIn OAuth callback
RAILWAY_URL         = os.getenv("RAILWAY_URL", "https://mark-bot.up.railway.app")

# ── BRAND CONFIG ──────────────────────────────────────────────────────────────
BRANDS = {
    "nucassa_re": {
        "name": "Nucassa Real Estate",
        "handle": "@nucassadubai",
        "website": "www.nucassa.com",
        "platforms": ["instagram", "facebook", "linkedin"],
        "ig_account_id": IG_NUCASSA_RE,
        "fb_page_id": FB_NUCASSA_RE,
        "li_page_id": LI_NUCASSA_RE_PAGE,
        "tone": "authoritative, data-led, facts about Dubai property market, lifestyle. Bold, confident, never salesy.",
        "cta": "DM us today",
        "color_primary": "#1C1C1C",
        "color_accent": "#CDA17F",
        "color_secondary": "#3b3b3b",
        "font_headline": "Montserrat SemiBold",
        "font_body": "Varta Medium",
        "logo_style": "rose_gold",
        "topics": [
            "Dubai property transaction volume — latest DLD data",
            "Area spotlight — Marina sold prices vs asking prices",
            "Why Dubai beats London and Singapore for yield",
            "Dubai population growth and housing demand",
            "Off-plan vs ready — what the data actually shows",
            "Rental yield by area — where the numbers are strongest",
            "Dubai infrastructure pipeline — what is being built",
            "Q1 2026 luxury market — the numbers that matter",
            "Zero income tax — what it really means for investors",
            "Dubai vs global real estate — a fact-based comparison",
            "Downtown Dubai — market snapshot",
            "Business Bay — who is buying and why",
            "Palm Jumeirah resale market — the reality",
            "Life in Dubai — cost of living vs quality of living",
        ],
        "posting_weekdays": [0, 2, 4],  # Mon Wed Fri
        "post_hour_utc": 14,             # 6pm Dubai
    },
    "nucassa_holdings": {
        "name": "Nucassa Holdings Ltd",
        "handle": "@nucassaholdings_ltd",
        "website": "www.nucassa.holdings",
        "platforms": ["instagram", "linkedin"],
        "ig_account_id": IG_HOLDINGS,
        "fb_page_id": None,
        "li_page_id": LI_HOLDINGS_PAGE,
        "tone": "institutional, precise, investor-grade. ADGM SPV, DBS custody, capital protection. Goldman Sachs meets Dubai.",
        "cta": "Message us to learn more",
        "color_primary": "#1C1C1C",
        "color_accent": "#CDA17F",
        "color_secondary": "#3b3b3b",
        "font_headline": "Montserrat SemiBold",
        "font_body": "Varta Medium",
        "logo_style": "rose_gold",
        "topics": [
            "Why ADGM gives investors real legal protection",
            "SPV ring-fencing — what it means in practice",
            "DBS custodial banking — dual authorisation explained",
            "Fixed income vs equity — which structure suits you",
            "The three-year investment cycle — how it works",
            "UAE economic stability vs global market volatility",
            "Dubai vs Switzerland for capital protection",
            "Why family offices are increasing UAE allocation",
            "Real estate as an asset class — 2026 data",
            "ADGM vs Cayman — structural comparison for investors",
            "Rental income as yield buffer — the mechanics",
            "Exit strategy — how we time asset realisation",
            "No performance fees — why that matters to investors",
            "Why $1M minimum creates a better investor community",
        ],
        "posting_weekdays": [0, 2, 4],  # Mon Wed Fri
        "post_hour_utc": 14,             # 6pm Dubai
    },
    "listr": {
        "name": "ListR.ae",
        "handle": "@listr.ae",
        "website": "ListR.ae",
        "platforms": ["instagram"],
        "ig_account_id": IG_LISTR,
        "fb_page_id": None,
        "li_page_id": None,
        "tone": "modern, direct, disruptive. Cutting unnecessary fees, empowering buyers and sellers. Sharp and confident.",
        "cta": "Sign up free at ListR.ae",
        "color_primary": "#000000",
        "color_accent": "#B8962E",
        "color_secondary": "#FFFFFF",
        "font_headline": "Montserrat SemiBold",
        "font_body": "Varta Medium",
        "logo_style": "listr",
        "topics": [
            "Why you should never pay 2% agency fees again",
            "How to buy direct from a seller in Dubai",
            "What agents actually do vs what you pay them",
            "The 90% commission — why agents love ListR",
            "Verified listings only — how we keep fakes out",
            "Off-plan resale explained simply",
            "How to list your property for free on ListR",
            "ListR vs Bayut vs Property Finder — the difference",
            "Seller managed vs full managed — which suits you",
            "How buyers save tens of thousands with ListR",
            "RERA — what it means and why it matters",
            "Dubai secondary market — buying from real owners",
            "Why agents prefer 90% commission over 50%",
            "ListR.ae — what we built and why",
        ],
        "posting_weekdays": [1, 3, 5],  # Tue Thu Sat
        "post_hour_utc": 14,             # 6pm Dubai
    },
}

# ── IN-MEMORY STATE ───────────────────────────────────────────────────────────
pending_approvals: dict = {}   # telegram_msg_id → {content, brand, batch_id, idx}
pending_batches: dict = {}     # batch_id → list[content_dict]
last_batch: dict = {}          # brand → list[content_dict]
li_oauth_states: dict = {}     # state → brand (for OAuth flow)

app = FastAPI()

# ── MARK SYSTEM PROMPT ────────────────────────────────────────────────────────
MARK_SYSTEM_PROMPT = """
You are Mark, the AI marketing brain for three Dubai real estate brands.

BRANDS:
1. Nucassa Real Estate (@nucassadubai) — www.nucassa.com
   Facts, Dubai property data, market stats, lifestyle. Bold and authoritative.
2. Nucassa Holdings Ltd (@nucassaholdings_ltd) — www.nucassa.holdings
   Institutional investment platform. ADGM SPV. $1M+ investors. DBS custody.
3. ListR.ae (@listr.ae) — ListR.ae
   UAE property marketplace. No agency fees. Direct buyer-seller deals.

NUCASSA VISUAL BRAND (RE + Holdings):
- Background: #1C1C1C primary, #3b3b3b secondary
- Accent: #CDA17F (rose gold) for stats, numbers, CTAs
- White for headlines and body on dark slides
- Montserrat SemiBold headlines, Varta Medium body
- Portrait 4:5 (1080x1350px)
- Logo: hexagonal NU mark — rose gold on dark bg, never rotated or distorted

LISTR VISUAL BRAND:
- Background: #000000
- Accent: #B8962E (gold)
- White wordmark "ListR" with capital R outlined in gold
- Location pin icon in gold above the L
- Same portrait 4:5 ratio

PROVEN CAROUSEL FORMULA — 3 slides, never deviate:
Slide 1 COVER: Bold hook headline over dark atmospheric Dubai photo. One punchy statement or question. Arrow → bottom centre.
Slide 2 DATA: 3 key stats. Large rose gold/gold numbers, white labels. Dark photo bg with logo watermark top-centre.
Slide 3 CTA: Pure #1C1C1C (or #000 for ListR) background. Short bold question. CTA line. Logo bottom-centre.

STATIC: Single powerful stat or statement. Dark atmospheric bg or solid dark. Logo watermark. Short punchy caption.

REELS SCRIPT (max 25 seconds):
Hook (0-3s): Shocking stat or question — stops the scroll
Body (3-20s): 3 rapid-fire facts, one per beat
CTA (20-25s): "Follow for more" + brand handle

STORY: Single screen. Bold text overlay. One stat or question. Brand logo. Swipe CTA.

CAPTION RULES:
- Instagram/Facebook: 3-5 lines max. 5-8 hashtags at end. Brand CTA always last.
- LinkedIn: 4-6 lines. Professional. No hashtags. Data-led. CTA last.
- Never use dashes. Numbers must be accurate and verifiable (DLD, CBRE, JLL, Dubai Statistics Centre).
- Always use correct website per brand — nucassa.com for RE, nucassa.holdings for Holdings, ListR.ae for ListR.

OUTPUT — valid JSON only, no prose:
{
  "brand": "nucassa_re|nucassa_holdings|listr",
  "content_type": "carousel|static|reels|story",
  "topic": "one line description",
  "slides": [
    {"slide": 1, "headline": "...", "subtext": "...", "photo_direction": "dark atmospheric Dubai skyline at dusk etc"},
    {"slide": 2, "stats": ["LABEL: VALUE", "LABEL: VALUE", "LABEL: VALUE"], "photo_direction": "..."},
    {"slide": 3, "headline": "...", "cta_line": "..."}
  ],
  "caption_instagram": "...",
  "caption_linkedin": "...",
  "hashtags": ["#Dubai", "#DubaiRealEstate"],
  "design_notes": "key visual direction for Canva"
}

For reels replace slides with:
  "script": {"hook": "...", "body": ["beat 1", "beat 2", "beat 3"], "cta": "..."}
For static use one slide only.
"""


# ── CLAUDE ────────────────────────────────────────────────────────────────────

async def call_claude(prompt: str, pdf_b64: str = None) -> str:
    content = []
    if pdf_b64:
        content.append({"type": "document", "source": {
            "type": "base64", "media_type": "application/pdf", "data": pdf_b64
        }})
    content.append({"type": "text", "text": prompt})
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": CLAUDE_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 2048,
                "system": MARK_SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": content}],
            },
        )
    return r.json()["content"][0]["text"]


def parse_json(raw: str) -> dict | None:
    try:
        m = re.search(r'\{[\s\S]*\}', raw)
        if m:
            return json.loads(m.group())
    except Exception:
        pass
    try:
        return json.loads(re.sub(r'```(?:json)?', '', raw).strip())
    except Exception as e:
        log.error(f"JSON parse failed: {e}")
    return None


async def generate_single(brand: str, content_type: str, topic: str = "", pdf_b64: str = None, pdf_name: str = "") -> dict | None:
    cfg = BRANDS[brand]
    if not topic and not pdf_b64:
        log.warning(f"generate_single called without topic for {brand} — Alex must provide the angle")
        return None

    async def _call_with_images(prompt: str, ref_images: list[str], extra_b64: str = None) -> str:
        """Call Claude with optional reference images and/or a PDF."""
        content_parts = []
        for img_b64 in ref_images:
            content_parts.append({
                "type": "image",
                "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64}
            })
        if extra_b64:
            content_parts.append({
                "type": "document",
                "source": {"type": "base64", "media_type": "application/pdf", "data": extra_b64}
            })
        content_parts.append({"type": "text", "text": prompt})
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": CLAUDE_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 2048,
                    "system": MARK_SYSTEM_PROMPT,
                    "messages": [{"role": "user", "content": content_parts}],
                },
            )
        return r.json()["content"][0]["text"]

    if pdf_b64:
        # PDF brochure post — fetch brand reference images to maintain style
        if brand == "listr":
            ref_images = await fetch_listr_reference_images()
        else:
            ref_images = await fetch_nucassa_reference_images()
        prompt = f"""You are looking at example posts from {cfg['name']}'s Instagram account.
Study the visual style, tone and layout carefully.
Now read the attached developer brochure and create ORIGINAL {content_type} content for {cfg['name']} using the brochure facts.
Do NOT reproduce brochure text — extract the facts and write original content in the brand's voice.
Brand tone: {cfg['tone']} | CTA: {cfg['cta']} | Website: {cfg['website']}
Return JSON only."""
        raw = await _call_with_images(prompt, ref_images, pdf_b64)

    elif brand == "listr":
        ref_images = await fetch_listr_reference_images()
        prompt = f"""You are looking at example posts from the ListR.ae Instagram account.
Study the visual style, tone, layout and content approach in these examples carefully.
Now create a NEW ORIGINAL {content_type} post for ListR.ae in the same style.
Topic: {topic}
Tone: {cfg['tone']} | CTA: {cfg['cta']} | Website: {cfg['website']}
Match the energy and style you see in the examples but make completely original content.
Return JSON only."""
        if ref_images:
            raw = await _call_with_images(prompt, ref_images)
        else:
            raw = await call_claude(prompt)

    elif brand in ("nucassa_re", "nucassa_holdings"):
        ref_images = await fetch_nucassa_reference_images()
        prompt = f"""You are looking at example posts from {cfg['name']}'s Instagram account.
Study the visual style, tone, layout and content approach in these examples carefully.
Now create a NEW ORIGINAL {content_type} post for {cfg['name']} in the same style.
Topic: {topic}
Tone: {cfg['tone']} | CTA: {cfg['cta']} | Handle: {cfg['handle']} | Website: {cfg['website']}
Use accurate verifiable Dubai real estate data. Match the style in the examples but make completely original content.
Return JSON only."""
        if ref_images:
            raw = await _call_with_images(prompt, ref_images)
        else:
            raw = await call_claude(prompt)

    else:
        prompt = f"""Create a {content_type} post for {cfg['name']}.
Topic: {topic}
Tone: {cfg['tone']} | CTA: {cfg['cta']} | Handle: {cfg['handle']} | Website: {cfg['website']}
Use accurate verifiable Dubai real estate data. Return JSON only."""
        raw = await call_claude(prompt)

    result = parse_json(raw)
    if result:
        result["_topic"] = topic
        result["_brand"] = brand
        result["_content_type"] = content_type
    return result


# ── BATCH GENERATION ──────────────────────────────────────────────────────────

async def generate_batch(brand: str, days: int = 14) -> list:
    cfg = BRANDS[brand]
    now = datetime.now(timezone.utc)
    slots = []
    for i in range(days):
        day = now + timedelta(days=i)
        if day.weekday() in cfg["posting_weekdays"]:
            slot = day.replace(hour=cfg["post_hour_utc"], minute=0, second=0, microsecond=0)
            if slot > now:
                slots.append(slot)

    type_rotation = ["carousel", "carousel", "static", "carousel", "reels", "carousel", "story"]
    import random
    topics = list(cfg["topics"])
    random.shuffle(topics)

    batch = []
    for idx, slot in enumerate(slots):
        ct = type_rotation[idx % len(type_rotation)]
        topic = topics[idx % len(topics)]
        content = await generate_single(brand, ct, topic)
        if content:
            content["_scheduled_at"] = slot.isoformat()
            content["_slot_label"] = slot.strftime("%a %d %b — %H:%M UTC")
            content["_approved"] = False
            content["_skipped"] = False
            batch.append(content)
        await asyncio.sleep(1.5)
    return batch


# ── CANVA VIA PLAYWRIGHT ──────────────────────────────────────────────────────

async def create_canva_slide(content: dict, slide_index: int, brand: str) -> bytes | None:
    """
    Open Canva in a headless browser, create a design from scratch matching
    Nucassa/ListR brand guidelines, export as PNG bytes.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("Playwright not installed — run: pip install playwright && playwright install chromium")
        return None

    cfg = BRANDS[brand]
    slides = content.get("slides", [])
    if slide_index >= len(slides):
        return None
    slide = slides[slide_index]

    # Build the slide HTML that matches brand exactly
    html = _build_slide_html(slide, slide_index, cfg, content)

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(viewport={"width": 1080, "height": 1350})
            await page.set_content(html, wait_until="networkidle")
            await asyncio.sleep(1)  # let fonts render
            screenshot = await page.screenshot(type="png", full_page=False)
            await browser.close()
            return screenshot
    except Exception as e:
        log.error(f"Canva/Playwright error: {e}")
        return None


def _build_slide_html(slide: dict, index: int, cfg: dict, content: dict) -> str:
    """Generate brand-accurate HTML for a slide — rendered to PNG by Playwright."""
    primary = cfg["color_primary"]
    accent = cfg["color_accent"]
    secondary = cfg.get("color_secondary", "#3b3b3b")
    is_listr = cfg["name"] == "ListR.ae"

    if index == 0:
        # COVER SLIDE
        headline = slide.get("headline", "")
        subtext = slide.get("subtext", "")
        photo_dir = slide.get("photo_direction", "Dubai skyline at dusk")
        return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@600;700&family=Varta:wght@400;500&display=swap" rel="stylesheet">
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
  width: 1080px; height: 1350px; overflow: hidden;
  background: {primary};
  font-family: 'Montserrat', sans-serif;
  position: relative;
  display: flex; flex-direction: column; justify-content: flex-end;
}}
.bg-overlay {{
  position: absolute; inset: 0;
  background: linear-gradient(180deg, rgba(0,0,0,0.2) 0%, rgba(0,0,0,0.7) 60%, rgba(0,0,0,0.9) 100%);
  z-index: 1;
}}
.bg-text {{
  position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%);
  font-size: 11px; color: rgba(255,255,255,0.15); text-align: center;
  font-family: 'Varta', sans-serif; z-index: 0; max-width: 900px;
  letter-spacing: 2px; text-transform: uppercase;
}}
.logo-watermark {{
  position: absolute; top: 60px; right: 60px; z-index: 10;
  display: flex; flex-direction: column; align-items: center; gap: 8px;
}}
.logo-icon {{
  width: 64px; height: 64px;
  border: 2px solid {accent}; border-radius: 8px;
  display: flex; align-items: center; justify-content: center;
}}
.logo-text {{
  font-family: 'Montserrat', sans-serif; font-weight: 600;
  font-size: 14px; color: {accent}; letter-spacing: 3px;
}}
.content {{
  position: relative; z-index: 2;
  padding: 0 80px 100px 80px;
}}
.headline {{
  font-family: 'Montserrat', sans-serif; font-weight: 700;
  font-size: 72px; line-height: 1.1;
  color: #FFFFFF; margin-bottom: 24px;
  text-transform: uppercase; letter-spacing: -1px;
}}
.headline span {{ color: {accent}; }}
.subtext {{
  font-family: 'Varta', sans-serif; font-weight: 400;
  font-size: 28px; color: rgba(255,255,255,0.8);
  line-height: 1.4; margin-bottom: 48px;
}}
.arrow-icon {{
  display: inline-flex; align-items: center; justify-content: center;
  width: 56px; height: 56px;
  border: 2px solid {accent}; border-radius: 50%;
  color: {accent}; font-size: 28px;
}}
</style>
</head>
<body>
<div class="bg-text">PHOTO: {photo_dir.upper()}</div>
<div class="bg-overlay"></div>
<div class="logo-watermark">
  <div class="logo-icon">
    <svg width="36" height="36" viewBox="0 0 36 36" fill="none">
      <path d="M8 28V8L18 20L28 8V28" stroke="{accent}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>
    </svg>
  </div>
  <div class="logo-text">{'LISTR.AE' if is_listr else 'NUCASSA'}</div>
</div>
<div class="content">
  <div class="headline">{headline}</div>
  <div class="subtext">{subtext}</div>
  <div class="arrow-icon">→</div>
</div>
</body>
</html>"""

    elif index == 1:
        # DATA SLIDE
        stats = slide.get("stats", ["—", "—", "—"])
        while len(stats) < 3:
            stats.append("—")
        def split_stat(s):
            parts = s.split(":", 1)
            return (parts[1].strip(), parts[0].strip()) if len(parts) == 2 else (s, "")

        s1v, s1l = split_stat(stats[0])
        s2v, s2l = split_stat(stats[1])
        s3v, s3l = split_stat(stats[2])

        return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@600;700&family=Varta:wght@400;500&display=swap" rel="stylesheet">
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
  width: 1080px; height: 1350px; overflow: hidden;
  background: {secondary};
  font-family: 'Montserrat', sans-serif;
  position: relative;
  display: flex; flex-direction: column; align-items: center; justify-content: center;
}}
.logo-watermark {{
  position: absolute; top: 60px; left: 50%; transform: translateX(-50%);
  display: flex; flex-direction: column; align-items: center; gap: 6px;
  opacity: 0.4;
}}
.logo-text {{
  font-family: 'Montserrat', sans-serif; font-weight: 600;
  font-size: 13px; color: {accent}; letter-spacing: 4px;
}}
.stats-grid {{
  display: flex; flex-direction: column; gap: 80px;
  align-items: center; width: 100%;
  padding: 160px 80px 80px;
}}
.stat-item {{ text-align: center; }}
.stat-value {{
  font-family: 'Montserrat', sans-serif; font-weight: 700;
  font-size: 110px; line-height: 1;
  color: {accent}; letter-spacing: -3px;
}}
.stat-label {{
  font-family: 'Varta', sans-serif; font-weight: 500;
  font-size: 26px; color: #FFFFFF;
  text-transform: uppercase; letter-spacing: 4px;
  margin-top: 12px;
}}
.divider {{
  width: 120px; height: 1px;
  background: rgba(205, 161, 127, 0.3);
}}
</style>
</head>
<body>
<div class="logo-watermark">
  <svg width="28" height="28" viewBox="0 0 36 36" fill="none">
    <path d="M8 28V8L18 20L28 8V28" stroke="{accent}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>
  </svg>
  <div class="logo-text">{'LISTR.AE' if is_listr else 'NUCASSA'}</div>
</div>
<div class="stats-grid">
  <div class="stat-item">
    <div class="stat-value">{s1v}</div>
    <div class="stat-label">{s1l}</div>
  </div>
  <div class="divider"></div>
  <div class="stat-item">
    <div class="stat-value">{s2v}</div>
    <div class="stat-label">{s2l}</div>
  </div>
  <div class="divider"></div>
  <div class="stat-item">
    <div class="stat-value">{s3v}</div>
    <div class="stat-label">{s3l}</div>
  </div>
</div>
</body>
</html>"""

    else:
        # CTA SLIDE
        headline = slide.get("headline", "")
        cta_line = slide.get("cta_line", cfg["cta"])
        website = cfg["website"]
        return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@600;700&family=Varta:wght@400;500&display=swap" rel="stylesheet">
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
  width: 1080px; height: 1350px; overflow: hidden;
  background: {primary};
  font-family: 'Montserrat', sans-serif;
  display: flex; flex-direction: column;
  align-items: center; justify-content: center;
  text-align: center; padding: 80px;
}}
.headline {{
  font-family: 'Montserrat', sans-serif; font-weight: 700;
  font-size: 64px; line-height: 1.15;
  color: #FFFFFF; text-transform: uppercase;
  letter-spacing: -1px; margin-bottom: 40px;
}}
.cta-line {{
  font-family: 'Varta', sans-serif; font-weight: 500;
  font-size: 32px; color: {accent};
  margin-bottom: 80px; letter-spacing: 1px;
}}
.logo-block {{
  display: flex; flex-direction: column;
  align-items: center; gap: 16px;
  margin-top: 40px;
}}
.logo-icon {{
  width: 90px; height: 90px;
  border: 2.5px solid {accent}; border-radius: 12px;
  display: flex; align-items: center; justify-content: center;
}}
.logo-name {{
  font-family: 'Montserrat', sans-serif; font-weight: 600;
  font-size: 22px; color: {accent}; letter-spacing: 5px;
}}
.website {{
  font-family: 'Varta', sans-serif; font-weight: 400;
  font-size: 18px; color: rgba(255,255,255,0.4);
  margin-top: 8px; letter-spacing: 2px;
}}
</style>
</head>
<body>
<div class="headline">{headline}</div>
<div class="cta-line">{cta_line}</div>
<div class="logo-block">
  <div class="logo-icon">
    <svg width="52" height="52" viewBox="0 0 36 36" fill="none">
      <path d="M8 28V8L18 20L28 8V28" stroke="{accent}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>
    </svg>
  </div>
  <div class="logo-name">{'LISTR.AE' if is_listr else 'NUCASSA'}</div>
  <div class="website">{website}</div>
</div>
</body>
</html>"""


async def render_carousel_images(content: dict, brand: str) -> list[bytes]:
    """Render all 3 carousel slides and return list of PNG bytes."""
    images = []
    slides = content.get("slides", [])
    for i in range(len(slides)):
        img = await create_canva_slide(content, i, brand)
        if img:
            images.append(img)
        await asyncio.sleep(0.5)
    return images


async def render_static_image(content: dict, brand: str) -> bytes | None:
    """Render a single static post image."""
    return await create_canva_slide(content, 0, brand)


# ── META GRAPH API ────────────────────────────────────────────────────────────

async def get_page_token(page_id: str) -> str | None:
    """Get a Page Access Token for a specific Facebook Page."""
    url = f"https://graph.facebook.com/v18.0/{page_id}"
    params = {"fields": "access_token", "access_token": META_SYSTEM_TOKEN}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, params=params)
            return r.json().get("access_token")
    except Exception as e:
        log.error(f"Page token error: {e}")
    return None


async def upload_image_to_ig(ig_account_id: str, image_bytes: bytes, caption: str, is_carousel_item: bool = False) -> str | None:
    """Upload image to Instagram as a container. Returns container ID."""
    # Upload image bytes to a temporary hosting solution
    # Instagram requires a public URL — we use the image upload endpoint
    upload_url = f"https://graph.facebook.com/v18.0/{ig_account_id}/media"

    # Encode image as base64 data URL isn't supported — need public URL
    # Upload to Facebook's CDN first via the media upload endpoint
    params = {
        "access_token": META_SYSTEM_TOKEN,
        "caption": caption if not is_carousel_item else "",
        "is_carousel_item": "true" if is_carousel_item else "false",
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            # Post image bytes directly
            r = await client.post(
                upload_url,
                params=params,
                files={"image": ("slide.png", image_bytes, "image/png")},
            )
            data = r.json()
            if "id" in data:
                return data["id"]
            log.error(f"IG upload error: {data}")
    except Exception as e:
        log.error(f"IG upload exception: {e}")
    return None


async def publish_ig_carousel(ig_account_id: str, container_ids: list[str], caption: str) -> dict:
    """Publish a carousel post from multiple container IDs."""
    # Create carousel container
    carousel_url = f"https://graph.facebook.com/v18.0/{ig_account_id}/media"
    carousel_params = {
        "access_token": META_SYSTEM_TOKEN,
        "media_type": "CAROUSEL",
        "caption": caption,
        "children": ",".join(container_ids),
    }
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(carousel_url, params=carousel_params)
            carousel_id = r.json().get("id")
            if not carousel_id:
                return {"error": r.json()}
            # Publish
            pub_url = f"https://graph.facebook.com/v18.0/{ig_account_id}/media_publish"
            pub_params = {"access_token": META_SYSTEM_TOKEN, "creation_id": carousel_id}
            r2 = await client.post(pub_url, params=pub_params)
            return r2.json()
    except Exception as e:
        log.error(f"Carousel publish error: {e}")
        return {"error": str(e)}


async def publish_ig_single(ig_account_id: str, image_bytes: bytes, caption: str) -> dict:
    """Publish a single image post to Instagram."""
    container_id = await upload_image_to_ig(ig_account_id, image_bytes, caption)
    if not container_id:
        return {"error": "Upload failed"}
    pub_url = f"https://graph.facebook.com/v18.0/{ig_account_id}/media_publish"
    params = {"access_token": META_SYSTEM_TOKEN, "creation_id": container_id}
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(pub_url, params=params)
            return r.json()
    except Exception as e:
        return {"error": str(e)}


async def publish_facebook(page_id: str, image_bytes: bytes, caption: str) -> dict:
    """Post an image to a Facebook Page."""
    page_token = await get_page_token(page_id)
    if not page_token:
        return {"error": "Could not get page token"}
    url = f"https://graph.facebook.com/v18.0/{page_id}/photos"
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                url,
                params={"access_token": page_token, "caption": caption},
                files={"source": ("post.png", image_bytes, "image/png")},
            )
            return r.json()
    except Exception as e:
        return {"error": str(e)}


# ── LINKEDIN API ──────────────────────────────────────────────────────────────

@app.get("/linkedin/auth")
async def linkedin_auth_start():
    """Start LinkedIn OAuth flow. Visit this URL in browser to authenticate."""
    state = str(uuid.uuid4())
    li_oauth_states[state] = "mark"
    scope = "w_member_social,r_organization_social,w_organization_social"
    auth_url = (
        f"https://www.linkedin.com/oauth/v2/authorization"
        f"?response_type=code"
        f"&client_id={LI_CLIENT_ID}"
        f"&redirect_uri={RAILWAY_URL}/linkedin/callback"
        f"&state={state}"
        f"&scope={scope}"
    )
    return RedirectResponse(auth_url)


@app.get("/linkedin/callback")
async def linkedin_auth_callback(code: str = None, state: str = None, error: str = None):
    """Handle LinkedIn OAuth callback and store access token."""
    global LI_ACCESS_TOKEN, LI_REFRESH_TOKEN, LI_TOKEN_EXPIRY

    if error:
        await send_telegram(f"❌ LinkedIn auth failed: {error}")
        return JSONResponse({"error": error})

    if state not in li_oauth_states:
        return JSONResponse({"error": "Invalid state"})

    # Exchange code for token
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                "https://www.linkedin.com/oauth/v2/accessToken",
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": f"{RAILWAY_URL}/linkedin/callback",
                    "client_id": LI_CLIENT_ID,
                    "client_secret": LI_CLIENT_SECRET,
                },
            )
            data = r.json()
            LI_ACCESS_TOKEN = data.get("access_token", "")
            LI_REFRESH_TOKEN = data.get("refresh_token", "")
            LI_TOKEN_EXPIRY = int(time.time()) + data.get("expires_in", 5184000)

        li_oauth_states.pop(state, None)
        await send_telegram(
            f"✅ *LinkedIn connected successfully*\n"
            f"Token valid for {data.get('expires_in', 0) // 86400} days.\n"
            f"Mark can now post to Nucassa RE and Holdings on LinkedIn."
        )
        return JSONResponse({"status": "LinkedIn connected successfully"})
    except Exception as e:
        await send_telegram(f"❌ LinkedIn token exchange failed: {e}")
        return JSONResponse({"error": str(e)})


async def upload_image_to_li(image_bytes: bytes) -> str | None:
    """Upload image to LinkedIn and return asset URN."""
    if not LI_ACCESS_TOKEN:
        return None
    # Register upload
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.linkedin.com/v2/assets?action=registerUpload",
                headers={"Authorization": f"Bearer {LI_ACCESS_TOKEN}", "Content-Type": "application/json"},
                json={
                    "registerUploadRequest": {
                        "recipes": ["urn:li:digitalmediaRecipe:feedshare-image"],
                        "owner": "urn:li:organization:90919312",
                        "serviceRelationships": [{"relationshipType": "OWNER", "identifier": "urn:li:userGeneratedContent"}],
                    }
                },
            )
            data = r.json()
            upload_url = data["value"]["uploadMechanism"]["com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest"]["uploadUrl"]
            asset = data["value"]["asset"]

            # Upload image
            r2 = await client.put(
                upload_url,
                headers={"Authorization": f"Bearer {LI_ACCESS_TOKEN}"},
                content=image_bytes,
            )
            if r2.status_code in (200, 201):
                return asset
    except Exception as e:
        log.error(f"LI image upload error: {e}")
    return None


async def publish_linkedin(page_id: str, image_bytes: bytes, caption: str) -> dict:
    """Post to a LinkedIn Organization Page."""
    if not LI_ACCESS_TOKEN:
        return {"error": "LinkedIn not authenticated — visit /linkedin/auth"}

    asset = await upload_image_to_li(image_bytes)
    if not asset:
        # Post text-only if image upload fails
        asset = None

    post_body = {
        "author": f"urn:li:organization:{page_id}",
        "lifecycleState": "PUBLISHED",
        "specificContent": {
            "com.linkedin.ugc.ShareContent": {
                "shareCommentary": {"text": caption},
                "shareMediaCategory": "IMAGE" if asset else "NONE",
            }
        },
        "visibility": {"com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"},
    }
    if asset:
        post_body["specificContent"]["com.linkedin.ugc.ShareContent"]["media"] = [{
            "status": "READY",
            "description": {"text": caption[:200]},
            "media": asset,
            "title": {"text": ""},
        }]

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.linkedin.com/v2/ugcPosts",
                headers={
                    "Authorization": f"Bearer {LI_ACCESS_TOKEN}",
                    "Content-Type": "application/json",
                    "X-Restli-Protocol-Version": "2.0.0",
                },
                json=post_body,
            )
            return r.json()
    except Exception as e:
        return {"error": str(e)}


# ── POST CONTENT ──────────────────────────────────────────────────────────────

async def post_content(content: dict, brand: str) -> dict:
    """Render images and post to all configured platforms for this brand."""
    cfg = BRANDS[brand]
    results = {}
    ct = content.get("content_type", content.get("_content_type", "carousel"))
    cap_ig = content.get("caption_instagram", "")
    cap_li = content.get("caption_linkedin", "")
    tags = " ".join(content.get("hashtags", []))
    ig_caption = f"{cap_ig}\n\n{tags}" if tags else cap_ig

    ig_account_id = cfg["ig_account_id"]
    fb_page_id = cfg["fb_page_id"]
    li_page_id = cfg["li_page_id"]

    # Use cached renders if available, otherwise render fresh
    cached = content.pop("_cached_images", None)
    if cached:
        log.info(f"Using cached rendered images for {brand} — {ct}")
        images = cached
    else:
        log.info(f"Rendering images for {brand} — {ct}")
        if ct == "carousel":
            images = await render_carousel_images(content, brand)
        elif ct in ("static", "story"):
            img = await render_static_image(content, brand)
            images = [img] if img else []
        else:
            images = []  # reels = text/script only

    if not images and ct not in ("reels",):
        log.warning(f"No images rendered for {brand} — posting may fail")

    # ── INSTAGRAM ──
    if "instagram" in cfg["platforms"] and ig_account_id:
        if images:
            if ct == "carousel" and len(images) >= 2:
                container_ids = []
                for img in images:
                    cid = await upload_image_to_ig(ig_account_id, img, "", is_carousel_item=True)
                    if cid:
                        container_ids.append(cid)
                    await asyncio.sleep(1)
                if container_ids:
                    results["instagram"] = await publish_ig_carousel(ig_account_id, container_ids, ig_caption)
                else:
                    results["instagram"] = {"error": "No containers uploaded"}
            else:
                results["instagram"] = await publish_ig_single(ig_account_id, images[0], ig_caption)
        else:
            results["instagram"] = {"error": "No image rendered"}

    # ── FACEBOOK (Nucassa RE only) ──
    if "facebook" in cfg["platforms"] and fb_page_id and images:
        results["facebook"] = await publish_facebook(fb_page_id, images[0], ig_caption)

    # ── LINKEDIN ──
    if "linkedin" in cfg["platforms"] and li_page_id:
        if not LI_ACCESS_TOKEN:
            results["linkedin"] = {"error": f"LinkedIn not authenticated — visit {RAILWAY_URL}/linkedin/auth"}
        elif images:
            results["linkedin"] = await publish_linkedin(li_page_id, images[0], cap_li)
        else:
            results["linkedin"] = {"error": "No image to post to LinkedIn"}

    # Save approved content to Google Drive Mark Marketing folder
    if images:
        cap_text = f"Instagram:\n{ig_caption}\n\nLinkedIn:\n{cap_li}" if cap_li else ig_caption
        drive_ok = await save_to_drive(brand, images, cap_text, ct)
        results["drive_saved"] = drive_ok
        if drive_ok:
            log.info(f"Saved {brand} content to Mark Marketing Drive folder")
        else:
            log.warning(f"Failed to save {brand} content to Drive")

    log.info(f"Post results for {brand}: {results}")
    return results


# ── GOOGLE DRIVE ──────────────────────────────────────────────────────────────

async def list_drive_files(folder_id: str, mime_filter: str = None) -> list:
    """List files in any Drive folder."""
    if not GDRIVE_API_KEY:
        return []
    q = f"'{folder_id}' in parents and trashed=false"
    if mime_filter:
        q += f" and mimeType='{mime_filter}'"
    params = {"q": q, "fields": "files(id,name,mimeType)", "key": GDRIVE_API_KEY, "pageSize": 20}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get("https://www.googleapis.com/drive/v3/files", params=params)
            return r.json().get("files", [])
    except Exception as e:
        log.error(f"Drive list error: {e}")
    return []


async def _collect_images_recursive(folder_id: str, max_depth: int = 3, _depth: int = 0) -> list:
    """Recursively search Drive subfolders for image files, up to max_depth levels."""
    if _depth > max_depth:
        return []
    files = await list_drive_files(folder_id)
    images = []
    subfolder_ids = []
    for f in files:
        mime = f.get("mimeType", "")
        if "image" in mime:
            images.append(f)
        elif mime == "application/vnd.google-apps.folder":
            subfolder_ids.append(f["id"])
    for sub_id in subfolder_ids:
        images.extend(await _collect_images_recursive(sub_id, max_depth, _depth + 1))
    return images


async def fetch_listr_reference_images() -> list[str]:
    """Fetch up to 3 ListR example images from Drive as base64 (searches subfolders)."""
    image_files = await _collect_images_recursive(GDRIVE_LISTR_REF_ID)
    image_files = image_files[-3:]  # most recent (last returned by API)
    return await _fetch_images_as_b64(image_files)


async def fetch_nucassa_reference_images() -> list[str]:
    """Fetch up to 3 Nucassa example images from Drive as base64 (searches subfolders)."""
    image_files = await _collect_images_recursive(GDRIVE_NUCASSA_REF_ID)
    image_files = image_files[-3:]  # most recent (last returned by API)
    return await _fetch_images_as_b64(image_files)


async def _fetch_images_as_b64(image_files: list) -> list[str]:
    """Download a list of Drive image files and return as base64 strings."""
    images_b64 = []
    for f in image_files:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.get(
                    f"https://www.googleapis.com/drive/v3/files/{f['id']}",
                    params={"alt": "media", "key": GDRIVE_API_KEY},
                )
                if r.status_code == 200:
                    images_b64.append(base64.b64encode(r.content).decode("utf-8"))
        except Exception as e:
            log.error(f"Reference image fetch error: {e}")
    log.info(f"Fetched {len(images_b64)} reference images from Drive")
    return images_b64
    if not GDRIVE_API_KEY:
        return []
    params = {
        "q": f"'{GDRIVE_FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false",
        "fields": "files(id,name)", "key": GDRIVE_API_KEY, "pageSize": 50,
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get("https://www.googleapis.com/drive/v3/files", params=params)
            return r.json().get("files", [])
    except Exception as e:
        log.error(f"Drive list error: {e}")
    return []


async def fetch_pdf_b64(file_id: str) -> str | None:
    if not GDRIVE_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(
                f"https://www.googleapis.com/drive/v3/files/{file_id}",
                params={"alt": "media", "key": GDRIVE_API_KEY},
            )
            if r.status_code == 200:
                return base64.b64encode(r.content).decode("utf-8")
    except Exception as e:
        log.error(f"Drive fetch error: {e}")
    return None


# ── GOOGLE DRIVE UPLOAD (Service Account) ────────────────────────────────────

_drive_token_cache: dict = {"token": None, "expires": 0}

async def _get_drive_upload_token() -> str | None:
    """Get an OAuth2 token from the service account for Drive uploads."""
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        log.warning("GOOGLE_SERVICE_ACCOUNT_JSON not set — cannot upload to Drive")
        return None
    now = int(time.time())
    if _drive_token_cache["token"] and _drive_token_cache["expires"] > now + 60:
        return _drive_token_cache["token"]
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
        sa = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        header = base64.urlsafe_b64encode(json.dumps({"alg": "RS256", "typ": "JWT"}).encode()).rstrip(b"=")
        claims = {
            "iss": sa["client_email"],
            "scope": "https://www.googleapis.com/auth/drive.file",
            "aud": sa["token_uri"],
            "iat": now,
            "exp": now + 3600,
        }
        payload_b = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=")
        signing_input = header + b"." + payload_b
        key = serialization.load_pem_private_key(sa["private_key"].encode(), password=None)
        sig = key.sign(signing_input, asym_padding.PKCS1v15(), hashes.SHA256())
        signature = base64.urlsafe_b64encode(sig).rstrip(b"=")
        jwt_token = (signing_input + b"." + signature).decode()
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(sa["token_uri"], data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": jwt_token,
            })
            tok = r.json()
            _drive_token_cache["token"] = tok["access_token"]
            _drive_token_cache["expires"] = now + tok.get("expires_in", 3600)
            return _drive_token_cache["token"]
    except Exception as e:
        log.error(f"Drive service account auth error: {e}")
        return None


async def save_to_drive(brand: str, images: list[bytes], caption: str, content_type: str = "carousel") -> bool:
    """Save rendered images + caption to the brand's Mark Marketing subfolder."""
    token = await _get_drive_upload_token()
    if not token:
        return False
    folder_id = GDRIVE_MARKETING_BRAND_FOLDERS.get(brand)
    if not folder_id:
        log.error(f"No Drive folder mapped for brand: {brand}")
        return False
    brand_name = BRANDS[brand]["name"]
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")
    saved = 0
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(timeout=30) as client:
        for i, img_bytes in enumerate(images):
            fname = f"{brand_name} — {content_type} — {ts} — slide{i+1}.png"
            metadata = json.dumps({"name": fname, "parents": [folder_id]})
            boundary = "mark_upload_boundary"
            body = (
                f"--{boundary}\r\n"
                f"Content-Type: application/json; charset=UTF-8\r\n\r\n"
                f"{metadata}\r\n"
                f"--{boundary}\r\n"
                f"Content-Type: image/png\r\n\r\n"
            ).encode() + img_bytes + f"\r\n--{boundary}--\r\n".encode()
            try:
                r = await client.post(
                    "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
                    headers={**headers, "Content-Type": f"multipart/related; boundary={boundary}"},
                    content=body,
                )
                if r.status_code in (200, 201):
                    saved += 1
                else:
                    log.error(f"Drive upload failed ({r.status_code}): {r.text[:200]}")
            except Exception as e:
                log.error(f"Drive upload error: {e}")
        # Save caption as text file
        cap_fname = f"{brand_name} — {content_type} — {ts} — caption.txt"
        cap_meta = json.dumps({"name": cap_fname, "parents": [folder_id]})
        cap_boundary = "mark_cap_boundary"
        cap_body = (
            f"--{cap_boundary}\r\n"
            f"Content-Type: application/json; charset=UTF-8\r\n\r\n"
            f"{cap_meta}\r\n"
            f"--{cap_boundary}\r\n"
            f"Content-Type: text/plain; charset=UTF-8\r\n\r\n"
            f"{caption}\r\n"
            f"--{cap_boundary}--\r\n"
        ).encode()
        try:
            r = await client.post(
                "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
                headers={**headers, "Content-Type": f"multipart/related; boundary={cap_boundary}"},
                content=cap_body,
            )
            if r.status_code in (200, 201):
                saved += 1
        except Exception as e:
            log.error(f"Drive caption upload error: {e}")
    log.info(f"Saved {saved} files to Drive for {brand_name}")
    return saved > 0


# ── TELEGRAM ──────────────────────────────────────────────────────────────────

async def send_telegram(message: str, reply_markup: dict = None) -> dict:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    last = {}
    for i, chunk in enumerate([message[i:i+4000] for i in range(0, len(message), 4000)]):
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": chunk, "parse_mode": "Markdown"}
        if reply_markup and i == 0:
            payload["reply_markup"] = json.dumps(reply_markup)
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post(url, json=payload)
                last = r.json()
        except Exception as e:
            log.error(f"Telegram error: {e}")
    return last


async def answer_callback(cb_id: str, text: str = ""):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(url, json={"callback_query_id": cb_id, "text": text})


async def get_updates(offset=None) -> list:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    params = {"timeout": 30, "allowed_updates": ["message", "callback_query"]}
    if offset:
        params["offset"] = offset
    try:
        async with httpx.AsyncClient(timeout=35) as client:
            r = await client.get(url, params=params)
            return r.json().get("result", [])
    except:
        return []


def format_preview(content: dict, brand: str, idx: int = None, total: int = None) -> str:
    cfg = BRANDS[brand]
    ct = content.get("content_type", "post").upper()
    topic = content.get("_topic", content.get("topic", ""))
    slot = content.get("_slot_label", "")
    breaking = content.get("_breaking", False)

    header = "⚡ *BREAKING NEWS POST*" if breaking else "📋 *Mark — Content Preview*"
    counter = f" ({idx}/{total})" if idx is not None and total else ""

    lines = [f"{header}{counter}", f"Brand: *{cfg['name']}*  |  Type: *{ct}*", f"Topic: _{topic}_"]
    if slot:
        lines.append(f"Scheduled: `{slot}`")
    lines.append("")

    if ct == "REELS":
        s = content.get("script", {})
        lines += [
            "*🎬 Script:*",
            f"Hook: {s.get('hook', '')}",
            f"Body: {' | '.join(s.get('body', []))}",
            f"CTA: {s.get('cta', '')}",
        ]
    else:
        for slide in content.get("slides", []):
            n = slide.get("slide", "")
            lines.append(f"*Slide {n}:*")
            if slide.get("headline"):
                lines.append(f"  {slide['headline']}")
            if slide.get("subtext"):
                lines.append(f"  _{slide['subtext']}_")
            if slide.get("stats"):
                for s in slide["stats"]:
                    lines.append(f"  • {s}")
            if slide.get("cta_line"):
                lines.append(f"  CTA: {slide['cta_line']}")
            if slide.get("photo_direction"):
                lines.append(f"  _Photo: {slide['photo_direction']}_")
            lines.append("")

    cap = content.get("caption_instagram", "")
    tags = " ".join(content.get("hashtags", []))
    notes = content.get("design_notes", "")

    lines.append(f"*Caption:*\n{cap}")
    if tags:
        lines.append(f"\n*Tags:* {tags}")
    if notes:
        lines.append(f"\n*Design:* {notes}")

    return "\n".join(lines)


async def send_approval_request(content: dict, brand: str, batch_id: str = None, idx: int = None, total: int = None) -> int | None:
    preview = format_preview(content, brand, idx, total)
    breaking = content.get("_breaking", False)

    if breaking:
        markup = {"inline_keyboard": [
            [{"text": "✅ Approve", "callback_data": f"approve_single|{brand}"},
             {"text": "❌ Reject", "callback_data": f"reject_single|{brand}"}],
            [{"text": "🔄 Regenerate", "callback_data": f"regen_single|{brand}|{batch_id or ''}|{idx or ''}"}],
        ]}
    else:
        markup = {"inline_keyboard": [
            [{"text": "✅ Approve", "callback_data": f"approve_one|{brand}|{batch_id}|{idx}"},
             {"text": "⏭ Skip", "callback_data": f"skip_one|{brand}|{batch_id}|{idx}"}],
            [{"text": "🔄 Regenerate", "callback_data": f"regen_one|{brand}|{batch_id}|{idx}"}],
        ]}

    result = await send_telegram(preview, reply_markup=markup)
    msg_id = result.get("result", {}).get("message_id")
    if msg_id:
        pending_approvals[msg_id] = {"content": content, "brand": brand, "batch_id": batch_id, "idx": idx}
    return msg_id


# ── CALLBACK HANDLER ──────────────────────────────────────────────────────────

async def handle_callback(update: dict):
    cq = update.get("callback_query", {})
    cb_id = cq.get("id")
    data = cq.get("data", "")
    msg_id = cq.get("message", {}).get("message_id")
    approval = pending_approvals.get(msg_id, {})
    content = approval.get("content", {})
    brand = approval.get("brand", "")
    batch_id = approval.get("batch_id")
    idx = approval.get("idx")

    parts = data.split("|")
    action = parts[0]

    if action == "approve_single":
        await answer_callback(cb_id, "Approved ✅")
        pending_approvals.pop(msg_id, None)
        await send_telegram(f"_Saving to Drive and posting {BRANDS[brand]['name']} content..._")
        # Use cached rendered images if available to avoid re-rendering
        cached_b64 = content.pop("_rendered_images_b64", [])
        if cached_b64:
            cached_images = [base64.b64decode(b) for b in cached_b64]
            content["_cached_images"] = cached_images
        results = await post_content(content, brand)
        platform_count = sum(1 for k, v in results.items() if k != "drive_saved" and "error" not in str(v))
        total_platforms = sum(1 for k in results if k != "drive_saved")
        drive_line = "\n📁 Saved to Mark Marketing" if results.get("drive_saved") else "\n⚠️ Drive save failed"
        await send_telegram(f"✅ *Approved — {BRANDS[brand]['name']}*\n{platform_count}/{total_platforms} platforms posted{drive_line}")

    elif action == "post_now":
        # Legacy support — treat same as approve_single
        await answer_callback(cb_id, "Approved ✅")
        pending_approvals.pop(msg_id, None)
        cached_b64 = content.pop("_rendered_images_b64", [])
        if cached_b64:
            content["_cached_images"] = [base64.b64decode(b) for b in cached_b64]
        results = await post_content(content, brand)
        platform_count = sum(1 for k, v in results.items() if k != "drive_saved" and "error" not in str(v))
        total_platforms = sum(1 for k in results if k != "drive_saved")
        drive_line = "\n📁 Saved to Mark Marketing" if results.get("drive_saved") else "\n⚠️ Drive save failed"
        await send_telegram(f"✅ *Approved — {BRANDS[brand]['name']}*\n{platform_count}/{total_platforms} platforms posted{drive_line}")

    elif action == "approve_one":
        await answer_callback(cb_id, "Approved ✅")
        if batch_id and batch_id in pending_batches:
            b = pending_batches[batch_id]
            if idx is not None and idx < len(b):
                b[idx]["_approved"] = True
        await send_telegram(f"✅ Post {(idx or 0) + 1} approved. Send `approve batch {brand}` to push all to Meta/LinkedIn.")
        pending_approvals.pop(msg_id, None)

    elif action == "skip_one":
        await answer_callback(cb_id, "Skipped")
        if batch_id and batch_id in pending_batches:
            b = pending_batches[batch_id]
            if idx is not None and idx < len(b):
                b[idx]["_skipped"] = True
        pending_approvals.pop(msg_id, None)

    elif action in ("regen_single", "regen_one"):
        await answer_callback(cb_id, "Regenerating...")
        pending_approvals.pop(msg_id, None)
        topic = content.get("_topic", "")
        ct = content.get("_content_type", "carousel")
        breaking = content.get("_breaking", False)
        new = await generate_single(brand, ct, topic)
        if new:
            if breaking:
                new["_breaking"] = True
            await send_approval_request(new, brand, batch_id, idx)
        else:
            await send_telegram("⚠️ Regeneration failed.")

    elif action == "reject_single":
        await answer_callback(cb_id, "Rejected")
        pending_approvals.pop(msg_id, None)
        await send_telegram("❌ Rejected. Send a new `breaking:` command.")


# ── FASTAPI ENDPOINTS (Alex integration) ─────────────────────────────────────

@app.get("/")
async def health():
    li_status = "connected" if LI_ACCESS_TOKEN else "needs_auth"
    return {
        "status": "Mark is running",
        "version": "3.0",
        "linkedin": li_status,
        "pending_approvals": len(pending_approvals),
        "pending_batches": {k: len(v) for k, v in pending_batches.items()},
    }


@app.post("/generate")
async def api_generate(request: Request, background_tasks: BackgroundTasks):
    """Alex calls this to generate content."""
    body = await request.json()
    brand = body.get("brand")
    content_type = body.get("content_type", "carousel")
    topic = body.get("topic", "")
    breaking = body.get("breaking", False)
    days = body.get("days", 14)
    action = body.get("action", "single")  # single | batch | batch_all

    if brand and brand not in BRANDS:
        return JSONResponse({"error": f"Unknown brand: {brand}"}, status_code=400)

    if action == "batch_all":
        background_tasks.add_task(_run_batch_all, days)
        return {"status": "generating", "action": "batch_all", "days": days}

    elif action == "batch" and brand:
        background_tasks.add_task(_run_batch, brand, days)
        return {"status": "generating", "action": "batch", "brand": brand, "days": days}

    elif action == "single" and brand:
        if not topic or not topic.strip():
            return JSONResponse(
                {"status": "error", "message": "Topic required — Alex must provide the content angle"},
                status_code=400,
            )
        background_tasks.add_task(_run_single, brand, content_type, topic, breaking)
        return {"status": "generating", "action": "single", "brand": brand, "content_type": content_type}

    return JSONResponse({"error": "Invalid request"}, status_code=400)


@app.post("/approve_batch")
async def api_approve_batch(request: Request, background_tasks: BackgroundTasks):
    """Alex calls this to approve and push an entire batch."""
    body = await request.json()
    brand = body.get("brand")
    brands_to_push = [brand] if brand else list(last_batch.keys())
    background_tasks.add_task(_push_approved_batches, brands_to_push)
    return {"status": "pushing", "brands": brands_to_push}


@app.get("/status")
async def api_status():
    status = {}
    for b in BRANDS:
        batch = last_batch.get(b, [])
        status[b] = {
            "batch_size": len(batch),
            "approved": sum(1 for p in batch if p.get("_approved")),
            "skipped": sum(1 for p in batch if p.get("_skipped")),
        }
    return {
        "mark_status": "online",
        "linkedin_connected": bool(LI_ACCESS_TOKEN),
        "pending_approvals": len(pending_approvals),
        "brands": status,
    }


@app.post("/pdf_post")
async def api_pdf_post(request: Request, background_tasks: BackgroundTasks):
    """Generate a post from a developer brochure PDF."""
    body = await request.json()
    brand = body.get("brand")
    content_type = body.get("content_type", "carousel")
    pdf_name = body.get("pdf_name", "")
    background_tasks.add_task(_run_pdf_post, brand, content_type, pdf_name)
    return {"status": "generating", "pdf": pdf_name, "brand": brand}


# ── BACKGROUND TASKS ──────────────────────────────────────────────────────────

async def _run_single(brand: str, content_type: str, topic: str, breaking: bool = False):
    if not topic or not topic.strip():
        await send_telegram("⚠️ No topic provided — Alex must provide the content angle. Tell Alex what you want.")
        return
    await send_telegram(f"_Generating {BRANDS[brand]['name']} {content_type}..._")
    content = await generate_single(brand, content_type, topic)
    if not content:
        await send_telegram(f"⚠️ Mark failed to generate content for {brand}")
        return
    if breaking:
        content["_breaking"] = True
    # Render images and send as photos so GG can see what he's approving
    await send_telegram(f"_Rendering slides..._")
    rendered_images = []
    if content_type == "carousel":
        rendered_images = await render_carousel_images(content, brand)
    elif content_type in ("static", "story"):
        img = await render_static_image(content, brand)
        rendered_images = [img] if img else []
    for i, img_bytes in enumerate(rendered_images):
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
            async with httpx.AsyncClient(timeout=30) as client:
                await client.post(url, data={"chat_id": TELEGRAM_CHAT_ID}, files={"photo": (f"slide{i+1}.png", img_bytes, "image/png")})
        except Exception as e:
            log.error(f"Photo send error: {e}")
    # Cache rendered images so we don't re-render on approval
    content["_rendered_images_b64"] = [base64.b64encode(img).decode() for img in rendered_images]
    # Show caption + approve/reject buttons — all posts go at 6pm Dubai
    cap = content.get("caption_instagram", "")[:300]
    markup = {"inline_keyboard": [
        [{"text": "✅ Approve", "callback_data": f"approve_single|{brand}"},
         {"text": "❌ Reject", "callback_data": f"reject_single|{brand}"}],
        [{"text": "🔄 Regenerate", "callback_data": f"regen_single|{brand}||"}],
    ]}
    msg = await send_telegram(
        f"*{BRANDS[brand]['name']}*\n\n{cap}\n\n_Tap Approve to schedule for 6pm Dubai. Nothing posts until you approve._",
        reply_markup=markup
    )
    if msg.get("result", {}).get("message_id"):
        pending_approvals[msg["result"]["message_id"]] = {"content": content, "brand": brand, "batch_id": None, "idx": None}


async def _run_batch(brand: str, days: int):
    await send_telegram(f"_Generating {days}-day batch for {BRANDS[brand]['name']}..._")
    batch = await generate_batch(brand, days)
    bid = str(uuid.uuid4())[:8]
    pending_batches[bid] = batch
    last_batch[brand] = batch
    for p in batch:
        p["_batch_id"] = bid

    # Send summary
    lines = [f"📅 *{days}-day batch ready — {BRANDS[brand]['name']}*\n"]
    for i, c in enumerate(batch, 1):
        ct = c.get("_content_type", "post")
        topic = c.get("_topic", "")[:50]
        slot = c.get("_slot_label", "")
        lines.append(f"{i}. `{ct}` — {topic}\n   _{slot}_")
    lines += ["", "Reply to Alex: *approve batch* to schedule all, or *review [number]* to inspect one."]
    await send_telegram("\n".join(lines))


async def _run_batch_all(days: int):
    for brand in BRANDS:
        await _run_batch(brand, days)
        await asyncio.sleep(2)


async def _push_approved_batches(brands: list):
    for brand in brands:
        batch = last_batch.get(brand, [])
        to_push = [p for p in batch if not p.get("_skipped")]
        if not to_push:
            await send_telegram(f"⚠️ No posts to push for {BRANDS[brand]['name']}")
            continue
        await send_telegram(f"_Posting {len(to_push)} items for {BRANDS[brand]['name']}..._")
        success = 0
        for content in to_push:
            results = await post_content(content, brand)
            if any("error" not in str(v) for v in results.values()):
                success += 1
            await asyncio.sleep(3)  # rate limit courtesy
        await send_telegram(f"✅ *{BRANDS[brand]['name']} batch complete*\n{success}/{len(to_push)} posts published")


async def _run_pdf_post(brand: str, content_type: str, pdf_name: str):
    await send_telegram(f"_Reading PDF: {pdf_name}..._")
    pdfs = await list_drive_pdfs()
    match = next((p for p in pdfs if pdf_name.lower() in p["name"].lower()), None)
    if not match:
        await send_telegram(f"⚠️ No PDF matching '{pdf_name}' found in Drive.")
        return
    pdf_b64 = await fetch_pdf_b64(match["id"])
    content = await generate_single(brand, content_type, pdf_b64=pdf_b64, pdf_name=match["name"])
    if content:
        await send_approval_request(content, brand)
    else:
        await send_telegram("⚠️ PDF content generation failed.")


# ── TELEGRAM LISTENER ─────────────────────────────────────────────────────────

async def telegram_listener():
    offset = None
    await send_telegram(
        "🎨 *Mark v3 is online*\n\n"
        "I am controlled by Alex. GG approves content via the buttons below each preview.\n\n"
        f"LinkedIn: {'✅ Connected' if LI_ACCESS_TOKEN else f'⚠️ Not connected — visit {RAILWAY_URL}/linkedin/auth'}"
    )

    while True:
        try:
            updates = await get_updates(offset)
            for update in updates:
                offset = update["update_id"] + 1
                if "callback_query" in update:
                    await handle_callback(update)
        except Exception as e:
            log.error(f"Mark listener error: {e}")
            await asyncio.sleep(5)


# ── STARTUP ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    asyncio.create_task(telegram_listener())
    log.info("Mark v3 — AI Marketing Bot — online")
    # Alert if LinkedIn needs auth
    if not LI_ACCESS_TOKEN:
        asyncio.create_task(send_telegram(
            f"⚠️ *Mark needs LinkedIn auth*\n"
            f"Visit this URL in your browser to connect:\n"
            f"`{RAILWAY_URL}/linkedin/auth`"
        ))
