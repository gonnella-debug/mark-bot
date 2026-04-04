"""
Mark — AI Marketing Bot (Final)
Powered by Claude API | Pillow | Meta Graph API | LinkedIn API
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

from __future__ import annotations

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
import random
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
META_APP_ID         = os.getenv("META_APP_ID", "")
META_APP_SECRET     = os.getenv("META_APP_SECRET", "")
META_SYSTEM_TOKEN   = os.getenv("META_SYSTEM_TOKEN")

# LinkedIn
LI_CLIENT_ID        = os.getenv("LI_CLIENT_ID", "")
LI_CLIENT_SECRET    = os.getenv("LI_CLIENT_SECRET", "")
LI_NUCASSA_RE_PAGE  = os.getenv("LI_NUCASSA_RE_PAGE", "90919312")
LI_HOLDINGS_PAGE    = os.getenv("LI_HOLDINGS_PAGE", "109941216")
LI_ACCESS_TOKEN     = os.getenv("LI_ACCESS_TOKEN", "")       # set after first OAuth
LI_REFRESH_TOKEN    = os.getenv("LI_REFRESH_TOKEN", "")
_li_person_id       = ""  # fetched during OAuth
LI_TOKEN_EXPIRY     = int(os.getenv("LI_TOKEN_EXPIRY", "0"))

# Canva
CANVA_EMAIL         = os.getenv("CANVA_EMAIL", "")
CANVA_PASSWORD      = os.getenv("CANVA_PASSWORD", "")

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
FB_NUCASSA_RE       = "106173405736149"     # Nucassa Real Estate Dubai
FB_HOLDINGS         = "963897483477807"     # Nucassa.Holdings
FB_LISTR            = "1085489144643633"    # ListR

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
        "platforms": ["instagram", "facebook", "linkedin"],
        "ig_account_id": IG_HOLDINGS,
        "fb_page_id": FB_HOLDINGS,
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
        "platforms": ["instagram", "facebook"],
        "ig_account_id": IG_LISTR,
        "fb_page_id": FB_LISTR,
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
_temp_images: dict = {}        # image_id → bytes (temporary image hosting for IG uploads)
pending_batches: dict = {}     # batch_id → list[content_dict]
last_batch: dict = {}          # brand → list[content_dict]
li_oauth_states: dict = {}     # state → brand (for OAuth flow)
posting_log: list = []         # [{brand, platforms, topic, timestamp, success}] — persists per deploy

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
Slide 2 DATA: 3 key stats. Large rose gold/gold numbers, white labels. Dark photo bg with logo watermark top-centre. IMPORTANT: Every carousel must have DIFFERENT stats and a DIFFERENT layout angle — never repeat the same 3-stat pattern. Vary between: percentage stats, currency amounts, ranking comparisons, year-over-year changes, project-specific data, area-specific numbers. Each post must feel unique.
Slide 3 CTA: Pure #1C1C1C (or #000 for ListR) background. Short bold question. CTA line. Logo bottom-centre.

CRITICAL — VARIETY BETWEEN POSTS:
Each carousel must be visually and contextually distinct from the last. Never reuse the same headline structure, stat format, or photo direction across consecutive posts. Vary the hook style (question vs statement vs stat), the data angle (market-wide vs area-specific vs project-specific), and the photo mood (skyline vs interior vs aerial vs street-level).

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
    resp_json = r.json()
    if "content" not in resp_json:
        log.error(f"Claude API error: {resp_json.get('error', resp_json)}")
        return None
    return resp_json["content"][0]["text"]


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
            # Detect image type from header bytes
            import base64 as _b64
            raw = _b64.b64decode(img_b64[:32])
            mime = "image/png" if raw[:4] == b'\x89PNG' else "image/jpeg"
            content_parts.append({
                "type": "image",
                "source": {"type": "base64", "media_type": mime, "data": img_b64}
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
        resp_json = r.json()
        if "content" not in resp_json:
            log.error(f"Claude API error: {resp_json.get('error', resp_json)}")
            return None
        return resp_json["content"][0]["text"]

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

    if not raw:
        log.error(f"Claude returned no content for {brand}")
        return None
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
    topics = list(cfg["topics"])
    random.shuffle(topics)

    batch = []
    for idx, slot in enumerate(slots):
        ct = type_rotation[idx % len(type_rotation)]
        topic = topics[idx % len(topics)]
        log.warning(f"[generate_batch] No topic provided by Alex for slot {idx+1} — using fallback topic from config: '{topic}'")
        content = await generate_single(brand, ct, topic)
        if content:
            content["_scheduled_at"] = slot.isoformat()
            content["_slot_label"] = slot.strftime("%a %d %b — %H:%M UTC")
            content["_approved"] = False
            content["_skipped"] = False
            batch.append(content)
        await asyncio.sleep(1.5)
    return batch


# ── SLIDE RENDERER (Pillow + Unsplash + Real Logos) ──────────────────────────

from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance
import io
import textwrap

# Logo file paths (local)
_logo_cache: dict = {}  # brand → PIL Image

LOGO_PATHS = {
    "nucassa_re": "logo_nucassa.png",
    "nucassa_holdings": "logo_nucassa.png",
    "listr": "logo_listr.png",
}

LOGO_URLS = {
    "listr": "https://d2xsxph8kpxj0f.cloudfront.net/310519663442673192/8rDTGLeYbqNNdTnaNjEFq5/ListRLogo_white_97e809bf.png",
}

# Brand-specific search terms — each brand has its own visual identity
BRAND_SEARCH_TERMS = {
    "nucassa_re": {
        "cover": [
            "Dubai skyline sunset golden hour", "Dubai marina night aerial", "Dubai downtown towers night",
            "luxury villa pool Dubai", "Dubai penthouse interior modern", "Dubai aerial palm jumeirah",
            "Dubai waterfront sunset", "luxury apartment balcony city view", "Dubai beach resort aerial",
            "Dubai Burj Khalifa night", "modern Dubai architecture", "Dubai residential tower luxury",
        ],
        "data": [
            "dark modern architecture abstract", "luxury apartment interior moody",
            "dark office glass building", "abstract geometric dark building",
            "glass facade reflection night", "dark luxury marble interior",
        ],
    },
    "nucassa_holdings": {
        "cover": [
            "Dubai financial district DIFC tower night", "Abu Dhabi ADGM skyline night",
            "corporate boardroom skyline view dark", "luxury office interior dark modern",
            "bank vault door steel dark", "signing contract document pen dark",
            "Dubai skyline aerial night financial", "glass office tower dark reflection",
            "corporate meeting room dark modern", "Dubai business bay financial tower night",
            "safe deposit vault dark steel", "office desk skyline window dark",
        ],
        "data": [
            "financial chart dark screen", "portfolio analytics dark screen",
            "abstract dark gold geometric pattern", "dark marble texture luxury",
            "corporate office dark empty modern", "abstract dark lines architecture",
        ],
    },
    "listr": {
        "cover": [
            "Dubai skyline sunset golden hour", "Dubai marina aerial", "Dubai downtown night lights",
            "modern apartment building Dubai", "Dubai residential community aerial", "Dubai villa exterior",
            "Dubai property aerial view", "Dubai beach apartment view", "luxury Dubai townhouse",
            "Dubai JBR beach aerial", "Dubai sports city aerial", "modern Dubai neighbourhood",
        ],
        "data": [
            "dark modern architecture abstract", "abstract geometric dark building",
            "glass facade reflection night", "concrete texture modern building",
            "dark luxury marble interior", "modern skyscraper detail dark",
        ],
    },
}

# Fallback search terms if brand not specified
UNSPLASH_SEARCH_TERMS = BRAND_SEARCH_TERMS["nucassa_re"]

# Brand-specific curated fallback URLs
BRAND_FALLBACK_PHOTOS = {
    "nucassa_re": {
        "cover": [
            "https://images.unsplash.com/photo-1512453979798-5ea266f8880c?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1597659840241-37e2b9c2f55f?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1583417319070-4a69db38a482?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1580674684081-7617fbf3d745?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1518684079-3c830dcef090?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1570125909232-eb263c188f7e?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1546412414-e1885e51148b?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1514632595-4944383f2737?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1600585154340-be6161a56a0c?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1571003123894-1f0594d2b5d9?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1613490493576-7fde63acd811?w=2000&h=2500&fit=crop&q=95",
        ],
        "data": [
            "https://images.unsplash.com/photo-1486406146926-c627a92ad1ab?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1554469384-e58fac16e23a?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1448630360428-65456885c650?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1459767129954-1b1c1f9b9ace?w=2000&h=2500&fit=crop&q=95",
        ],
    },
    "nucassa_holdings": {
        "cover": [],  # Holdings uses ONLY Pexels/Unsplash API search with strict terms — no random curated fallbacks
        "data": [],
    },
    "listr": {
        "cover": [
            "https://images.unsplash.com/photo-1512453979798-5ea266f8880c?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1583417319070-4a69db38a482?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1580674684081-7617fbf3d745?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1518684079-3c830dcef090?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1570125909232-eb263c188f7e?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1600585154340-be6161a56a0c?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1545893835-abaa50cbe628?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1524492412937-b28074a5d7da?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1582672060674-bc2bd808a8b5?w=2000&h=2500&fit=crop&q=95",
        ],
        "data": [
            "https://images.unsplash.com/photo-1486406146926-c627a92ad1ab?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1554469384-e58fac16e23a?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1448630360428-65456885c650?w=2000&h=2500&fit=crop&q=95",
            "https://images.unsplash.com/photo-1459767129954-1b1c1f9b9ace?w=2000&h=2500&fit=crop&q=95",
        ],
    },
}

# Legacy alias
UNSPLASH_PHOTOS = BRAND_FALLBACK_PHOTOS["nucassa_re"]

UNSPLASH_ACCESS_KEY = os.getenv("UNSPLASH_ACCESS_KEY", "")
PEXELS_API_KEY = os.getenv("PEXELS_API_KEY", "")

# Font paths — try common macOS/Linux locations
def _find_font(name: str, fallbacks: list[str] = None) -> str:
    """Find a font file on the system."""
    search_dirs = [
        os.path.join(os.path.dirname(__file__), "fonts"),
        "/Users/gg/Library/Fonts",
        "/Library/Fonts",
        "/System/Library/Fonts",
        "/System/Library/Fonts/Supplemental",
        "/usr/share/fonts",
        "/usr/share/fonts/truetype",
    ]
    names_to_try = [name] + (fallbacks or [])
    for font_name in names_to_try:
        for d in search_dirs:
            for ext in [".ttf", ".otf", ".TTF", ".OTF"]:
                path = os.path.join(d, font_name + ext)
                if os.path.exists(path):
                    return path
                # Try with spaces/hyphens
                for variant in [font_name.replace(" ", ""), font_name.replace(" ", "-")]:
                    path = os.path.join(d, variant + ext)
                    if os.path.exists(path):
                        return path
    return None


def _get_font(name: str, size: int, fallbacks: list[str] = None) -> ImageFont.FreeTypeFont:
    """Load a font at the given size, with fallbacks."""
    path = _find_font(name, fallbacks)
    if path:
        return ImageFont.truetype(path, size)
    log.warning(f"Font '{name}' not found, using default")
    return ImageFont.load_default()


# Headline font: Montserrat SemiBold / Bold
FONT_HEADLINE = lambda size: _get_font("Montserrat-SemiBold", size, ["Montserrat-Bold", "Montserrat", "Arial Bold", "Helvetica-Bold"])
# Body font: Varta Medium
FONT_BODY = lambda size: _get_font("Varta-Medium", size, ["Varta", "Varta-Regular", "Arial", "Helvetica"])


def _hex_to_rgb(hex_color: str) -> tuple:
    """Convert hex color to RGB tuple."""
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))


def _hex_to_rgba(hex_color: str, alpha: int = 255) -> tuple:
    """Convert hex color to RGBA tuple."""
    r, g, b = _hex_to_rgb(hex_color)
    return (r, g, b, alpha)


# Tags/descriptions that indicate irrelevant photos — filter these out
_PHOTO_BLOCKLIST = [
    "bus", "tour", "tourist", "cartoon", "illustration", "drawing", "clip art",
    "meme", "selfie", "crowd", "protest", "accident", "trash", "garbage",
    "food truck", "street vendor", "tuk tuk", "rickshaw", "camping",
    "beef", "food", "plate", "restaurant", "wine", "cocktail", "dessert",
    "car interior", "steering wheel", "dashboard", "mercedes", "bmw", "audi",
    "watch", "rolex", "jewelry", "ring", "necklace", "bracelet",
    "cigar", "whiskey", "champagne", "bottle", "drink", "glass",
    "jet", "airplane", "aircraft", "yacht", "boat", "ship",
]


def _is_photo_relevant(photo_data: dict) -> bool:
    """Check if an Unsplash photo is relevant based on its metadata."""
    desc = (photo_data.get("description") or "").lower()
    alt = (photo_data.get("alt_description") or "").lower()
    tags = " ".join(t.get("title", "") for t in photo_data.get("tags", [])).lower()
    combined = f"{desc} {alt} {tags}"
    return not any(blocked in combined for blocked in _PHOTO_BLOCKLIST)


async def _fetch_unsplash_photo(search_term: str, topic: str = "") -> bytes | None:
    """Fetch a photo from Unsplash API search, sized for 1080x1350."""
    if UNSPLASH_ACCESS_KEY:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.get(
                    "https://api.unsplash.com/search/photos",
                    params={"query": search_term, "per_page": 20, "orientation": "portrait"},
                    headers={"Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}"},
                )
                if r.status_code == 200:
                    results = r.json().get("results", [])
                    # Filter out irrelevant photos
                    results = [r for r in results if _is_photo_relevant(r)]
                    if results:
                        photo = random.choice(results)
                        url = photo["urls"]["regular"] + "&w=1080&h=1350&fit=crop"
                        img_r = await client.get(url, timeout=20)
                        if img_r.status_code == 200:
                            return img_r.content
        except Exception as e:
            log.error(f"Unsplash API error: {e}")

    return None


async def _fetch_pexels_photo(search_term: str) -> bytes | None:
    """Fetch a photo from Pexels API as secondary source."""
    if not PEXELS_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                "https://api.pexels.com/v1/search",
                params={"query": search_term, "per_page": 20, "orientation": "portrait"},
                headers={"Authorization": PEXELS_API_KEY},
            )
            if r.status_code == 200:
                photos = r.json().get("photos", [])
                # Filter out irrelevant Pexels photos
                photos = [p for p in photos if not any(
                    blocked in (p.get("alt", "") or "").lower()
                    for blocked in _PHOTO_BLOCKLIST
                )]
                if photos:
                    photo = random.choice(photos)
                    url = photo["src"]["large2x"]
                    img_r = await client.get(url, timeout=20)
                    if img_r.status_code == 200:
                        return img_r.content
    except Exception as e:
        log.error(f"Pexels API error: {e}")
    return None


# ── Drive brochure image cache ──
_drive_image_cache: list[bytes] = []  # cached property images extracted from brochure PDFs
_drive_cache_time: float = 0.0


def _is_clean_photo(img_bytes: bytes, min_width: int = 800, min_height: int = 600) -> bool:
    """Check if an image is a clean photograph suitable as a background.
    Rejects: small images, floor plans, logos, text-heavy pages, icons."""
    try:
        img = Image.open(io.BytesIO(img_bytes))
        w, h = img.size

        # Too small — likely a logo, icon, or decorative element
        if w < min_width or h < min_height:
            return False

        # Very narrow or very tall — likely a banner, sidebar, or floor plan strip
        ratio = w / h if h > 0 else 0
        if ratio > 4.0 or ratio < 0.2:
            return False

        # Check for text-heavy / white-dominated images (brochure pages with text)
        img_rgb = img.convert("RGB")
        import numpy as np
        arr = np.array(img_rgb)

        # Percentage of near-white pixels (R>240, G>240, B>240)
        white_mask = (arr[:, :, 0] > 240) & (arr[:, :, 1] > 240) & (arr[:, :, 2] > 240)
        white_pct = white_mask.sum() / (w * h)
        if white_pct > 0.35:
            return False  # Too much white — likely a text page or floor plan

        # Percentage of near-black pixels — pure black pages with text overlays
        black_mask = (arr[:, :, 0] < 15) & (arr[:, :, 1] < 15) & (arr[:, :, 2] < 15)
        black_pct = black_mask.sum() / (w * h)
        if black_pct > 0.60:
            return False  # Too dark — likely a black page with white text

        # Colour variance — real photos have high variance, graphics/plans are flat
        std = arr.std()
        if std < 25:
            return False  # Very flat colours — likely a graphic, icon, or solid fill

        return True
    except Exception:
        return False


async def _extract_images_from_pdf_bytes(pdf_bytes: bytes, min_size: int = 80000) -> list[bytes]:
    """Extract clean photographs from a PDF using PyMuPDF.
    Filters out logos, floor plans, text pages, and small graphics."""
    images = []
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        for page in doc:
            for img_info in page.get_images(full=True):
                xref = img_info[0]
                try:
                    base_image = doc.extract_image(xref)
                    if not base_image or len(base_image["image"]) < min_size:
                        continue
                    if _is_clean_photo(base_image["image"]):
                        images.append(base_image["image"])
                except Exception:
                    continue
        doc.close()
    except Exception as e:
        log.error(f"PDF image extraction error: {e}")
    return images


async def _fetch_drive_property_image() -> bytes | None:
    """Fetch a random property image from Drive brochure PDFs. Caches extracted images for 6 hours."""
    global _drive_image_cache, _drive_cache_time

    # Return from cache if fresh (6 hours)
    if _drive_image_cache and (time.time() - _drive_cache_time) < 21600:
        return random.choice(_drive_image_cache)

    if not GDRIVE_API_KEY:
        return None

    try:
        # List all developer folders
        dev_folders = await list_drive_files(GDRIVE_FOLDER_ID)
        dev_folders = [f for f in dev_folders if f.get("mimeType") == "application/vnd.google-apps.folder"]

        if not dev_folders:
            return None

        # Pick 5 random developers and look for brochure PDFs
        sample_devs = random.sample(dev_folders, min(5, len(dev_folders)))
        all_images: list[bytes] = []

        async with httpx.AsyncClient(timeout=30) as client:
            for dev in sample_devs:
                if len(all_images) >= 30:
                    break
                # List projects under this developer
                projects = await list_drive_files(dev["id"])
                project_folders = [f for f in projects if f.get("mimeType") == "application/vnd.google-apps.folder"]
                sample_projects = random.sample(project_folders, min(3, len(project_folders)))

                for proj in sample_projects:
                    if len(all_images) >= 30:
                        break
                    # Look for Brochure subfolder
                    proj_contents = await list_drive_files(proj["id"])
                    brochure_folder = next(
                        (f for f in proj_contents if f["name"].lower() == "brochure" and "folder" in f.get("mimeType", "")),
                        None
                    )
                    if not brochure_folder:
                        continue

                    # Get PDFs from brochure folder
                    brochure_files = await list_drive_files(brochure_folder["id"])
                    pdfs = [f for f in brochure_files if f.get("mimeType") == "application/pdf"]

                    for pdf_file in pdfs[:1]:  # Just one PDF per project
                        try:
                            r = await client.get(
                                f"https://www.googleapis.com/drive/v3/files/{pdf_file['id']}",
                                params={"alt": "media", "key": GDRIVE_API_KEY},
                                timeout=30,
                            )
                            if r.status_code == 200:
                                imgs = await _extract_images_from_pdf_bytes(r.content)
                                if imgs:
                                    # Take the largest images (most likely hero renders)
                                    imgs.sort(key=len, reverse=True)
                                    all_images.extend(imgs[:3])
                                    log.info(f"Extracted {len(imgs[:3])} images from {pdf_file['name']}")
                        except Exception as e:
                            log.error(f"Drive PDF fetch error ({pdf_file['name']}): {e}")

        if all_images:
            _drive_image_cache = all_images
            _drive_cache_time = time.time()
            log.info(f"Drive image cache refreshed: {len(all_images)} property images")
            return random.choice(all_images)

    except Exception as e:
        log.error(f"Drive property image pipeline error: {e}")

    return None


def _create_branded_background(brand: str, slide_type: str = "cover") -> bytes:
    """Generate a clean branded background for Holdings and ListR — no stock photos."""
    cfg = BRANDS[brand]
    W, H = 1080, 1350
    primary = _hex_to_rgb(cfg["color_primary"])
    accent = _hex_to_rgb(cfg["color_accent"])
    secondary = _hex_to_rgb(cfg.get("color_secondary", "#3b3b3b"))

    img = Image.new("RGBA", (W, H), (*primary, 255))
    draw = ImageDraw.Draw(img)

    if brand == "nucassa_holdings":
        # Institutional: dark base with subtle gold geometric accents
        if slide_type == "data":
            # Very dark with thin accent lines
            for i in range(0, H, 90):
                opacity = random.randint(8, 20)
                draw.line([(0, i), (W, i)], fill=(*accent, opacity), width=1)
        else:
            # Diagonal accent stripe (subtle)
            for i in range(-H, W + H, 6):
                opacity = random.randint(5, 15)
                draw.line([(i, 0), (i + H, H)], fill=(*accent, opacity), width=1)
            # Bottom accent bar
            draw.rectangle([(0, H - 4), (W, H)], fill=(*accent, 60))

    elif brand == "listr":
        # Modern/bold: black base with gold geometric elements
        if slide_type == "data":
            # Grid pattern
            for x in range(0, W, 120):
                draw.line([(x, 0), (x, H)], fill=(*accent, 12), width=1)
            for y in range(0, H, 120):
                draw.line([(0, y), (W, y)], fill=(*accent, 12), width=1)
        else:
            # Bold angular accent
            points = [(0, H * 0.7), (W, H * 0.5), (W, H * 0.55), (0, H * 0.75)]
            draw.polygon(points, fill=(*accent, 25))
            # Second stripe
            points2 = [(0, H * 0.78), (W, H * 0.58), (W, H * 0.60), (0, H * 0.80)]
            draw.polygon(points2, fill=(*accent, 15))

    buf = io.BytesIO()
    img.save(buf, format="PNG", quality=95)
    return buf.getvalue()


# ── Verified image library — NO API search, NO randomness, every image guaranteed ──

# Every URL below has been verified: Dubai skyline, towers, luxury villas, modern architecture.
# NO buses, NO food, NO tourists, NO animals, NO random stock photos.
_VERIFIED_IMAGES = {
    "dubai_skyline": [
        "https://images.unsplash.com/photo-1512453979798-5ea266f8880c?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1518684079-3c830dcef090?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1546412414-e1885e51148b?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1583417319070-4a69db38a482?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1580674684081-7617fbf3d745?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1570125909232-eb263c188f7e?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1597659840241-37e2b9c2f55f?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1741380530342-10f7b1214eeb?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1619807038543-2ce7d4c9fb15?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1615747476328-41153cf6da54?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1642504447080-ca95df2d73a2?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1579390668747-a917e5dabc10?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1651063820152-d3e7a27b4d2b?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1580164541787-ea5633f2118e?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1559649900-40f09af87c9c?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1578419176695-7093f9faf11a?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1645640322813-25aaef578106?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1589395752253-a04b9ca0722a?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1575404215583-863f8d55a411?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1514632595-4944383f2737?w=1200&h=1500&fit=crop&q=90",
    ],
    "dubai_towers": [
        "https://images.unsplash.com/photo-1486406146926-c627a92ad1ab?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1608991156162-3c55b3cf05d3?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1643904736472-8b77e93ca3d7?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1582120031356-35f21bf61055?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1573755654354-4235c9ab1ac9?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1552051263-6eb5bb6905b9?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1533395427226-788cee25cc7b?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1594962591758-00dba6b83e39?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1554469384-e58fac16e23a?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1448630360428-65456885c650?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1459767129954-1b1c1f9b9ace?w=1200&h=1500&fit=crop&q=90",
    ],
    "luxury_property": [
        "https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1600585154340-be6161a56a0c?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1613490493576-7fde63acd811?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1571003123894-1f0594d2b5d9?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1728048756980-e21666532e24?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1651108066220-f61c22fc281f?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1603077864615-538e955d1ad1?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1564703048291-bcf7f001d83d?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1502005229762-cf1b2da7c5d6?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1551380789-5783a7c84669?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1583270423828-7af6195c4928?w=1200&h=1500&fit=crop&q=90",
    ],
    "night_city": [
        "https://images.unsplash.com/photo-1621073831231-faa453d28112?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1650217124806-36e7a0b7afb8?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1657106251952-2d584ebdf886?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1671618802338-682e248b48e8?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1663790080792-2129962584db?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1699118837104-72f6f62f09aa?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1694675272206-85c7e00da3e3?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1706464971762-bcacd432cf22?w=1200&h=1500&fit=crop&q=90",
    ],
    "corporate_financial": [
        "https://images.unsplash.com/photo-1486406146926-c627a92ad1ab?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1554469384-e58fac16e23a?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1768069794857-9306ac167c6e?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1768069794830-f2bd21c67621?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1760263137665-366b1dcf8e1f?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1760263137552-c42c62621220?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1608991156162-3c55b3cf05d3?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1573755654354-4235c9ab1ac9?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1604667758760-ffb2931593a0?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1619259896604-0fe0fd32ac43?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1574886248530-3063f166bf95?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1553696779-79d887fdd22a?w=1200&h=1500&fit=crop&q=90",
    ],
    "modern_architecture": [
        "https://images.unsplash.com/photo-1604667758760-ffb2931593a0?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1619259896604-0fe0fd32ac43?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1543218683-e3fdc8d9fd0d?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1601624311242-d7de9926253f?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1553696779-79d887fdd22a?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1553696718-0ff0872ee05f?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1574886248530-3063f166bf95?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1546617334-bb46979e8ecc?w=1200&h=1500&fit=crop&q=90",
        "https://images.unsplash.com/photo-1567096417093-abb2acca7cfc?w=1200&h=1500&fit=crop&q=90",
    ],
}

# Topic keyword → image category mapping
_TOPIC_TO_CATEGORY = {
    "marina": "dubai_skyline", "waterfront": "dubai_skyline", "creek": "dubai_skyline",
    "downtown": "dubai_towers", "burj": "dubai_towers", "khalifa": "dubai_towers", "tower": "dubai_towers",
    "villa": "luxury_property", "penthouse": "luxury_property", "luxury": "luxury_property",
    "interior": "luxury_property", "home": "luxury_property", "house": "luxury_property",
    "night": "night_city", "dark": "night_city", "moody": "night_city",
    "architecture": "modern_architecture", "glass": "modern_architecture", "modern": "modern_architecture",
    "yield": "dubai_skyline", "market": "dubai_skyline", "invest": "dubai_towers",
    "capital": "night_city", "data": "dubai_towers", "record": "dubai_skyline",
}

_recent_image_urls: list[str] = []


async def _fetch_photo_for_slide(slide_type: str, topic: str = "", brand: str = "nucassa_re") -> bytes | None:
    """Fetch a verified background image. No API search. Every image guaranteed quality."""
    global _recent_image_urls

    if slide_type == "cta":
        return None

    topic_lower = topic.lower()

    # Brand-specific default categories
    if brand == "nucassa_holdings":
        category = "corporate_financial"
    elif brand == "listr":
        # ListR = property marketplace — use property + Dubai images
        category = random.choice(["luxury_property", "dubai_skyline", "modern_architecture"])
    else:
        # Nucassa RE — pick category based on topic keywords
        category = None
        for keyword, cat in _TOPIC_TO_CATEGORY.items():
            if keyword in topic_lower:
                category = cat
                break
        if not category:
            category = random.choice(["dubai_skyline", "dubai_towers", "night_city"])

    pool = _VERIFIED_IMAGES.get(category, _VERIFIED_IMAGES["dubai_skyline"])
    available = [u for u in pool if u not in _recent_image_urls]
    if not available:
        # Try other categories before clearing
        for cat_name, cat_urls in _VERIFIED_IMAGES.items():
            available = [u for u in cat_urls if u not in _recent_image_urls]
            if available:
                break
    if not available:
        _recent_image_urls.clear()
        available = pool

    url = random.choice(available)
    _recent_image_urls.append(url)
    if len(_recent_image_urls) > 20:
        _recent_image_urls.pop(0)

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url)
            if r.status_code == 200:
                return r.content
            log.warning(f"Image fetch failed ({r.status_code}): {url}")
    except Exception as e:
        log.error(f"Image fetch error: {e}")

    return None


async def _load_logo_image(brand: str) -> Image.Image | None:
    """Load the brand logo as a PIL Image, cached in memory."""
    if brand in _logo_cache and _logo_cache[brand] is not None:
        return _logo_cache[brand]

    # Try local file first
    logo_path = LOGO_PATHS.get(brand)
    if logo_path:
        full_path = os.path.join(os.path.dirname(__file__), logo_path)
        if os.path.exists(full_path):
            try:
                img = Image.open(full_path).convert("RGBA")
                _logo_cache[brand] = img
                log.info(f"Loaded logo for {brand} from local file: {full_path}")
                return img
            except Exception as e:
                log.error(f"Logo load error: {e}")

    # Try from URL fallback (e.g. CloudFront CDN)
    logo_url = LOGO_URLS.get(brand)
    if logo_url:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.get(logo_url)
                if r.status_code == 200:
                    img = Image.open(io.BytesIO(r.content)).convert("RGBA")
                    _logo_cache[brand] = img
                    log.info(f"Loaded logo for {brand} from URL: {logo_url}")
                    return img
        except Exception as e:
            log.error(f"Logo URL fetch error for {brand}: {e}")

    # Try from Drive
    folder_id = GDRIVE_MARKETING_BRAND_FOLDERS.get(brand)
    if folder_id and GDRIVE_API_KEY:
        files = await list_drive_files(folder_id)
        logo_file = next((f for f in files if "logo" in f.get("name", "").lower() and "image" in f.get("mimeType", "")), None)
        if logo_file:
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    r = await client.get(
                        f"https://www.googleapis.com/drive/v3/files/{logo_file['id']}",
                        params={"alt": "media", "key": GDRIVE_API_KEY},
                    )
                    if r.status_code == 200:
                        img = Image.open(io.BytesIO(r.content)).convert("RGBA")
                        _logo_cache[brand] = img
                        log.info(f"Loaded logo for {brand} from Drive")
                        return img
            except Exception as e:
                log.error(f"Logo fetch error: {e}")

    _logo_cache[brand] = None
    return None


def _draw_text_wrapped(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont,
                       max_width: int, x: int, y: int, fill: tuple,
                       align: str = "left", line_spacing: int = 16) -> int:
    """Draw wrapped text and return the total height used."""
    words = text.split()
    lines = []
    current_line = ""
    for word in words:
        test_line = f"{current_line} {word}".strip() if current_line else word
        bbox = font.getbbox(test_line)
        if bbox[2] - bbox[0] <= max_width:
            current_line = test_line
        else:
            if current_line:
                lines.append(current_line)
            current_line = word
    if current_line:
        lines.append(current_line)

    total_height = 0
    for line in lines:
        bbox = font.getbbox(line)
        line_w = bbox[2] - bbox[0]
        line_h = bbox[3] - bbox[1]
        if align == "center":
            draw.text((x + (max_width - line_w) // 2, y + total_height), line, font=font, fill=fill)
        elif align == "right":
            draw.text((x + max_width - line_w, y + total_height), line, font=font, fill=fill)
        else:
            draw.text((x, y + total_height), line, font=font, fill=fill)
        total_height += line_h + line_spacing
    return total_height


def _apply_gradient_overlay(img: Image.Image, opacity_top: float = 0.25, opacity_bottom: float = 0.95) -> Image.Image:
    """Apply a dark gradient overlay to an image (top to bottom)."""
    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    for y in range(img.height):
        progress = y / img.height
        alpha = int((opacity_top + (opacity_bottom - opacity_top) * progress) * 255)
        for x in range(img.width):
            overlay.putpixel((x, y), (0, 0, 0, alpha))
    # Use a more efficient approach with ImageDraw
    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    for y in range(img.height):
        progress = y / img.height
        alpha = int((opacity_top + (opacity_bottom - opacity_top) * progress) * 255)
        draw.line([(0, y), (img.width, y)], fill=(0, 0, 0, alpha))
    return Image.alpha_composite(img.convert("RGBA"), overlay)


def _add_watermark_logo(img: Image.Image, logo: Image.Image, opacity: float = 0.15) -> Image.Image:
    """Add a faded centered watermark logo."""
    # Resize logo to ~30% of image width
    logo_w = int(img.width * 0.3)
    ratio = logo_w / logo.width
    logo_h = int(logo.height * ratio)
    logo_resized = logo.resize((logo_w, logo_h), Image.LANCZOS)

    # Apply opacity
    if logo_resized.mode == "RGBA":
        r, g, b, a = logo_resized.split()
        a = a.point(lambda p: int(p * opacity))
        logo_resized = Image.merge("RGBA", (r, g, b, a))

    # Center on image
    x = (img.width - logo_w) // 2
    y = (img.height - logo_h) // 2
    img.paste(logo_resized, (x, y), logo_resized)
    return img


async def create_slide_pillow(content: dict, slide_index: int, brand: str) -> bytes | None:
    """Render a slide matching the Nucassa design standard:
    - Cover: photo top 55%, fade to black, text on black, large watermark logo
    - Content: solid black, white headlines + accent emphasis, > chevron bullets, logo watermark top-right
    - CTA: clean black, accent CTA, large centered logo with brand name
    """
    cfg = BRANDS[brand]
    slides = content.get("slides", [])
    if slide_index >= len(slides):
        return None
    slide = slides[slide_index]
    topic = content.get("_topic", "")
    total_slides = len(slides)
    is_last = slide_index == total_slides - 1

    W, H = 1080, 1350
    accent = _hex_to_rgb(cfg["color_accent"])
    accent_rgba = _hex_to_rgba(cfg["color_accent"])
    primary = _hex_to_rgb(cfg["color_primary"])
    white = (255, 255, 255)
    white_muted = (255, 255, 255, 180)

    logo = await _load_logo_image(brand)

    def _paste_logo_top_left(img, size=80, opacity=0.50):
        """Small logo, top-left corner, consistent on every non-CTA slide."""
        if not logo:
            return
        wm = logo.copy()
        ratio = size / wm.width
        wm = wm.resize((size, int(wm.height * ratio)), Image.LANCZOS)
        if wm.mode == "RGBA":
            r, g, b, a = wm.split()
            a = a.point(lambda p: int(p * opacity))
            wm = Image.merge("RGBA", (r, g, b, a))
        img.paste(wm, (50, 50), wm)

    def _draw_swipe_arrow(draw_ctx, cx, cy, size=52):
        """Draw the circled arrow swipe indicator."""
        draw_ctx.ellipse([cx - size // 2, cy - size // 2, cx + size // 2, cy + size // 2], outline=white, width=2)
        arrow_font = FONT_HEADLINE(22)
        draw_ctx.text((cx - 7, cy - 13), "→", font=arrow_font, fill=white)

    # Helper: full-bleed photo with gradient overlay
    def _full_bleed_photo(bg_img, photo_bytes, grayscale=False):
        """Place photo covering entire slide, apply dark gradient over whole image."""
        if not photo_bytes:
            return bg_img
        photo = Image.open(io.BytesIO(photo_bytes)).convert("RGBA")
        pr = max(W / photo.width, H / photo.height)
        photo = photo.resize((int(photo.width * pr), int(photo.height * pr)), Image.LANCZOS)
        left = (photo.width - W) // 2
        top_c = (photo.height - H) // 2
        photo = photo.crop((left, top_c, left + W, top_c + H))
        if grayscale:
            photo = photo.convert("L").convert("RGBA")
        bg_img.paste(photo, (0, 0))
        # Gradient: light at top, dark at bottom — text sits on the dark bottom
        return _apply_gradient_overlay(bg_img, opacity_top=0.10, opacity_bottom=0.85)

    # ── SLIDE 1 & 2: Full-bleed photo + gradient + text over bottom ──
    if not is_last:
        bg = Image.new("RGBA", (W, H), (*primary, 255))
        photo_bytes = await _fetch_photo_for_slide("cover", topic + f"_slide{slide_index+1}", brand)

        # Randomly apply B&W to some slides for variety
        grayscale = random.choice([False, False, True]) if slide_index == 0 else random.choice([False, False, False, True])
        bg = _full_bleed_photo(bg, photo_bytes, grayscale=grayscale)

        # Small logo top-left, same spot every time
        _paste_logo_top_left(bg)

        draw = ImageDraw.Draw(bg)

        headline = slide.get("headline", "").upper()
        subtext = slide.get("subtext", "")
        emphasis = slide.get("emphasis", "")
        points = slide.get("stats", slide.get("points", []))
        padding_x = 70
        max_w = W - padding_x * 2
        dummy = ImageDraw.Draw(Image.new("RGBA", (1, 1)))

        # Calculate text block height to position from bottom
        font_h = FONT_HEADLINE(62) if slide_index == 0 else FONT_HEADLINE(48)
        font_sub = FONT_BODY(28)
        font_accent = FONT_HEADLINE(38)
        font_bullet = FONT_HEADLINE(30)

        block_height = 0
        if headline:
            block_height += _draw_text_wrapped(dummy, headline, font_h, max_w, 0, 0, white) + 25
        if subtext:
            block_height += _draw_text_wrapped(dummy, subtext.upper(), font_sub, max_w, 0, 0, accent) + 25
        if emphasis:
            block_height += _draw_text_wrapped(dummy, emphasis.upper(), font_accent, max_w, 0, 0, accent) + 30
        if points:
            for pt in points:
                display = pt.split(":", 1)[1].strip() if ":" in pt and len(pt.split(":", 1)[1].strip()) > len(pt.split(":", 1)[0].strip()) else pt
                block_height += _draw_text_wrapped(dummy, f"> {display.upper()}", font_bullet, max_w, 0, 0, white) + 22

        # Position text block so it ends ~130px from bottom (room for swipe arrow)
        text_y = H - 130 - block_height
        text_y = max(text_y, int(H * 0.45))  # Never higher than 45% — keep photo visible

        # Draw text elements
        if headline:
            h_height = _draw_text_wrapped(draw, headline, font_h, max_w, padding_x, text_y, white, align="center")
            text_y += h_height + 25

        if subtext and not points:
            # Subtext as accent line below headline (cover style)
            _draw_text_wrapped(draw, subtext.upper(), font_sub, max_w, padding_x, text_y, accent, align="center")
            text_y += _draw_text_wrapped(dummy, subtext.upper(), font_sub, max_w, 0, 0, accent) + 25

        if emphasis:
            _draw_text_wrapped(draw, emphasis.upper(), font_accent, max_w, padding_x, text_y, accent)
            text_y += _draw_text_wrapped(dummy, emphasis.upper(), font_accent, max_w, 0, 0, accent) + 30

        if points:
            for pt in points:
                display = pt.split(":", 1)[1].strip() if ":" in pt and len(pt.split(":", 1)[1].strip()) > len(pt.split(":", 1)[0].strip()) else pt
                bt_height = _draw_text_wrapped(draw, f"> {display.upper()}", font_bullet, max_w, padding_x, text_y, white)
                text_y += bt_height + 22

        if subtext and points:
            # Closing emphasis after bullets
            text_y += 10
            _draw_text_wrapped(draw, subtext.upper(), font_accent, max_w, padding_x, text_y, accent)

        # Swipe arrow
        _draw_swipe_arrow(draw, W // 2, H - 60)

        buf = io.BytesIO()
        bg.save(buf, format="PNG", quality=95)
        return buf.getvalue()

    # ── CTA SLIDE (last slide): Don't touch — it's fine ──
    elif is_last:
        cta_layout = random.choice(["logo_centered", "minimal"])
        bg = Image.new("RGBA", (W, H), (*primary, 255))
        draw = ImageDraw.Draw(bg)

        headline = slide.get("headline", slide.get("cta_line", cfg["cta"])).upper()
        subtext = slide.get("subtext", "")
        padding_x = 80
        max_w = W - padding_x * 2
        dummy = ImageDraw.Draw(Image.new("RGBA", (1, 1)))

        brand_name = "LISTR" if "listr" in brand else "NUCASSA"

        if cta_layout == "logo_centered":
            # Layout A: accent headline + white sub + large logo + brand name
            font_h = FONT_HEADLINE(48)
            font_sub = FONT_BODY(30)
            h_height = _draw_text_wrapped(dummy, headline, font_h, max_w, 0, 0, white)
            sub_height = _draw_text_wrapped(dummy, subtext.upper(), font_sub, max_w, 0, 0, accent) if subtext else 0
            logo_h = 200
            total_h = h_height + (30 + sub_height if subtext else 0) + 80 + logo_h
            start_y = (H - total_h) // 2
            _draw_text_wrapped(draw, headline, font_h, max_w, padding_x, start_y, accent, align="center")
            start_y += h_height
            if subtext:
                start_y += 30
                _draw_text_wrapped(draw, subtext.upper(), font_sub, max_w, padding_x, start_y, white, align="center")
                start_y += sub_height
            start_y += 80
            if logo:
                lw = 350 if brand == "listr" else 220
                lr = lw / logo.width
                lh = int(logo.height * lr)
                logo_big = logo.copy().resize((lw, lh), Image.LANCZOS)
                bg.paste(logo_big, ((W - lw) // 2, start_y), logo_big)
                start_y += lh + 20
            bn_font = FONT_HEADLINE(28)
            bn_w = bn_font.getbbox(brand_name)[2] - bn_font.getbbox(brand_name)[0]
            draw.text(((W - bn_w) // 2, start_y), brand_name, font=bn_font, fill=accent)

        elif cta_layout == "minimal":
            # Layout B: Just the message + logo, more breathing room
            font_h = FONT_HEADLINE(42)
            h_height = _draw_text_wrapped(dummy, headline, font_h, max_w, 0, 0, white)
            center_y = (H - h_height) // 2 - 80
            _draw_text_wrapped(draw, headline, font_h, max_w, padding_x, center_y, white, align="center")
            if logo:
                lw = 160
                lr = lw / logo.width
                lh = int(logo.height * lr)
                logo_sm = logo.copy().resize((lw, lh), Image.LANCZOS)
                bg.paste(logo_sm, ((W - lw) // 2, center_y + h_height + 60), logo_sm)

        buf = io.BytesIO()
        bg.save(buf, format="PNG", quality=95)
        return buf.getvalue()

    # No else needed — non-CTA slides handled above by `if not is_last`


async def render_carousel_images(content: dict, brand: str) -> list[bytes]:
    """Render all 3 carousel slides using Pillow and return list of PNG bytes."""
    images = []
    slides = content.get("slides", [])
    for i in range(len(slides)):
        img = await create_slide_pillow(content, i, brand)
        if img:
            images.append(img)
        await asyncio.sleep(0.3)
    return images


async def render_static_image(content: dict, brand: str) -> bytes | None:
    """Render a single static post image using Pillow."""
    return await create_slide_pillow(content, 0, brand)


# ── META GRAPH API ────────────────────────────────────────────────────────────

_page_token_cache: dict = {}


async def get_page_token(page_id: str) -> str | None:
    """Get a Page Access Token via the system user's /accounts endpoint."""
    if page_id in _page_token_cache:
        return _page_token_cache[page_id]
    try:
        # Get system user ID first
        async with httpx.AsyncClient(timeout=15) as client:
            me = await client.get("https://graph.facebook.com/v18.0/me",
                                  params={"access_token": META_SYSTEM_TOKEN})
            user_id = me.json().get("id")
            if not user_id:
                return None
            # List pages with their tokens
            r = await client.get(f"https://graph.facebook.com/v18.0/{user_id}/accounts",
                                 params={"access_token": META_SYSTEM_TOKEN})
            for page in r.json().get("data", []):
                if page["id"] == page_id:
                    _page_token_cache[page_id] = page["access_token"]
                    log.info(f"Got page token for {page['name']}")
                    return page["access_token"]
    except Exception as e:
        log.error(f"Page token error: {e}")
    return None


async def _get_public_image_url(image_bytes: bytes) -> str | None:
    """Host image on Mark's server and return a public URL via Cloudflare tunnel."""
    try:
        # Convert PNG to JPEG for Instagram compatibility
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=92)
        jpeg_bytes = buf.getvalue()

        image_id = str(uuid.uuid4())[:12]
        _temp_images[image_id] = jpeg_bytes
        public_url = f"{RAILWAY_URL}/img/{image_id}"
        log.info(f"Hosting image at {public_url} ({len(jpeg_bytes)} bytes)")
        return public_url
    except Exception as e:
        log.error(f"Public URL generation error: {e}")
    return None


async def upload_image_to_ig(ig_account_id: str, image_bytes: bytes, caption: str, is_carousel_item: bool = False) -> str | None:
    """Upload image to Instagram as a container. Returns container ID."""
    # Instagram requires a public URL — upload to Telegram first to get one
    image_url = await _get_public_image_url(image_bytes)
    if not image_url:
        log.error("Could not get public URL for IG upload")
        return None

    upload_url = f"https://graph.facebook.com/v18.0/{ig_account_id}/media"
    params = {
        "access_token": META_SYSTEM_TOKEN,
        "image_url": image_url,
        "is_carousel_item": "true" if is_carousel_item else "false",
    }
    if caption and not is_carousel_item:
        params["caption"] = caption

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(upload_url, params=params)
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
            # Wait for Meta to process media, then publish with retries
            pub_url = f"https://graph.facebook.com/v18.0/{ig_account_id}/media_publish"
            pub_params = {"access_token": META_SYSTEM_TOKEN, "creation_id": carousel_id}
            for attempt in range(5):
                await asyncio.sleep(5 + attempt * 3)  # 5s, 8s, 11s, 14s, 17s
                r2 = await client.post(pub_url, params=pub_params)
                result = r2.json()
                if "id" in result:
                    return result
                if result.get("error", {}).get("code") != 9007:
                    return result  # Different error, don't retry
                log.info(f"IG carousel not ready, retry {attempt+1}/5...")
            return result  # Return last attempt result
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
            for attempt in range(5):
                await asyncio.sleep(3 + attempt * 2)
                r = await client.post(pub_url, params=params)
                result = r.json()
                if "id" in result:
                    return result
                if result.get("error", {}).get("code") != 9007:
                    return result
                log.info(f"IG single not ready, retry {attempt+1}/5...")
            return result
    except Exception as e:
        return {"error": str(e)}


async def publish_facebook(page_id: str, image_bytes: bytes, caption: str) -> dict:
    """Post an image to a Facebook Page using page access token."""
    page_token = await get_page_token(page_id)
    if not page_token:
        return {"error": "Could not get page token"}

    try:
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=92)
        jpeg_bytes = buf.getvalue()
    except Exception:
        jpeg_bytes = image_bytes

    url = f"https://graph.facebook.com/v18.0/{page_id}/photos"
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                url,
                params={"access_token": page_token, "message": caption},
                files={"source": ("post.jpg", jpeg_bytes, "image/jpeg")},
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
    scope = "w_member_social"
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
    global LI_ACCESS_TOKEN, LI_REFRESH_TOKEN, LI_TOKEN_EXPIRY, _li_person_id

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

    # ── FACEBOOK ──
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

    # Save to Google Drive
    if images:
        cap_text = f"Instagram:\n{ig_caption}\n\nLinkedIn:\n{cap_li}" if cap_li else ig_caption
        drive_ok = await save_to_drive(brand, images, cap_text, ct)
        results["drive_saved"] = drive_ok
        if drive_ok:
            log.info(f"Saved {brand} content to Mark Marketing Drive folder")
        else:
            log.warning(f"Failed to save {brand} content to Drive")

    # Send full-quality images as Telegram documents (uncompressed)
    if images:
        brand_name = BRANDS[brand]["name"]
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")
        for i, img_bytes in enumerate(images):
            try:
                fname = f"{brand_name}_{ct}_{ts}_slide{i+1}.png"
                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
                async with httpx.AsyncClient(timeout=30) as client:
                    await client.post(url, data={"chat_id": TELEGRAM_CHAT_ID}, files={"document": (fname, img_bytes, "image/png")})
            except Exception as e:
                log.error(f"Document send error: {e}")
        # Send caption as text file
        cap_text = f"Instagram:\n{ig_caption}\n\nLinkedIn:\n{cap_li}" if cap_li else ig_caption
        try:
            cap_fname = f"{brand_name}_{ct}_{ts}_caption.txt"
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
            async with httpx.AsyncClient(timeout=30) as client:
                await client.post(url, data={"chat_id": TELEGRAM_CHAT_ID}, files={"document": (cap_fname, cap_text.encode(), "text/plain")})
        except Exception:
            pass
        results["files_sent"] = True

    # If IG posting failed, alert for manual posting
    ig_result = results.get("instagram", {})
    if isinstance(ig_result, dict) and "error" in str(ig_result):
        try:
            await send_telegram(f"⚠️ *{BRANDS[brand]['name']} — IG auto-post failed*\nFiles sent above — post manually.")
        except Exception:
            pass

    # Record to posting log so Alex can query what was published
    platform_count = sum(1 for k, v in results.items() if k not in ("drive_saved", "files_sent") and "error" not in str(v))
    total_platforms = sum(1 for k in results if k not in ("drive_saved", "files_sent"))
    posting_log.append({
        "brand": brand,
        "brand_name": BRANDS[brand]["name"],
        "content_type": ct,
        "topic": content.get("_topic", content.get("topic", "")),
        "platforms_ok": platform_count,
        "platforms_total": total_platforms,
        "platform_results": {k: ("ok" if "error" not in str(v) else str(v)) for k, v in results.items() if k not in ("drive_saved", "files_sent")},
        "drive_saved": results.get("drive_saved", False),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

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


async def _collect_images_recursive(folder_id: str, max_depth: int = 2, _depth: int = 0, _limit: int = 10) -> list:
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
            if len(images) >= _limit:
                return images
        elif mime == "application/vnd.google-apps.folder":
            subfolder_ids.append(f["id"])
    for sub_id in subfolder_ids:
        if len(images) >= _limit:
            break
        images.extend(await _collect_images_recursive(sub_id, max_depth, _depth + 1, _limit - len(images)))
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


def _resize_image_bytes(img_bytes: bytes, max_bytes: int = 4_000_000) -> bytes:
    """Resize image if it exceeds max_bytes using Pillow or simple quality reduction."""
    if len(img_bytes) <= max_bytes:
        return img_bytes
    try:
        from PIL import Image
        import io
        img = Image.open(io.BytesIO(img_bytes))
        # Resize to max 1200px on longest side
        max_dim = 1200
        if max(img.size) > max_dim:
            ratio = max_dim / max(img.size)
            new_size = (int(img.size[0] * ratio), int(img.size[1] * ratio))
            img = img.resize(new_size, Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=75)
        result = buf.getvalue()
        log.info(f"Resized image from {len(img_bytes)} to {len(result)} bytes")
        return result
    except ImportError:
        log.warning("Pillow not installed — skipping oversized image")
        return img_bytes if len(img_bytes) <= max_bytes else b""


async def _fetch_images_as_b64(image_files: list) -> list[str]:
    """Download a list of Drive image files, resize if needed, return as base64."""
    images_b64 = []
    for f in image_files:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.get(
                    f"https://www.googleapis.com/drive/v3/files/{f['id']}",
                    params={"alt": "media", "key": GDRIVE_API_KEY},
                )
                if r.status_code == 200:
                    resized = _resize_image_bytes(r.content)
                    if resized:
                        images_b64.append(base64.b64encode(resized).decode("utf-8"))
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

GDRIVE_CLIENT_ID = os.getenv("GDRIVE_CLIENT_ID", "")
GDRIVE_CLIENT_SECRET = os.getenv("GDRIVE_CLIENT_SECRET", "")
GDRIVE_REFRESH_TOKEN = os.getenv("GDRIVE_REFRESH_TOKEN", "")


async def _get_drive_upload_token() -> str | None:
    """Get an OAuth2 access token using the refresh token from Sarah's Gmail account."""
    if not GDRIVE_REFRESH_TOKEN:
        log.warning("GDRIVE_REFRESH_TOKEN not set — cannot upload to Drive")
        return None
    now = int(time.time())
    if _drive_token_cache["token"] and _drive_token_cache["expires"] > now + 60:
        return _drive_token_cache["token"]
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post("https://oauth2.googleapis.com/token", data={
                "client_id": GDRIVE_CLIENT_ID,
                "client_secret": GDRIVE_CLIENT_SECRET,
                "refresh_token": GDRIVE_REFRESH_TOKEN,
                "grant_type": "refresh_token",
            })
            tok = r.json()
            if "access_token" not in tok:
                log.error(f"Drive token refresh failed: {tok}")
                return None
            _drive_token_cache["token"] = tok["access_token"]
            _drive_token_cache["expires"] = now + tok.get("expires_in", 3600)
            return _drive_token_cache["token"]
    except Exception as e:
        log.error(f"Drive OAuth refresh error: {e}")
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
                    "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&supportsAllDrives=true",
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
            [{"text": "🚀 POST NOW", "callback_data": f"post_now|{brand}"},
             {"text": "🕕 POST 6PM", "callback_data": f"schedule_6pm|{brand}"}],
            [{"text": "❌ Reject", "callback_data": f"reject_single|{brand}"},
             {"text": "🔄 Regenerate", "callback_data": f"regen_single|{brand}|{batch_id or ''}|{idx or ''}"}],
        ]}
    else:
        markup = {"inline_keyboard": [
            [{"text": "🚀 POST NOW", "callback_data": f"post_now|{brand}|{batch_id}|{idx}"},
             {"text": "🕕 POST 6PM", "callback_data": f"schedule_6pm|{brand}|{batch_id}|{idx}"}],
            [{"text": "⏭ Skip", "callback_data": f"skip_one|{brand}|{batch_id}|{idx}"},
             {"text": "🔄 Regenerate", "callback_data": f"regen_one|{brand}|{batch_id}|{idx}"}],
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

    if not brand:
        await answer_callback(cb_id, "Expired — regenerate this post")
        await send_telegram("⚠️ This approval expired (Mark restarted). Please regenerate the content.")
        return

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
        await answer_callback(cb_id, "Posting now ✅")
        pending_approvals.pop(msg_id, None)
        await send_telegram(f"_Posting {BRANDS[brand]['name']} content now..._")
        cached_b64 = content.pop("_rendered_images_b64", [])
        if cached_b64:
            content["_cached_images"] = [base64.b64decode(b) for b in cached_b64]
        results = await post_content(content, brand)
        platform_count = sum(1 for k, v in results.items() if k != "drive_saved" and "error" not in str(v))
        total_platforms = sum(1 for k in results if k != "drive_saved")
        drive_line = "\n📁 Saved to Mark Marketing" if results.get("drive_saved") else "\n⚠️ Drive save failed"
        await send_telegram(f"✅ *Posted — {BRANDS[brand]['name']}*\n{platform_count}/{total_platforms} platforms posted{drive_line}")

    elif action == "schedule_6pm":
        await answer_callback(cb_id, "Scheduled for 6pm Dubai 🕕")
        pending_approvals.pop(msg_id, None)
        # Calculate seconds until next 6pm Dubai (UTC+4 = 14:00 UTC)
        now_utc = datetime.now(timezone.utc)
        today_6pm = now_utc.replace(hour=14, minute=0, second=0, microsecond=0)
        if now_utc >= today_6pm:
            target = today_6pm + timedelta(days=1)
        else:
            target = today_6pm
        delay_seconds = (target - now_utc).total_seconds()
        target_dubai = (target + timedelta(hours=4)).strftime("%a %d %b %H:%M")
        await send_telegram(f"🕕 *Scheduled — {BRANDS[brand]['name']}*\nWill post at 6:00 PM Dubai ({target_dubai})")

        async def _delayed_post():
            await asyncio.sleep(delay_seconds)
            cached_b64 = content.pop("_rendered_images_b64", [])
            if cached_b64:
                content["_cached_images"] = [base64.b64decode(b) for b in cached_b64]
            results = await post_content(content, brand)
            platform_count = sum(1 for k, v in results.items() if k != "drive_saved" and "error" not in str(v))
            total_platforms = sum(1 for k in results if k != "drive_saved")
            drive_line = "\n📁 Saved to Mark Marketing" if results.get("drive_saved") else "\n⚠️ Drive save failed"
            await send_telegram(f"✅ *6pm post live — {BRANDS[brand]['name']}*\n{platform_count}/{total_platforms} platforms posted{drive_line}")

        asyncio.create_task(_delayed_post())

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
        await answer_callback(cb_id, "Regenerating with new photos...")
        pending_approvals.pop(msg_id, None)
        topic = content.get("_topic", "")
        ct = content.get("_content_type", "carousel")
        # Add timestamp to force different photo selection
        new_topic = topic + f" _v{int(time.time())}"
        await _run_single(brand, ct, new_topic)

    elif action == "reject_single":
        await answer_callback(cb_id, "Rejected")
        pending_approvals.pop(msg_id, None)
        await send_telegram("❌ Rejected. Send a new `breaking:` command.")


# ── FASTAPI ENDPOINTS (Alex integration) ─────────────────────────────────────

from fastapi.responses import Response


@app.get("/img/{image_id}")
async def serve_temp_image(image_id: str):
    """Serve a temporary image for Instagram upload."""
    img_bytes = _temp_images.get(image_id)
    if not img_bytes:
        return JSONResponse({"error": "not found"}, status_code=404)
    return Response(content=img_bytes, media_type="image/jpeg")


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


@app.get("/posting_log")
async def api_posting_log():
    """Return today's posting log so Alex can query what was actually published."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_posts = [p for p in posting_log if p["timestamp"].startswith(today)]
    return {
        "date": today,
        "total_posts": len(today_posts),
        "posts": today_posts,
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
        await send_telegram("⚠️ No topic provided — Alex must provide the content angle. Use Alex to generate content.")
        return
    # Track for regeneration
    _last_render.update({"brand": brand, "content_type": content_type, "topic": topic})
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
    # Show caption + POST NOW / POST 6PM buttons
    cap = content.get("caption_instagram", "")[:300]
    # Sanitize markdown characters that break Telegram
    safe_cap = cap.replace("*", "").replace("_", "").replace("`", "").replace("[", "").replace("]", "")
    brand_name = BRANDS[brand]["name"]
    markup = {"inline_keyboard": [
        [{"text": "🚀 POST NOW", "callback_data": f"post_now|{brand}"},
         {"text": "🕕 POST 6PM", "callback_data": f"schedule_6pm|{brand}"}],
        [{"text": "❌ Reject", "callback_data": f"reject_single|{brand}"},
         {"text": "🔄 Regenerate", "callback_data": f"regen_single|{brand}||"}],
    ]}
    msg = await send_telegram(
        f"{brand_name}\n\n{safe_cap}\n\nTap POST NOW to publish immediately, or POST 6PM to schedule for 6pm Dubai.",
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


# ── LAST RENDER TRACKING (for regeneration) ──────────────────────────────────
_last_render: dict = {}  # {brand, content_type, topic}


# ── TELEGRAM LISTENER ─────────────────────────────────────────────────────────

async def telegram_listener():
    offset = None
    log.info("Mark v3 online — skipping Telegram startup message")

    while True:
        try:
            updates = await get_updates(offset)
            for update in updates:
                offset = update["update_id"] + 1
                if "callback_query" in update:
                    await handle_callback(update)
                    continue
                # Handle direct messages from GG
                msg = update.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))
                text = msg.get("text", "").strip()
                if not text or chat_id != str(TELEGRAM_CHAT_ID):
                    continue
                text_lower = text.lower()
                # Regenerate with different photos
                if any(w in text_lower for w in ["regenerate", "different photo", "new photo", "different background", "redo", "try again", "new render"]):
                    if _last_render:
                        await send_telegram("_Regenerating with different photos..._")
                        # Add random suffix to force different photo selection
                        new_topic = _last_render["topic"] + f" _v{int(time.time())}"
                        await _run_single(_last_render["brand"], _last_render["content_type"], new_topic)
                    else:
                        await send_telegram("No previous render to regenerate. Ask Alex for content first.")
                elif any(w in text_lower for w in ["darker", "too light", "more dark"]):
                    await send_telegram("Got it — noted for next render. Say 'regenerate' and I'll try again.")
                elif any(w in text_lower for w in ["lighter", "too dark", "brighter"]):
                    await send_telegram("Got it — noted for next render. Say 'regenerate' and I'll try again.")
        except Exception as e:
            log.error(f"Mark listener error: {e}")
            await asyncio.sleep(5)


# ── LINKEDIN TOKEN REFRESH ────────────────────────────────────────────────────

async def refresh_linkedin_token():
    global LI_ACCESS_TOKEN, LI_REFRESH_TOKEN, LI_TOKEN_EXPIRY
    if not LI_REFRESH_TOKEN:
        return
    remaining = LI_TOKEN_EXPIRY - time.time()
    if remaining > 432000:          # more than 5 days left
        return
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://www.linkedin.com/oauth/v2/accessToken",
                data={
                    "grant_type":    "refresh_token",
                    "refresh_token": LI_REFRESH_TOKEN,
                    "client_id":     LI_CLIENT_ID,
                    "client_secret": LI_CLIENT_SECRET,
                },
            )
            resp.raise_for_status()
            data = resp.json()
        LI_ACCESS_TOKEN  = data["access_token"]
        LI_REFRESH_TOKEN = data.get("refresh_token", LI_REFRESH_TOKEN)
        LI_TOKEN_EXPIRY  = int(time.time()) + data.get("expires_in", 5184000)
        days_valid = data.get("expires_in", 5184000) // 86400
        await send_telegram(f"LinkedIn token refreshed — valid for {days_valid} days")
        log.info(f"LinkedIn token refreshed, expires in {days_valid} days")
    except Exception as e:
        log.error(f"LinkedIn token refresh failed: {e}")
        await send_telegram(
            f"LinkedIn token refresh FAILED — manual re-auth needed at {RAILWAY_URL}/linkedin/auth"
        )


async def linkedin_token_monitor():
    while True:
        try:
            await refresh_linkedin_token()
        except Exception as e:
            log.error(f"LinkedIn token monitor error: {e}")
        await asyncio.sleep(86400)


# ── STARTUP ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    _required = ["CLAUDE_API_KEY", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "META_SYSTEM_TOKEN"]
    _missing = [v for v in _required if not os.getenv(v)]
    if _missing:
        log.error(f"MISSING REQUIRED ENV VARS: {', '.join(_missing)} — Mark cannot start properly")

    asyncio.create_task(telegram_listener())
    asyncio.create_task(linkedin_token_monitor())
    log.info("Mark v3 — AI Marketing Bot — online")
