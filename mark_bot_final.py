"""
Mark v2 — Autonomous AI Marketing Brain
Powered by Claude API | Playwright | Meta Graph API | LinkedIn API
Google Drive | Telegram | FastAPI

Brands:
  - Nucassa Real Estate  → Instagram + Facebook + LinkedIn (@nucassadubai)
  - Nucassa Holdings     → Instagram + LinkedIn (@nucassaholdings_ltd)
  - ListR.ae             → Instagram + Facebook (@listr.ae)

Architecture:
  - GG talks to Mark directly on Telegram
  - Mark autonomously searches news, creates content, renders carousels, publishes
  - Alex is NOT involved in content — she only monitors system health
  - Mark renders via HTML/CSS + Playwright (no more Pillow)
  - Background image library: templates/backgrounds/
"""

from __future__ import annotations

import os

import sentry_sdk
sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN", ""),
    environment=os.getenv("SENTRY_ENVIRONMENT", "production"),
    traces_sample_rate=0.1,
    send_default_pii=False,
)

# BetterStack log shipping — active when BETTERSTACK_SOURCE_TOKEN is set.
if os.getenv("BETTERSTACK_SOURCE_TOKEN"):
    try:
        import logging as _logging
        from logtail import LogtailHandler
        _logging.getLogger().addHandler(LogtailHandler(
            source_token=os.environ["BETTERSTACK_SOURCE_TOKEN"],
            host="https://" + os.environ.get("BETTERSTACK_HOST", "s2392783.eu-fsn-3.betterstackdata.com"),
        ))
    except Exception:
        pass  # observability must never crash the bot

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
from anthropic_limiter import anthropic_post, AnthropicBudgetExceeded  # noqa: E402

load_dotenv()

# Startup validation — Mark refuses to run if required env vars are missing
# or contain placeholder values.
from schemas import REQUIRED_ENV_VARS, AIRTABLE_SCHEMAS, EXTERNAL_APIS
from validator import validate_environment
_failures = validate_environment(REQUIRED_ENV_VARS, AIRTABLE_SCHEMAS, EXTERNAL_APIS)
if _failures:
    raise RuntimeError("🚨 STARTUP VALIDATION FAILED: " + "; ".join(_failures))

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

# LinkedIn — multi-account, multi-app (Nucassa app for gg/emma, Forza app for sue/gg_forza)
LI_CLIENT_ID            = os.getenv("LI_CLIENT_ID", "")
LI_CLIENT_SECRET        = os.getenv("LI_CLIENT_SECRET", "")
LI_FORZA_CLIENT_ID      = os.getenv("LI_FORZA_CLIENT_ID", "")
LI_FORZA_CLIENT_SECRET  = os.getenv("LI_FORZA_CLIENT_SECRET", "")
# Separate app dedicated to Community Management API (LinkedIn requires CMA be the ONLY product on its app)
LI_FORZA_CMA_CLIENT_ID      = os.getenv("LI_FORZA_CMA_CLIENT_ID", "")
LI_FORZA_CMA_CLIENT_SECRET  = os.getenv("LI_FORZA_CMA_CLIENT_SECRET", "")
LI_NUCASSA_RE_PAGE      = os.getenv("LI_NUCASSA_RE_PAGE", "90919312")
LI_HOLDINGS_PAGE        = os.getenv("LI_HOLDINGS_PAGE", "109941216")
LI_SCOPE                = os.getenv("LI_SCOPE", "w_member_social")
LI_TOKENS_FILE          = os.path.join(os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data"), "li_tokens.json")

# Three LinkedIn apps: Nucassa (gg/emma), Forza personal (sue), Forza CMA-only (sue_cma for company page)
LI_FORZA_ACCOUNTS      = {"sue"}             # Forza personal app (Share + OIDC)
LI_FORZA_CMA_ACCOUNTS  = {"sue_cma"}         # Forza CMA-only app (w_organization_social only)


def _li_app_creds(account: str) -> tuple[str, str]:
    """Return (client_id, client_secret) for the LinkedIn app that owns this account's OAuth."""
    if account in LI_FORZA_CMA_ACCOUNTS:
        return LI_FORZA_CMA_CLIENT_ID, LI_FORZA_CMA_CLIENT_SECRET
    if account in LI_FORZA_ACCOUNTS:
        return LI_FORZA_CLIENT_ID, LI_FORZA_CLIENT_SECRET
    return LI_CLIENT_ID, LI_CLIENT_SECRET


def _li_scope_for(account: str) -> str:
    """Return the OAuth scope string for this account's app."""
    if account in LI_FORZA_CMA_ACCOUNTS:
        return "w_organization_social"  # CMA-only app: nothing else is allowed
    return LI_SCOPE

def _load_li_tokens() -> dict:
    """Load per-account LinkedIn tokens from persistent volume, fall back to env vars."""
    try:
        with open(LI_TOKENS_FILE, "r") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    except Exception as e:
        log.warning(f"Could not load LI tokens from {LI_TOKENS_FILE}: {e}")
    # Env var fallback (for first-run or if volume is missing)
    tokens = {}
    for acct in ("gg", "emma"):
        token = os.getenv(f"LI_TOKEN_{acct.upper()}", "")
        person = os.getenv(f"LI_PERSON_{acct.upper()}", "")
        if token:
            tokens[acct] = {
                "access_token": token,
                "refresh_token": os.getenv(f"LI_REFRESH_{acct.upper()}", ""),
                "person_id": person,
                "expiry": int(os.getenv(f"LI_EXPIRY_{acct.upper()}", "0")),
                "name": os.getenv(f"LI_NAME_{acct.upper()}", acct.title()),
            }
    return tokens

def _save_li_tokens():
    """Persist LI tokens to volume so they survive Railway redeploys."""
    try:
        os.makedirs(os.path.dirname(LI_TOKENS_FILE), exist_ok=True)
        with open(LI_TOKENS_FILE, "w") as f:
            json.dump(LI_TOKENS, f)
    except Exception as e:
        log.error(f"Failed to save LI tokens: {e}")

LI_TOKENS: dict = _load_li_tokens()

# Canva
CANVA_EMAIL         = os.getenv("CANVA_EMAIL", "")
CANVA_PASSWORD      = os.getenv("CANVA_PASSWORD", "")

# Google Drive
GDRIVE_API_KEY        = os.getenv("GDRIVE_API_KEY")
# Sarah's Projects folder — the ONLY Drive folder Mark touches for images
GDRIVE_FOLDER_ID        = "1QoloKwEVPojBMfkTcSkbRL1ryo0a8jif"
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

# Mark Marketing brand subfolders in Google Drive (for saving rendered content)
GDRIVE_MARKETING_BRAND_FOLDERS = {
    "nucassa_re": "1h9-rHbwy_u781I5JK9AfC9Ig4UGgJ9lV",
    "nucassa_holdings": "1jZXVpp4zxKD4my1CU2NBEerlH81iNpJ2",
    "listr": "1DTSCTvgN_nMR9Wn71vzGHphF8QKXfZbM",
    "forza": os.getenv("GDRIVE_FORZA_FOLDER", ""),   # set when Drive folder created
}

# Instagram Account IDs (from Meta Business Manager)
IG_NUCASSA_RE       = "17841457839005074"   # @nucassadubai
IG_HOLDINGS         = "17841406888818689"   # @nucassaholdings_ltd
IG_LISTR            = "17841475489496432"   # @listr.ae
IG_FORZA            = os.getenv("IG_FORZA", "")      # @forza_ai — set when IG account created

# Facebook Page IDs
FB_NUCASSA_RE       = "106173405736149"     # Nucassa Real Estate Dubai
FB_HOLDINGS         = "963897483477807"     # Nucassa.Holdings
FB_LISTR            = "1085489144643633"    # ListR
FB_FORZA            = os.getenv("FB_FORZA", "")      # Forza Systems — set when FB page created

# LinkedIn Forza Page ID
LI_FORZA_PAGE       = os.getenv("LI_FORZA_PAGE", "") # set when LinkedIn company page created

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
        "li_personal_accounts": [],
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
        "li_personal_accounts": ["gg", "emma"],
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
        "li_personal_accounts": [],
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
    "forza": {
        "name": "Forza",
        "handle": "@forza_ai_",
        "website": "forzasystems.ai",
        "platforms": ["linkedin", "instagram", "facebook"],  # LinkedIn primary for B2B
        "ig_account_id": IG_FORZA,
        "fb_page_id": FB_FORZA,
        "li_page_id": LI_FORZA_PAGE,
        "li_admin_account": "sue_cma",   # Forza company page via dedicated CMA-only app
        "li_personal_accounts": ["sue"], # Sue's personal feed via the other Forza app
        "tone": (
            "premium, operator-led, direct. No startup hype, no emojis, no exclamation marks. "
            "Classical serif confidence. Talks about systems, infrastructure, operational leverage. "
            "Goldman Sachs meets Dubai operator. Assumes the reader is running a real business."
        ),
        "cta": "Book a Systems Audit at forzasystems.ai",
        "color_primary": "#0A0A0A",      # ink black
        "color_accent":  "#C5A86C",      # rich gold
        "color_secondary": "#F7F3EA",    # ivory
        "font_headline": "Cinzel SemiBold",
        "font_body": "Inter Medium",
        "logo_style": "forza",
        "topics": [
            "The four-hour follow-up rule — why it quietly kills deals",
            "Revenue Infrastructure — the five layers every serious business needs",
            "Why AI operations beat a growing SDR team on cost and consistency",
            "What a Systems Audit actually reveals about your business",
            "From WhatsApp chaos to CRM clarity — a real operator case study",
            "The hidden cost of founder bandwidth — numbers most operators ignore",
            "Operating Pictures — why serious founders get briefed twice a day",
            "Sub-60-second response time — what it changes for your pipeline",
            "The case for selective intake — why we take fewer clients by design",
            "Institutional outreach without a BDR team on payroll",
            "Instant property valuations as a marketplace differentiator",
            "Brand Infrastructure — daily content without an agency retainer",
            "Team discipline without micromanagement — how the system runs it",
            "When growth becomes chaos — the operator's decision matrix",
        ],
        "posting_weekdays": [1, 3],      # Tue / Thu — B2B cadence, less noisy
        "post_hour_utc": 13,             # 5pm Dubai — late-afternoon decision-maker scroll
    },
}

# ── IN-MEMORY STATE ───────────────────────────────────────────────────────────
pending_approvals: dict = {}   # telegram_msg_id → {content, brand, batch_id, idx}
_temp_images: dict = {}        # image_id → bytes (temporary image hosting for IG uploads)
pending_batches: dict = {}     # batch_id → list[content_dict]
last_batch: dict = {}          # brand → list[content_dict]
li_oauth_states: dict = {}     # state → brand (for OAuth flow)
_last_rendered: dict = {}      # brand → {"content": dict, "images": list[bytes], "timestamp": float}
POSTING_LOG_FILE = os.path.join(os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data"), "mark_posting_log.json")

def _load_posting_log() -> list:
    """Load posting log from persistent storage — survives Railway redeploys if volume attached."""
    try:
        with open(POSTING_LOG_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []
    except Exception as e:
        log.warning(f"Could not load posting log from {POSTING_LOG_FILE}: {e}")
        return []

def _save_posting_log():
    """Persist posting log to file."""
    try:
        os.makedirs(os.path.dirname(POSTING_LOG_FILE), exist_ok=True)
        with open(POSTING_LOG_FILE, "w") as f:
            json.dump(posting_log, f)
    except Exception as e:
        log.error(f"Failed to save posting log: {e}")

posting_log: list = _load_posting_log()

app = FastAPI()

# ── MARK SYSTEM PROMPT ────────────────────────────────────────────────────────
MARK_SYSTEM_PROMPT = """
You are Mark, the AI marketing brain for FOUR brands. Three are GG's Dubai real estate businesses. The fourth is Forza — the consultancy that productises the entire bot fleet for external clients.

When GG says "Forza" in chat, he is referring to BRAND #4 below. NEVER interpret "Forza" as hype or an exclamation. It's a business name.

BRANDS:
1. Nucassa Real Estate (@nucassadubai) — www.nucassa.com
   Facts, Dubai property data, market stats, lifestyle. Bold and authoritative.
2. Nucassa Holdings Ltd (@nucassaholdings_ltd) — www.nucassa.holdings
   Institutional investment platform. ADGM SPV. $1M+ investors. DBS custody.
3. ListR.ae (@listr.ae) — ListR.ae
   UAE property marketplace. No agency fees. Direct buyer-seller deals.
4. Forza (@forza_ai_) — forzasystems.ai
   Business operating systems for growth-stage service companies. Custom AI infrastructure for revenue, brand, team, and founder intelligence. Audience: founders of brokerages, clinics, law firms, recruitment, institutional investment, agencies. NOT Dubai RE — global B2B consultancy. Operator-led tone. NEVER say chatbot, automation agency, AI agency, consultant, startup, disrupt. NEVER use emojis or exclamation marks. Think Goldman Sachs sales memo, not SaaS launch tweet.

FORZA VISUAL BRAND:
- Background: #0A0A0A (ink black) with #14110c or #F7F3EA (ivory) section blocks
- Accent: #C5A86C (rich gold) for stats, numbers, lines, key words
- Typography: Cinzel SemiBold headlines (classical serif), Cormorant Garamond body, Inter UI
- Portrait 4:5 (1080x1350px) — same ratio as Nucassa, different palette
- Logo: geometric F chevron (three angled parallelograms in gold gradient) + FORZA Cinzel wordmark
- No lifestyle photography — abstract textures, minimal gold linework, or studio-lit operator portraits. No Dubai skyline.

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
  "brand": "nucassa_re|nucassa_holdings|listr|forza",
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

async def call_claude(prompt: str, pdf_b64: str = None, max_retries: int = 3) -> str:
    content = []
    if pdf_b64:
        content.append({"type": "document", "source": {
            "type": "base64", "media_type": "application/pdf", "data": pdf_b64
        }})
    content.append({"type": "text", "text": prompt})
    log.info(f"[call_claude] Sending prompt ({len(prompt)} chars): {prompt[:300]}...")
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                r = await anthropic_post(client,
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
            log.info(f"[call_claude] HTTP status: {r.status_code} (attempt {attempt+1}/{max_retries})")
            if r.status_code in (429, 529, 500, 502, 503, 504):
                if attempt < max_retries - 1:
                    wait = 5 * (attempt + 1)
                    log.warning(f"[call_claude] Retryable error {r.status_code}, waiting {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                log.error(f"[call_claude] All {max_retries} attempts failed with {r.status_code}")
                return None
            resp_json = r.json()
            if "content" not in resp_json:
                log.error(f"[call_claude] Claude API error response: {json.dumps(resp_json, indent=2)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
                    continue
                return None
            raw_text = resp_json["content"][0]["text"]
            log.info(f"[call_claude] Raw response ({len(raw_text)} chars): {raw_text[:500]}...")
            return raw_text
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError) as e:
            log.warning(f"[call_claude] Network error attempt {attempt+1}/{max_retries}: {type(e).__name__}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5)
                continue
            return None
        except Exception as e:
            log.error(f"[call_claude] Exception calling Claude API: {type(e).__name__}: {e}")
            return None
    return None


def parse_json(raw: str) -> dict | None:
    if not raw:
        log.error("[parse_json] Received empty/None input")
        return None

    # Step 1: Strip markdown code fences and whitespace
    cleaned = re.sub(r'```(?:json)?\s*', '', raw).strip()
    cleaned = cleaned.rstrip('`').strip()
    log.info(f"[parse_json] Attempting to parse ({len(cleaned)} chars): {cleaned[:300]}...")

    # Step 2: Try direct parse of cleaned text
    try:
        result = json.loads(cleaned)
        log.info("[parse_json] Direct parse succeeded")
        return result
    except json.JSONDecodeError as e:
        log.warning(f"[parse_json] Direct parse failed: {e}")

    # Step 3: Try extracting JSON between first { and last }
    try:
        first_brace = cleaned.index('{')
        last_brace = cleaned.rindex('}')
        json_str = cleaned[first_brace:last_brace + 1]
        result = json.loads(json_str)
        log.info("[parse_json] Brace extraction parse succeeded")
        return result
    except (ValueError, json.JSONDecodeError) as e:
        log.error(f"[parse_json] Brace extraction failed: {e}")

    # Step 4: Try regex match as last resort
    try:
        m = re.search(r'\{[\s\S]*\}', raw)
        if m:
            result = json.loads(m.group())
            log.info("[parse_json] Regex extraction parse succeeded")
            return result
    except json.JSONDecodeError as e:
        log.error(f"[parse_json] Regex extraction failed: {e}")

    log.error(f"[parse_json] All parse attempts failed. Raw input: {raw[:500]}")
    return None


async def generate_single(brand: str, content_type: str, topic: str = "", pdf_b64: str = None, pdf_name: str = "") -> dict | None:
    cfg = BRANDS[brand]
    if not topic and not pdf_b64:
        log.warning(f"generate_single called without topic for {brand} — Alex must provide the angle")
        return None

    async def _call_with_images(prompt: str, ref_images: list[str], extra_b64: str = None) -> str:
        """Call Claude with optional reference images and/or a PDF. Retries on 429/529."""
        content_parts = []
        for img_b64 in ref_images:
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
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=60) as client:
                    r = await anthropic_post(client,
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
                log.info(f"[_call_with_images] HTTP status: {r.status_code} (attempt {attempt+1}/3)")
                if r.status_code in (429, 529, 500, 502, 503, 504):
                    if attempt < 2:
                        wait = 5 * (attempt + 1)
                        log.warning(f"[_call_with_images] Retryable {r.status_code}, waiting {wait}s...")
                        await asyncio.sleep(wait)
                        continue
                    return None
                resp_json = r.json()
                if "content" not in resp_json:
                    log.error(f"[_call_with_images] Claude API error: {json.dumps(resp_json, indent=2)}")
                    return None
                raw_text = resp_json["content"][0]["text"]
                log.info(f"[_call_with_images] Raw response ({len(raw_text)} chars): {raw_text[:500]}...")
                return raw_text
            except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
                log.warning(f"[_call_with_images] Timeout attempt {attempt+1}/3: {e}")
                if attempt < 2:
                    await asyncio.sleep(5)
                    continue
                return None
        return None

    if pdf_b64:
        # PDF brochure post
        prompt = f"""Read the attached developer brochure and create ORIGINAL {content_type} content for {cfg['name']} using the brochure facts.
Do NOT reproduce brochure text — extract the facts and write original content in the brand's voice.
Brand tone: {cfg['tone']} | CTA: {cfg['cta']} | Website: {cfg['website']}
Return JSON only."""
        raw = await _call_with_images(prompt, [], pdf_b64)

    else:
        prompt = f"""Create a {content_type} post for {cfg['name']}.
Topic: {topic}
Tone: {cfg['tone']} | CTA: {cfg['cta']} | Handle: {cfg['handle']} | Website: {cfg['website']}
Use accurate verifiable Dubai real estate data. Return JSON only."""
        raw = await call_claude(prompt)

    if not raw:
        log.error(f"[generate_single] Claude returned no content for {brand}/{content_type} topic='{topic}'")
        # Fallback chain: reels→carousel→static
        if content_type in ("reels", "carousel"):
            fallback_type = "carousel" if content_type == "reels" else "static"
            log.warning(f"[generate_single] {content_type} failed (no response), trying {fallback_type}")
            fb_prompt = f"""Create a {fallback_type} post for {cfg['name']}.
Topic: {topic}
Tone: {cfg['tone']} | CTA: {cfg['cta']} | Handle: {cfg['handle']} | Website: {cfg['website']}
Use accurate verifiable Dubai real estate data. Return JSON only."""
            fb_raw = await call_claude(fb_prompt)
            if fb_raw:
                fb_result = parse_json(fb_raw)
                if fb_result:
                    fb_result["_topic"] = topic
                    fb_result["_brand"] = brand
                    fb_result["_content_type"] = fallback_type
                    fb_result["_fallback_from"] = content_type
                    log.info(f"[generate_single] Fallback {fallback_type} succeeded for {brand}")
                    return fb_result
        return None
    log.info(f"[generate_single] Got raw response for {brand}/{content_type}, parsing JSON...")
    result = parse_json(raw)
    if not result:
        log.error(f"[generate_single] JSON parse failed for {brand}/{content_type}. Raw response: {raw[:1000]}")
        # Fallback chain on parse failure: reels→carousel→static
        fallback_types = []
        if content_type == "reels":
            fallback_types = ["carousel", "static"]
        elif content_type == "carousel":
            fallback_types = ["static"]
        for fb_type in fallback_types:
            log.warning(f"[generate_single] {content_type} parse failed for {brand}, trying {fb_type}")
            fb_prompt = f"""Create a {fb_type} post for {cfg['name']}.
Topic: {topic}
Tone: {cfg['tone']} | CTA: {cfg['cta']} | Handle: {cfg['handle']} | Website: {cfg['website']}
Use accurate verifiable Dubai real estate data. Return JSON only."""
            fb_raw = await call_claude(fb_prompt)
            if fb_raw:
                fb_result = parse_json(fb_raw)
                if fb_result:
                    fb_result["_topic"] = topic
                    fb_result["_brand"] = brand
                    fb_result["_content_type"] = fb_type
                    fb_result["_fallback_from"] = content_type
                    log.info(f"[generate_single] Fallback {fb_type} succeeded for {brand}")
                    return fb_result
        return None
    # Validate reels has required 'script' field
    if content_type == "reels" and "script" not in result:
        log.warning(f"[generate_single] Reels response missing 'script' field for {brand}. Keys: {list(result.keys())}")
        # If Claude returned carousel format instead, use it
        if "slides" in result:
            log.info(f"[generate_single] Reels response has 'slides' — treating as carousel fallback")
            result["_content_type"] = "carousel"
            result["_fallback_from"] = "reels"
        else:
            log.error(f"[generate_single] Reels response has neither 'script' nor 'slides' for {brand}")
            return None
    result["_topic"] = topic
    result["_brand"] = brand
    result["_content_type"] = content_type if "_content_type" not in result else result["_content_type"]
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


# ── SLIDE RENDERER (Pillow + Google Drive + Real Logos) ──────────────────────

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

# ── Google Drive image source (service account auth) ─────────────────────────
# All background images from Sarah's Projects folder. No Unsplash. No Pexels.
_gdrive_service = None


def _get_drive_service():
    """Get or create Google Drive API service using service account credentials."""
    global _gdrive_service
    if _gdrive_service:
        return _gdrive_service

    import base64 as b64mod
    sa_b64 = os.getenv("GDRIVE_SERVICE_ACCOUNT", "")
    sa_raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

    sa_info = None
    if sa_b64:
        try:
            sa_info = json.loads(b64mod.b64decode(sa_b64).decode())
        except Exception as e:
            log.error(f"Failed to decode GDRIVE_SERVICE_ACCOUNT: {e}")
    if not sa_info and sa_raw:
        try:
            sa_info = json.loads(sa_raw)
        except Exception as e:
            log.error(f"Failed to parse GOOGLE_SERVICE_ACCOUNT_JSON: {e}")

    if not sa_info:
        log.error("No Google Drive service account credentials found")
        return None

    try:
        from google.oauth2 import service_account as sa_mod
        from googleapiclient.discovery import build as gbuild
        creds = sa_mod.Credentials.from_service_account_info(
            sa_info, scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        _gdrive_service = gbuild("drive", "v3", credentials=creds)
        log.info(f"Drive service initialized: {sa_info.get('client_email', '?')}")
        return _gdrive_service
    except Exception as e:
        log.error(f"Drive service init error: {e}")
        return None


def _scan_drive_images_flat(folder_id: str, min_size_bytes: int = 500_000) -> list[dict]:
    """Scan Drive folder for ALL images using a single flat query (no recursion).
    Uses Drive API 'parents in' query which searches all descendants.
    Only returns images > min_size_bytes (500KB = real photos, not text slides)."""
    svc = _get_drive_service()
    if not svc:
        return []

    images = []
    page_token = None
    try:
        while True:
            results = svc.files().list(
                q=f"mimeType contains 'image/' and size > {min_size_bytes}",
                corpora="allDrives",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                spaces="drive",
                fields="nextPageToken, files(id, name, size, parents)",
                pageSize=1000,
                pageToken=page_token
            ).execute()

            for f in results.get("files", []):
                images.append({
                    "id": f["id"],
                    "name": f["name"],
                    "size_kb": int(f.get("size", 0)) // 1024
                })

            page_token = results.get("nextPageToken")
            if not page_token:
                break

        log.info(f"Drive flat scan complete: {len(images)} images found")
    except Exception as e:
        log.error(f"Drive flat scan error: {e}")

    return images


def _scan_folder_recursive(folder_id: str, min_size_bytes: int = 500_000) -> list[dict]:
    """Scan a specific folder recursively. Uses batched folder listing."""
    svc = _get_drive_service()
    if not svc:
        return []

    images = []
    folders_to_scan = [folder_id]
    scanned = 0

    while folders_to_scan:
        fid = folders_to_scan.pop(0)
        scanned += 1
        if scanned > 200:  # Safety limit
            log.warning(f"Drive scan hit 200 folder limit, stopping")
            break

        try:
            page_token = None
            while True:
                results = svc.files().list(
                    q=f"'{fid}' in parents and trashed = false",
                    fields="nextPageToken, files(id, name, mimeType, size)",
                    pageSize=1000,
                    pageToken=page_token
                ).execute()

                for f in results.get("files", []):
                    if f["mimeType"] == "application/vnd.google-apps.folder":
                        folders_to_scan.append(f["id"])
                    elif "image" in f.get("mimeType", ""):
                        size = int(f.get("size", 0))
                        if size >= min_size_bytes:
                            images.append({
                                "id": f["id"],
                                "name": f["name"],
                                "size_kb": size // 1024
                            })

                page_token = results.get("nextPageToken")
                if not page_token:
                    break
        except Exception as e:
            log.error(f"Drive scan error for folder {fid}: {e}")

    log.info(f"Drive recursive scan: {len(images)} images in {scanned} folders")
    return images

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


# Headline font: Montserrat ExtraBold (spec requirement)
FONT_HEADLINE = lambda size: _get_font("Montserrat-ExtraBold", size, ["Montserrat-Bold", "Montserrat-SemiBold", "Montserrat", "Arial Bold", "Helvetica-Bold"])
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
    W, H = 1080, 1080
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


# ── Background images — Sarah's Projects folder ONLY ──
# Folder ID: 1QoloKwEVPojBMfkTcSkbRL1ryo0a8jif
# Contains PDF brochures with property photos inside.
# On startup: download PDFs, extract images with PyMuPDF, filter, cache clean photos.

_bg_photo_cache: list[bytes] = []   # Clean property photo bytes
_bg_pdfs_found: int = 0
_bg_extracted: int = 0
_bg_index_ready = False
_bg_recent_idx: list[int] = []


def _find_pdfs_in_sarahs_projects() -> list[dict]:
    """Recursively search ALL subfolders of Sarah's Projects for PDF brochures."""
    svc = _get_drive_service()
    if not svc:
        return []

    pdfs = []
    folders_to_scan = [GDRIVE_FOLDER_ID]
    scanned = 0

    while folders_to_scan:
        fid = folders_to_scan.pop(0)
        scanned += 1
        try:
            page_token = None
            while True:
                results = svc.files().list(
                    q=f"'{fid}' in parents and trashed = false",
                    fields="nextPageToken, files(id, name, mimeType, size)",
                    pageSize=1000,
                    pageToken=page_token
                ).execute()
                for f in results.get("files", []):
                    mt = f.get("mimeType", "")
                    if mt == "application/vnd.google-apps.folder":
                        folders_to_scan.append(f["id"])
                    elif mt == "application/pdf":
                        size = int(f.get("size", 0))
                        if size > 200_000:  # Skip tiny PDFs (<200KB)
                            pdfs.append({"id": f["id"], "name": f["name"], "size": size})
                page_token = results.get("nextPageToken")
                if not page_token:
                    break
        except Exception as e:
            log.error(f"Drive scan error: {e}")

    log.info(f"Sarah's Projects: {len(pdfs)} PDF brochures in {scanned} folders")
    return pdfs


async def _build_background_index():
    """ONE-TIME on startup: download PDFs from Sarah's Projects, extract property photos."""
    global _bg_photo_cache, _bg_pdfs_found, _bg_extracted, _bg_index_ready

    if _bg_index_ready:
        return

    log.info("Extracting property photos from Sarah's Projects PDF brochures...")

    # Step 1: find all PDFs
    pdfs = await asyncio.get_event_loop().run_in_executor(
        None, _find_pdfs_in_sarahs_projects
    )
    _bg_pdfs_found = len(pdfs)

    if not pdfs:
        log.error(f"ZERO PDFs in Sarah's Projects folder ({GDRIVE_FOLDER_ID})")
        _bg_index_ready = True
        return

    # Step 2: download a sample of PDFs, extract images
    sample = random.sample(pdfs, min(15, len(pdfs)))
    svc = _get_drive_service()
    from googleapiclient.http import MediaIoBaseDownload

    for pdf in sample:
        if len(_bg_photo_cache) >= 50:
            break
        try:
            log.info(f"Processing: {pdf['name']} ({pdf['size']//1024}KB)")
            request = svc.files().get_media(fileId=pdf["id"])
            buf = io.BytesIO()
            downloader = MediaIoBaseDownload(buf, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

            imgs = await _extract_images_from_pdf_bytes(buf.getvalue())
            if imgs:
                # Sort by size (largest = hero renders), take top 3
                imgs.sort(key=len, reverse=True)
                for img in imgs[:3]:
                    # _is_clean_photo rejects text-heavy pages, floor plans, icons
                    if _is_clean_photo(img):
                        _bg_photo_cache.append(img)
                        _bg_extracted += 1
                        log.info(f"  ✓ Extracted clean photo ({len(img)//1024}KB)")
                    else:
                        log.info(f"  ✗ Rejected (text/plan/icon)")
        except Exception as e:
            log.error(f"PDF error ({pdf['name']}): {e}")

    _bg_index_ready = True
    log.info(f"Cache ready: {len(_bg_photo_cache)} clean photos from {len(sample)} PDFs ({_bg_pdfs_found} total PDFs)")
    if not _bg_photo_cache:
        log.error("ZERO clean photos extracted — renders will have no backgrounds")


async def _fetch_photo_for_slide(slide_type: str, topic: str = "", brand: str = "nucassa_re") -> bytes | None:
    """Return a random property photo extracted from Sarah's Projects PDFs.
    Sarah's Projects folder is the ONLY image source. No exceptions."""
    global _bg_recent_idx

    if slide_type == "cta":
        return None

    if not _bg_index_ready:
        await _build_background_index()

    if not _bg_photo_cache:
        log.error("No background photos available")
        return None

    available = [i for i in range(len(_bg_photo_cache)) if i not in _bg_recent_idx]
    if not available:
        _bg_recent_idx.clear()
        available = list(range(len(_bg_photo_cache)))

    idx = random.choice(available)
    _bg_recent_idx.append(idx)
    if len(_bg_recent_idx) > 20:
        _bg_recent_idx.pop(0)

    log.info(f"Background photo #{idx} ({len(_bg_photo_cache[idx])//1024}KB)")
    return _bg_photo_cache[idx]


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
    """Render a 1080x1080 carousel slide. Exactly 3 slides per carousel.
    SLIDE 1 (COVER): Full bleed photo, gradient 0.55-0.60, watermark logo 8-10% white centred,
        small logo top-left, massive 80px+ headline bottom third, accent subtitle, swipe arrow.
    SLIDE 2 (CONTENT): Same photo, gradient 0.65, logo top-right 40%, 3 stats/checklist,
        stat numbers in accent, labels white, vertically centred.
    SLIDE 3 (CTA): Pure dark bg (#1C1C1C or #000000), white question headline,
        accent DM US TODAY, large 250px logo, brand name, website URL.
    """
    cfg = BRANDS[brand]
    slides = content.get("slides", [])
    if slide_index >= len(slides):
        return None
    slide = slides[slide_index]
    topic = content.get("_topic", "")

    W, H = 1080, 1080  # Square output
    accent = _hex_to_rgb(cfg["color_accent"])  # #CDA17F or #B8962E per brand config
    primary = _hex_to_rgb(cfg["color_primary"])  # #1C1C1C or #000000
    white = (255, 255, 255)

    logo = await _load_logo_image(brand)

    def _paste_logo_small(img, x, y, size=60, opacity=0.50):
        if not logo:
            return
        wm = logo.copy()
        ratio = size / wm.width
        wm = wm.resize((size, int(wm.height * ratio)), Image.LANCZOS)
        if wm.mode == "RGBA":
            r, g, b, a = wm.split()
            a = a.point(lambda p: int(p * opacity))
            wm = Image.merge("RGBA", (r, g, b, a))
        img.paste(wm, (x, y), wm)

    def _paste_watermark(img, opacity=0.09):
        """Faded white watermark logo centred. 8-10% opacity."""
        if not logo:
            return
        wm = logo.copy()
        wm_size = int(W * 0.25)  # 25% of canvas width
        ratio = wm_size / wm.width
        wm = wm.resize((wm_size, int(wm.height * ratio)), Image.LANCZOS)
        # Convert to white silhouette at low opacity
        if wm.mode == "RGBA":
            r, g, b, a = wm.split()
            r = r.point(lambda p: 255)
            g = g.point(lambda p: 255)
            b = b.point(lambda p: 255)
            a = a.point(lambda p: int(p * opacity))
            wm = Image.merge("RGBA", (r, g, b, a))
        x = (img.width - wm.width) // 2
        y = (img.height - wm.height) // 2
        img.paste(wm, (x, y), wm)

    def _draw_swipe_arrow(draw_ctx, cx, cy, size=48):
        draw_ctx.ellipse([cx - size // 2, cy - size // 2, cx + size // 2, cy + size // 2], outline=white, width=2)
        arrow_font = FONT_HEADLINE(20)
        draw_ctx.text((cx - 6, cy - 12), "→", font=arrow_font, fill=white)

    def _full_bleed_photo(bg_img, photo_bytes, grad_top=0.55, grad_bottom=0.60):
        if not photo_bytes:
            return bg_img
        photo = Image.open(io.BytesIO(photo_bytes)).convert("RGBA")
        pr = max(W / photo.width, H / photo.height)
        photo = photo.resize((int(photo.width * pr), int(photo.height * pr)), Image.LANCZOS)
        left = (photo.width - W) // 2
        top_c = (photo.height - H) // 2
        photo = photo.crop((left, top_c, left + W, top_c + H))
        bg_img.paste(photo, (0, 0))
        return _apply_gradient_overlay(bg_img, opacity_top=grad_top, opacity_bottom=grad_bottom)

    # ── SLIDE 1: COVER ──
    if slide_index == 0:
        bg = Image.new("RGBA", (W, H), (*primary, 255))
        photo_bytes = await _fetch_photo_for_slide("cover", topic, brand)
        bg = _full_bleed_photo(bg, photo_bytes, grad_top=0.55, grad_bottom=0.60)

        # Faded watermark logo centred at 8-10% opacity in white
        _paste_watermark(bg, opacity=0.09)
        # Small logo top-left corner
        _paste_logo_small(bg, x=40, y=40, size=60, opacity=0.50)

        draw = ImageDraw.Draw(bg)
        headline = slide.get("headline", "").upper()
        subtext = slide.get("subtext", "")
        padding_x = 60
        max_w = W - padding_x * 2
        dummy = ImageDraw.Draw(Image.new("RGBA", (1, 1)))

        # Massive bold headline minimum 80px in bottom third
        font_h = FONT_HEADLINE(80)
        font_sub = FONT_BODY(24)

        block_height = 0
        if headline:
            block_height += _draw_text_wrapped(dummy, headline, font_h, max_w, 0, 0, white) + 16
        if subtext:
            block_height += _draw_text_wrapped(dummy, subtext.upper(), font_sub, max_w, 0, 0, accent) + 16

        # Bottom third positioning
        text_y = H - 100 - block_height
        text_y = max(text_y, int(H * 0.55))

        if headline:
            h_h = _draw_text_wrapped(draw, headline, font_h, max_w, padding_x, text_y, white, align="center")
            text_y += h_h + 16
        if subtext:
            _draw_text_wrapped(draw, subtext.upper(), font_sub, max_w, padding_x, text_y, accent, align="center")

        _draw_swipe_arrow(draw, W // 2, H - 50)

        buf = io.BytesIO()
        bg.save(buf, format="PNG", quality=95)
        return buf.getvalue()

    # ── SLIDE 2: CONTENT/STATS ──
    elif slide_index == 1:
        bg = Image.new("RGBA", (W, H), (*primary, 255))
        photo_bytes = await _fetch_photo_for_slide("content", topic, brand)
        bg = _full_bleed_photo(bg, photo_bytes, grad_top=0.65, grad_bottom=0.65)

        # Small logo top-right at 40% opacity
        _paste_logo_small(bg, x=W - 100, y=40, size=60, opacity=0.40)

        draw = ImageDraw.Draw(bg)
        headline = slide.get("headline", "").upper()
        points = slide.get("stats", slide.get("points", []))
        subtext = slide.get("subtext", "")
        padding_x = 70
        max_w = W - padding_x * 2
        dummy = ImageDraw.Draw(Image.new("RGBA", (1, 1)))

        font_h = FONT_HEADLINE(42)
        font_stat_num = FONT_HEADLINE(48)
        font_stat_label = FONT_BODY(24)
        font_accent = FONT_HEADLINE(32)

        # Calculate block height for vertical centering
        block_height = 0
        if headline:
            block_height += _draw_text_wrapped(dummy, headline, font_h, max_w, 0, 0, white) + 30
        if points:
            block_height += len(points) * 90  # ~90px per stat row
        if subtext:
            block_height += 50

        text_y = max((H - block_height) // 2, 80)

        if headline:
            h_h = _draw_text_wrapped(draw, headline, font_h, max_w, padding_x, text_y, white, align="center")
            text_y += h_h + 30

        if points:
            for pt in points:
                # Split "label: value" or just show whole thing
                if ":" in pt:
                    parts = pt.split(":", 1)
                    num_text = parts[0].strip().upper()
                    label_text = parts[1].strip().upper()
                else:
                    num_text = pt.upper()
                    label_text = ""

                # Stat number in accent colour, large and bold
                num_bbox = font_stat_num.getbbox(num_text)
                num_w = num_bbox[2] - num_bbox[0]
                draw.text((padding_x + (max_w - num_w) // 2, text_y), num_text, font=font_stat_num, fill=accent)
                text_y += 52

                # Label in white below
                if label_text:
                    label_bbox = font_stat_label.getbbox(label_text)
                    label_w = label_bbox[2] - label_bbox[0]
                    draw.text((padding_x + (max_w - label_w) // 2, text_y), label_text, font=font_stat_label, fill=white)
                    text_y += 38

        if subtext:
            text_y += 20
            _draw_text_wrapped(draw, subtext.upper(), font_accent, max_w, padding_x, text_y, accent, align="center")

        buf = io.BytesIO()
        bg.save(buf, format="PNG", quality=95)
        return buf.getvalue()

    # ── SLIDE 3: CTA ──
    elif slide_index == 2:
        bg = Image.new("RGBA", (W, H), (*primary, 255))
        draw = ImageDraw.Draw(bg)

        headline = slide.get("headline", slide.get("cta_line", "")).upper()
        if not headline:
            headline = cfg.get("cta", "DM US TODAY").upper()
        subtext = slide.get("subtext", cfg.get("cta", "DM US TODAY")).upper()
        website = cfg.get("website", "")
        padding_x = 80
        max_w = W - padding_x * 2
        dummy = ImageDraw.Draw(Image.new("RGBA", (1, 1)))
        brand_name = "LISTR" if "listr" in brand else "NUCASSA"

        font_h = FONT_HEADLINE(44)
        font_cta = FONT_HEADLINE(36)
        font_brand = FONT_HEADLINE(28)
        font_url = FONT_BODY(18)

        # Calculate total block for vertical centering
        h_height = _draw_text_wrapped(dummy, headline, font_h, max_w, 0, 0, white) if headline else 0
        cta_height = _draw_text_wrapped(dummy, subtext, font_cta, max_w, 0, 0, accent) if subtext else 0
        logo_h_est = 280  # logo + brand name + url
        gap = 40
        total_h = h_height + gap + cta_height + gap + logo_h_est
        start_y = (H - total_h) // 2

        # Bold white uppercase question headline centred
        if headline:
            _draw_text_wrapped(draw, headline, font_h, max_w, padding_x, start_y, white, align="center")
            start_y += h_height + gap

        # Accent colour CTA below headline
        if subtext:
            _draw_text_wrapped(draw, subtext, font_cta, max_w, padding_x, start_y, accent, align="center")
            start_y += cta_height + gap

        # Large logo centred 250px
        if logo:
            lw = 350 if brand == "listr" else 250
            lr = lw / logo.width
            lh = int(logo.height * lr)
            logo_big = logo.copy().resize((lw, lh), Image.LANCZOS)
            bg.paste(logo_big, ((W - lw) // 2, start_y), logo_big)
            start_y += lh + 20

        # Brand name below logo in accent colour
        bn_bbox = font_brand.getbbox(brand_name)
        bn_w = bn_bbox[2] - bn_bbox[0]
        draw.text(((W - bn_w) // 2, start_y), brand_name, font=font_brand, fill=accent)
        start_y += 40

        # Website URL small at bottom
        if website:
            url_bbox = font_url.getbbox(website)
            url_w = url_bbox[2] - url_bbox[0]
            draw.text(((W - url_w) // 2, H - 60), website, font=font_url, fill=(180, 180, 180))

        buf = io.BytesIO()
        bg.save(buf, format="PNG", quality=95)
        return buf.getvalue()

    return None


def _recent_visuals_for_brand(brand: str, days: int = 7) -> dict:
    """Return the list of backgrounds + Forza cover variants used for this
    brand in the last `days` days, so the renderer can avoid repeating them."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    bgs: list[str] = []
    variants: list[str] = []
    for entry in posting_log:
        if entry.get("brand") != brand:
            continue
        if entry.get("timestamp", "") < cutoff:
            continue
        vis = entry.get("visuals_used") or {}
        bgs.extend(vis.get("backgrounds") or [])
        v = vis.get("forza_cover_variant")
        if v:
            variants.append(v)
    return {"backgrounds": bgs, "forza_cover_variants": variants}


async def render_carousel_images(content: dict, brand: str) -> tuple[list[bytes], dict]:
    """Render all carousel slides. Returns (images, visuals_used). visuals_used
    records which backgrounds + Forza cover variant were picked so the caller
    can persist them to posting_log for next-run dedup.

    Playwright occasionally hangs/crashes mid-render on Railway. Retry up to 3x
    with a short backoff before giving up and returning [] — callers treat []
    as a hard render failure."""
    from renderer import render_carousel
    slides = content.get("slides", [])
    if not slides:
        return [], {"backgrounds": [], "forza_cover_variant": None}
    recent = _recent_visuals_for_brand(brand, days=7)
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            imgs, vis = await asyncio.wait_for(
                render_carousel(
                    slides, brand,
                    exclude_backgrounds=recent["backgrounds"],
                    exclude_forza_variants=recent["forza_cover_variants"],
                ),
                timeout=180,
            )
            if imgs and len(imgs) == len(slides):
                return imgs, vis
            log.warning(f"render_carousel attempt {attempt+1}/3: got {len(imgs or [])} of {len(slides)} slides — retrying")
        except Exception as e:
            last_exc = e
            log.error(f"render_carousel attempt {attempt+1}/3 failed: {type(e).__name__}: {e}")
        if attempt < 2:
            await asyncio.sleep(30)
    log.error(f"render_carousel gave up after 3 attempts for {brand}; last_exc={last_exc}")
    return [], {"backgrounds": [], "forza_cover_variant": None}


async def render_static_image(content: dict, brand: str) -> bytes | None:
    """Render a single static post image via Playwright (first slide only)."""
    from renderer import render_carousel
    slides = content.get("slides", [])
    if not slides:
        return None
    recent = _recent_visuals_for_brand(brand, days=7)
    imgs, _ = await render_carousel(
        slides[:1], brand,
        exclude_backgrounds=recent["backgrounds"],
        exclude_forza_variants=recent["forza_cover_variants"],
    )
    return imgs[0] if imgs else None


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


GDRIVE_IG_UPLOAD_FOLDER_ID = os.getenv("GDRIVE_IG_UPLOAD_FOLDER_ID", "")


async def _upload_jpeg_to_drive_public(jpeg_bytes: bytes) -> str | None:
    """Upload a JPEG to Drive, grant anyone-with-link reader, return an
    lh3.googleusercontent.com direct-download URL. IG/Meta refuses to fetch
    from Railway `.up.railway.app` hosts — the lh3 domain is reliably fetched.

    Returns the public lh3 URL or None on error."""
    if not GDRIVE_IG_UPLOAD_FOLDER_ID:
        log.error("GDRIVE_IG_UPLOAD_FOLDER_ID not configured")
        return None
    token = await _get_drive_upload_token()
    if not token:
        return None
    fname = f"ig-{int(time.time()*1000)}-{str(uuid.uuid4())[:8]}.jpg"
    metadata = json.dumps({"name": fname, "parents": [GDRIVE_IG_UPLOAD_FOLDER_ID]})
    boundary = "mark_ig_upload_boundary"
    body = (
        f"--{boundary}\r\n"
        f"Content-Type: application/json; charset=UTF-8\r\n\r\n"
        f"{metadata}\r\n"
        f"--{boundary}\r\n"
        f"Content-Type: image/jpeg\r\n\r\n"
    ).encode() + jpeg_bytes + f"\r\n--{boundary}--\r\n".encode()
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id",
                headers={"Authorization": f"Bearer {token}", "Content-Type": f"multipart/related; boundary={boundary}"},
                content=body,
            )
            if r.status_code not in (200, 201):
                log.error(f"Drive IG-cache upload failed {r.status_code}: {r.text[:300]}")
                return None
            file_id = r.json().get("id")
            if not file_id:
                log.error(f"Drive IG-cache upload: no id in response: {r.text[:300]}")
                return None
            # Make publicly readable
            perm = await client.post(
                f"https://www.googleapis.com/drive/v3/files/{file_id}/permissions",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                content=json.dumps({"type": "anyone", "role": "reader"}).encode(),
            )
            if perm.status_code not in (200, 201):
                log.error(f"Drive IG-cache perm failed {perm.status_code}: {perm.text[:200]}")
                return None
            return f"https://lh3.googleusercontent.com/d/{file_id}"
    except Exception as e:
        log.error(f"Drive IG-cache upload exception: {e}")
    return None


async def _get_public_image_url(image_bytes: bytes) -> str | None:
    """Return a public image URL that Meta's fetcher will accept. Uploads to a
    Drive cache folder (public read) and returns the lh3.googleusercontent.com
    URL — Meta refuses .up.railway.app hosts."""
    try:
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=92, optimize=True, progressive=False)
        jpeg_bytes = buf.getvalue()
    except Exception as e:
        log.error(f"JPEG re-encode failed: {e}")
        return None
    url = await _upload_jpeg_to_drive_public(jpeg_bytes)
    if url:
        log.info(f"IG image public URL: {url} ({len(jpeg_bytes)} bytes)")
        # Also keep a copy locally for debugging via /img/ (TTL-bounded).
        image_id = str(uuid.uuid4())[:12]
        _temp_images[image_id] = jpeg_bytes
        _temp_images_ts[image_id] = time.time()
        return url
    log.error("_get_public_image_url: Drive upload failed; no Railway fallback (Meta cannot fetch it)")
    return None


async def upload_image_to_ig(ig_account_id: str, image_bytes: bytes, caption: str, is_carousel_item: bool = False) -> str | None:
    """Upload image to Instagram as a container. Returns container ID."""
    # Instagram requires a public URL — upload to Telegram first to get one
    image_url = await _get_public_image_url(image_bytes)
    if not image_url:
        log.error("Could not get public URL for IG upload")
        return None

    # Small delay to ensure image is fully served before Meta fetches it
    await asyncio.sleep(2)
    upload_url = f"https://graph.facebook.com/v18.0/{ig_account_id}/media"
    params = {
        "access_token": META_SYSTEM_TOKEN,
        "image_url": image_url,
        "is_carousel_item": "true" if is_carousel_item else "false",
    }
    if caption and not is_carousel_item:
        params["caption"] = caption

    last_error = None
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            for attempt in range(3):
                r = await client.post(upload_url, params=params)
                try:
                    data = r.json()
                except Exception:
                    data = {"raw": r.text[:300]}
                if "id" in data:
                    return data["id"]
                # Pull Meta's actual error detail out — this is what we need to see
                err = data.get("error") or data
                code = err.get("code") if isinstance(err, dict) else None
                subcode = err.get("error_subcode") if isinstance(err, dict) else None
                msg = err.get("message") or err.get("raw") or str(err) if isinstance(err, dict) else str(err)
                fbtrace = err.get("fbtrace_id") if isinstance(err, dict) else None
                last_error = f"code={code} subcode={subcode} status={r.status_code} ig={ig_account_id} msg={msg}"
                log.error(f"[IG upload attempt {attempt+1}/3] {last_error} trace={fbtrace} url={image_url}")
                if r.status_code == 500 and attempt < 2:
                    await asyncio.sleep(3 + attempt * 2)
                    continue
                # Non-500 errors are unlikely to fix themselves on retry
                if r.status_code != 500:
                    break
    except Exception as e:
        last_error = f"exception {type(e).__name__}: {e}"
        log.error(f"IG upload exception: {e}")
    # Surface the last error back to Telegram via the caller so GG sees it
    global _last_ig_upload_error
    _last_ig_upload_error = last_error
    return None


_last_ig_upload_error: str = ""


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


def _jpeg_bytes(image_bytes: bytes) -> bytes:
    try:
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=92)
        return buf.getvalue()
    except Exception:
        return image_bytes


async def publish_facebook(page_id: str, image_bytes: bytes, caption: str) -> dict:
    """Post a single image to a Facebook Page."""
    page_token = await get_page_token(page_id)
    if not page_token:
        return {"error": "Could not get page token"}
    jpeg = _jpeg_bytes(image_bytes)
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                f"https://graph.facebook.com/v18.0/{page_id}/photos",
                params={"access_token": page_token, "message": caption},
                files={"source": ("post.jpg", jpeg, "image/jpeg")},
            )
            return r.json()
    except Exception as e:
        return {"error": str(e)}


async def publish_facebook_carousel(page_id: str, images_bytes_list: list, caption: str) -> dict:
    """Post multi-photo carousel to a Facebook Page: upload each photo unpublished, then create a feed post attaching them in order."""
    page_token = await get_page_token(page_id)
    if not page_token:
        return {"error": "Could not get page token"}
    media_ids = []
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            for i, img_bytes in enumerate(images_bytes_list):
                jpeg = _jpeg_bytes(img_bytes)
                r = await client.post(
                    f"https://graph.facebook.com/v18.0/{page_id}/photos",
                    params={"access_token": page_token, "published": "false"},
                    files={"source": (f"slide{i+1}.jpg", jpeg, "image/jpeg")},
                )
                d = r.json()
                if "id" in d:
                    media_ids.append(d["id"])
                else:
                    log.error(f"FB carousel slide {i+1} upload failed: {d}")
            if not media_ids:
                return {"error": "No FB carousel photos uploaded"}
            if len(media_ids) < len(images_bytes_list):
                log.warning(f"FB carousel: only {len(media_ids)}/{len(images_bytes_list)} photos uploaded — aborting to avoid partial post")
                return {"error": f"Only {len(media_ids)}/{len(images_bytes_list)} photos uploaded — carousel aborted"}
            attached = [{"media_fbid": mid} for mid in media_ids]
            r = await client.post(
                f"https://graph.facebook.com/v18.0/{page_id}/feed",
                data={
                    "access_token": page_token,
                    "message": caption,
                    "attached_media": json.dumps(attached),
                },
            )
            return r.json()
    except Exception as e:
        return {"error": str(e)}


# ── LINKEDIN API ──────────────────────────────────────────────────────────────

@app.get("/linkedin/auth")
async def linkedin_auth_start(account: str = "gg"):
    """Start LinkedIn OAuth for a specific account. Visit while logged in as that person.
      /linkedin/auth?account=gg         — Nucassa app, admin of Nucassa pages
      /linkedin/auth?account=emma       — Nucassa app, Emma's personal (cross-posts Holdings)
      /linkedin/auth?account=sue        — Forza personal app, Sue's feed (cross-posts Forza)
      /linkedin/auth?account=sue_cma    — Forza CMA-only app, Sue admin → Forza company page
    """
    account = account.lower().strip()
    if account not in ("gg", "emma", "sue", "sue_cma"):
        return JSONResponse({"error": f"Unknown account '{account}' — use gg, emma, sue, or sue_cma"})
    client_id, _ = _li_app_creds(account)
    if not client_id:
        return JSONResponse({"error": f"No LinkedIn client_id configured for {account}"})
    state = str(uuid.uuid4())
    li_oauth_states[state] = {"account": account}
    scope = _li_scope_for(account)
    auth_url = (
        f"https://www.linkedin.com/oauth/v2/authorization"
        f"?response_type=code"
        f"&client_id={client_id}"
        f"&redirect_uri={RAILWAY_URL}/linkedin/callback"
        f"&state={state}"
        f"&scope={scope.replace(' ', '%20')}"
    )
    return RedirectResponse(auth_url)


@app.get("/linkedin/callback")
async def linkedin_auth_callback(code: str = None, state: str = None, error: str = None):
    """Handle LinkedIn OAuth callback — stores token under the account chosen in /linkedin/auth."""
    if error:
        await send_telegram(f"❌ LinkedIn auth failed: {error}")
        return JSONResponse({"error": error})

    state_data = li_oauth_states.pop(state, None) if state else None
    if not state_data:
        return JSONResponse({"error": "Invalid state"})
    account = state_data.get("account", "gg")

    cb_client_id, cb_client_secret = _li_app_creds(account)
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                "https://www.linkedin.com/oauth/v2/accessToken",
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": f"{RAILWAY_URL}/linkedin/callback",
                    "client_id": cb_client_id,
                    "client_secret": cb_client_secret,
                },
            )
            tok = r.json()
            access_token = tok.get("access_token", "")
            refresh_token = tok.get("refresh_token", "")
            expires_in = tok.get("expires_in", 5184000)
            if not access_token:
                await send_telegram(f"❌ LinkedIn token exchange returned no access_token: {tok}")
                return JSONResponse({"error": "No access_token", "detail": tok})

            # Try to fetch person URN — /v2/userinfo (needs openid/profile) then /v2/me (needs r_liteprofile).
            # If both fail, accept token anyway (company-page posts still work via w_member_social).
            person_id = ""
            person_name = account.title()
            for path, id_key, name_key in (
                ("https://api.linkedin.com/v2/userinfo", "sub", "name"),
                ("https://api.linkedin.com/v2/me", "id", "localizedFirstName"),
            ):
                try:
                    u = await client.get(path, headers={"Authorization": f"Bearer {access_token}"})
                    if u.status_code == 200:
                        info = u.json()
                        person_id = info.get(id_key, "") or person_id
                        person_name = info.get(name_key, "") or person_name
                        if person_id:
                            break
                except Exception:
                    continue
            # Env var override for person URN if LinkedIn profile endpoints are locked
            env_pid = os.getenv(f"LI_PERSON_{account.upper()}", "")
            if env_pid and not person_id:
                person_id = env_pid

        LI_TOKENS[account] = {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "person_id": person_id,
            "expiry": int(time.time()) + expires_in,
            "name": person_name,
            "app": "forza_cma" if account in LI_FORZA_CMA_ACCOUNTS else ("forza" if account in LI_FORZA_ACCOUNTS else "nucassa"),
        }
        _save_li_tokens()

        await send_telegram(
            f"✅ *LinkedIn connected — {account}*\n"
            f"Signed in as: {person_name}\n"
            f"Person URN: `{person_id}`\n"
            f"Token valid for {expires_in // 86400} days."
        )
        return JSONResponse({
            "status": "connected",
            "account": account,
            "name": person_name,
            "person_id": person_id,
        })
    except Exception as e:
        await send_telegram(f"❌ LinkedIn token exchange failed: {e}")
        return JSONResponse({"error": str(e)})


async def upload_image_to_li(image_bytes: bytes, owner_urn: str, access_token: str) -> str | None:
    """Upload image via Versioned /rest/images API. Returns urn:li:image:..."""
    if not access_token:
        return None
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.linkedin.com/rest/images?action=initializeUpload",
                headers=_li_versioned_headers(access_token),
                json={"initializeUploadRequest": {"owner": owner_urn}},
            )
            if r.status_code not in (200, 201):
                log.error(f"LI image init HTTP {r.status_code} ({owner_urn}): {r.text[:300]}")
                return None
            value = (r.json() or {}).get("value") or {}
            upload_url = value.get("uploadUrl")
            img_urn = value.get("image")
            if not upload_url or not img_urn:
                log.error(f"LI image init missing fields ({owner_urn}): {value}")
                return None
            r2 = await client.put(upload_url, content=image_bytes)
            if r2.status_code in (200, 201):
                return img_urn
            log.error(f"LI image PUT HTTP {r2.status_code} ({owner_urn}): {r2.text[:200]}")
    except Exception as e:
        log.error(f"LI image upload error ({owner_urn}): {e}")
    return None


async def _li_post(author_urn: str, access_token: str, asset: str | None, caption: str) -> dict:
    """Publish a single-image (or text-only) post via Versioned /rest/posts API."""
    post_body = {
        "author": author_urn,
        "commentary": caption or "",
        "visibility": "PUBLIC",
        "distribution": {
            "feedDistribution": "MAIN_FEED",
            "targetEntities": [],
            "thirdPartyDistributionChannels": [],
        },
        "lifecycleState": "PUBLISHED",
        "isReshareDisabledByAuthor": False,
    }
    if asset:
        post_body["content"] = {"media": {"id": asset, "title": ""}}
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.linkedin.com/rest/posts",
                headers=_li_versioned_headers(access_token),
                json=post_body,
            )
            if r.status_code in (200, 201):
                post_id = r.headers.get("x-restli-id") or r.headers.get("X-RestLi-Id") or ""
                return {"id": post_id, "status": r.status_code}
            return {"error": f"LI posts HTTP {r.status_code}: {r.text[:300]}"}
    except Exception as e:
        return {"error": str(e)}


async def publish_linkedin(page_id: str, image_bytes: bytes, caption: str, admin_account: str = "gg") -> dict:
    """Post to a LinkedIn Organization Page using the specified admin account's token."""
    tok = LI_TOKENS.get(admin_account, {})
    access_token = tok.get("access_token", "")
    if not access_token:
        return {"error": f"LinkedIn ({admin_account}) not authenticated — visit {RAILWAY_URL}/linkedin/auth?account={admin_account}"}
    owner = f"urn:li:organization:{page_id}"
    asset = await upload_image_to_li(image_bytes, owner, access_token)
    return await _li_post(owner, access_token, asset, caption)


async def publish_linkedin_personal(account: str, image_bytes: bytes, caption: str) -> dict:
    """Post a single image to a personal LinkedIn feed (gg or emma)."""
    tok = LI_TOKENS.get(account, {})
    access_token = tok.get("access_token", "")
    person_id = tok.get("person_id", "")
    if not access_token or not person_id:
        return {"error": f"LinkedIn ({account}) not authenticated — visit {RAILWAY_URL}/linkedin/auth?account={account}"}
    owner = f"urn:li:person:{person_id}"
    asset = await upload_image_to_li(image_bytes, owner, access_token)
    return await _li_post(owner, access_token, asset, caption)


def _images_to_pdf_bytes(images_bytes_list: list) -> bytes:
    """Combine carousel PNG slides into a single PDF, one slide per page."""
    pil_images = []
    for b in images_bytes_list:
        pil_images.append(Image.open(io.BytesIO(b)).convert("RGB"))
    buf = io.BytesIO()
    pil_images[0].save(buf, format="PDF", save_all=True, append_images=pil_images[1:])
    return buf.getvalue()


LI_API_VERSION = os.getenv("LI_API_VERSION", "202602")  # LinkedIn Versioned API YYYYMM (rolling 12mo window)


def _li_versioned_headers(access_token: str) -> dict:
    return {
        "Authorization": f"Bearer {access_token}",
        "LinkedIn-Version": LI_API_VERSION,
        "X-Restli-Protocol-Version": "2.0.0",
        "Content-Type": "application/json",
    }


_last_li_upload_error: str = ""


async def _upload_pdf_to_li(pdf_bytes: bytes, owner_urn: str, access_token: str) -> str | None:
    """Upload PDF doc via LinkedIn Versioned /rest/documents API. Returns urn:li:document:..."""
    global _last_li_upload_error
    _last_li_upload_error = ""
    if not access_token:
        _last_li_upload_error = "no access_token"
        return None
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(
                "https://api.linkedin.com/rest/documents?action=initializeUpload",
                headers=_li_versioned_headers(access_token),
                json={"initializeUploadRequest": {"owner": owner_urn}},
            )
            if r.status_code not in (200, 201):
                _last_li_upload_error = f"init HTTP {r.status_code}: {r.text[:400]}"
                log.error(f"LI doc init HTTP {r.status_code} ({owner_urn}): {r.text[:400]}")
                return None
            data = r.json()
            value = data.get("value") or {}
            upload_url = value.get("uploadUrl")
            doc_urn = value.get("document")
            if not upload_url or not doc_urn:
                _last_li_upload_error = f"init missing upload fields: {str(data)[:400]}"
                log.error(f"LI doc init missing fields ({owner_urn}): {data}")
                return None
            r2 = await client.put(upload_url, content=pdf_bytes)
            if r2.status_code in (200, 201):
                return doc_urn
            _last_li_upload_error = f"PUT HTTP {r2.status_code}: {r2.text[:300]}"
            log.error(f"LI doc PUT HTTP {r2.status_code} ({owner_urn}): {r2.text[:300]}")
    except Exception as e:
        _last_li_upload_error = f"exception {type(e).__name__}: {e}"
        log.error(f"LI doc upload error ({owner_urn}): {e}")
    return None


async def _li_post_document(author_urn: str, access_token: str, asset: str, caption: str, title: str) -> dict:
    """Publish a document carousel via Versioned /rest/posts API."""
    post_body = {
        "author": author_urn,
        "commentary": caption or "",
        "visibility": "PUBLIC",
        "distribution": {
            "feedDistribution": "MAIN_FEED",
            "targetEntities": [],
            "thirdPartyDistributionChannels": [],
        },
        "content": {
            "media": {
                "id": asset,
                "title": (title or "Carousel")[:100],
            }
        },
        "lifecycleState": "PUBLISHED",
        "isReshareDisabledByAuthor": False,
    }
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.linkedin.com/rest/posts",
                headers=_li_versioned_headers(access_token),
                json=post_body,
            )
            if r.status_code in (200, 201):
                post_id = r.headers.get("x-restli-id") or r.headers.get("X-RestLi-Id") or ""
                return {"id": post_id, "status": r.status_code}
            return {"error": f"LI posts HTTP {r.status_code}: {r.text[:300]}"}
    except Exception as e:
        return {"error": str(e)}


async def publish_linkedin_personal_carousel(account: str, images_bytes_list: list, caption: str, title: str = "") -> dict:
    """Post a multi-slide PDF carousel to a personal LinkedIn feed (gg or emma)."""
    tok = LI_TOKENS.get(account, {})
    access_token = tok.get("access_token", "")
    person_id = tok.get("person_id", "")
    if not access_token or not person_id:
        return {"error": f"LinkedIn ({account}) not authenticated"}
    owner = f"urn:li:person:{person_id}"
    try:
        pdf_bytes = _images_to_pdf_bytes(images_bytes_list)
    except Exception as e:
        return {"error": f"PDF build failed: {e}"}
    asset = await _upload_pdf_to_li(pdf_bytes, owner, access_token)
    if not asset:
        return {"error": f"PDF upload to LinkedIn failed ({_last_li_upload_error or 'no detail'})"}
    return await _li_post_document(owner, access_token, asset, caption, title)


async def publish_linkedin_carousel(page_id: str, images_bytes_list: list, caption: str, title: str = "", admin_account: str = "gg") -> dict:
    """Post a multi-slide PDF carousel to a LinkedIn organization page.

    LinkedIn's /rest/documents initializeUpload rejects an organization URN
    as owner when the calling token has personal (w_member_social) auth
    only — "Organization permissions must be used when using organization
    as owner". The reliable pattern: upload the document owned by the
    admin PERSON, then publish the post authored by the ORGANIZATION.
    LinkedIn allows a cross-owner reference in the post body."""
    tok = LI_TOKENS.get(admin_account, {})
    access_token = tok.get("access_token", "")
    person_id = tok.get("person_id", "")
    if not access_token:
        return {"error": f"LinkedIn ({admin_account}) not authenticated"}
    try:
        pdf_bytes = _images_to_pdf_bytes(images_bytes_list)
    except Exception as e:
        return {"error": f"PDF build failed: {e}"}
    org_owner = f"urn:li:organization:{page_id}"
    # Attempt 1: upload owned by the org (works if the token has
    # w_organization_social AND the person is ADMIN of this org).
    asset = await _upload_pdf_to_li(pdf_bytes, org_owner, access_token)
    # Attempt 2: fallback — upload owned by the admin person, then post as org.
    # This works for any admin with w_member_social even if LinkedIn's newer
    # org-scope gates have tightened on documents.
    if not asset and person_id:
        log.warning(f"LI org doc upload failed ({_last_li_upload_error}); retrying with person owner {admin_account}")
        asset = await _upload_pdf_to_li(pdf_bytes, f"urn:li:person:{person_id}", access_token)
    if not asset:
        return {"error": f"PDF upload to LinkedIn failed ({_last_li_upload_error or 'no detail'})"}
    return await _li_post_document(org_owner, access_token, asset, caption, title)


# ── PERMALINK FETCHING ────────────────────────────────────────────────────────

async def _ig_permalink(media_id: str) -> str:
    """Fetch IG permalink for a published media_id. Returns url or ''. Never raises."""
    if not media_id:
        return ""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://graph.facebook.com/v18.0/{media_id}",
                params={"access_token": META_SYSTEM_TOKEN, "fields": "permalink"},
            )
            return r.json().get("permalink", "") or ""
    except Exception:
        return ""


async def _fb_permalink(post_id: str, page_id: str) -> str:
    """Fetch FB permalink for a published post_id (form `{pageid}_{postid}` or just postid)."""
    if not post_id:
        return ""
    try:
        page_token = await get_page_token(page_id) or META_SYSTEM_TOKEN
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://graph.facebook.com/v18.0/{post_id}",
                params={"access_token": page_token, "fields": "permalink_url"},
            )
            return r.json().get("permalink_url", "") or ""
    except Exception:
        return ""


def _li_permalink(post_id: str) -> str:
    """Build a LinkedIn feed URL from a returned URN like 'urn:li:share:...' or 'urn:li:activity:...'."""
    if not post_id:
        return ""
    if post_id.startswith("urn:li:"):
        return f"https://www.linkedin.com/feed/update/{post_id}/"
    return post_id  # already a URL


def _err_str(v) -> str:
    """Extract a readable error message from a platform-result dict, else ''."""
    if not isinstance(v, dict):
        return ""
    if "error" in v:
        e = v.get("error")
        if isinstance(e, dict):
            return str(e.get("message") or e)[:200]
        return str(e)[:200]
    return ""


async def _build_post_summary(brand: str, results: dict, cfg: dict) -> str:
    """Build the single consolidated Telegram message for GG: one line per channel with link-or-reason."""
    brand_name = cfg["name"]
    fb_page_id = cfg.get("fb_page_id") or ""
    lines: list[str] = []
    any_fail = False

    # Instagram
    ig = results.get("instagram")
    if ig is not None:
        err = _err_str(ig)
        if err:
            any_fail = True
            lines.append(f"IG: FAIL — {err}")
        else:
            media_id = ig.get("id") if isinstance(ig, dict) else ""
            url = await _ig_permalink(media_id)
            lines.append(f"IG: OK {url}".rstrip())

    # Facebook
    fb = results.get("facebook")
    if fb is not None:
        err = _err_str(fb)
        if err:
            any_fail = True
            lines.append(f"FB: FAIL — {err}")
        else:
            post_id = fb.get("id") or fb.get("post_id") if isinstance(fb, dict) else ""
            url = await _fb_permalink(post_id, fb_page_id)
            lines.append(f"FB: OK {url}".rstrip())

    # LinkedIn (company + any personal cross-posts)
    li = results.get("linkedin")
    if isinstance(li, dict) and li:
        for sub, v in li.items():
            err = _err_str(v)
            if err:
                any_fail = True
                lines.append(f"LinkedIn/{sub}: FAIL — {err}")
            else:
                pid = v.get("id") if isinstance(v, dict) else ""
                url = _li_permalink(pid)
                lines.append(f"LinkedIn/{sub}: OK {url}".rstrip())

    header = ("⚠️ " if any_fail else "✅ ") + f"{brand_name} posted"
    if any_fail:
        header += " (partial)"
    return header + "\n" + "\n".join(lines)


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

    # Platform override — lets callers post to a subset of the brand's configured platforms
    override = content.pop("_platforms_override", None)
    active_platforms = set(override) if override else set(cfg["platforms"])

    ig_account_id = cfg["ig_account_id"]
    fb_page_id = cfg["fb_page_id"]
    li_page_id = cfg["li_page_id"]

    # Use cached renders if available, otherwise render fresh
    cached = content.pop("_cached_images", None)
    cached_visuals = content.pop("_cached_visuals_used", None)
    visuals_used: dict = {"backgrounds": [], "forza_cover_variant": None}
    if cached:
        log.info(f"Using cached rendered images for {brand} — {ct}")
        images = cached
        if cached_visuals:
            visuals_used = cached_visuals
    else:
        log.info(f"Rendering images for {brand} — {ct}")
        if ct == "carousel":
            images, visuals_used = await render_carousel_images(content, brand)
        elif ct in ("static", "story"):
            img = await render_static_image(content, brand)
            images = [img] if img else []
        else:
            images = []  # reels = text/script only

    # Slide-1-is-non-negotiable guard. GG's rule: a post without a working
    # cover never goes live. If posting is about to fire with no images, or
    # the cover slide is empty, abort the whole post, tell GG, and record
    # the failure — do not half-post to any channel.
    if ct not in ("reels",):
        slide_1_ok = bool(images) and bool(images[0]) and len(images[0]) > 1024
        if not slide_1_ok:
            reason = "no slides rendered" if not images else "cover slide is empty or too small"
            log.error(f"post_content aborted for {brand} — {reason}")
            try:
                await _tg_send_plain(
                    f"⚠️ {BRANDS[brand]['name']} post ABORTED — {reason}. Nothing posted to any channel."
                )
            except Exception:
                pass
            results["error"] = f"aborted: {reason}"
            return results

    # Cache the successful render so we can re-post to a single platform without re-rendering
    if images:
        _last_rendered[brand] = {
            "content": {k: v for k, v in content.items() if k != "_rendered_images_b64"},
            "images": list(images),
            "visuals_used": visuals_used,
            "timestamp": time.time(),
        }

    # ── INSTAGRAM ──
    if "instagram" in active_platforms and ig_account_id:
        if images:
            if ct == "carousel" and len(images) >= 2:
                container_ids = []
                failed_slide = None
                for idx, img in enumerate(images):
                    cid = None
                    for attempt in range(3):
                        cid = await upload_image_to_ig(ig_account_id, img, "", is_carousel_item=True)
                        if cid:
                            break
                        log.warning(f"IG slide {idx+1} upload attempt {attempt+1}/3 failed — retrying with fresh URL")
                        await asyncio.sleep(3 + attempt * 2)
                    if cid:
                        container_ids.append(cid)
                    else:
                        failed_slide = idx + 1
                        break
                    await asyncio.sleep(1)
                if failed_slide:
                    err_detail = _last_ig_upload_error or "no detail captured"
                    # IG silent-skip: don't blast a separate "ABORTED" Telegram
                    # — the consolidated post-summary already shows IG: FAIL
                    # with detail. FB + LinkedIn still publish below. Forza IG
                    # is currently account-restricted (GG handling) so this
                    # path fires every Forza carousel; spamming the noisy
                    # alert reads like the whole post died, which it didn't.
                    log.warning(
                        f"[{BRANDS[brand]['name']}] IG carousel skipped — "
                        f"slide {failed_slide} upload failed after 3 retries: {err_detail[:300]}"
                    )
                    results["instagram"] = {"error": f"Slide {failed_slide} upload failed after 3 retries (FB/LI continue): {err_detail}"}
                elif len(container_ids) == len(images):
                    results["instagram"] = await publish_ig_carousel(ig_account_id, container_ids, ig_caption)
                else:
                    results["instagram"] = {"error": f"Only {len(container_ids)}/{len(images)} slides uploaded (FB/LI continue)"}
            else:
                results["instagram"] = await publish_ig_single(ig_account_id, images[0], ig_caption)
        else:
            results["instagram"] = {"error": "No image rendered"}

    # ── FACEBOOK ──
    if "facebook" in active_platforms and fb_page_id and images:
        if ct == "carousel" and len(images) >= 2:
            results["facebook"] = await publish_facebook_carousel(fb_page_id, images, ig_caption)
        else:
            results["facebook"] = await publish_facebook(fb_page_id, images[0], ig_caption)

    # ── LINKEDIN (company page + any personal feeds this brand cross-posts to) ──
    if "linkedin" in active_platforms and images:
        li_results: dict = {}
        li_title = content.get("topic", "") or BRANDS[brand]["name"]
        use_carousel = (ct == "carousel" and len(images) >= 2)
        admin_account = cfg.get("li_admin_account", "gg")  # Forza uses "gg_forza"; others default to "gg"
        if li_page_id:
            if use_carousel:
                li_results["company"] = await publish_linkedin_carousel(li_page_id, images, cap_li, li_title, admin_account=admin_account)
            else:
                li_results["company"] = await publish_linkedin(li_page_id, images[0], cap_li, admin_account=admin_account)
        for acct in cfg.get("li_personal_accounts", []) or []:
            if use_carousel:
                li_results[f"personal_{acct}"] = await publish_linkedin_personal_carousel(acct, images, cap_li, li_title)
            else:
                li_results[f"personal_{acct}"] = await publish_linkedin_personal(acct, images[0], cap_li)
        if li_results:
            results["linkedin"] = li_results

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

    # Single consolidated summary — one Telegram per brand post, with live links
    try:
        summary = await _build_post_summary(brand, results, cfg)
        await _tg_send_plain(summary)
    except Exception as e:
        log.error(f"summary build/send failed: {e}")

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
        "visuals_used": visuals_used,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    _save_posting_log()

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

async def _tg_send_plain(message: str) -> dict:
    """Send Telegram message as plain text — no Markdown parsing, so stray chars never 400."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    last: dict = {}
    for chunk in [message[i:i+4000] for i in range(0, len(message), 4000)]:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk})
                last = r.json()
                if not last.get("ok"):
                    log.error(f"_tg_send_plain non-ok: {last}")
        except Exception as e:
            log.error(f"_tg_send_plain error: {e}")
    return last


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
        # Use cached rendered images if available to avoid re-rendering
        cached_b64 = content.pop("_rendered_images_b64", [])
        if cached_b64:
            cached_images = [base64.b64decode(b) for b in cached_b64]
            content["_cached_images"] = cached_images
        await post_content(content, brand)
        # post_content sends the single consolidated summary — no extra message.

    elif action == "post_now":
        await answer_callback(cb_id, "Posting now ✅")
        pending_approvals.pop(msg_id, None)
        cached_b64 = content.pop("_rendered_images_b64", [])
        if cached_b64:
            content["_cached_images"] = [base64.b64decode(b) for b in cached_b64]
        await post_content(content, brand)
        # post_content sends the single consolidated summary — no extra message.

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
            await post_content(content, brand)
            # post_content sends the single consolidated summary — no extra message.

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


def _img_headers(n: int) -> dict:
    return {
        "Content-Type": "image/jpeg",
        "Content-Length": str(n),
        "Cache-Control": "public, max-age=3600",
        "Accept-Ranges": "bytes",
    }


@app.head("/img/{image_id}")
async def head_temp_image(image_id: str):
    img_bytes = _temp_images.get(image_id)
    if not img_bytes:
        return Response(status_code=404)
    return Response(status_code=200, headers=_img_headers(len(img_bytes)))


@app.get("/img/{image_id}")
async def serve_temp_image(image_id: str):
    """Serve a temporary image for Instagram/LinkedIn fetchers. Meta's fetcher
    issues HEAD first; the paired @app.head above makes that return 200.

    Note: the primary IG path now hosts images via Drive (lh3 URLs). _temp_images
    is kept as a local diagnostic cache only — a TTL sweeper bounds its size so
    it can't grow unbounded across many renders."""
    img_bytes = _temp_images.get(image_id)
    if not img_bytes:
        return JSONResponse({"error": "not found"}, status_code=404)
    return Response(content=img_bytes, media_type="image/jpeg", headers=_img_headers(len(img_bytes)))


_temp_images_ts: dict = {}   # image_id → timestamp inserted, used by the TTL sweeper


async def _temp_images_sweeper():
    """Evict entries older than 30 min so _temp_images can't bloat memory on
    long-running containers. Runs silently in the background."""
    while True:
        try:
            cutoff = time.time() - 30 * 60
            stale = [k for k, ts in list(_temp_images_ts.items()) if ts < cutoff]
            for k in stale:
                _temp_images.pop(k, None)
                _temp_images_ts.pop(k, None)
            if stale:
                log.info(f"_temp_images sweeper: dropped {len(stale)} stale entries")
        except Exception as e:
            log.error(f"_temp_images sweeper error: {e}")
        await asyncio.sleep(300)


@app.post("/admin/li_diag")
async def admin_li_diag(request: Request):
    """Dump LinkedIn token state + organizationAcls per account to pinpoint
    org-post failures. Returns scope inferences without exposing tokens."""
    body = await request.json()
    if body.get("token", "") != os.getenv("ADMIN_TEST_TOKEN", "") or not body.get("token"):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    out: dict = {}
    for acct, tok in LI_TOKENS.items():
        at = tok.get("access_token", "")
        if not at:
            out[acct] = {"connected": False}
            continue
        info: dict = {
            "connected": True,
            "name": tok.get("name", ""),
            "person_id": tok.get("person_id", ""),
            "app": tok.get("app", ""),
            "expiry_days_left": max(0, (int(tok.get("expiry", 0) or 0) - int(time.time())) // 86400),
        }
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.get(
                    "https://api.linkedin.com/rest/organizationAcls",
                    params={"q": "roleAssignee", "state": "APPROVED", "role": "ADMINISTRATOR"},
                    headers=_li_versioned_headers(at),
                )
                info["orgAcls_status"] = r.status_code
                if r.status_code == 200:
                    elts = (r.json() or {}).get("elements", []) or []
                    info["org_admin_of"] = [
                        (e.get("organization") or e.get("organizationalTarget") or "") for e in elts
                    ]
                else:
                    info["orgAcls_error"] = r.text[:300]
        except Exception as e:
            info["orgAcls_exc"] = f"{type(e).__name__}: {e}"
        out[acct] = info
    return out


@app.post("/admin/test_channels")
async def admin_test_channels(request: Request):
    """Internal E2E verifier — renders one slide, tries IG container upload +
    LI doc upload without publishing. Confirms media pipes are unblocked.
    Body: {"brand": "nucassa_re", "token": "<shared_secret>"}"""
    body = await request.json()
    token = body.get("token", "")
    if token != os.getenv("ADMIN_TEST_TOKEN", "") or not token:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    brand = body.get("brand", "nucassa_re")
    if brand not in BRANDS:
        return JSONResponse({"error": f"unknown brand {brand}"}, status_code=400)
    cfg = BRANDS[brand]

    # Tiny 2-slide render so we exercise the cover + data path
    sample = {
        "slides": [
            {"type": "cover", "headline_top": "Pipeline check",
             "headline_gold": cfg["name"], "headline_bottom": "internal test"},
            {"type": "data", "headline_gold": "Test",
             "headline_white": "Pipeline", "bullets": ["alpha: 1", "beta: 2", "gamma: 3"]},
        ]
    }
    images, _vis = await render_carousel_images(sample, brand)
    out: dict = {"brand": brand, "slides_rendered": len(images)}

    # IG container upload (doesn't publish)
    ig_id = cfg.get("ig_account_id")
    if "instagram" in cfg["platforms"] and ig_id and images:
        cid = await upload_image_to_ig(ig_id, images[0], "", is_carousel_item=True)
        out["ig_container"] = cid or f"FAIL: {_last_ig_upload_error or 'no detail'}"

    # LinkedIn doc upload (doesn't publish). Try org owner first; on failure
    # retry with the admin person URN (production fallback path used by
    # publish_linkedin_carousel).
    if "linkedin" in cfg["platforms"] and cfg.get("li_page_id") and images:
        admin_account = cfg.get("li_admin_account", "gg")
        tok = LI_TOKENS.get(admin_account, {})
        at = tok.get("access_token", "")
        pid = tok.get("person_id", "")
        if not at:
            out["li_org_doc"] = f"FAIL: {admin_account} not authenticated"
        else:
            try:
                pdf = _images_to_pdf_bytes(images)
                org_owner = f"urn:li:organization:{cfg['li_page_id']}"
                asset = await _upload_pdf_to_li(pdf, org_owner, at)
                if asset:
                    out["li_org_doc"] = asset
                elif pid:
                    # Fallback path: upload as admin person, post would author as org.
                    asset2 = await _upload_pdf_to_li(pdf, f"urn:li:person:{pid}", at)
                    out["li_org_doc"] = (
                        f"OK via person-owner fallback: {asset2}"
                        if asset2 else f"FAIL: {_last_li_upload_error or 'no detail'}"
                    )
                else:
                    out["li_org_doc"] = f"FAIL: {_last_li_upload_error or 'no detail'}"
            except Exception as e:
                out["li_org_doc"] = f"FAIL: PDF build {type(e).__name__}: {e}"

    # LinkedIn personal doc upload for cross-post accounts
    for acct in cfg.get("li_personal_accounts", []) or []:
        if not images:
            break
        tok = LI_TOKENS.get(acct, {})
        at = tok.get("access_token", "")
        pid = tok.get("person_id", "")
        if not at or not pid:
            out[f"li_personal_{acct}_doc"] = f"FAIL: {acct} not authenticated"
            continue
        try:
            pdf = _images_to_pdf_bytes(images)
            owner = f"urn:li:person:{pid}"
            asset = await _upload_pdf_to_li(pdf, owner, at)
            out[f"li_personal_{acct}_doc"] = asset or f"FAIL: {_last_li_upload_error or 'no detail'}"
        except Exception as e:
            out[f"li_personal_{acct}_doc"] = f"FAIL: PDF build {type(e).__name__}: {e}"

    return out


@app.get("/")
async def health():
    li_accounts = {a: bool(LI_TOKENS.get(a, {}).get("access_token")) for a in ("gg", "emma")}
    return {
        "status": "Mark is running",
        "version": "4.0",
        "image_source": "Google Drive",
        "drive_photos_cached": len(_bg_photo_cache),
        "drive_pdfs_found": _bg_pdfs_found,
        "drive_photos_extracted": _bg_extracted,
        "drive_index_ready": _bg_index_ready,
        "linkedin": li_accounts,
        "pending_approvals": len(pending_approvals),
        "pending_batches": {k: len(v) for k, v in pending_batches.items()},
    }


@app.get("/health")
async def health_probe():
    return {"status": "ok", "service": "mark", "timestamp": datetime.now(timezone.utc).isoformat()}


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
        platforms_override = body.get("platforms")  # e.g. ["instagram"] to post only to IG
        background_tasks.add_task(_run_single, brand, content_type, topic, breaking, platforms_override)
        return {
            "status": "generating",
            "action": "single",
            "brand": brand,
            "content_type": content_type,
            "platforms_override": platforms_override,
        }

    return JSONResponse({"error": "Invalid request"}, status_code=400)


@app.post("/repost")
async def api_repost(request: Request, background_tasks: BackgroundTasks):
    """Re-post the last rendered content for a brand to a subset of platforms — no re-render, no re-generation.

    Body: {"brand": "nucassa_holdings", "platforms": ["instagram"]}
    """
    body = await request.json()
    brand = body.get("brand")
    platforms = body.get("platforms")
    if not brand or brand not in BRANDS:
        return JSONResponse({"error": f"Unknown brand: {brand}"}, status_code=400)
    cached = _last_rendered.get(brand)
    if not cached or not cached.get("images"):
        return JSONResponse({"error": f"No cached render for {brand} — run /generate first"}, status_code=404)
    content = dict(cached["content"])
    content["_cached_images"] = cached["images"]
    if platforms:
        content["_platforms_override"] = list(platforms)

    async def _do_post():
        result = await post_content(content, brand)
        await send_telegram(
            f"Re-post {BRANDS[brand]['name']} — platforms: {platforms or 'all'}\n"
            f"Results: {json.dumps({k: ('ok' if 'error' not in str(v) else 'err') for k, v in result.items()})}"
        )

    background_tasks.add_task(_do_post)
    return {
        "status": "reposting",
        "brand": brand,
        "platforms": platforms or "all",
        "slides_cached": len(cached["images"]),
        "age_seconds": int(time.time() - cached["timestamp"]),
    }


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
        "linkedin": {
            a: {
                "connected": bool(LI_TOKENS.get(a, {}).get("access_token")),
                "name": LI_TOKENS.get(a, {}).get("name", ""),
                "person_id": LI_TOKENS.get(a, {}).get("person_id", ""),
                "app": LI_TOKENS.get(a, {}).get("app", "?"),
            } for a in ("gg", "emma", "sue", "sue_cma")
        },
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

async def _run_single(brand: str, content_type: str, topic: str, breaking: bool = False, platforms_override: list | None = None):
    if not topic or not topic.strip():
        await send_telegram("⚠️ No topic provided — Alex must provide the content angle. Use Alex to generate content.")
        return
    # Track for regeneration
    _last_render.update({"brand": brand, "content_type": content_type, "topic": topic})
    await send_telegram(f"_Generating {BRANDS[brand]['name']} {content_type}..._")
    content = await generate_single(brand, content_type, topic)
    if not content:
        await send_telegram(f"⚠️ Mark failed to generate content for {brand}/{content_type} — check Railway logs for details")
        return
    if content.get("_fallback_from") == "reels":
        await send_telegram(f"⚠️ Reels generation failed for {BRANDS[brand]['name']} — fell back to carousel successfully")
        content_type = "carousel"  # Update for rendering
    if breaking:
        content["_breaking"] = True
    if platforms_override:
        content["_platforms_override"] = list(platforms_override)
    # Render images and send as photos so GG can see what he's approving
    await send_telegram(f"_Rendering slides..._")
    rendered_images = []
    if content_type == "reels" and "script" in content and "slides" not in content:
        # Convert reel script beats into slides for visual preview
        script = content["script"]
        fake_slides = [
            {"slide": 1, "headline": script.get("hook", ""), "subtext": "REEL HOOK", "photo_direction": "cinematic Dubai"},
        ]
        for j, beat in enumerate(script.get("body", []), start=2):
            fake_slides.append({"slide": j, "headline": beat, "subtext": "", "photo_direction": "cinematic Dubai"})
        cta = script.get("cta", "")
        if cta:
            fake_slides.append({"slide": len(fake_slides) + 1, "headline": cta, "cta_line": cta})
        content["slides"] = fake_slides
        rendered_images, _ = await render_carousel_images(content, brand)
    elif content_type == "carousel":
        rendered_images, _ = await render_carousel_images(content, brand)
    elif content_type in ("static", "story"):
        img = await render_static_image(content, brand)
        rendered_images = [img] if img else []
    # Guard: slide 1 is non-negotiable. GG's rule — a post without a working
    # cover never goes live. If the renderer produced NO images at all, or
    # the cover image (slot 0) is empty/zero-bytes, abort before showing any
    # POST buttons. GG never has to look at approve-on-ghost-content.
    slide_1_ok = bool(rendered_images) and bool(rendered_images[0]) and len(rendered_images[0]) > 1024
    if not slide_1_ok:
        why = "no slides rendered" if not rendered_images else (
            "cover slide (slide 1) is empty or too small — abort" if not rendered_images[0] or len(rendered_images[0]) <= 1024
            else "unknown render state"
        )
        await send_telegram(
            f"⚠️ Render failed for *{BRANDS[brand]['name']}* ({content_type}) — {why}. "
            f"Not showing POST buttons — no post goes out without a working slide 1."
        )
        return
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
    cap = content.get("caption_instagram", "")
    tags = " ".join(content.get("hashtags", []))
    if tags:
        cap = f"{cap}\n\n{tags}"
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


# ── BOOT-DEDUP ───────────────────────────────────────────────────────────────
# Suppresses the "Mark v2 online" / "LinkedIn daily health check" Telegram
# sends when Railway restarts the container within a 10 min window. The
# state lives on the Railway volume so it survives redeploys.

_BOOT_DIR = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data")


def should_send_boot_message(bot_slug: str, gap_seconds: int = 600) -> bool:
    """True when last recorded boot was >gap_seconds ago (cold boot).
    Always writes the current boot timestamp before returning."""
    boot_file = os.path.join(_BOOT_DIR, f"{bot_slug}_last_boot.json")
    now_ts = int(time.time())
    last_ts = 0
    try:
        with open(boot_file) as f:
            last_ts = int(json.load(f).get("ts", 0))
    except (FileNotFoundError, json.JSONDecodeError, ValueError, TypeError):
        pass
    try:
        os.makedirs(_BOOT_DIR, exist_ok=True)
        tmp = boot_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"ts": now_ts, "iso": datetime.now(timezone.utc).isoformat()}, f)
        os.replace(tmp, boot_file)
    except OSError as e:
        log.warning(f"boot-dedup write failed: {e}")
    return (now_ts - last_ts) > gap_seconds


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
    """Refresh any per-account LI token within 5 days of expiry."""
    refreshed_any = False
    for account in list(LI_TOKENS.keys()):
        tok = LI_TOKENS.get(account) or {}
        refresh_token = tok.get("refresh_token", "")
        expiry = tok.get("expiry", 0)
        if not refresh_token:
            continue
        if (expiry - time.time()) > 432000:  # more than 5 days left
            continue
        try:
            rf_client_id, rf_client_secret = _li_app_creds(account)
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    "https://www.linkedin.com/oauth/v2/accessToken",
                    data={
                        "grant_type":    "refresh_token",
                        "refresh_token": refresh_token,
                        "client_id":     rf_client_id,
                        "client_secret": rf_client_secret,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
            LI_TOKENS[account]["access_token"] = data["access_token"]
            LI_TOKENS[account]["refresh_token"] = data.get("refresh_token", refresh_token)
            LI_TOKENS[account]["expiry"] = int(time.time()) + data.get("expires_in", 5184000)
            LI_TOKENS[account]["scope"] = data.get("scope", LI_TOKENS[account].get("scope", ""))
            refreshed_any = True
            days_valid = data.get("expires_in", 5184000) // 86400
            await send_telegram(f"LinkedIn token refreshed ({account}) — valid for {days_valid} days")
            log.info(f"LinkedIn token refreshed for {account}, expires in {days_valid} days")
        except Exception as e:
            log.error(f"LinkedIn token refresh failed for {account}: {e}")
            await send_telegram(
                f"LinkedIn token refresh FAILED ({account}) — re-auth at {RAILWAY_URL}/linkedin/auth?account={account}"
            )
    if refreshed_any:
        _save_li_tokens()


async def linkedin_token_monitor():
    while True:
        try:
            await refresh_linkedin_token()
        except Exception as e:
            log.error(f"LinkedIn token monitor error: {e}")
        await asyncio.sleep(86400)


async def linkedin_health_check():
    """Daily liveness probe — calls /rest/posts with author validation only and reports status per account."""
    await asyncio.sleep(60)  # let the app settle
    # On warm restarts (<10 min since last boot), skip the first immediate
    # send; fall through to the 24h sleep so the next fire stays on cadence.
    first_pass_skip = not should_send_boot_message("mark_linkedin_health", gap_seconds=600)
    while True:
        if first_pass_skip:
            first_pass_skip = False
            await asyncio.sleep(86400)
            continue
        try:
            lines = ["LinkedIn daily health check:"]
            for acct, tok in LI_TOKENS.items():
                at = tok.get("access_token", "")
                pid = tok.get("person_id", "")
                expiry = int(tok.get("expiry", 0) or 0)
                days_left = max(0, (expiry - int(time.time())) // 86400) if expiry else 0
                if not at or not pid:
                    lines.append(f"  - {acct}: NOT AUTHENTICATED — visit {RAILWAY_URL}/linkedin/auth?account={acct}")
                    continue
                # Cheap probe: initializeUpload for an image (won't actually upload)
                try:
                    async with httpx.AsyncClient(timeout=15) as client:
                        r = await client.post(
                            "https://api.linkedin.com/rest/images?action=initializeUpload",
                            headers=_li_versioned_headers(at),
                            json={"initializeUploadRequest": {"owner": f"urn:li:person:{pid}"}},
                        )
                    if r.status_code in (200, 201):
                        lines.append(f"  - {acct}: OK ({days_left}d token left)")
                    else:
                        lines.append(f"  - {acct}: FAIL HTTP {r.status_code} — {r.text[:140]}")
                except Exception as e:
                    lines.append(f"  - {acct}: probe error {e}")
            await _tg_send_plain("\n".join(lines))
        except Exception as e:
            log.error(f"linkedin_health_check error: {e}")
        await asyncio.sleep(86400)  # daily


# ── STARTUP ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    _required = ["CLAUDE_API_KEY", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "META_SYSTEM_TOKEN"]
    _missing = [v for v in _required if not os.getenv(v)]
    if _missing:
        log.error(f"MISSING REQUIRED ENV VARS: {', '.join(_missing)} — Mark cannot start properly")

    # Mark v2 — autonomous mode, GG talks to Mark directly
    from mark_v2_brain import mark_v2_listener
    asyncio.create_task(mark_v2_listener())
    asyncio.create_task(linkedin_token_monitor())
    asyncio.create_task(linkedin_health_check())
    asyncio.create_task(_temp_images_sweeper())
    log.info("Mark v2 — Autonomous Marketing Brain — online")
