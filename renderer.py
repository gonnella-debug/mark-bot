"""
Mark v2 Renderer — HTML/CSS + Playwright
Generates Instagram carousel slides matching Nucassa designer quality.
"""

import os
import json
import base64
import random
import asyncio
import logging
from pathlib import Path

log = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).parent / "templates"
BG_DIR = TEMPLATE_DIR / "backgrounds"
FORZA_COVER_DIR = TEMPLATE_DIR / "forza_covers"
ASSETS_DIR = TEMPLATE_DIR / "assets"


DATA_DIR = Path(os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or os.getenv("DATA_DIR") or "/data")
DRIVE_BG_CACHE = DATA_DIR / "mark_drive_bg"


def _drive_cached_photos() -> list[str]:
    """Local paths of images mirrored from Drive by drive_bg_sync_loop.
    Empty list while the cache is cold or the folder isn't shared yet."""
    if not DRIVE_BG_CACHE.exists():
        return []
    return [
        str(p) for p in (
            list(DRIVE_BG_CACHE.glob("*.png"))
            + list(DRIVE_BG_CACHE.glob("*.jpg"))
            + list(DRIVE_BG_CACHE.glob("*.jpeg"))
            + list(DRIVE_BG_CACHE.glob("*.webp"))
            + list(DRIVE_BG_CACHE.glob("*.PNG"))
            + list(DRIVE_BG_CACHE.glob("*.JPG"))
        )
    ]


def pick_forza_cover_photo(exclude: list[str] | None = None) -> str:
    """Pick one photo from templates/forza_covers/ + the Drive-mirrored
    /data cache to sit behind the Forza slide-1 composition. `exclude`
    is a list of full paths used in the last 7 days (same convention as
    pick_background). Falls back to the full pool if exclusion empties it."""
    local = [str(p) for p in sorted(FORZA_COVER_DIR.glob("*.png")) + sorted(FORZA_COVER_DIR.glob("*.jpg"))]
    drive = _drive_cached_photos()
    all_photos = local + drive
    if not all_photos:
        return ""
    excl = set(exclude or [])
    pool = [p for p in all_photos if p not in excl]
    if not pool:
        pool = all_photos
    return random.choice(pool)


def _file_to_data_uri(path: str) -> str:
    """Read a local image and return a base64 data: URI so Chromium never hits file://.
    Chromium headless blocks file:// cross-directory reads, which made the cover
    slide's background image load as a blank slide."""
    if not path:
        return ""
    try:
        p = Path(path)
        if not p.exists():
            return ""
        ext = p.suffix.lower().lstrip(".")
        mime = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg", "webp": "image/webp"}.get(ext, "image/png")
        b64 = base64.b64encode(p.read_bytes()).decode("ascii")
        return f"data:{mime};base64,{b64}"
    except Exception as e:
        log.error(f"data-uri conversion failed for {path}: {e}")
        return ""

# ── BACKGROUND IMAGE LIBRARY ──
# Tags for automatic selection based on content topic
BG_TAGS = {
    "skyline": [],
    "business": [],
    "luxury": [],
    "construction": [],
    "interior": [],
    "aerial": [],
    "sunset": [],
    "night": [],
}

def _init_backgrounds():
    """Scan backgrounds folder and auto-tag images by filename. Also
    pulls in Drive-mirrored /data cache so a fresh upload to MARK SOCIALS
    is available across all Nucassa+ListR cover renders, not just Forza."""
    # Drive cache changes between calls (sync loop downloads new images),
    # so we re-scan on every call. The cost is just os.listdir — cheap.
    BG_TAGS["all"] = []
    for k in BG_TAGS:
        if k != "all":
            BG_TAGS[k] = []

    all_bgs = sorted(BG_DIR.glob("*.png")) + sorted(BG_DIR.glob("*.jpg"))
    drive_paths = [Path(p) for p in _drive_cached_photos()]
    all_bgs = list(all_bgs) + drive_paths

    # Simple round-robin assignment for now — can be refined with Claude vision later
    categories = list(BG_TAGS.keys())
    for i, bg in enumerate(all_bgs):
        cat = categories[i % len(categories)]
        BG_TAGS[cat].append(str(bg))

    # Also make a flat "all" list
    BG_TAGS["all"] = [str(bg) for bg in all_bgs]
    log.info(f"Loaded {len(all_bgs)} background images across {len(categories)} categories")


def pick_background(topic: str = "", category: str = "", exclude: list[str] | None = None) -> str:
    """Pick a background image matching the topic/category.
    If `exclude` is provided (e.g. backgrounds used in the last 7 days for this
    brand), those paths are filtered out. Falls back to the full pool if
    exclusion leaves nothing to choose from."""
    _init_backgrounds()

    # Map topics to categories
    topic_lower = topic.lower()
    if any(w in topic_lower for w in ["invest", "capital", "fund", "office", "spv", "adgm"]):
        cat = "business"
    elif any(w in topic_lower for w in ["luxury", "villa", "penthouse", "premium"]):
        cat = "luxury"
    elif any(w in topic_lower for w in ["construction", "build", "develop", "off-plan"]):
        cat = "construction"
    elif any(w in topic_lower for w in ["sunset", "golden", "evening"]):
        cat = "sunset"
    elif any(w in topic_lower for w in ["night", "dark", "tower"]):
        cat = "night"
    elif any(w in topic_lower for w in ["aerial", "drone", "palm", "view"]):
        cat = "aerial"
    elif any(w in topic_lower for w in ["interior", "apartment", "kitchen", "bathroom"]):
        cat = "interior"
    else:
        cat = category or "skyline"

    pool = BG_TAGS.get(cat, BG_TAGS.get("all", []))
    if not pool:
        pool = BG_TAGS.get("all", [])

    if exclude:
        filtered = [p for p in pool if p not in set(exclude)]
        # Only use the filtered pool if it has at least one option, else fall
        # back to the full category pool (the whole library was posted recently).
        if filtered:
            pool = filtered

    return random.choice(pool) if pool else ""


# ── HTML TEMPLATE GENERATORS ──

def _base_css():
    """Shared CSS reset + font import."""
    return """
    @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;700;800;900&display=swap');
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { width: 1080px; height: 1080px; overflow: hidden; font-family: 'Montserrat', sans-serif; background: #0a0a0a; }
    .slide { width: 1080px; height: 1080px; position: relative; overflow: hidden; }
    """

def _stripe_bg_css():
    """Diagonal stripe texture + light streaks for data slides."""
    return """
    .stripe-bg {
        position: absolute; top: 0; left: 0; width: 100%; height: 100%;
        background: repeating-linear-gradient(-35deg, transparent, transparent 80px, rgba(255,255,255,0.018) 80px, rgba(255,255,255,0.018) 82px);
    }
    .streak1 {
        position: absolute; top: -20%; right: -10%; width: 60%; height: 140%;
        background: linear-gradient(-35deg, transparent 0%, rgba(255,255,255,0.012) 45%, rgba(255,255,255,0.035) 50%, rgba(255,255,255,0.012) 55%, transparent 100%);
    }
    .streak2 {
        position: absolute; top: -20%; right: 20%; width: 35%; height: 140%;
        background: linear-gradient(-35deg, transparent 0%, rgba(255,255,255,0.008) 45%, rgba(255,255,255,0.02) 50%, rgba(255,255,255,0.008) 55%, transparent 100%);
    }
    """

def _stripe_bg_html():
    return '<div class="stripe-bg"></div><div class="streak1"></div><div class="streak2"></div>'


def _forza_cover_blueprint(headline_top: str, headline_gold: str, headline_bottom: str,
                            logo_path: str, accent_color: str = "#C5A86C",
                            bg_image: str = "") -> str:
    """Forza cover variant: Blueprint — central hex core + 4 labelled infrastructure
    nodes on a gridded ink background. Original Forza cover design."""
    photo_uri = _file_to_data_uri(bg_image) if bg_image else ""
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
    <link href="https://fonts.googleapis.com/css2?family=Cinzel:wght@500;600;700;800&family=Cormorant+Garamond:ital,wght@0,500;0,600;1,400&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
    {_base_css()}
    body {{ background: #0d0906; font-family: 'Cormorant Garamond', serif; }}
    .slide {{
        background:
          radial-gradient(circle at 72% 32%, rgba(230,195,120,0.28) 0%, transparent 42%),
          radial-gradient(ellipse at 22% 82%, rgba(180,140,70,0.18) 0%, transparent 55%),
          linear-gradient(135deg, rgba(42,32,24,{0.60 if photo_uri else 1}) 0%, rgba(26,19,12,{0.72 if photo_uri else 1}) 50%, rgba(13,9,6,{0.82 if photo_uri else 1}) 100%),
          {f"url('{photo_uri}') center/cover no-repeat," if photo_uri else ""}
          #0d0906;
        overflow: hidden;
    }}
    /* architectural blueprint grid — brighter so it reads at feed-thumbnail size */
    .grid {{
        position: absolute; inset: 0;
        background:
          repeating-linear-gradient(0deg, transparent 0 59px, rgba(197,168,108,0.07) 59px 60px),
          repeating-linear-gradient(90deg, transparent 0 59px, rgba(197,168,108,0.07) 59px 60px);
        z-index: 0;
    }}
    /* corner gold L-bracket — luxury print frame */
    .corner-tl, .corner-tr, .corner-bl, .corner-br {{
        position: absolute; width: 38px; height: 38px; z-index: 6;
        border-color: {accent_color};
    }}
    .corner-tl {{ top: 36px; left: 36px; border-top: 2px solid; border-left: 2px solid; }}
    .corner-tr {{ top: 36px; right: 36px; border-top: 2px solid; border-right: 2px solid; }}
    .corner-bl {{ bottom: 36px; left: 36px; border-bottom: 2px solid; border-left: 2px solid; }}
    .corner-br {{ bottom: 36px; right: 36px; border-bottom: 2px solid; border-right: 2px solid; }}
    /* HERO: blueprint of an operating system */
    .hero-os {{
        position: absolute; top: 200px; right: 60px; width: 480px; height: 540px;
        z-index: 4; opacity: .95;
        filter: drop-shadow(0 0 60px rgba(197,168,108,0.25));
    }}
    .hero-os svg {{ width: 100%; height: 100%; }}
    .header {{
        position: absolute; top: 70px; left: 90px; right: 90px;
        display: flex; justify-content: space-between; align-items: center; z-index: 8;
    }}
    .brandmark {{ display: flex; align-items: center; gap: 16px; }}
    .brandmark svg {{ width: 50px; height: 56px; }}
    .brandmark .wm {{
        font-family: 'Cinzel', serif; font-weight: 700; color: #f5ecd6;
        letter-spacing: 9px; font-size: 19px;
    }}
    .tagline {{
        font-family: 'Cinzel', serif; font-size: 12px; color: {accent_color};
        letter-spacing: 6px; text-transform: uppercase; font-weight: 500;
        display: flex; align-items: center; gap: 14px;
    }}
    .tagline::before {{
        content: ''; width: 38px; height: 1px; background: {accent_color}; opacity: .7;
    }}
    /* page indicator */
    .page-no {{
        position: absolute; top: 175px; left: 90px; z-index: 8;
        font-family: 'Cinzel', serif; color: {accent_color};
        font-size: 13px; letter-spacing: 5px; font-weight: 500;
    }}
    .page-no .dim {{ color: #5a5347; }}
    .content {{
        position: absolute; left: 90px; right: 90px; bottom: 240px;
        z-index: 8; max-width: 720px;
    }}
    .kicker {{
        font-family: 'Cinzel', serif; font-size: 15px; font-weight: 600;
        color: {accent_color}; letter-spacing: 9px; text-transform: uppercase;
        margin-bottom: 28px; display: inline-flex; align-items: center; gap: 18px;
    }}
    .kicker::before {{
        content: ''; width: 26px; height: 1px; background: {accent_color};
    }}
    .headline {{
        font-family: 'Cormorant Garamond', serif; font-weight: 600;
        font-size: 84px; line-height: 1.02; color: #f7f3ea;
        letter-spacing: -1.2px; margin-bottom: 24px;
        text-shadow: 0 4px 40px rgba(0,0,0,0.6);
    }}
    .headline em {{ color: {accent_color}; font-style: italic; font-weight: 500; }}
    .sub {{
        font-family: 'Cormorant Garamond', serif; font-size: 30px; font-weight: 400;
        font-style: italic; color: #c8bfae; line-height: 1.35; max-width: 660px;
    }}
    /* gold accent bar */
    .accent-bar {{
        position: absolute; left: 90px; bottom: 200px;
        width: 110px; height: 3px;
        background: linear-gradient(to right, {accent_color}, transparent);
        z-index: 6;
    }}
    .footer {{
        position: absolute; bottom: 80px; left: 90px; right: 90px;
        display: flex; justify-content: space-between; align-items: flex-end; z-index: 7;
    }}
    .footer-l {{
        font-family: 'Cinzel', serif; font-weight: 700; color: {accent_color};
        letter-spacing: 11px; font-size: 16px;
    }}
    .footer-r {{
        text-align: right;
        font-family: 'Inter', sans-serif; font-weight: 500;
        font-size: 11px; letter-spacing: 3px; color: #8a8275;
        text-transform: uppercase;
    }}
    .footer-r .domain {{ color: #d8cfb8; font-weight: 600; letter-spacing: 4px; font-size: 12px; }}
    .swipe {{
        position: absolute; bottom: 32px; left: 50%; transform: translateX(-50%);
        display: flex; gap: 6px; z-index: 7;
    }}
    .swipe .dot {{
        width: 6px; height: 6px; border-radius: 50%; background: rgba(255,255,255,0.18);
    }}
    .swipe .dot.active {{ background: {accent_color}; width: 18px; border-radius: 4px; }}
    </style></head><body>
    <div class="slide">
        <div class="grid"></div>
        <!-- HERO BLUEPRINT: 4 infrastructures wired to a central OS core -->
        <div class="hero-os">
          <svg viewBox="0 0 480 540" xmlns="http://www.w3.org/2000/svg">
            <defs>
              <linearGradient id="goldwire" x1="0" y1="0" x2="1" y2="1">
                <stop offset="0%" stop-color="#e8d098" stop-opacity="0.95"/>
                <stop offset="100%" stop-color="#8a6f3e" stop-opacity="0.6"/>
              </linearGradient>
              <radialGradient id="coreglow" cx="0.5" cy="0.5" r="0.5">
                <stop offset="0%" stop-color="#f0d9a3" stop-opacity="1"/>
                <stop offset="60%" stop-color="#c5a86c" stop-opacity="0.4"/>
                <stop offset="100%" stop-color="#c5a86c" stop-opacity="0"/>
              </radialGradient>
              <linearGradient id="nodefill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="#3a2d1e"/>
                <stop offset="100%" stop-color="#1a130c"/>
              </linearGradient>
            </defs>
            <!-- connection paths (dashed gold) -->
            <g stroke="url(#goldwire)" stroke-width="1.8" fill="none" stroke-dasharray="4,5">
              <path d="M 240 270 L 100 110"/>
              <path d="M 240 270 L 380 110"/>
              <path d="M 240 270 L 100 430"/>
              <path d="M 240 270 L 380 430"/>
            </g>
            <!-- solid gold orbital ring -->
            <circle cx="240" cy="270" r="120" fill="none" stroke="{accent_color}" stroke-width="1.3" stroke-opacity="0.60"/>
            <circle cx="240" cy="270" r="170" fill="none" stroke="{accent_color}" stroke-width="1.0" stroke-opacity="0.35" stroke-dasharray="3,6"/>
            <!-- central core glow -->
            <circle cx="240" cy="270" r="95" fill="url(#coreglow)" opacity="0.7"/>
            <!-- central OS hexagon (Forza chevron motif rotated to a hex) -->
            <polygon points="240,200 305,235 305,305 240,340 175,305 175,235"
                     fill="url(#nodefill)" stroke="{accent_color}" stroke-width="1.5"/>
            <text x="240" y="264" text-anchor="middle" fill="{accent_color}"
                  font-family="Cinzel, serif" font-size="11" letter-spacing="3" font-weight="600">FORZA</text>
            <text x="240" y="285" text-anchor="middle" fill="#d8cfb8"
                  font-family="Cinzel, serif" font-size="9" letter-spacing="2.5">OS · v1</text>
            <!-- 4 outer nodes: REVENUE / OPERATIONS / BRAND / PEOPLE -->
            <g font-family="Cinzel, serif" font-size="10" font-weight="600" letter-spacing="2.5" fill="{accent_color}">
              <!-- TL: Revenue -->
              <circle cx="100" cy="110" r="32" fill="url(#nodefill)" stroke="{accent_color}" stroke-width="1.8"/>
              <text x="100" y="108" text-anchor="middle">REVENUE</text>
              <text x="100" y="122" text-anchor="middle" font-size="7" fill="#8a8275" letter-spacing="2">CRM · Sales</text>
              <!-- TR: Operations -->
              <circle cx="380" cy="110" r="32" fill="url(#nodefill)" stroke="{accent_color}" stroke-width="1.8"/>
              <text x="380" y="108" text-anchor="middle">OPS</text>
              <text x="380" y="122" text-anchor="middle" font-size="7" fill="#8a8275" letter-spacing="2">Workflow</text>
              <!-- BL: Brand -->
              <circle cx="100" cy="430" r="32" fill="url(#nodefill)" stroke="{accent_color}" stroke-width="1.8"/>
              <text x="100" y="428" text-anchor="middle">BRAND</text>
              <text x="100" y="442" text-anchor="middle" font-size="7" fill="#8a8275" letter-spacing="2">Web · Content</text>
              <!-- BR: People -->
              <circle cx="380" cy="430" r="32" fill="url(#nodefill)" stroke="{accent_color}" stroke-width="1.8"/>
              <text x="380" y="428" text-anchor="middle">PEOPLE</text>
              <text x="380" y="442" text-anchor="middle" font-size="7" fill="#8a8275" letter-spacing="2">Hiring · Ops</text>
            </g>
            <!-- coordinate ticks (blueprint feel) -->
            <g stroke="{accent_color}" stroke-width="0.6" stroke-opacity="0.4">
              <line x1="240" y1="60" x2="240" y2="72"/>
              <line x1="240" y1="468" x2="240" y2="480"/>
              <line x1="60" y1="270" x2="72" y2="270"/>
              <line x1="408" y1="270" x2="420" y2="270"/>
            </g>
          </svg>
        </div>
        <div class="corner-tl"></div>
        <div class="corner-tr"></div>
        <div class="corner-bl"></div>
        <div class="corner-br"></div>
        <div class="header">
          <div class="brandmark">
            <svg viewBox="0 0 100 110" xmlns="http://www.w3.org/2000/svg">
              <defs>
                <linearGradient id="fgoldbrand" x1="0" y1="0" x2="1" y2="1">
                  <stop offset="0%" stop-color="#e2c78e"/>
                  <stop offset="50%" stop-color="#c5a86c"/>
                  <stop offset="100%" stop-color="#8e7646"/>
                </linearGradient>
              </defs>
              <polygon points="12,8 88,8 74,30 0,30" fill="url(#fgoldbrand)"/>
              <polygon points="17,40 66,40 54,60 6,60" fill="url(#fgoldbrand)"/>
              <polygon points="22,70 48,70 30,100 0,100" fill="url(#fgoldbrand)"/>
            </svg>
            <span class="wm">FORZA</span>
          </div>
          <div class="tagline">Systems That Scale</div>
        </div>
        <div class="page-no">01<span class="dim"> / 04</span></div>
        <div class="content">
            <div class="kicker">{headline_top or 'Revenue Infrastructure'}</div>
            <h1 class="headline">{headline_gold}</h1>
            <div class="sub">{headline_bottom}</div>
        </div>
        <div class="accent-bar"></div>
        <div class="footer">
            <div class="footer-l">FORZA</div>
            <div class="footer-r">
              <div class="domain">FORZASYSTEMS.AI</div>
              <div>Operating Systems for Service Businesses</div>
            </div>
        </div>
        <div class="swipe">
          <div class="dot active"></div><div class="dot"></div><div class="dot"></div><div class="dot"></div>
        </div>
        <div class="arrow">
          <svg viewBox="0 0 24 24"><path d="M5 12h14M12 5l7 7-7 7"/></svg>
        </div>
    </div></body></html>"""


# ── FORZA COVER: common scaffold helpers (shared across variants) ───────────
# DNA preserved across variants: lifted ink gradient, gold (#C5A86C), Cinzel+
# Cormorant typography, corner brackets, FORZA brandmark top-left, tagline
# top-right, page number, bottom swipe dots, FORZASYSTEMS.AI footer.
# What VARIES: hero composition, background texture, headline position.

def _forza_common_head(accent_color: str) -> str:
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
    <link href="https://fonts.googleapis.com/css2?family=Cinzel:wght@500;600;700;800&family=Cormorant+Garamond:ital,wght@0,500;0,600;1,400&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
    {_base_css()}
    body {{ background: #0d0906; font-family: 'Cormorant Garamond', serif; }}
    .corner-tl, .corner-tr, .corner-bl, .corner-br {{
        position: absolute; width: 38px; height: 38px; z-index: 6;
        border-color: {accent_color};
    }}
    .corner-tl {{ top: 36px; left: 36px; border-top: 2px solid; border-left: 2px solid; }}
    .corner-tr {{ top: 36px; right: 36px; border-top: 2px solid; border-right: 2px solid; }}
    .corner-bl {{ bottom: 36px; left: 36px; border-bottom: 2px solid; border-left: 2px solid; }}
    .corner-br {{ bottom: 36px; right: 36px; border-bottom: 2px solid; border-right: 2px solid; }}
    .header {{
        position: absolute; top: 70px; left: 90px; right: 90px;
        display: flex; justify-content: space-between; align-items: center; z-index: 8;
    }}
    .brandmark {{ display: flex; align-items: center; gap: 16px; }}
    .brandmark svg {{ width: 50px; height: 56px; }}
    .brandmark .wm {{
        font-family: 'Cinzel', serif; font-weight: 700; color: #f5ecd6;
        letter-spacing: 9px; font-size: 19px;
    }}
    .tagline {{
        font-family: 'Cinzel', serif; font-size: 12px; color: {accent_color};
        letter-spacing: 6px; text-transform: uppercase; font-weight: 500;
        display: flex; align-items: center; gap: 14px;
    }}
    .tagline::before {{ content: ''; width: 38px; height: 1px; background: {accent_color}; opacity: .7; }}
    .page-no {{
        position: absolute; top: 175px; left: 90px; z-index: 8;
        font-family: 'Cinzel', serif; color: {accent_color};
        font-size: 13px; letter-spacing: 5px; font-weight: 500;
    }}
    .page-no .dim {{ color: #5a5347; }}
    .footer {{
        position: absolute; bottom: 80px; left: 90px; right: 90px;
        display: flex; justify-content: space-between; align-items: flex-end; z-index: 7;
    }}
    .footer-l {{
        font-family: 'Cinzel', serif; font-weight: 700; color: {accent_color};
        letter-spacing: 11px; font-size: 16px;
    }}
    .footer-r {{
        text-align: right;
        font-family: 'Inter', sans-serif; font-weight: 500;
        font-size: 11px; letter-spacing: 3px; color: #8a8275;
        text-transform: uppercase;
    }}
    .footer-r .domain {{ color: #d8cfb8; font-weight: 600; letter-spacing: 4px; font-size: 12px; }}
    .swipe {{
        position: absolute; bottom: 32px; left: 50%; transform: translateX(-50%);
        display: flex; gap: 6px; z-index: 7;
    }}
    .swipe .dot {{ width: 6px; height: 6px; border-radius: 50%; background: rgba(255,255,255,0.18); }}
    .swipe .dot.active {{ background: {accent_color}; width: 18px; border-radius: 4px; }}
    .headline {{
        font-family: 'Cormorant Garamond', serif; font-weight: 600;
        line-height: 1.02; color: #f7f3ea;
        letter-spacing: -1.2px;
        text-shadow: 0 4px 40px rgba(0,0,0,0.6);
    }}
    .headline em {{ color: {accent_color}; font-style: italic; font-weight: 500; }}
    .kicker {{
        font-family: 'Cinzel', serif; font-size: 15px; font-weight: 600;
        color: {accent_color}; letter-spacing: 9px; text-transform: uppercase;
        display: inline-flex; align-items: center; gap: 18px;
    }}
    .kicker::before {{ content: ''; width: 26px; height: 1px; background: {accent_color}; }}
    .sub {{
        font-family: 'Cormorant Garamond', serif; font-weight: 400;
        font-style: italic; color: #c8bfae; line-height: 1.35;
    }}"""


def _forza_common_chrome(accent_color: str, page_no: str = "01", total: str = "04", dots_active: int = 0, dots_total: int = 4) -> str:
    """Top-of-slide chrome: corners + brandmark + tagline + page number + bottom swipe dots + footer."""
    dots = "".join(f'<div class="dot{" active" if i == dots_active else ""}"></div>' for i in range(dots_total))
    return f"""
        <div class="corner-tl"></div><div class="corner-tr"></div>
        <div class="corner-bl"></div><div class="corner-br"></div>
        <div class="header">
          <div class="brandmark">
            <svg viewBox="0 0 100 110" xmlns="http://www.w3.org/2000/svg">
              <defs>
                <linearGradient id="fgoldbrand" x1="0" y1="0" x2="1" y2="1">
                  <stop offset="0%" stop-color="#e2c78e"/>
                  <stop offset="50%" stop-color="#c5a86c"/>
                  <stop offset="100%" stop-color="#8e7646"/>
                </linearGradient>
              </defs>
              <polygon points="12,8 88,8 74,30 0,30" fill="url(#fgoldbrand)"/>
              <polygon points="17,40 66,40 54,60 6,60" fill="url(#fgoldbrand)"/>
              <polygon points="22,70 48,70 30,100 0,100" fill="url(#fgoldbrand)"/>
            </svg>
            <span class="wm">FORZA</span>
          </div>
          <div class="tagline">Systems That Scale</div>
        </div>
        <div class="page-no">{page_no}<span class="dim"> / {total}</span></div>
        <div class="footer">
            <div class="footer-l">FORZA</div>
            <div class="footer-r">
              <div class="domain">FORZASYSTEMS.AI</div>
              <div>Operating Systems for Service Businesses</div>
            </div>
        </div>
        <div class="swipe">{dots}</div>"""


def _forza_cover_monolith(headline_top: str, headline_gold: str, headline_bottom: str,
                           logo_path: str, accent_color: str = "#C5A86C",
                           bg_image: str = "") -> str:
    """Variant: Monolith — a single towering gold chevron stack on the right half,
    strong directional light, headline anchored bottom-left. Vertical architecture,
    minimal composition. Reads at thumbnail because the chevron is a single bold silhouette."""
    photo_uri = _file_to_data_uri(bg_image) if bg_image else ""
    return f"""{_forza_common_head(accent_color)}
    .slide {{
        background:
          radial-gradient(ellipse at 78% 50%, rgba(230,195,120,0.32) 0%, transparent 55%),
          radial-gradient(ellipse at 15% 15%, rgba(197,168,108,0.12) 0%, transparent 45%),
          linear-gradient(105deg, rgba(26,19,12,{0.60 if photo_uri else 1}) 0%, rgba(35,26,18,{0.72 if photo_uri else 1}) 40%, rgba(17,12,8,{0.82 if photo_uri else 1}) 100%),
          {f"url('{photo_uri}') center/cover no-repeat," if photo_uri else ""}
          #110c08;
        overflow: hidden;
    }}
    .stripes {{
        position: absolute; inset: 0; z-index: 0; opacity: 0.6;
        background: repeating-linear-gradient(90deg, transparent 0 96px, rgba(197,168,108,0.05) 96px 97px);
    }}
    .monolith {{
        position: absolute; top: 90px; right: 0; bottom: 90px; width: 520px;
        z-index: 3;
        filter: drop-shadow(-30px 0 80px rgba(230,195,120,0.25));
    }}
    .monolith svg {{ width: 100%; height: 100%; }}
    .content {{
        position: absolute; left: 90px; right: 560px; bottom: 220px;
        z-index: 8; max-width: 560px;
    }}
    .kicker {{ margin-bottom: 30px; }}
    .headline {{ font-size: 76px; margin-bottom: 26px; }}
    .sub {{ font-size: 28px; max-width: 500px; }}
    .accent-bar {{
        position: absolute; left: 90px; bottom: 180px;
        width: 140px; height: 3px;
        background: linear-gradient(to right, {accent_color}, transparent);
        z-index: 6;
    }}
    </style></head><body>
    <div class="slide">
        <div class="stripes"></div>
        <div class="monolith">
          <svg viewBox="0 0 520 900" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="xMidYMid meet">
            <defs>
              <linearGradient id="monoGold" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="#f0d9a3" stop-opacity="1"/>
                <stop offset="55%" stop-color="#c5a86c" stop-opacity="0.95"/>
                <stop offset="100%" stop-color="#6b5432" stop-opacity="0.75"/>
              </linearGradient>
              <linearGradient id="monoShadow" x1="0" y1="0" x2="1" y2="0">
                <stop offset="0%" stop-color="rgba(10,7,4,0.8)"/>
                <stop offset="100%" stop-color="rgba(10,7,4,0)"/>
              </linearGradient>
            </defs>
            <!-- Ground shadow strip -->
            <rect x="0" y="830" width="520" height="2" fill="{accent_color}" opacity="0.5"/>
            <!-- Stacked chevron tower: 6 descending chevrons, largest at bottom -->
            <g fill="url(#monoGold)">
              <polygon points="60,90 420,90 380,170 20,170"/>
              <polygon points="90,200 410,200 372,280 52,280"/>
              <polygon points="120,310 400,310 364,390 84,390"/>
              <polygon points="150,420 390,420 356,500 116,500"/>
              <polygon points="180,530 380,530 348,610 148,610"/>
              <polygon points="210,640 370,640 340,720 180,720"/>
            </g>
            <!-- Front-lit edge highlights -->
            <g stroke="#f5e7c2" stroke-width="1.5" stroke-opacity="0.45" fill="none">
              <path d="M 60 90 L 420 90"/>
              <path d="M 90 200 L 410 200"/>
              <path d="M 120 310 L 400 310"/>
              <path d="M 150 420 L 390 420"/>
              <path d="M 180 530 L 380 530"/>
              <path d="M 210 640 L 370 640"/>
            </g>
            <!-- Shadow wash on left of tower -->
            <rect x="0" y="0" width="220" height="900" fill="url(#monoShadow)"/>
            <!-- Altitude ticks on left -->
            <g stroke="{accent_color}" stroke-width="1" stroke-opacity="0.55">
              <line x1="20" y1="130" x2="50" y2="130"/>
              <line x1="20" y1="240" x2="50" y2="240"/>
              <line x1="20" y1="350" x2="50" y2="350"/>
              <line x1="20" y1="460" x2="50" y2="460"/>
              <line x1="20" y1="570" x2="50" y2="570"/>
              <line x1="20" y1="680" x2="50" y2="680"/>
            </g>
            <!-- Floor label strip -->
            <g font-family="Cinzel, serif" font-size="9" letter-spacing="3" fill="#d8cfb8" font-weight="600">
              <text x="20" y="137">L6 · REVENUE</text>
              <text x="20" y="247">L5 · OPERATIONS</text>
              <text x="20" y="357">L4 · BRAND</text>
              <text x="20" y="467">L3 · PEOPLE</text>
              <text x="20" y="577">L2 · DATA</text>
              <text x="20" y="687">L1 · FOUNDATION</text>
            </g>
          </svg>
        </div>
        <div class="content">
            <div class="kicker">{headline_top or 'Revenue Infrastructure'}</div>
            <h1 class="headline">{headline_gold}</h1>
            <div class="sub">{headline_bottom}</div>
        </div>
        <div class="accent-bar"></div>
        {_forza_common_chrome(accent_color)}
    </div></body></html>"""


def _forza_cover_flow(headline_top: str, headline_gold: str, headline_bottom: str,
                      logo_path: str, accent_color: str = "#C5A86C",
                      bg_image: str = "") -> str:
    """Variant: Flow — horizontal INPUT → ENGINE → OUTPUT process diagram with
    gold pipes. Headline at top, diagram fills bottom half. Different reading
    direction from the other variants, works at thumbnail because the three
    anchored boxes + arrows are legible."""
    photo_uri = _file_to_data_uri(bg_image) if bg_image else ""
    return f"""{_forza_common_head(accent_color)}
    .slide {{
        background:
          radial-gradient(ellipse at 50% 35%, rgba(230,195,120,0.22) 0%, transparent 55%),
          linear-gradient(180deg, rgba(38,32,14,{0.60 if photo_uri else 1}) 0%, rgba(21,16,10,{0.72 if photo_uri else 1}) 55%, rgba(10,8,5,{0.82 if photo_uri else 1}) 100%),
          {f"url('{photo_uri}') center/cover no-repeat," if photo_uri else ""}
          #0a0805;
        overflow: hidden;
    }}
    .dots {{
        position: absolute; inset: 0; z-index: 0;
        background-image: radial-gradient(circle, rgba(197,168,108,0.12) 1.5px, transparent 1.8px);
        background-size: 34px 34px; opacity: 0.8;
    }}
    .content {{
        position: absolute; left: 90px; right: 90px; top: 270px;
        z-index: 8;
    }}
    .kicker {{ margin-bottom: 26px; }}
    .headline {{ font-size: 72px; margin-bottom: 20px; max-width: 880px; }}
    .sub {{ font-size: 26px; max-width: 760px; }}
    .flow {{
        position: absolute; left: 0; right: 0; bottom: 170px;
        display: flex; justify-content: center; align-items: center;
        gap: 0; z-index: 5;
    }}
    .node {{
        width: 220px; height: 180px;
        background: linear-gradient(160deg, #2d2216 0%, #1a130b 100%);
        border: 1.5px solid {accent_color};
        border-radius: 4px;
        display: flex; flex-direction: column; align-items: center; justify-content: center;
        box-shadow: 0 8px 40px rgba(0,0,0,0.6), inset 0 0 0 1px rgba(230,195,120,0.15);
        position: relative;
    }}
    .node .idx {{
        position: absolute; top: 12px; left: 16px;
        font-family: 'Cinzel', serif; font-size: 11px; font-weight: 700;
        color: {accent_color}; letter-spacing: 3px;
    }}
    .node .label {{
        font-family: 'Cinzel', serif; font-size: 18px; font-weight: 700;
        color: #f5ecd6; letter-spacing: 6px; margin-bottom: 10px;
    }}
    .node .desc {{
        font-family: 'Inter', sans-serif; font-size: 11px; font-weight: 500;
        color: #c8bfae; letter-spacing: 2px; text-transform: uppercase;
        text-align: center;
    }}
    .pipe {{
        width: 96px; height: 2px;
        background: linear-gradient(to right, {accent_color}, {accent_color});
        position: relative;
        opacity: 0.85;
    }}
    .pipe::before {{
        content: ''; position: absolute; right: -2px; top: -7px;
        width: 0; height: 0; border: 8px solid transparent; border-left-color: {accent_color};
    }}
    .pipe::after {{
        content: ''; position: absolute; left: 20%; top: -12px; width: 8px; height: 8px;
        border-radius: 50%; background: {accent_color};
        box-shadow: 0 0 12px {accent_color};
    }}
    </style></head><body>
    <div class="slide">
        <div class="dots"></div>
        <div class="content">
            <div class="kicker">{headline_top or 'The Forza Operating System'}</div>
            <h1 class="headline">{headline_gold}</h1>
            <div class="sub">{headline_bottom}</div>
        </div>
        <div class="flow">
            <div class="node">
              <span class="idx">01</span>
              <div class="label">INPUT</div>
              <div class="desc">Leads · Calls · DMs<br>Forms · Inbox</div>
            </div>
            <div class="pipe"></div>
            <div class="node">
              <span class="idx">02</span>
              <div class="label">ENGINE</div>
              <div class="desc">Qualify · Route<br>Follow-up · Learn</div>
            </div>
            <div class="pipe"></div>
            <div class="node">
              <span class="idx">03</span>
              <div class="label">OUTPUT</div>
              <div class="desc">Booked Revenue<br>Clean Pipeline</div>
            </div>
        </div>
        {_forza_common_chrome(accent_color)}
    </div></body></html>"""


def _forza_cover_topology(headline_top: str, headline_gold: str, headline_bottom: str,
                           logo_path: str, accent_color: str = "#C5A86C",
                           bg_image: str = "") -> str:
    """Variant: Topology — concentric topographic contour rings radiating from the
    upper-right corner, like an elevation map. No hero SVG module; the background
    IS the visual. Headline gets centre-stage."""
    # Generate 18 contour curves as concentric elliptical arcs from the upper-right focal point.
    contours = ""
    for i, r in enumerate(range(140, 1620, 82)):
        opacity = max(0.08, 0.55 - (i * 0.025))
        stroke_width = 1.8 if i % 4 == 0 else 1.0
        contours += f'<circle cx="1080" cy="0" r="{r}" fill="none" stroke="{accent_color}" stroke-opacity="{opacity:.3f}" stroke-width="{stroke_width}"/>'
    photo_uri = _file_to_data_uri(bg_image) if bg_image else ""
    return f"""{_forza_common_head(accent_color)}
    .slide {{
        background:
          radial-gradient(ellipse at 90% 0%, rgba(230,195,120,0.35) 0%, transparent 50%),
          radial-gradient(ellipse at 30% 100%, rgba(140,110,60,0.18) 0%, transparent 55%),
          linear-gradient(160deg, rgba(38,29,20,{0.60 if photo_uri else 1}) 0%, rgba(21,16,10,{0.72 if photo_uri else 1}) 55%, rgba(10,8,5,{0.82 if photo_uri else 1}) 100%),
          {f"url('{photo_uri}') center/cover no-repeat," if photo_uri else ""}
          #0a0805;
        overflow: hidden;
    }}
    .topo {{
        position: absolute; inset: 0; z-index: 1; opacity: 0.85;
    }}
    .topo svg {{ width: 100%; height: 100%; }}
    .content {{
        position: absolute; left: 90px; right: 420px; top: 300px;
        z-index: 8;
    }}
    .kicker {{ margin-bottom: 30px; }}
    .headline {{ font-size: 94px; margin-bottom: 28px; }}
    .sub {{ font-size: 30px; max-width: 620px; }}
    .accent-bar {{
        position: absolute; left: 90px; bottom: 200px;
        width: 110px; height: 3px;
        background: linear-gradient(to right, {accent_color}, transparent);
        z-index: 6;
    }}
    .summit {{
        position: absolute; top: 70px; right: 70px;
        z-index: 4;
        font-family: 'Cinzel', serif; color: {accent_color};
        font-size: 10px; letter-spacing: 4px; font-weight: 600;
        text-align: right; opacity: 0.7;
    }}
    .summit .big {{
        font-size: 14px; color: #f5ecd6; letter-spacing: 6px;
        margin-top: 4px;
    }}
    </style></head><body>
    <div class="slide">
        <div class="topo">
          <svg viewBox="0 0 1080 1080" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="xMidYMid slice">
            {contours}
          </svg>
        </div>
        <div class="content">
            <div class="kicker">{headline_top or 'Navigate Complexity'}</div>
            <h1 class="headline">{headline_gold}</h1>
            <div class="sub">{headline_bottom}</div>
        </div>
        <div class="accent-bar"></div>
        {_forza_common_chrome(accent_color)}
    </div></body></html>"""


# ── FORZA COVER: dispatcher + variant picker ─────────────────────────────────
FORZA_COVER_VARIANTS = {
    "blueprint": _forza_cover_blueprint,
    "monolith": _forza_cover_monolith,
    "flow": _forza_cover_flow,
    "topology": _forza_cover_topology,
}


def pick_forza_cover_variant(exclude: list[str] | None = None) -> str:
    """Pick a Forza cover variant, excluding any in `exclude` (typically the
    variants used in the last 7 days for Forza). Falls back to the full set if
    exclusion leaves nothing to choose from (e.g. every variant used this week)."""
    pool = [name for name in FORZA_COVER_VARIANTS if name not in set(exclude or [])]
    if not pool:
        pool = list(FORZA_COVER_VARIANTS.keys())
    return random.choice(pool)


def generate_forza_cover_slide(headline_top: str, headline_gold: str, headline_bottom: str,
                                logo_path: str, accent_color: str = "#C5A86C",
                                variant: str | None = None,
                                bg_image: str = "") -> tuple[str, str]:
    """Dispatch to one of the Forza cover variants. Returns (html, variant_name)
    so callers can record which variant was used for 7-day dedup. `bg_image`
    is the photo path (from templates/forza_covers/) that sits behind the
    variant's existing hero + chrome."""
    if not variant or variant not in FORZA_COVER_VARIANTS:
        variant = pick_forza_cover_variant()
    fn = FORZA_COVER_VARIANTS[variant]
    return fn(headline_top, headline_gold, headline_bottom, logo_path, accent_color, bg_image), variant


def generate_forza_cta_slide(cta_text: str, accent_color: str = "#C5A86C") -> str:
    """Forza CTA — ink bg, F-chevron mark, wordmark, single CTA line, gold arc.
    Never uses logo_forza.png (which has the wordmark baked in and would duplicate)."""
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
    <link href="https://fonts.googleapis.com/css2?family=Cinzel:wght@500;600;700&family=Cormorant+Garamond:ital,wght@0,400;0,500;1,400&display=swap" rel="stylesheet">
    <style>
    {_base_css()}
    body {{ background: #0a0a0a; font-family: 'Cormorant Garamond', serif; }}
    .slide {{
        background: radial-gradient(ellipse at 50% 20%, #1a1610 0%, #0a0a0a 55%, #050503 100%);
        display: flex; flex-direction: column; align-items: center; justify-content: center;
        gap: 36px; padding: 0 80px;
    }}
    .gold-arc {{
        position: absolute; top: -25%; left: -25%;
        width: 150%; height: 150%;
        background: radial-gradient(circle, rgba(197,168,108,0.08) 0%, transparent 50%);
        pointer-events: none;
    }}
    .mark svg {{ width: 120px; height: 132px; }}
    .wordmark {{
        font-family: 'Cinzel', serif; font-weight: 700; color: {accent_color};
        letter-spacing: 16px; font-size: 38px; margin-top: -6px;
    }}
    .tagline {{
        font-family: 'Cinzel', serif; font-weight: 500; color: #6e6a62;
        letter-spacing: 6px; font-size: 14px; text-transform: uppercase; margin-top: 4px;
    }}
    .gold-line {{ width: 64px; height: 2px; background: {accent_color}; }}
    .cta {{
        font-family: 'Cormorant Garamond', serif; font-style: italic; font-weight: 500;
        font-size: 48px; line-height: 1.25; color: #f7f3ea;
        text-align: center; max-width: 820px; margin-top: 8px;
    }}
    .cta .hl {{ color: {accent_color}; font-style: normal; font-weight: 600; }}
    .site {{
        font-family: 'Cinzel', serif; font-weight: 600; color: {accent_color};
        letter-spacing: 6px; font-size: 18px; text-transform: uppercase; margin-top: 24px;
    }}
    </style></head><body>
    <div class="slide">
        <div class="gold-arc"></div>
        <div class="mark">
          <svg viewBox="0 0 100 110" xmlns="http://www.w3.org/2000/svg">
            <defs>
              <linearGradient id="fgoldcta" x1="0" y1="0" x2="1" y2="1">
                <stop offset="0%" stop-color="#e2c78e"/>
                <stop offset="50%" stop-color="#c5a86c"/>
                <stop offset="100%" stop-color="#8e7646"/>
              </linearGradient>
            </defs>
            <polygon points="12,8 88,8 74,30 0,30" fill="url(#fgoldcta)"/>
            <polygon points="17,40 66,40 54,60 6,60" fill="url(#fgoldcta)"/>
            <polygon points="22,70 48,70 30,100 0,100" fill="url(#fgoldcta)"/>
          </svg>
        </div>
        <div class="wordmark">FORZA</div>
        <div class="tagline">Systems That Scale</div>
        <div class="gold-line"></div>
        <div class="cta">{cta_text}</div>
        <div class="site">forzasystems.ai</div>
    </div></body></html>"""


def generate_cover_slide(headline_top: str, headline_gold: str, headline_bottom: str,
                         bg_image: str, logo_path: str, accent_color: str = "#C9A06C") -> str:
    """Cover slide — dramatic background, headline, logo top-left, swipe arrow."""
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><style>
    {_base_css()}
    .bg-image {{
        position: absolute; top: 0; left: 0; width: 100%; height: 100%;
        background-image: url('{_file_to_data_uri(bg_image)}');
        background-size: cover; background-position: center;
        filter: brightness(0.65) saturate(0.85);
    }}
    .gradient-overlay {{
        position: absolute; bottom: 0; left: 0; width: 100%; height: 55%;
        background: linear-gradient(to top, rgba(0,0,0,0.92) 0%, rgba(0,0,0,0.5) 50%, transparent 100%);
    }}
    .logo-watermark {{
        position: absolute; top: 40px; left: 40px;
        width: 100px; opacity: 0.2;
    }}
    .content {{
        position: absolute; bottom: 0; left: 0; width: 100%;
        padding: 0 70px 100px; text-align: center; z-index: 10;
    }}
    .headline-small {{
        font-size: 32px; font-weight: 700; color: #ffffff;
        text-transform: uppercase; letter-spacing: 3px; line-height: 1.25; margin-bottom: 10px;
    }}
    .headline-big {{
        font-size: 48px; font-weight: 700; color: {accent_color};
        text-transform: uppercase; letter-spacing: 1px; line-height: 1.1; margin-bottom: 10px;
    }}
    .headline-sub {{
        font-size: 28px; font-weight: 700; color: rgba(255,255,255,0.85);
        text-transform: uppercase; letter-spacing: 3px; line-height: 1.25;
    }}
    .swipe-arrow {{
        position: absolute; bottom: 30px; left: 50%; transform: translateX(-50%);
        width: 44px; height: 44px; border: 2px solid rgba(255,255,255,0.7);
        border-radius: 50%; display: flex; align-items: center; justify-content: center; z-index: 10;
    }}
    .swipe-arrow svg {{ width: 20px; height: 20px; fill: none; stroke: rgba(255,255,255,0.8); stroke-width: 2.5; }}
    </style></head><body>
    <div class="slide">
        <div class="bg-image"></div>
        <div class="gradient-overlay"></div>
        <img class="logo-watermark" src="{_file_to_data_uri(logo_path)}" alt="">
        <div class="content">
            <div class="headline-small">{headline_top}</div>
            <div class="headline-big">{headline_gold}</div>
            <div class="headline-sub">{headline_bottom}</div>
        </div>
        <div class="swipe-arrow">
            <svg viewBox="0 0 24 24"><path d="M5 12h14M12 5l7 7-7 7"/></svg>
        </div>
    </div></body></html>"""


def generate_data_slide(headline_gold: str, headline_white: str, bullets: list[str],
                        logo_path: str, accent_color: str = "#C9A06C") -> str:
    """Data/stats slide — dark bg with stripes, bullet points centered."""
    bullet_html = "\n".join(
        f'<div class="stat-text"><span class="bullet">&bull;</span> {b}</div>'
        for b in bullets
    )
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><style>
    {_base_css()}
    {_stripe_bg_css()}
    .content {{
        position: relative; z-index: 10;
        padding: 80px 70px;
        height: 100%; display: flex; flex-direction: column; justify-content: center;
        text-align: center;
    }}
    .headline {{
        font-size: 42px; font-weight: 900; color: {accent_color};
        text-transform: uppercase; letter-spacing: 1px; line-height: 1.15;
        margin-bottom: 15px;
    }}
    .headline-white {{
        font-size: 42px; font-weight: 900; color: #ffffff;
        text-transform: uppercase; letter-spacing: 1px; line-height: 1.15;
        margin-bottom: 55px;
    }}
    .stats {{ display: flex; flex-direction: column; gap: 35px; }}
    .stat-text {{
        font-size: 28px; font-weight: 700; color: #ffffff;
        text-transform: uppercase; letter-spacing: 1.5px; line-height: 1.4;
    }}
    .bullet {{ color: {accent_color}; font-weight: 700; }}
    .stat-highlight {{ color: {accent_color}; font-weight: 800; }}
    .logo-bottom {{
        position: absolute; bottom: 40px; left: 50%; transform: translateX(-50%);
        width: 65px; opacity: 0.5;
    }}
    </style></head><body>
    <div class="slide">
        {_stripe_bg_html()}
        <div class="content">
            <div class="headline">{headline_gold}</div>
            <div class="headline-white">{headline_white}</div>
            <div class="stats">{bullet_html}</div>
        </div>
        <img class="logo-bottom" src="{_file_to_data_uri(logo_path)}" alt="">
    </div></body></html>"""


def generate_insight_slide(headline_gold: str, headline_white: str, bullets: list[str],
                           closing_white: str, closing_gold: str,
                           logo_path: str, accent_color: str = "#C9A06C") -> str:
    """Insight slide — data + closing statement with divider."""
    bullet_html = "\n".join(
        f'<div class="stat-text"><span class="bullet">&bull;</span> {b}</div>'
        for b in bullets
    )
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><style>
    {_base_css()}
    {_stripe_bg_css()}
    .content {{
        position: relative; z-index: 10;
        padding: 80px 70px;
        height: 100%; display: flex; flex-direction: column; justify-content: center;
        text-align: center;
    }}
    .headline {{
        font-size: 40px; font-weight: 900; color: {accent_color};
        text-transform: uppercase; letter-spacing: 1px; line-height: 1.15;
        margin-bottom: 10px;
    }}
    .headline-white {{
        font-size: 40px; font-weight: 900; color: #ffffff;
        text-transform: uppercase; letter-spacing: 1px; line-height: 1.15;
        margin-bottom: 45px;
    }}
    .stats {{ display: flex; flex-direction: column; gap: 28px; margin-bottom: 40px; }}
    .stat-text {{
        font-size: 26px; font-weight: 700; color: #ffffff;
        text-transform: uppercase; letter-spacing: 1.5px; line-height: 1.4;
    }}
    .bullet {{ color: {accent_color}; font-weight: 700; }}
    .divider {{ width: 180px; height: 2px; background: linear-gradient(to right, transparent, {accent_color}, transparent); margin: 0 auto 35px; }}
    .closing {{ font-size: 30px; font-weight: 900; color: #ffffff; text-transform: uppercase; letter-spacing: 2px; line-height: 1.4; }}
    .closing-gold {{ font-size: 30px; font-weight: 900; color: {accent_color}; text-transform: uppercase; letter-spacing: 2px; line-height: 1.4; }}
    .logo-bottom {{
        position: absolute; bottom: 40px; left: 50%; transform: translateX(-50%);
        width: 65px; opacity: 0.5;
    }}
    </style></head><body>
    <div class="slide">
        {_stripe_bg_html()}
        <div class="content">
            <div class="headline">{headline_gold}</div>
            <div class="headline-white">{headline_white}</div>
            <div class="stats">{bullet_html}</div>
            <div class="divider"></div>
            <div class="closing">{closing_white}</div>
            <div class="closing-gold">{closing_gold}</div>
        </div>
        <img class="logo-bottom" src="{_file_to_data_uri(logo_path)}" alt="">
    </div></body></html>"""


def generate_photo_data_slide(headline_gold: str, headline_white: str, bullets: list[str],
                              bg_image: str, logo_path: str, accent_color: str = "#C9A06C") -> str:
    """Photo background + data overlay — image top half, text bottom half."""
    bullet_html = "\n".join(
        f'<div class="stat-text"><span class="bullet">&bull;</span> {b}</div>'
        for b in bullets
    )
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><style>
    {_base_css()}
    .bg-image {{
        position: absolute; top: 0; left: 0; width: 100%; height: 100%;
        background-image: url('{_file_to_data_uri(bg_image)}');
        background-size: cover; background-position: center;
        filter: brightness(0.5) saturate(0.8);
    }}
    .gradient-overlay {{
        position: absolute; bottom: 0; left: 0; width: 100%; height: 70%;
        background: linear-gradient(to top, rgba(0,0,0,0.95) 0%, rgba(0,0,0,0.6) 50%, transparent 100%);
    }}
    .logo-watermark {{
        position: absolute; top: 35%; left: 50%; transform: translate(-50%, -50%);
        width: 140px; opacity: 0.1;
    }}
    .content {{
        position: absolute; bottom: 0; left: 0; width: 100%;
        padding: 0 70px 50px; text-align: center; z-index: 10;
    }}
    .headline {{
        font-size: 36px; font-weight: 900; color: {accent_color};
        text-transform: uppercase; letter-spacing: 1px; line-height: 1.15;
        margin-bottom: 8px;
    }}
    .headline-white {{
        font-size: 36px; font-weight: 900; color: #ffffff;
        text-transform: uppercase; letter-spacing: 1px; line-height: 1.15;
        margin-bottom: 30px;
    }}
    .stats {{ display: flex; flex-direction: column; gap: 20px; }}
    .stat-text {{
        font-size: 24px; font-weight: 700; color: #ffffff;
        text-transform: uppercase; letter-spacing: 1.5px; line-height: 1.4;
    }}
    .bullet {{ color: {accent_color}; font-weight: 700; }}
    </style></head><body>
    <div class="slide">
        <div class="bg-image"></div>
        <div class="gradient-overlay"></div>
        <img class="logo-watermark" src="{_file_to_data_uri(logo_path)}" alt="">
        <div class="content">
            <div class="headline">{headline_gold}</div>
            <div class="headline-white">{headline_white}</div>
            <div class="stats">{bullet_html}</div>
        </div>
    </div></body></html>"""


def generate_cta_slide(cta_text: str, brand_name: str, logo_path: str,
                       accent_color: str = "#C9A06C") -> str:
    """CTA slide — black background, centered text + logo."""
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><style>
    {_base_css()}
    .slide {{
        width: 1080px; height: 1080px; position: relative;
        display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 40px;
    }}
    .cta-text {{
        font-size: 42px; font-weight: 800; color: #ffffff;
        text-transform: uppercase; text-align: center; letter-spacing: 3px; line-height: 1.35;
        padding: 0 80px;
    }}
    .logo-center {{ width: 120px; }}
    .brand-name {{
        font-size: 28px; font-weight: 800; color: {accent_color};
        text-transform: uppercase; letter-spacing: 8px; margin-top: -15px;
        font-family: 'Montserrat', sans-serif;
    }}
    </style></head><body>
    <div class="slide">
        <div class="cta-text">{cta_text}</div>
        <img class="logo-center" src="{_file_to_data_uri(logo_path)}" alt="">
        <div class="brand-name">{brand_name}</div>
    </div></body></html>"""


async def render_html_to_png(html_content: str, output_path: str) -> bool:
    """Render HTML string to PNG using Playwright. Uses temp file for local image access."""
    try:
        import tempfile
        from playwright.async_api import async_playwright

        # Write HTML to temp file so file:// URLs for images work
        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', dir=str(TEMPLATE_DIR), delete=False) as f:
            f.write(html_content)
            tmp_path = f.name

        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page(viewport={"width": 1080, "height": 1080})
            await page.goto(f"file://{tmp_path}", wait_until="networkidle")
            await page.wait_for_timeout(500)
            await page.screenshot(path=output_path, type="png")
            await browser.close()

        os.unlink(tmp_path)
        log.info(f"Rendered: {output_path}")
        return True
    except Exception as e:
        log.error(f"Render failed: {e}")
        return False


async def render_carousel(slides_content: list[dict], brand: str,
                           exclude_backgrounds: list[str] | None = None,
                           exclude_forza_variants: list[str] | None = None) -> tuple[list[bytes], dict]:
    """
    Render a full carousel from structured content.

    slides_content format:
    [
        {"type": "cover", "headline_top": "...", "headline_gold": "...", "headline_bottom": "..."},
        {"type": "data", "headline_gold": "...", "headline_white": "...", "bullets": [...]},
        {"type": "insight", "headline_gold": "...", "headline_white": "...", "bullets": [...], "closing_white": "...", "closing_gold": "..."},
        {"type": "photo_data", "headline_gold": "...", "headline_white": "...", "bullets": [...]},
        {"type": "cta", "cta_text": "...", "brand_name": "..."},
    ]
    """
    from playwright.async_api import async_playwright

    _init_backgrounds()

    # Brand-specific assets
    BRAND_ASSETS = {
        "nucassa_re": {"logo": str(ASSETS_DIR / "logo_nucassa.png"), "accent": "#C9A06C", "name": "NUCASSA"},
        "nucassa_holdings": {"logo": str(ASSETS_DIR / "logo_nucassa.png"), "accent": "#C9A06C", "name": "NUCASSA HOLDINGS LTD"},
        "listr": {"logo": str(ASSETS_DIR / "logo_listr.png"), "accent": "#B8962E", "name": "LISTR.AE"},
        "forza": {"logo": str(ASSETS_DIR / "logo_forza.png"), "accent": "#C5A86C", "name": "FORZA"},
    }
    brand_cfg = BRAND_ASSETS.get(brand, BRAND_ASSETS["nucassa_re"])
    logo_path = brand_cfg["logo"]
    accent = brand_cfg["accent"]

    images = []
    # Track visuals picked so the caller can log them for 7-day dedup.
    visuals_used: dict = {"backgrounds": [], "forza_cover_variant": None}

    import tempfile

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport={"width": 1080, "height": 1080})

        def _coerce(slide: dict, i: int, total: int) -> dict:
            """Map the older {headline, subtext, stats|points, cta_line} schema into the renderer's expected fields."""
            s = dict(slide)
            # Normalize type to lowercase. Claude has been seen to return
            # "Cover", "Title", "Intro" etc. — without normalising, the renderer
            # would silently `continue` past slide 0 and produce a 3-slide
            # carousel with no cover, which is exactly the "no slide 1" bug.
            raw_type = (s.get("type") or "").strip().lower()
            valid = {"cover", "data", "insight", "photo_data", "cta"}
            if raw_type not in valid:
                if i == 0:
                    s["type"] = "cover"
                elif i == total - 1 and (s.get("cta_line") or s.get("cta_text") or "CTA" in str(s.get("headline", "")).upper()):
                    s["type"] = "cta"
                elif s.get("stats") or s.get("points") or s.get("bullets"):
                    s["type"] = "data"
                else:
                    s["type"] = "insight"
                if raw_type:
                    log.warning(f"[render] slide {i} unrecognised type '{slide.get('type')}' → coerced to '{s['type']}'")
            else:
                s["type"] = raw_type
            # Slide 0 is always a cover. If Claude returned 4 slides but the
            # first one is "data" or "insight", the carousel still has no
            # cover. Force it.
            if i == 0 and s["type"] != "cover":
                log.warning(f"[render] slide 0 was '{s['type']}' — forcing 'cover' so the carousel has a slide 1")
                s["type"] = "cover"
            # Fill missing fields from the old schema
            headline = s.get("headline", "")
            subtext = s.get("subtext", "")
            bullets = s.get("bullets") or s.get("stats") or s.get("points") or []
            bullets = [str(b) for b in bullets] if bullets else []
            s.setdefault("headline_gold", subtext or headline)
            s.setdefault("headline_white", headline)
            s.setdefault("headline_top", subtext or "")
            s.setdefault("headline_bottom", "")
            s.setdefault("bullets", bullets)
            if s["type"] == "cta":
                s.setdefault("cta_text", s.get("cta_line") or s.get("cta") or headline or subtext or "")
            return s

        total_slides = len(slides_content)
        for i, raw_slide in enumerate(slides_content):
            slide = _coerce(raw_slide, i, total_slides)
            slide_type = slide.get("type", "data")

            is_forza = (brand == "forza")

            if slide_type == "cover":
                if is_forza:
                    # Forza cover: pick one of the 4 template variants + one of
                    # the Forza background photos. Both dedup across 7 days
                    # (variant via exclude_forza_variants, photo via exclude_backgrounds).
                    variant = pick_forza_cover_variant(exclude_forza_variants)
                    bg = pick_forza_cover_photo(exclude=exclude_backgrounds)
                    html, picked_variant = generate_forza_cover_slide(
                        slide["headline_top"], slide["headline_gold"], slide.get("headline_bottom", ""),
                        logo_path, accent, variant=variant, bg_image=bg,
                    )
                    visuals_used["forza_cover_variant"] = picked_variant
                    if bg:
                        visuals_used["backgrounds"].append(bg)
                else:
                    bg = slide.get("bg_image") or pick_background(
                        slide.get("headline_gold", ""), exclude=exclude_backgrounds,
                    )
                    if bg:
                        visuals_used["backgrounds"].append(bg)
                    html = generate_cover_slide(
                        slide["headline_top"], slide["headline_gold"], slide.get("headline_bottom", ""),
                        bg, logo_path, accent
                    )
            elif slide_type == "data":
                html = generate_data_slide(
                    slide["headline_gold"], slide["headline_white"], slide["bullets"],
                    logo_path, accent
                )
            elif slide_type == "insight":
                html = generate_insight_slide(
                    slide["headline_gold"], slide["headline_white"], slide["bullets"],
                    slide.get("closing_white", ""), slide.get("closing_gold", ""),
                    logo_path, accent
                )
            elif slide_type == "photo_data":
                if is_forza:
                    # Forza has no photo variant — degrade to data slide, keeps type contract.
                    html = generate_data_slide(
                        slide["headline_gold"], slide["headline_white"], slide["bullets"],
                        logo_path, accent
                    )
                else:
                    # Dedup against backgrounds already picked for this post AND
                    # those used in the last 7 days (passed via exclude_backgrounds).
                    already = set(exclude_backgrounds or []) | set(visuals_used["backgrounds"])
                    bg = slide.get("bg_image") or pick_background(
                        slide.get("headline_gold", ""), exclude=list(already),
                    )
                    if bg:
                        visuals_used["backgrounds"].append(bg)
                    html = generate_photo_data_slide(
                        slide["headline_gold"], slide["headline_white"], slide["bullets"],
                        bg, logo_path, accent
                    )
            elif slide_type == "cta":
                if is_forza:
                    # Forza CTA has the F-chevron + wordmark built-in. Passing the
                    # regular logo_forza.png would duplicate "FORZA" on the slide.
                    html = generate_forza_cta_slide(slide["cta_text"], accent)
                else:
                    html = generate_cta_slide(
                        slide["cta_text"], slide.get("brand_name", brand_cfg["name"]),
                        logo_path, accent
                    )
            else:
                # Should be unreachable now that _coerce normalises every type,
                # but if a new slide_type ever lands in JSON we want the noise.
                log.error(f"[render] slide {i} unhandled type '{slide_type}' — appending blank placeholder so caller sees the gap")
                images.append(b"")
                continue

            # Write to temp file so file:// URLs work for images
            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', dir=str(TEMPLATE_DIR), delete=False) as f:
                f.write(html)
                tmp_path = f.name

            try:
                # networkidle can hang for ~30s if Google Fonts is slow on
                # Railway egress — fall back to domcontentloaded so we still
                # get a screenshot rather than a Playwright timeout.
                await page.goto(f"file://{tmp_path}", wait_until="networkidle", timeout=15000)
            except Exception as e:
                log.warning(f"[render] slide {i+1} networkidle timed out ({e}); falling back to domcontentloaded")
                await page.goto(f"file://{tmp_path}", wait_until="domcontentloaded", timeout=15000)
            await page.wait_for_timeout(800 if slide_type == "cover" else 400)
            img_bytes = await page.screenshot(type="png")
            images.append(img_bytes)
            os.unlink(tmp_path)
            log.info(f"[render] slide {i+1}/{len(slides_content)} type={slide_type} bytes={len(img_bytes)}")

        await browser.close()

    return images, visuals_used
