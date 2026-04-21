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
ASSETS_DIR = TEMPLATE_DIR / "assets"


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
    """Scan backgrounds folder and auto-tag images by filename."""
    if BG_TAGS.get("all"):
        return  # already initialized

    all_bgs = sorted(BG_DIR.glob("*.png")) + sorted(BG_DIR.glob("*.jpg"))

    # Simple round-robin assignment for now — can be refined with Claude vision later
    categories = list(BG_TAGS.keys())
    for i, bg in enumerate(all_bgs):
        cat = categories[i % len(categories)]
        BG_TAGS[cat].append(str(bg))

    # Also make a flat "all" list
    BG_TAGS["all"] = [str(bg) for bg in all_bgs]
    log.info(f"Loaded {len(all_bgs)} background images across {len(categories)} categories")


def pick_background(topic: str = "", category: str = "") -> str:
    """Pick a background image matching the topic/category."""
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


def generate_forza_cover_slide(headline_top: str, headline_gold: str, headline_bottom: str,
                                logo_path: str, accent_color: str = "#C5A86C") -> str:
    """Forza cover — premium blueprint visualisation of an Operating System: central core +
    four labelled infrastructure nodes + connecting paths. The visual *is* what we sell.
    Bold typography, gold-on-ink, no Dubai photos (Forza is a global B2B brand)."""
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
    <link href="https://fonts.googleapis.com/css2?family=Cinzel:wght@500;600;700;800&family=Cormorant+Garamond:ital,wght@0,500;0,600;1,400&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
    {_base_css()}
    body {{ background: #050505; font-family: 'Cormorant Garamond', serif; }}
    .slide {{
        background:
          radial-gradient(circle at 75% 28%, rgba(197,168,108,0.20) 0%, transparent 36%),
          radial-gradient(ellipse at 18% 88%, rgba(140,110,60,0.10) 0%, transparent 50%),
          linear-gradient(135deg, #181410 0%, #0a0805 50%, #050402 100%);
        overflow: hidden;
    }}
    /* faint architectural blueprint grid */
    .grid {{
        position: absolute; inset: 0;
        background:
          repeating-linear-gradient(0deg, transparent 0 59px, rgba(197,168,108,0.04) 59px 60px),
          repeating-linear-gradient(90deg, transparent 0 59px, rgba(197,168,108,0.04) 59px 60px);
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
                <stop offset="0%" stop-color="#1a1410"/>
                <stop offset="100%" stop-color="#050402"/>
              </linearGradient>
            </defs>
            <!-- connection paths (dashed gold) -->
            <g stroke="url(#goldwire)" stroke-width="1.2" fill="none" stroke-dasharray="3,4">
              <path d="M 240 270 L 100 110"/>
              <path d="M 240 270 L 380 110"/>
              <path d="M 240 270 L 100 430"/>
              <path d="M 240 270 L 380 430"/>
            </g>
            <!-- solid gold orbital ring -->
            <circle cx="240" cy="270" r="120" fill="none" stroke="{accent_color}" stroke-width="0.8" stroke-opacity="0.35"/>
            <circle cx="240" cy="270" r="170" fill="none" stroke="{accent_color}" stroke-width="0.6" stroke-opacity="0.18" stroke-dasharray="2,5"/>
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
              <circle cx="100" cy="110" r="32" fill="url(#nodefill)" stroke="{accent_color}" stroke-width="1.2"/>
              <text x="100" y="108" text-anchor="middle">REVENUE</text>
              <text x="100" y="122" text-anchor="middle" font-size="7" fill="#8a8275" letter-spacing="2">CRM · Sales</text>
              <!-- TR: Operations -->
              <circle cx="380" cy="110" r="32" fill="url(#nodefill)" stroke="{accent_color}" stroke-width="1.2"/>
              <text x="380" y="108" text-anchor="middle">OPS</text>
              <text x="380" y="122" text-anchor="middle" font-size="7" fill="#8a8275" letter-spacing="2">Workflow</text>
              <!-- BL: Brand -->
              <circle cx="100" cy="430" r="32" fill="url(#nodefill)" stroke="{accent_color}" stroke-width="1.2"/>
              <text x="100" y="428" text-anchor="middle">BRAND</text>
              <text x="100" y="442" text-anchor="middle" font-size="7" fill="#8a8275" letter-spacing="2">Web · Content</text>
              <!-- BR: People -->
              <circle cx="380" cy="430" r="32" fill="url(#nodefill)" stroke="{accent_color}" stroke-width="1.2"/>
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


async def render_carousel(slides_content: list[dict], brand: str) -> list[bytes]:
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

    import tempfile

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport={"width": 1080, "height": 1080})

        def _coerce(slide: dict, i: int, total: int) -> dict:
            """Map the older {headline, subtext, stats|points, cta_line} schema into the renderer's expected fields."""
            s = dict(slide)
            # Infer type if missing
            if "type" not in s:
                if i == 0:
                    s["type"] = "cover"
                elif i == total - 1 and (s.get("cta_line") or s.get("cta_text") or "CTA" in str(s.get("headline", "")).upper()):
                    s["type"] = "cta"
                elif s.get("stats") or s.get("points") or s.get("bullets"):
                    s["type"] = "data"
                else:
                    s["type"] = "insight"
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
                    # Forza cover is typography + gold chevron pattern, never a Dubai photo.
                    html = generate_forza_cover_slide(
                        slide["headline_top"], slide["headline_gold"], slide.get("headline_bottom", ""),
                        logo_path, accent
                    )
                else:
                    bg = slide.get("bg_image") or pick_background(slide.get("headline_gold", ""))
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
                    bg = slide.get("bg_image") or pick_background(slide.get("headline_gold", ""))
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
                continue

            # Write to temp file so file:// URLs work for images
            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', dir=str(TEMPLATE_DIR), delete=False) as f:
                f.write(html)
                tmp_path = f.name

            await page.goto(f"file://{tmp_path}", wait_until="networkidle")
            await page.wait_for_timeout(400)
            img_bytes = await page.screenshot(type="png")
            images.append(img_bytes)
            os.unlink(tmp_path)
            log.info(f"Rendered slide {i+1}/{len(slides_content)} ({slide_type})")

        await browser.close()

    return images
