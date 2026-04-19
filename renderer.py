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
    """Forza cover — ink bg, gold chevron texture, Cinzel headline. No photo.
    Forza is a global B2B brand — never uses Dubai lifestyle imagery."""
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
    <link href="https://fonts.googleapis.com/css2?family=Cinzel:wght@500;600;700&family=Cormorant+Garamond:ital,wght@0,400;0,500;1,400&display=swap" rel="stylesheet">
    <style>
    {_base_css()}
    body {{ background: #0a0a0a; font-family: 'Cormorant Garamond', serif; }}
    .slide {{
        background: radial-gradient(ellipse at 20% 10%, #1a1610 0%, #0a0a0a 55%, #050503 100%);
        overflow: hidden;
    }}
    .chevron-pattern {{
        position: absolute; top: -10%; right: -15%; width: 90%; height: 110%;
        background:
          repeating-linear-gradient(-25deg, transparent 0 80px, rgba(197,168,108,0.025) 80px 82px),
          repeating-linear-gradient(25deg, transparent 0 120px, rgba(197,168,108,0.02) 120px 121px);
        opacity: .9;
    }}
    .gold-arc {{
        position: absolute; top: -30%; right: -20%;
        width: 95%; height: 95%;
        background: radial-gradient(circle, rgba(197,168,108,0.14) 0%, transparent 55%);
    }}
    .mini-mark {{
        position: absolute; top: 60px; left: 60px;
        display: flex; align-items: center; gap: 12px; opacity: .9;
    }}
    .mini-mark svg {{ width: 44px; height: 50px; }}
    .mini-mark .tick {{
        color: {accent_color}; font-family: 'Cinzel', serif; font-weight: 600;
        font-size: 20px; letter-spacing: 6px;
    }}
    .divider {{
        position: absolute; top: 60px; right: 60px;
        font-family: 'Cinzel', serif; font-size: 12px; color: #8a8378;
        letter-spacing: 4px; text-transform: uppercase; font-weight: 500;
    }}
    .content {{
        position: absolute; inset: 0;
        display: flex; flex-direction: column; justify-content: center;
        padding: 0 90px; z-index: 5;
    }}
    .kicker {{
        font-family: 'Cinzel', serif; font-size: 22px; font-weight: 500;
        color: {accent_color}; letter-spacing: 8px; text-transform: uppercase;
        margin-bottom: 28px;
    }}
    .headline {{
        font-family: 'Cormorant Garamond', serif; font-style: italic; font-weight: 500;
        font-size: 78px; line-height: 1.05; color: #f7f3ea;
        margin-bottom: 24px;
    }}
    .headline .hl {{ color: {accent_color}; font-style: normal; font-weight: 600; }}
    .sub {{
        font-family: 'Cormorant Garamond', serif; font-size: 34px; font-weight: 400;
        color: #b5ada0; line-height: 1.35; max-width: 780px;
    }}
    .gold-line {{
        position: absolute; bottom: 170px; left: 90px;
        width: 80px; height: 2px; background: {accent_color};
    }}
    .footer {{
        position: absolute; bottom: 70px; left: 90px; right: 90px;
        display: flex; justify-content: space-between; align-items: center;
    }}
    .footer .brand {{
        font-family: 'Cinzel', serif; font-weight: 700; color: {accent_color};
        letter-spacing: 10px; font-size: 18px;
    }}
    .footer .site {{
        font-family: 'Cinzel', serif; font-weight: 500; color: #6e6a62;
        letter-spacing: 4px; font-size: 12px; text-transform: uppercase;
    }}
    .arrow {{
        position: absolute; bottom: 40px; left: 50%; transform: translateX(-50%);
        width: 42px; height: 42px; border: 1.5px solid rgba(197,168,108,.6);
        border-radius: 50%; display: flex; align-items: center; justify-content: center;
    }}
    .arrow svg {{ width: 18px; height: 18px; stroke: {accent_color}; fill: none; stroke-width: 2; }}
    </style></head><body>
    <div class="slide">
        <div class="chevron-pattern"></div>
        <div class="gold-arc"></div>
        <div class="mini-mark">
          <svg viewBox="0 0 100 110" xmlns="http://www.w3.org/2000/svg">
            <defs>
              <linearGradient id="fgoldcover" x1="0" y1="0" x2="1" y2="1">
                <stop offset="0%" stop-color="#e2c78e"/>
                <stop offset="50%" stop-color="#c5a86c"/>
                <stop offset="100%" stop-color="#8e7646"/>
              </linearGradient>
            </defs>
            <polygon points="12,8 88,8 74,30 0,30" fill="url(#fgoldcover)"/>
            <polygon points="17,40 66,40 54,60 6,60" fill="url(#fgoldcover)"/>
            <polygon points="22,70 48,70 30,100 0,100" fill="url(#fgoldcover)"/>
          </svg>
        </div>
        <div class="divider">Systems That Scale</div>
        <div class="content">
            <div class="kicker">{headline_top or 'Forza'}</div>
            <h1 class="headline">{headline_gold}</h1>
            <div class="sub">{headline_bottom}</div>
        </div>
        <div class="gold-line"></div>
        <div class="footer">
            <div class="brand">FORZA</div>
            <div class="site">forza-ai.com</div>
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
        <div class="site">forza-ai.com</div>
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
