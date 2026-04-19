"""Generate logo_forza.png from brand SVG using Playwright."""
import asyncio
from pathlib import Path

from playwright.async_api import async_playwright

OUT = Path(__file__).parent / "logo_forza.png"

HTML = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<style>
  body { margin: 0; background: transparent; }
  .wrap {
    width: 800px; height: 400px;
    display: flex; align-items: center; justify-content: center;
    gap: 32px;
    padding: 20px;
  }
  .mark { width: 180px; height: 200px; }
  .text {
    font-family: 'Cinzel', 'Trajan Pro', Georgia, serif;
    font-weight: 600; letter-spacing: 14px;
    font-size: 120px; line-height: 1;
    color: #c5a86c;
    background: linear-gradient(180deg, #e8cd8a 0%, #c5a86c 45%, #8a6a2a 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
  }
</style>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Cinzel:wght@400;500;600;700&display=swap" rel="stylesheet">
</head>
<body>
<div class="wrap">
  <svg class="mark" viewBox="0 0 100 110" xmlns="http://www.w3.org/2000/svg">
    <defs>
      <linearGradient id="gold" x1="10%" y1="0%" x2="90%" y2="100%">
        <stop offset="0%" stop-color="#e8cd8a"/>
        <stop offset="45%" stop-color="#c5a86c"/>
        <stop offset="100%" stop-color="#8a6a2a"/>
      </linearGradient>
    </defs>
    <polygon points="12,8 88,8 74,30 0,30" fill="url(#gold)"/>
    <polygon points="17,40 66,40 54,60 6,60" fill="url(#gold)"/>
    <polygon points="22,70 48,70 30,100 0,100" fill="url(#gold)"/>
  </svg>
  <div class="text">FORZA</div>
</div>
</body></html>
"""


async def main() -> None:
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        ctx = await browser.new_context(viewport={"width": 800, "height": 400}, device_scale_factor=2)
        page = await ctx.new_page()
        await page.set_content(HTML, wait_until="networkidle")
        # Give fonts 1 extra second to finish
        await page.wait_for_timeout(1200)
        await page.screenshot(path=str(OUT), omit_background=True, full_page=False,
                              clip={"x": 20, "y": 100, "width": 760, "height": 240})
        await browser.close()
    print(f"Wrote {OUT}")


asyncio.run(main())
