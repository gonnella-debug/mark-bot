"""
Mark v2 — Autonomous Marketing Brain
GG talks to Mark directly on Telegram. No Alex dependency.

Daily flow (10am Dubai):
  1. Mark searches news → suggests 1 topic per brand
  2. Each suggestion has buttons: Approve | Regenerate | Static | Carousel
  3. GG approves → Mark renders slides
  4. Mark sends render preview → buttons: Post Now | Post at 6pm
  5. GG chooses → Mark saves to Drive + posts to all platforms

Manual commands:
  "suggest" — trigger morning suggestions now
  "generate [brand] [topic]" — manual content creation
  "status" — what's been posted today
"""

import os
import json
import time
import asyncio
import logging
import base64
from datetime import datetime, timezone, timedelta

import httpx
from anthropic_limiter import anthropic_post, AnthropicBudgetExceeded  # noqa: E402

log = logging.getLogger(__name__)

CLAUDE_API_KEY = os.getenv("CLAUDE_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

DUBAI_TZ = timezone(timedelta(hours=4))

# State
_pending_suggestions = {}  # brand -> {"topic": str, "angle": str}
_pending_render = None  # {"brand": str, "content": dict, "images": list[bytes]}
_posted_today = {}  # brand -> list of topics
_suggestion_sent_today = None  # date string


# ── TELEGRAM HELPERS ──

# Persistent bottom-of-chat menu — always visible so GG never has to type
# or scroll for the common actions.
PERSISTENT_KEYBOARD = {
    "keyboard": [
        [{"text": "💡 Suggestions"}, {"text": "📊 Status"}],
        [{"text": "❓ Help"}],
    ],
    "resize_keyboard": True,
    "is_persistent": True,
    "input_field_placeholder": "Tap a button or type…",
}


async def send_tg(text: str, reply_markup: dict = None) -> dict:
    """Send a Telegram message. If reply_markup is None, attach the persistent
    keyboard so the menu stays visible. Inline keyboards override (they need
    to attach to the specific message)."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    effective_markup = reply_markup if reply_markup is not None else PERSISTENT_KEYBOARD
    last = {}
    for i, chunk in enumerate([text[i:i+4000] for i in range(0, len(text), 4000)]):
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": chunk, "parse_mode": "Markdown"}
        if effective_markup and i == 0:
            payload["reply_markup"] = json.dumps(effective_markup)
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post(url, json=payload)
                last = r.json()
        except Exception as e:
            log.error(f"Telegram send error: {e}")
    return last


async def send_tg_photo(image_bytes: bytes, caption: str = "") -> dict:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                url,
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption[:1024], "parse_mode": "Markdown"},
                files={"photo": ("slide.png", image_bytes, "image/png")},
            )
            return r.json()
    except Exception as e:
        log.error(f"Telegram photo error: {e}")
        return {}


async def answer_cb(cb_id: str, text: str = ""):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(url, json={"callback_query_id": cb_id, "text": text})
    except:
        pass


# ── MORNING SUGGESTIONS ──

async def send_morning_suggestions():
    """Search news and send topic suggestions for all 3 brands."""
    global _pending_suggestions, _suggestion_sent_today
    from content_brain import get_morning_suggestions

    await send_tg("_Researching today's news and market data..._")

    result = await get_morning_suggestions()
    if "error" in result:
        await send_tg(f"Morning research failed: {result['error']}")
        return

    suggestions = result.get("suggestions", [])
    if not suggestions:
        await send_tg("No suggestions generated. Try 'suggest' again later.")
        return

    _pending_suggestions = {}

    for s in suggestions:
        brand = s["brand"]
        topic = s["topic"]
        angle = s["angle"]
        _pending_suggestions[brand] = {"topic": topic, "angle": angle}

        brand_display = brand.replace("_", " ").title()
        if brand == "nucassa_re":
            brand_display = "Nucassa Real Estate"
        elif brand == "nucassa_holdings":
            brand_display = "Nucassa Holdings"
        elif brand == "listr":
            brand_display = "ListR.ae"

        markup = {"inline_keyboard": [
            [{"text": "📸 Static", "callback_data": f"sug_static|{brand}"},
             {"text": "🎠 Carousel", "callback_data": f"sug_carousel|{brand}"}],
            [{"text": "🔄 Regenerate", "callback_data": f"sug_regen|{brand}"}],
        ]}

        await send_tg(
            f"*{brand_display}*\n\n"
            f"*Topic:* {topic}\n\n"
            f"_{angle}_",
            reply_markup=markup,
        )

    _suggestion_sent_today = datetime.now(DUBAI_TZ).strftime("%Y-%m-%d")


# ── CONTENT GENERATION + RENDERING ──

async def generate_and_render(brand: str, topic: str, content_type: str = "carousel"):
    """Generate content and render slides for a brand."""
    global _pending_render
    from content_brain import search_and_generate
    from renderer import render_carousel

    brand_display = {"nucassa_re": "Nucassa RE", "nucassa_holdings": "Holdings", "listr": "ListR"}
    await send_tg(f"_Creating {content_type} for {brand_display.get(brand, brand)}..._")

    content = await search_and_generate(brand, topic)
    if "error" in content:
        await send_tg(f"Content generation failed: {content['error']}")
        return

    slides = content.get("slides", [])
    if not slides:
        await send_tg("No slides generated. Try a different topic.")
        return

    # For static, just use cover slide
    if content_type == "static":
        slides = [s for s in slides if s.get("type") == "cover"][:1]
        if not slides:
            slides = content["slides"][:1]

    await send_tg(f"_Rendering {len(slides)} slides..._")
    images = await render_carousel(slides, brand)

    if not images:
        await send_tg("Rendering failed.")
        return

    # Send preview
    for i, img in enumerate(images):
        cap = f"*Slide {i+1}/{len(images)}*"
        await send_tg_photo(img, cap)

    caption = content.get("caption", "")
    if caption:
        await send_tg(f"*Caption:*\n{caption[:500]}")

    # Store pending
    _pending_render = {
        "brand": brand,
        "content": content,
        "images": images,
        "content_type": content_type,
    }

    # Post timing buttons
    markup = {"inline_keyboard": [
        [{"text": "🚀 POST NOW", "callback_data": "render_post_now"},
         {"text": "🕕 POST AT 6PM", "callback_data": "render_post_6pm"}],
        [{"text": "❌ REJECT", "callback_data": "render_reject"},
         {"text": "🔄 REGENERATE", "callback_data": "render_regen"}],
    ]}
    await send_tg("*Post this content?*", reply_markup=markup)


async def post_content_now(brand: str, content: dict, images: list):
    """Post to all platforms immediately."""
    from mark_bot_final import post_content, BRANDS

    content["content_type"] = "carousel" if len(images) > 1 else "static"
    content["_cached_images"] = images
    content["caption_instagram"] = content.get("caption", "")
    content["caption_linkedin"] = content.get("caption", "")
    content["hashtags"] = []

    results = await post_content(content, brand)

    lines = [f"*Posted — {BRANDS[brand]['name']}*"]
    for platform, result in results.items():
        if isinstance(result, dict) and "error" in str(result):
            lines.append(f"  ❌ {platform}: {str(result)[:80]}")
        elif isinstance(result, bool):
            lines.append(f"  {'✅' if result else '❌'} {platform}")
        else:
            lines.append(f"  ✅ {platform}")

    await send_tg("\n".join(lines))

    # Track
    today = datetime.now(DUBAI_TZ).strftime("%Y-%m-%d")
    if brand not in _posted_today:
        _posted_today[brand] = []
    _posted_today[brand].append(content.get("topic_summary", "posted"))


# ── SCHEDULED POSTING ──

_scheduled_posts = []  # list of {"brand", "content", "images", "post_at"}


async def schedule_post_6pm(brand: str, content: dict, images: list):
    """Schedule a post for 6pm Dubai."""
    now = datetime.now(DUBAI_TZ)
    target = now.replace(hour=18, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)

    _scheduled_posts.append({
        "brand": brand,
        "content": content,
        "images": images,
        "post_at": target,
    })

    await send_tg(f"Scheduled for 6pm Dubai ({target.strftime('%d %B %Y')})")


async def scheduled_post_checker():
    """Background task — checks every 60s if any scheduled posts are due."""
    while True:
        try:
            now = datetime.now(DUBAI_TZ)
            due = [p for p in _scheduled_posts if now >= p["post_at"]]
            for post in due:
                _scheduled_posts.remove(post)
                await send_tg(f"_Posting scheduled content for {post['brand'].replace('_', ' ').title()}..._")
                await post_content_now(post["brand"], post["content"], post["images"])
        except Exception as e:
            log.error(f"Scheduled post checker error: {e}")
        await asyncio.sleep(60)


# ── DAILY SCHEDULER ──

async def daily_morning_scheduler():
    """Runs at 10am Dubai daily — triggers morning suggestions."""
    while True:
        try:
            now = datetime.now(DUBAI_TZ)
            today_str = now.strftime("%Y-%m-%d")

            if now.hour == 10 and _suggestion_sent_today != today_str:
                log.info("10am Dubai — sending morning suggestions")
                await send_morning_suggestions()

        except Exception as e:
            log.error(f"Morning scheduler error: {e}")
        await asyncio.sleep(60)


# ── CALLBACK HANDLER ──

async def handle_callback_v2(data: str, cb_id: str):
    """Handle all inline button callbacks."""
    global _pending_render, _pending_suggestions
    await answer_cb(cb_id)

    parts = data.split("|")
    action = parts[0]

    # Main menu buttons
    if action == "v2_suggest":
        await send_morning_suggestions()
        return

    elif action == "v2_status":
        if not _posted_today:
            markup = {"inline_keyboard": [
                [{"text": "💡 Get Suggestions", "callback_data": "v2_suggest"}],
            ]}
            await send_tg("No posts today.", reply_markup=markup)
        else:
            lines = ["*Today's posts:*"]
            for brand, topics in _posted_today.items():
                lines.append(f"\n*{brand.replace('_', ' ').title()}:*")
                for t in topics:
                    lines.append(f"  • {t}")
            await send_tg("\n".join(lines))
        return

    # Suggestion buttons
    elif action == "sug_approve" or action == "sug_carousel":
        brand = parts[1]
        sug = _pending_suggestions.get(brand)
        if sug:
            await generate_and_render(brand, sug["topic"], "carousel")
        else:
            await send_tg("Suggestion expired. Say 'suggest' for new ones.")

    elif action == "sug_static":
        brand = parts[1]
        sug = _pending_suggestions.get(brand)
        if sug:
            await generate_and_render(brand, sug["topic"], "static")
        else:
            await send_tg("Suggestion expired.")

    elif action == "sug_regen":
        brand = parts[1]
        await send_tg(f"_Regenerating suggestion for {brand.replace('_', ' ').title()}..._")
        from content_brain import search_and_generate
        # Quick regen — just get new content for this brand
        _pending_suggestions.pop(brand, None)
        from content_brain import get_morning_suggestions
        result = await get_morning_suggestions()
        suggestions = result.get("suggestions", [])
        for s in suggestions:
            if s["brand"] == brand:
                _pending_suggestions[brand] = {"topic": s["topic"], "angle": s["angle"]}
                markup = {"inline_keyboard": [
                    [{"text": "📸 Static", "callback_data": f"sug_static|{brand}"},
                     {"text": "🎠 Carousel", "callback_data": f"sug_carousel|{brand}"}],
                    [{"text": "🔄 Regenerate", "callback_data": f"sug_regen|{brand}"}],
                ]}
                brand_display = brand.replace("_", " ").title()
                await send_tg(
                    f"*{brand_display} — New suggestion:*\n\n"
                    f"*Topic:* {s['topic']}\n\n"
                    f"_{s['angle']}_",
                    reply_markup=markup,
                )
                break

    # Render/post buttons
    elif action == "render_post_now":
        if _pending_render:
            await send_tg("_Posting now..._")
            await post_content_now(_pending_render["brand"], _pending_render["content"], _pending_render["images"])
            _pending_render = None
        else:
            await send_tg("Nothing pending.")

    elif action == "render_post_6pm":
        if _pending_render:
            await schedule_post_6pm(_pending_render["brand"], _pending_render["content"], _pending_render["images"])
            _pending_render = None
        else:
            await send_tg("Nothing pending.")

    elif action == "render_reject":
        _pending_render = None
        await send_tg("Rejected.")

    elif action == "render_regen":
        if _pending_render:
            brand = _pending_render["brand"]
            topic = _pending_render["content"].get("topic_summary", "")
            ct = _pending_render.get("content_type", "carousel")
            _pending_render = None
            await generate_and_render(brand, topic + " (different angle)", ct)
        else:
            await send_tg("Nothing to regenerate.")


# ── MAIN LISTENER ──

async def mark_v2_listener():
    """Main Telegram listener — GG talks to Mark directly."""
    offset = None
    log.info("Mark v2 listener active — GG talks to Mark directly")

    # Startup — no inline keyboard so the PERSISTENT_KEYBOARD
    # (Suggestions / Status / Help) attaches at the bottom of the chat
    # and stays there. Suppresses the duplicate per-message inline buttons.
    from mark_bot_final import should_send_boot_message
    if should_send_boot_message("mark_v2", gap_seconds=600):
        await send_tg("*Mark v2 online* — tap a button below.")
    else:
        log.info("mark_v2 boot within 10 min of previous boot — skipping startup Telegram")

    # Start background tasks
    asyncio.create_task(daily_morning_scheduler())
    asyncio.create_task(scheduled_post_checker())

    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            params = {"timeout": 30, "allowed_updates": ["message", "callback_query"]}
            if offset:
                params["offset"] = offset

            async with httpx.AsyncClient(timeout=35) as client:
                r = await client.get(url, params=params)
                updates = r.json().get("result", [])

            for update in updates:
                offset = update["update_id"] + 1

                # Callbacks
                if "callback_query" in update:
                    cq = update["callback_query"]
                    data = cq.get("data", "")
                    cb_id = cq.get("id", "")
                    if data.startswith("sug_") or data.startswith("render_") or data.startswith("v2_"):
                        await handle_callback_v2(data, cb_id)
                    else:
                        from mark_bot_final import handle_callback
                        await handle_callback(update)
                    continue

                # Messages
                msg = update.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))
                text = msg.get("text", "").strip()

                if not text or chat_id != str(TELEGRAM_CHAT_ID):
                    continue

                text_lower = text.lower()
                # Strip emoji prefix from persistent keyboard buttons
                text_clean = text_lower
                for emoji in ("💡 ", "📊 ", "❓ "):
                    text_clean = text_clean.replace(emoji, "")
                text_clean = text_clean.strip()

                # Suggest — also matches "💡 Suggestions" button
                if text_clean.startswith("suggest") or any(text_lower.startswith(w) for w in ["suggest", "morning", "ideas", "topics", "💡"]):
                    await send_morning_suggestions()

                # Generate specific
                elif any(text_lower.startswith(w) for w in ["generate", "create", "make"]):
                    parts = text_lower.replace("generate", "").replace("create", "").replace("make", "").strip()
                    brand = None
                    topic = parts
                    if "holdings" in parts:
                        brand = "nucassa_holdings"
                        topic = parts.replace("holdings", "").strip()
                    elif "listr" in parts:
                        brand = "listr"
                        topic = parts.replace("listr", "").strip()
                    elif "nucassa" in parts:
                        brand = "nucassa_re"
                        topic = parts.replace("nucassa", "").strip()
                    else:
                        brand = "nucassa_re"
                    await generate_and_render(brand, topic or None, "carousel")

                # Status — also matches "📊 Status" button
                elif text_clean == "status" or any(w in text_lower for w in ["status", "what posted", "today", "📊"]):
                    if not _posted_today:
                        await send_tg("No posts today. Say 'suggest' for content ideas.")
                    else:
                        lines = ["*Today's posts:*"]
                        for brand, topics in _posted_today.items():
                            lines.append(f"\n*{brand.replace('_', ' ').title()}:*")
                            for t in topics:
                                lines.append(f"  • {t}")
                        await send_tg("\n".join(lines))

                # Help — also matches "❓ Help" button
                elif text_clean == "help" or any(w in text_lower for w in ["help", "commands", "❓"]):
                    await send_tg(
                        "*Mark v2 — Commands:*\n\n"
                        "Tap a button below or type:\n"
                        "• `suggest` — get today's topic suggestions\n"
                        "• `generate holdings [topic]` — create content for a brand\n"
                        "• `status` — what's been posted today\n\n"
                        "_I also auto-suggest topics at 10am Dubai_"
                    )

                # Chat
                else:
                    await _chat_with_mark(text)

        except Exception as e:
            log.error(f"Mark v2 listener error: {e}")
            await asyncio.sleep(5)


_chat_history = []  # recent messages for context

async def _chat_with_mark(question: str):
    """Full conversation with Mark — understands feedback and acts on it."""
    global _pending_render, _pending_suggestions

    # Build context about current state
    state_context = ""
    if _pending_render:
        state_context = f"\nYou have a pending render for {_pending_render['brand']} about: {_pending_render['content'].get('topic_summary', '?')}"
    if _pending_suggestions:
        brands = ", ".join(_pending_suggestions.keys())
        state_context += f"\nYou have pending suggestions for: {brands}"

    # Keep last 10 messages for context
    _chat_history.append({"role": "user", "content": question})
    if len(_chat_history) > 20:
        _chat_history[:] = _chat_history[-20:]

    system = f"""You are Mark, the autonomous marketing director for Nucassa (real estate + holdings) and ListR.ae in Dubai. You talk to GG directly on Telegram.

CURRENT STATE:{state_context}

IMPORTANT: You are having a CONVERSATION only. Do NOT try to generate content, create posts, or take actions. Just talk.

When GG gives feedback about content he doesn't like:
- Acknowledge it
- If there's a pending render, say "Want me to regenerate? Just tap Regenerate or say 'regenerate'"

When GG asks about marketing ideas or strategy:
- Give a direct, helpful answer
- Don't try to immediately create a post — just discuss

NEVER generate slide content, HTML, or try to render anything in chat. That happens through the suggest/generate flow with buttons.

Keep responses SHORT — 2-3 sentences max. Talk like a human colleague."""

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await anthropic_post(client,
                headers={
                    "x-api-key": CLAUDE_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 1024,
                    "system": system,
                    "messages": _chat_history[-10:],
                },
            )
            data = resp.json()
            text = data.get("content", [{}])[0].get("text", "")
            if text:
                _chat_history.append({"role": "assistant", "content": text})
                await send_tg(text)
    except Exception as e:
        log.error(f"Chat error: {e}")
        await send_tg("Sorry, had a brain glitch. Try again.")
