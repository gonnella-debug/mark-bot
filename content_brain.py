"""
Mark v2 Content Brain — Autonomous marketing intelligence
Searches real news sources, creates content strategies, generates carousel content.
"""

import os
import json
import logging
import httpx
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)

CLAUDE_API_KEY = os.getenv("CLAUDE_API_KEY")

BRAND_CONTEXT = {
    "nucassa_re": {
        "name": "Nucassa Real Estate",
        "handle": "@nucassadubai",
        "audience": "Property buyers, sellers, and investors in Dubai. Mix of end-users and investors.",
        "tone": "Authoritative, data-led, factual. Bold and confident but never salesy. Uses real DLD data and market statistics.",
        "topics": "Dubai property transactions, area spotlights, yield data, off-plan vs ready, population growth, infrastructure, market comparisons",
        "cta": "DM us today",
    },
    "nucassa_holdings": {
        "name": "Nucassa Holdings Ltd",
        "handle": "@nucassaholdings_ltd",
        "audience": "Institutional investors, family offices, UHNW individuals. $1M+ ticket size.",
        "tone": "Institutional, precise, investor-grade. Goldman Sachs meets Dubai. Data-heavy, comparison-driven.",
        "topics": "ADGM SPV structures, capital protection, tax efficiency, family office migration, UAE vs London/Singapore, DBS custody, fixed income vs equity",
        "cta": "Message us to learn more",
    },
    "listr": {
        "name": "ListR.ae",
        "handle": "@listr.ae",
        "audience": "Property buyers and sellers in Dubai who want to save on fees. Tech-savvy, value-conscious.",
        "tone": "Modern, direct, disruptive. Cutting unnecessary fees. Sharp and confident.",
        "topics": "Commission savings, direct buyer-seller, verified listings, RERA, secondary market, fee comparison, agent empowerment (90% commission)",
        "cta": "Sign up free at ListR.ae",
    },
}

CONTENT_SYSTEM_PROMPT = """You are Mark, the autonomous marketing brain for Nucassa's three brands. You create Instagram carousel content that is factual, data-driven, and matches the premium dark aesthetic of the brand.

CRITICAL RULES:
1. NEVER make up statistics. Every number must come from the web search results or be clearly labeled as an estimate.
2. NEVER use 2025 data — only 2026 figures or timeless insights. Nobody cares about last year.
3. ABSOLUTELY NEVER mention war, missiles, conflict, bombs, attacks, depression, crashes, or ANY negativity in headlines or slides. Not even to contrast with positivity. Do NOT say "while missiles flew" or "despite tensions" or "amid conflict". Just focus on the POSITIVE DATA. If the market grew, talk about growth. If investors came, talk about investors. The negative context is IMPLIED — you never state it.
4. ALWAYS cite your source mentally — if you can't trace a stat to a search result, don't use it.
5. Headlines should be SHORT and PUNCHY — max 6-8 words per line.
6. Bullet points should be CONCISE — max 10 words each.
7. Gold-highlighted text should be the KEY NUMBER or INSIGHT — the thing that makes someone stop scrolling.
8. The closing statement should be a sharp, memorable insight — not generic motivation.
9. ALL text must be centered. No left-aligned content.

CAROUSEL STRUCTURE (always 4 slides):
- Slide 1: COVER — dramatic headline that hooks, uses background image
- Slide 2: DATA — key statistics with bullet points
- Slide 3: INSIGHT — deeper analysis + closing statement
- Slide 4: CTA — call to action

OUTPUT FORMAT — return ONLY valid JSON, no markdown:
{
  "brand": "nucassa_re",
  "topic_summary": "one line describing the angle",
  "caption": "Instagram caption text (include hashtags)",
  "slides": [
    {"type": "cover", "headline_top": "...", "headline_gold": "...", "headline_bottom": "..."},
    {"type": "data", "headline_gold": "...", "headline_white": "...", "bullets": ["...", "...", "...", "..."]},
    {"type": "insight", "headline_gold": "...", "headline_white": "...", "bullets": ["...", "..."], "closing_white": "...", "closing_gold": "..."},
    {"type": "cta", "cta_text": "...", "brand_name": "..."}
  ]
}

Use <span class="stat-highlight">TEXT</span> inside bullet strings to highlight key numbers in gold."""


SUGGESTION_SYSTEM_PROMPT = """You are Mark, a creative marketing director for Nucassa's three brands in Dubai. You have your own brain and imagination — you don't just regurgitate news headlines. You use the news as INSPIRATION to create original, creative content angles that position each brand as a thought leader.

YOUR CREATIVE PROCESS:
1. Search the news to understand what's happening RIGHT NOW in Dubai/UAE (2026 only).
2. Use that context to INSPIRE an original angle — don't just repackage the headline.
3. Think like a luxury brand creative director: what would make someone stop scrolling?
4. Mix data with storytelling. A stat alone is boring. A stat with a narrative is powerful.

RULES:
1. NEVER use 2025 data. Only 2026 or timeless insights.
2. NEVER mention war, conflict, crashes, or negativity. Find the opportunity angle.
3. Each suggestion should be 2-3 sentences: the creative angle and why it works for today.
4. Be ORIGINAL — don't just say "Q1 data is out". Say something like "Why 29,000 first-time investors chose Dubai over every other city this quarter".
5. Make each brand feel distinct — Nucassa RE is market authority, Holdings is institutional sophistication, ListR is disruptive and sharp.

OUTPUT FORMAT — return ONLY valid JSON:
{
  "suggestions": [
    {"brand": "nucassa_re", "topic": "...", "angle": "2-3 sentence creative pitch"},
    {"brand": "nucassa_holdings", "topic": "...", "angle": "2-3 sentence creative pitch"},
    {"brand": "listr", "topic": "...", "angle": "2-3 sentence creative pitch"}
  ]
}"""


POSTING_LOG_FILE = os.path.join(os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data"), "mark_posting_log.json")


def _load_recent_topics(brand: str, days: int = 7) -> list[str]:
    """Read the posting log and return topics posted for `brand` in last `days`.
    Used to prevent Mark from repeating himself within a week."""
    try:
        if not os.path.exists(POSTING_LOG_FILE):
            return []
        with open(POSTING_LOG_FILE, "r") as f:
            log_entries = json.load(f)
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        recent = []
        for entry in log_entries:
            if entry.get("brand") != brand:
                continue
            ts = entry.get("timestamp", "")
            try:
                entry_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue
            if entry_dt >= cutoff:
                topic = (entry.get("topic") or "").strip()
                if topic:
                    recent.append(topic)
        return recent
    except Exception as e:
        log.warning(f"Could not load recent topics for {brand}: {e}")
        return []


async def search_and_generate(brand: str, specific_topic: str = None) -> dict:
    """
    Search real news, analyze market conditions, generate carousel content.
    Returns structured content ready for the renderer.
    """
    brand_ctx = BRAND_CONTEXT.get(brand, BRAND_CONTEXT["nucassa_re"])
    today = datetime.now(timezone(timedelta(hours=4))).strftime("%A %d %B %Y")

    # Pull last 7 days of posted topics so Mark doesn't repeat himself
    recent_topics = _load_recent_topics(brand, days=7)
    dedup_block = ""
    if recent_topics:
        formatted = "\n".join(f"  - {t}" for t in recent_topics[-15:])  # cap to last 15 for prompt size
        dedup_block = f"""
ALREADY POSTED IN THE LAST 7 DAYS — DO NOT REPEAT OR PARAPHRASE ANY OF THESE:
{formatted}

If you can only find a similar angle, find a NEW data point, NEW comparison, or NEW vertical within the brand's topic space. Repetition kills engagement — your topic must feel fresh next to the recent posts above.
"""

    search_prompt = f"""Today is {today} (Dubai time).

Brand: {brand_ctx['name']} ({brand_ctx['handle']})
Audience: {brand_ctx['audience']}
Tone: {brand_ctx['tone']}
Topics: {brand_ctx['topics']}
CTA: {brand_ctx['cta']}
{dedup_block}
{"Specific topic requested: " + specific_topic if specific_topic else "Find the most relevant, timely topic for today's post."}

Search for the latest Dubai real estate news, data, and market sentiment. Look for:
- DLD transaction data
- Recent market reports from CBRE, JLL, Knight Frank, Savills
- Gulf News, Arabian Business, Zawya, Bloomberg Middle East
- Any geopolitical factors affecting Dubai market sentiment

Then create a 4-slide Instagram carousel that is factual and data-driven.
If the market is down, don't pretend it's up — find the honest angle that still serves the brand.

Return ONLY the JSON structure described in your instructions."""

    try:
        async with httpx.AsyncClient(timeout=90) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": CLAUDE_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 4096,
                    "system": CONTENT_SYSTEM_PROMPT,
                    "messages": [{"role": "user", "content": search_prompt}],
                    "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 5}],
                },
            )

            if resp.status_code != 200:
                log.error(f"Claude API error: {resp.status_code} — {resp.text[:300]}")
                return {"error": f"Claude API {resp.status_code}"}

            data = resp.json()
            # Extract text from response (may have tool_use blocks mixed in)
            text_blocks = [
                b.get("text", "")
                for b in data.get("content", [])
                if b.get("type") == "text" and b.get("text", "").strip()
            ]

            if not text_blocks:
                return {"error": "No text in Claude response"}

            raw = "\n".join(text_blocks).strip()

            # Parse JSON from response
            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start < 0 or end <= start:
                log.error(f"No JSON found in response: {raw[:200]}")
                return {"error": "No JSON in response", "raw": raw[:500]}

            content = json.loads(raw[start:end])
            content["brand"] = brand  # ensure brand is set
            log.info(f"Content generated for {brand}: {content.get('topic_summary', '?')}")
            return content

    except json.JSONDecodeError as e:
        log.error(f"JSON parse error: {e}")
        return {"error": f"JSON parse: {e}"}
    except Exception as e:
        log.error(f"Content generation failed: {e}")
        return {"error": str(e)}


async def get_morning_suggestions() -> dict:
    """
    10am daily — search news and suggest one topic per brand.
    Returns structured suggestions for GG to approve/reject.
    """
    today = datetime.now(timezone(timedelta(hours=4))).strftime("%A %d %B %Y")

    prompt = f"""Today is {today} (Dubai time).

Search for the latest Dubai and UAE real estate news, economic data, and market developments.
Focus on 2026 data only. Find what is happening RIGHT NOW that each brand can create content about.

Brand contexts:
- Nucassa Real Estate (@nucassadubai): Property market data, transactions, areas, buyer guides
- Nucassa Holdings (@nucassaholdings_ltd): Institutional investment, family offices, ADGM/DIFC, macro
- ListR.ae (@listr.ae): Property marketplace, fee disruption, buyer/seller empowerment

Suggest ONE topic per brand with a clear angle. Each suggestion should explain WHY this is the right topic for today."""

    try:
        async with httpx.AsyncClient(timeout=90) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": CLAUDE_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 2048,
                    "system": SUGGESTION_SYSTEM_PROMPT,
                    "messages": [{"role": "user", "content": prompt}],
                    "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 5}],
                },
            )

            if resp.status_code != 200:
                return {"error": f"Claude API {resp.status_code}"}

            data = resp.json()
            text_blocks = [
                b.get("text", "")
                for b in data.get("content", [])
                if b.get("type") == "text" and b.get("text", "").strip()
            ]

            raw = "\n".join(text_blocks).strip()
            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start >= 0 and end > start:
                return json.loads(raw[start:end])

            return {"error": "No JSON in response"}

    except Exception as e:
        log.error(f"Morning suggestions failed: {e}")
        return {"error": str(e)}
