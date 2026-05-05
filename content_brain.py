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

# ============================================================================
# FORZA LAUNCH MODE — first 30 days (2026-04-19 → 2026-05-19) focuses ALL
# Forza content on promoting services, not reacting to news. After the launch
# window, Forza can pivot to news-aware content alongside service promotion.
# ============================================================================

FORZA_LAUNCH_STARTED = "2026-04-19"
FORZA_LAUNCH_WINDOW_DAYS = 30

FORZA_LAUNCH_TOPIC_BANK = [
    # ── BOTTLENECK / PAIN-FIRST HOOKS ──
    "What's your company's actual bottleneck — and why it isn't the one you think",
    "The 18-hour gap between lead arrival and first reply — what it costs you per month",
    "Your sales pipeline looks healthy. Your cash flow doesn't. Here's the disconnect",
    "Lead leakage — where the 60% of enquiries you never closed actually went",
    "Why qualified leads go cold while your team is in meetings",
    "Your team isn't lazy — they're drowning in tools that weren't built for how you sell",
    "Founder-as-bottleneck — what it costs you per month, calculated honestly",
    "The cost of unread WhatsApp messages — a number most founders never run",
    "Why your CRM became a graveyard, and what to do instead of nagging the team",
    "When growth becomes chaos — the operator's decision matrix",
    # ── BITRIX / OFF-THE-SHELF vs CUSTOM CRM ──
    "You pay Bitrix for 70 features. Your team uses 6. Here's the maths",
    "The case against off-the-shelf CRMs — pay for what you actually use, nothing else",
    "Bitrix vs HubSpot vs Salesforce vs custom — the real cost-per-feature comparison",
    "Why a 12-field CRM beats a 200-field CRM in every quarter that matters",
    "Your CRM should fit your sales process. Not the other way around",
    "Migration from Bitrix to a custom system — the 30-day breakdown",
    "Off-the-shelf CRM admin tax — the hours your team spends configuring instead of selling",
    # ── SYSTEM-CHASES-STAFF / ACCOUNTABILITY → REVENUE ──
    "A system that chases your team for follow-ups, so you don't have to",
    "How automated accountability adds 20-40% to monthly revenue without a single new hire",
    "The conversion math — what one extra follow-up per lead is actually worth",
    "Stop chasing your team. Let the infrastructure do it, and watch revenue compound",
    "Why follow-up consistency beats lead volume — every time, in every vertical",
    "The four-hour follow-up rule — why it quietly kills deals you thought were closing",
    "Real-time accountability without micromanagement — how the system handles it",
    # ── POSSIBILITY / VISION ──
    "What a fully wired service business actually looks like, from inbound to billing",
    "Five things you're doing manually right now that shouldn't be — and what they cost",
    "If your business ran itself for 30 days, what would break first? That's your priority",
    "The infrastructure stack that quietly replaces 4 SaaS tools and 2 hires",
    "Twice-daily operating brief — what your business actually did today, in your inbox",
    # ── ROI / NUMBERS ──
    "BDR salary vs autonomous outreach — the spreadsheet most founders never build",
    "Agency retainer vs in-house brand infrastructure — the 12-month cost difference",
    "Cost of one missed lead vs the entire Forza Build — run the comparison",
    "What 'no-show reduction' actually saves a clinic per month — real numbers",
    "Sub-60-second response time — what it changes for close rates and pipeline velocity",
    # ── FOUR INFRASTRUCTURES (the offer, framed as solutions to the pain above) ──
    "Revenue Infrastructure — 24/7 inbound capture, sub-60s response, qualification, routing",
    "Brand Infrastructure — autonomous content production across every platform, no agency retainer",
    "Team Infrastructure — real-time CRM monitoring and accountability without the founder chasing anyone",
    "Founder Intelligence — twice-daily operating pictures: what happened, what's broken, what needs you",
    # ── SERVICE DEPTH ──
    "What a Systems Audit reveals in 30 minutes — the free first step",
    "The 30-day Build — week-by-week breakdown from audit to live production",
    "Build + Operate pricing — why both matter and what each pays for",
    "Direct principal access — no account managers, no handoffs",
    "Client owns the data — your WABA, your CRM, your Meta pages, always",
    # ── VERTICAL PAIN → SOLUTION ──
    "Brokerages — why your 50-agent CRM works for 5 of them, and what to fix",
    "Clinics — the no-show problem, solved with three automated touches",
    "Law firms — matter updates and intake qualification without paralegal time",
    "Recruitment — dual-channel candidate and client follow-up at scale",
    "Institutional wealth — LP outreach and reporting cadence without a BDR team",
    "Agencies — client status updates that happen without weekly status calls",
    # ── PHILOSOPHY ──
    "Systems over staff — where the maths actually works, and where it doesn't",
    "Why we don't call ourselves an AI agency",
    "Operators, not consultants — the engagement model that follows from that",
    "Infrastructure that compounds vs tools that depreciate",
]


def is_forza_launch_mode() -> bool:
    """True for the first 30 days after launch. Forza content stays service-promotion only."""
    from datetime import date
    try:
        start = datetime.strptime(FORZA_LAUNCH_STARTED, "%Y-%m-%d").date()
    except Exception:
        return False
    elapsed = (date.today() - start).days
    return 0 <= elapsed <= FORZA_LAUNCH_WINDOW_DAYS


# ── CLAUDE ARTIFACT STRIPPER ──────────────────────────────────────────────────
# When Claude responds with web_search enabled, the JSON content fields
# sometimes contain literal `<cite index="...">…</cite>` markers, footnote
# numerals like `[1]`, and the occasional `&nbsp;` / zero-width-space. None of
# that should ever land in a published Instagram caption or LinkedIn post.
# 2026-04-28: GG caught a Nucassa Holdings post that went live with raw
# `<cite index="11-2,11-3">` tags — patient capital looking unprofessional.
# We sanitise at parse time AND defensively at post time so cached content
# from older runs also gets cleaned.
import re as _re
_CITE_TAG = _re.compile(r"<\s*/?\s*cite\b[^>]*>", _re.IGNORECASE)
_TRAILING_FOOTNOTES = _re.compile(r"\s*\[\d+(?:[,\s\d]*)\]")
_ZW = _re.compile(r"[​-‍﻿]")

def _strip_claude_artifacts(text):
    """Remove Claude web-search citation markers + footnote numerals + zero-
    width junk from a single string. Returns the cleaned string (or the
    original value untouched if it isn't a string)."""
    if not isinstance(text, str):
        return text
    s = _CITE_TAG.sub("", text)
    s = _TRAILING_FOOTNOTES.sub("", s)
    s = _ZW.sub("", s)
    s = _re.sub(r"[ \t]+", " ", s)
    s = _re.sub(r"\n{3,}", "\n\n", s).strip()
    return s


def _clean_content_dict(content):
    """Recursively strip Claude artifacts from every string in a content
    dict (caption fields, slide copy, hashtags, topic_summary). Mutates and
    returns the same dict for chaining. Safe to call on partially-parsed
    or non-dict inputs (returns input unchanged)."""
    if isinstance(content, dict):
        for k, v in list(content.items()):
            if isinstance(v, str):
                content[k] = _strip_claude_artifacts(v)
            elif isinstance(v, list):
                content[k] = [_clean_content_dict(x) if isinstance(x, dict) else _strip_claude_artifacts(x) for x in v]
            elif isinstance(v, dict):
                content[k] = _clean_content_dict(v)
        return content
    return content


BRAND_CONTEXT = {
    "nucassa_re": {
        "name": "Nucassa Real Estate",
        "handle": "@nucassadubai",
        "website": "nucassa.com",
        "audience": "Property buyers, sellers, and investors in Dubai. HNW global buyers seeking Dubai luxury residential. End-users and individual investors — NOT institutional.",
        "tone": "Authoritative, data-led, factual. Bold and confident but never salesy. Uses real DLD data and market statistics.",
        "topics": "Dubai property transactions, area spotlights, yield data, off-plan vs ready, population growth, infrastructure, market comparisons, golden visa / residency for buyers, neighbourhood guides",
        "search_universe": "Dubai/UAE residential real estate news. DLD transaction data, CBRE/JLL/Knight Frank/Savills market reports, Gulf News property, Arabian Business property, Zawya, Bloomberg Middle East property. Focus: prices, transactions, areas, buyer/investor angles for individual properties.",
        "off_brand": "NEVER write institutional/SPV/family-office/fund content (that's Holdings). NEVER write marketplace-fee-disruption content (that's ListR). NEVER write commercial RE / office leasing. NEVER write properties outside UAE. NEVER write business operating systems / Forza content.",
        "cta": "DM us today",
    },
    "nucassa_holdings": {
        "name": "Nucassa Holdings Ltd",
        "handle": "@nucassaholdings_ltd",
        "website": "nucassa.holdings",
        "audience": "Institutional investors, family offices, private bank allocators, UHNW with allocation mandates. $1M+ minimum ticket. Sophisticated capital — they speak SPVs, ring-fencing, fixed-income vs equity, distribution waterfalls.",
        "tone": "Institutional, precise, investor-grade. Goldman Sachs sales memo, not retail RE pitch. Data-heavy, comparison-driven, no hype.",
        "topics": "ADGM SPV structures, ring-fencing & non-voting economic shares, DBS Singapore custody mechanics, three-year cycles & capital deployment phases, fixed-income notes vs equity participation, the Platinum multi-investor option, UAE as institutional capital destination vs London/Singapore/Cayman, family office migration to ADGM, regulated RE alternatives vs direct ownership, 5% one-off mgmt fee economics.",
        "search_universe": "INSTITUTIONAL real estate alternatives + family office news. NOT Dubai property news. Sources: PERE, IPE Real Assets, WSJ Pro Private Markets, Reuters Wealth, Bloomberg Family Office / Wealth, Preqin reports, FSRA/ADGM regulatory news, DIFC vs ADGM fund flow data, family office migration to UAE, sovereign wealth allocation trends, private RE fund raises, fixed-income credit alternatives.",
        "off_brand": "NEVER write Dubai property listings, neighbourhood guides, area spotlights, or 'buy a Dubai apartment' angles (that's Nucassa RE). NEVER mention specific property addresses, individual unit prices, or DLD transaction tickers as the post subject. NEVER write retail/budget content. NEVER pitch as a quick flip or short-term yield play — capital is locked 24+ months. NEVER discuss other brands (RE, ListR, Forza) in copy.",
        "cta": "Message us to learn more",
    },
    "listr": {
        "name": "ListR.ae",
        "handle": "@listr.ae",
        "website": "listr.ae",
        "audience": "UAE property buyers, sellers, and agents. Buyers/sellers want lower fees and verified listings. Agents want qualified leads and 90% commission. Tech-savvy, value-conscious, fee-aware.",
        "tone": "Modern, direct, disruptive. Cutting unnecessary fees. Sharp and confident, but factual not preachy.",
        "topics": "1% seller commission vs traditional 2%, direct buyer-seller flow, title-deed verification, RERA & secondary market mechanics, AI-WhatsApp matching, agent empowerment (90% commission split), fee comparison case studies, off-plan vs ready in emerging Dubai districts, traditional-agency markup teardowns.",
        "search_universe": "Real estate marketplace + commission disruption news. Compass / Zillow / Redfin / OpenAgent outcomes, fee-model shifts, secondary market direct-sale stories, RERA regulatory updates, DLD secondary transaction data, prop-tech raises and exits, AI-driven property matching trends.",
        "off_brand": "NEVER write institutional/SPV/family-office content (that's Holdings). NEVER write Dubai luxury lifestyle / neighbourhood guides as the lead angle (that's RE). NEVER write mortgage lending or financing products (ListR doesn't sell those). NEVER write property management / rental services. NEVER write properties outside UAE.",
        "cta": "Sign up free at ListR.ae",
    },
    "forza": {
        "name": "Forza",
        "handle": "@forza_ai_",
        "audience": (
            "Founders and owners of growth-stage service businesses — real estate brokerages, "
            "clinics (medical/dental/cosmetic), law firms, recruitment agencies, institutional "
            "wealth/investment firms, car dealers, home services, creative agencies. "
            "Revenue-generating, team-led, operationally bottlenecked. The founder is still the "
            "chokepoint. They don't want more staff, they want the business to run without them."
        ),
        "tone": (
            "Premium, operator-led, direct. No startup hype, no emojis, no exclamation marks. "
            "Classical serif confidence — Cinzel headlines, Cormorant body. Reads like Goldman "
            "Sachs meets Dubai operator. Short sentences. Concrete numbers. Zero marketing fluff. "
            "Talks about systems, infrastructure, operational leverage as facts — never as pitches. "
            "Assumes the reader is running a real business with real P&L, not a pre-revenue idea."
        ),
        "positioning": (
            "Forza builds BUSINESS OPERATING SYSTEMS — custom AI-powered infrastructure that "
            "handles revenue, brand, team, and founder intelligence. NOT chatbots. NOT automation. "
            "NOT AI agency. We are operators who have run the playbook on our own businesses "
            "(Dubai brokerage, ADGM institutional investment platform, property marketplace). "
            "$5,000+ one-time Build, $1,000+/month Operate. Selective intake — small number of "
            "engagements per quarter. Direct principal access, no account managers."
        ),
        "four_infrastructures": (
            "1) Revenue Infrastructure: inbound lead capture (WhatsApp/IG/LinkedIn/web), sub-minute "
            "response, qualification bot, CRM auto-routing, follow-up sequences, pipeline reporting. "
            "2) Brand Infrastructure: autonomous content engine (like Mark itself), cross-platform "
            "posting, daily/weekly cadence without an agency retainer. "
            "3) Team Infrastructure: internal agent assistant (like Dave), SLA chasing, task "
            "management, integrity checks, accountability loops without micromanagement. "
            "4) Founder Intelligence: daily/twice-daily operating pictures, anomaly detection, "
            "trend reports, decision-ready briefs delivered to the founder's phone."
        ),
        "topics": (
            "Revenue Infrastructure, Brand Infrastructure, Team Infrastructure, Founder Intelligence, "
            "sub-60-second follow-up, operator case studies from Dubai RE + ADGM + marketplace, "
            "selective intake, systems vs hiring more staff, institutional outreach without a BDR "
            "team, daily operating pictures, the four-hour follow-up rule, when growth becomes chaos, "
            "the true cost of founder bandwidth, AI as operating leverage not automation"
        ),
        "proof_points": (
            "Dubai Brokerage Platform (full Revenue + Team infra — 6-bot fleet), "
            "Institutional Investment Platform (ADGM SPVs, family office outreach, daily briefs), "
            "Property Marketplace Platform (instant valuations, lead routing, commission savings). "
            "All operator-grade, all live, all GG's own businesses — not client case studies."
        ),
        "never_say": (
            "NEVER use: 'AI agency', 'automation agency', 'chatbot', 'consultant', 'prompt engineer', "
            "'ChatGPT', 'GPT wrapper', 'startup', 'disrupt', emojis, exclamation marks, 'revolutionary', "
            "'game-changer', 'cutting-edge', 'leverage synergies', 'unlock potential'. "
            "NEVER mention real client brand names. Case studies referred to neutrally: "
            "'a Dubai brokerage', 'an institutional investment platform', 'a property marketplace'."
        ),
        "cta": "Book a Systems Audit at forzasystems.ai",
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


SUGGESTION_SYSTEM_PROMPT = """You are Mark, a creative marketing director for four brands: three Nucassa brands in Dubai real estate + Forza (B2B consultancy selling business operating systems). You have your own brain and imagination — you don't just regurgitate news headlines. You use the news as INSPIRATION to create original, creative content angles that position each brand as a thought leader.

YOUR CREATIVE PROCESS:
1. For Nucassa brands: search Dubai/UAE real estate news (2026 only) — DLD, CBRE/JLL/Knight Frank, Gulf News, Arabian Business.
2. For Forza: search global operator / AI-in-business / service-business trends — McKinsey, HBR, Bain, a16z, ops benchmarks, BDR/SDR economics, service business M&A. NOT Dubai RE news (Forza is global B2B).
3. Use that context to INSPIRE an original angle — don't just repackage the headline.
4. Think like a luxury brand creative director: what would make someone stop scrolling?
5. Mix data with storytelling. A stat alone is boring. A stat with a narrative is powerful.

RULES:
1. NEVER use 2025 data. Only 2026 or timeless insights.
2. NEVER mention war, conflict, crashes, or negativity. Find the opportunity angle.
3. Each suggestion should be 2-3 sentences: the creative angle and why it works for today.
4. Be ORIGINAL — don't just say "Q1 data is out". Say something like "Why 29,000 first-time investors chose Dubai over every other city this quarter".
5. Make each brand feel distinct — Nucassa RE is market authority, Holdings is institutional sophistication, ListR is disruptive and sharp, Forza is operator-led systems thinking for founders drowning in growth chaos.
6. For Forza specifically: never use the words chatbot, automation agency, AI agency, consultant, prompt engineer, startup, disrupt. Never use emojis or exclamation marks. Think Goldman Sachs sales memo, not SaaS launch tweet.

OUTPUT FORMAT — return ONLY valid JSON, include ALL FOUR brands:
{
  "suggestions": [
    {"brand": "nucassa_re", "topic": "...", "angle": "2-3 sentence creative pitch"},
    {"brand": "nucassa_holdings", "topic": "...", "angle": "2-3 sentence creative pitch"},
    {"brand": "listr", "topic": "...", "angle": "2-3 sentence creative pitch"},
    {"brand": "forza", "topic": "...", "angle": "2-3 sentence creative pitch"}
  ]
}"""


POSTING_LOG_FILE = os.path.join(os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data"), "mark_posting_log.json")
SUGGESTION_LOG_FILE = os.path.join(os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data"), "mark_suggestion_log.json")


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


def _load_recent_suggested_topics(brand: str, days: int = 7) -> list[str]:
    """Read the suggestion log and return topics SUGGESTED for `brand` in last `days`.
    Suggested-but-not-posted still counts — otherwise Mark re-suggests the same
    topic every morning until GG approves it, which is the bug."""
    try:
        if not os.path.exists(SUGGESTION_LOG_FILE):
            return []
        with open(SUGGESTION_LOG_FILE, "r") as f:
            entries = json.load(f)
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        recent = []
        for entry in entries:
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
        log.warning(f"Could not load recent suggestions for {brand}: {e}")
        return []


def _log_suggestion(brand: str, topic: str) -> None:
    """Append a suggestion entry. Bounded to last 500 entries to keep the file small."""
    try:
        os.makedirs(os.path.dirname(SUGGESTION_LOG_FILE), exist_ok=True)
        entries = []
        if os.path.exists(SUGGESTION_LOG_FILE):
            try:
                with open(SUGGESTION_LOG_FILE, "r") as f:
                    entries = json.load(f)
            except (json.JSONDecodeError, ValueError):
                entries = []
        entries.append({
            "brand": brand,
            "topic": topic,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        entries = entries[-500:]
        with open(SUGGESTION_LOG_FILE, "w") as f:
            json.dump(entries, f)
    except Exception as e:
        log.warning(f"Could not log suggestion for {brand}: {e}")


def _pick_forza_topic() -> str:
    """Programmatically pick the next Forza topic. Excludes anything posted OR
    suggested in the last 7 days. Falls back to the full bank only if every
    topic has been touched in that window. No LLM in the selection path —
    Claude only writes the angle."""
    excluded = set(_load_recent_topics("forza", days=7)) | set(_load_recent_suggested_topics("forza", days=7))
    available = [t for t in FORZA_LAUNCH_TOPIC_BANK if t not in excluded]
    if not available:
        available = list(FORZA_LAUNCH_TOPIC_BANK)
    import random as _r
    return _r.choice(available)


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

    # Forza in launch mode: promote services, never search news. Pick a topic
    # from the launch bank (deduped against recent posts) and generate the
    # carousel off Forza's knowledge — no web search.
    forza_launch = (brand == "forza" and is_forza_launch_mode())

    if forza_launch:
        # Exclude both posted and previously-suggested topics so we never
        # carousel a topic GG already saw and rejected this week.
        excluded = set(recent_topics) | set(_load_recent_suggested_topics(brand, days=7))
        available_topics = [t for t in FORZA_LAUNCH_TOPIC_BANK if t not in excluded]
        if not available_topics:
            available_topics = FORZA_LAUNCH_TOPIC_BANK  # fall back if we've somehow cycled through
        topic_bank_block = "\n".join(f"  - {t}" for t in available_topics)
        search_prompt = f"""Today is {today} (Dubai time).

Brand: {brand_ctx['name']} ({brand_ctx['handle']})
Audience: {brand_ctx['audience']}
Tone: {brand_ctx['tone']}
Positioning: {brand_ctx.get('positioning', '')}
Four Infrastructures: {brand_ctx.get('four_infrastructures', '')}
Proof points: {brand_ctx.get('proof_points', '')}
Never say: {brand_ctx.get('never_say', '')}
CTA: {brand_ctx['cta']}
{dedup_block}

LAUNCH MODE (first 30 days from {FORZA_LAUNCH_STARTED}): Forza is a brand-new venture. The goal right now is to PROMOTE THE SERVICES WE OFFER — not to react to news, not to comment on the market, not to ride trends. Every post must explain, illustrate, or argue for ONE of Forza's services or philosophies.

DO NOT use web search. DO NOT cite news. DO NOT reference current events. Work only from the brand knowledge above.

{"Specific topic requested: " + specific_topic if specific_topic else "Pick ONE topic from this service-promotion bank that hasn't been posted yet, and create a 4-slide carousel that sells/explains that service with concrete depth:"}

TOPIC BANK:
{topic_bank_block}

Build a 4-slide carousel with the Forza tone (no emojis, no exclamation marks, no startup hype, no 'game-changer', no 'leverage', no 'disrupt'). Use specific numbers from the brand knowledge where relevant (pricing tiers, 30-day Build timeline, sub-60s response, 500 daily outbound limits, four infrastructures). The CTA on slide 4 should always be a direct Systems Audit booking, not a vague 'learn more'.

Return ONLY the JSON structure described in your instructions."""
        use_web_search = False
    else:
        # Per-brand search universe and off-brand list. Without this, every
        # non-Forza brand defaults to "search Dubai real estate news" — which
        # is correct for Nucassa RE but wrong for Holdings (institutional
        # investment platform) and ListR (marketplace/fee disruption). That
        # was the brand-bleed bug: Holdings posts kept reading like RE listings.
        search_universe = brand_ctx.get("search_universe", "")
        off_brand = brand_ctx.get("off_brand", "")
        search_prompt = f"""Today is {today} (Dubai time).

Brand: {brand_ctx['name']} ({brand_ctx.get('website', brand_ctx['handle'])})
Audience: {brand_ctx['audience']}
Tone: {brand_ctx['tone']}
Topics this brand covers: {brand_ctx['topics']}
CTA: {brand_ctx['cta']}

OFF-BRAND — DO NOT WRITE ABOUT THESE:
{off_brand}

{dedup_block}
{"Specific topic requested: " + specific_topic if specific_topic else "Find the most relevant, timely topic for today's post — within this brand's universe only."}

SEARCH UNIVERSE (use ONLY these source families — do NOT default to generic Dubai property news):
{search_universe}

Then create a 4-slide Instagram carousel that is factual and data-driven, framed inside this brand's audience and tone. Every slide must be unmistakably about THIS brand's offering — not a sister brand's. If the angle starts drifting toward an off-brand topic, pick a different angle.

If the market is down, don't pretend it's up — find the honest angle that still serves the brand.

Return ONLY the JSON structure described in your instructions."""
        use_web_search = True

    try:
        request_body = {
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 4096,
            "system": CONTENT_SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": search_prompt}],
        }
        if use_web_search:
            request_body["tools"] = [{"type": "web_search_20250305", "name": "web_search", "max_uses": 5}]

        async with httpx.AsyncClient(timeout=90) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": CLAUDE_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json=request_body,
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

            # 4-slide validator. Claude occasionally returns 3 slides; the
            # carousel renderer + post pipeline assume exactly 4 (cover, data,
            # insight, cta). Retry once with an explicit count correction
            # before failing — silent 3-slide carousels look broken in feed.
            slides = content.get("slides", [])
            if len(slides) != 4:
                log.warning(f"[search_and_generate] {brand} returned {len(slides)} slides — retrying once for exactly 4")
                retry_body = {
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 4096,
                    "system": CONTENT_SYSTEM_PROMPT,
                    "messages": [
                        {"role": "user", "content": search_prompt},
                        {"role": "assistant", "content": raw},
                        {"role": "user", "content": (
                            f"You returned {len(slides)} slides. The carousel MUST have exactly 4 slides "
                            "(cover, data, insight, cta) — no more, no fewer. Re-output the full JSON "
                            "with all 4 slides."
                        )},
                    ],
                }
                if use_web_search:
                    retry_body["tools"] = [{"type": "web_search_20250305", "name": "web_search", "max_uses": 5}]
                retry_resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": CLAUDE_API_KEY,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json=retry_body,
                )
                if retry_resp.status_code == 200:
                    retry_blocks = [
                        b.get("text", "")
                        for b in retry_resp.json().get("content", [])
                        if b.get("type") == "text" and b.get("text", "").strip()
                    ]
                    retry_raw = "\n".join(retry_blocks).strip()
                    rs, re_ = retry_raw.find("{"), retry_raw.rfind("}") + 1
                    if rs >= 0 and re_ > rs:
                        try:
                            retried = json.loads(retry_raw[rs:re_])
                            if len(retried.get("slides", [])) == 4:
                                retried["brand"] = brand
                                log.info(f"Content regenerated for {brand} with correct 4 slides")
                                return _clean_content_dict(retried)
                        except json.JSONDecodeError:
                            pass
                log.error(f"[search_and_generate] {brand} still wrong slide count after retry — failing")
                return {"error": f"slide count {len(slides)} (expected 4) after retry"}

            # Slide 0 MUST be type "cover" with cover-specific fields. Claude has
            # been observed returning 4 valid slides where slide[0] is a data
            # slide — the renderer's _coerce force-cover then produces an empty
            # cover (no headline_top/bottom). Reject and retry once with an
            # explicit correction so we get real cover copy, not a degraded
            # data-slide-rebadged-as-cover.
            s0 = slides[0] if slides else {}
            s0_type = (s0.get("type") or "").strip().lower()
            cover_ok = (
                s0_type == "cover"
                and bool(s0.get("headline_gold"))
                and not s0.get("bullets")
                and not s0.get("stats")
                and not s0.get("points")
            )
            if not cover_ok:
                log.warning(
                    f"[search_and_generate] {brand} slide 0 not a clean cover "
                    f"(type={s0_type!r}, has_bullets={bool(s0.get('bullets') or s0.get('stats') or s0.get('points'))}) — retrying"
                )
                fix_body = {
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 4096,
                    "system": CONTENT_SYSTEM_PROMPT,
                    "messages": [
                        {"role": "user", "content": search_prompt},
                        {"role": "assistant", "content": raw},
                        {"role": "user", "content": (
                            f"Slide 0 is wrong. It must be type \"cover\" with EXACTLY these fields: "
                            f"headline_top (short kicker, ≤4 words), headline_gold (the dramatic hook, "
                            f"≤6 words), headline_bottom (sub-line, ≤6 words). NO bullets, NO stats, "
                            f"NO data fields on slide 0 — that belongs on slide 1 (data). Re-output the "
                            f"full JSON with slide 0 as a proper cover."
                        )},
                    ],
                }
                if use_web_search:
                    fix_body["tools"] = [{"type": "web_search_20250305", "name": "web_search", "max_uses": 5}]
                fix_resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": CLAUDE_API_KEY,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json=fix_body,
                )
                if fix_resp.status_code == 200:
                    fix_blocks = [
                        b.get("text", "")
                        for b in fix_resp.json().get("content", [])
                        if b.get("type") == "text" and b.get("text", "").strip()
                    ]
                    fix_raw = "\n".join(fix_blocks).strip()
                    fs, fe = fix_raw.find("{"), fix_raw.rfind("}") + 1
                    if fs >= 0 and fe > fs:
                        try:
                            fixed = json.loads(fix_raw[fs:fe])
                            fixed_slides = fixed.get("slides", [])
                            fs0 = fixed_slides[0] if fixed_slides else {}
                            fs0_type = (fs0.get("type") or "").strip().lower()
                            if (
                                len(fixed_slides) == 4
                                and fs0_type == "cover"
                                and fs0.get("headline_gold")
                                and not (fs0.get("bullets") or fs0.get("stats") or fs0.get("points"))
                            ):
                                fixed["brand"] = brand
                                log.info(f"Content slide-0 corrected for {brand}")
                                return _clean_content_dict(fixed)
                        except json.JSONDecodeError:
                            pass
                log.warning(f"[search_and_generate] {brand} slide-0 retry failed — letting renderer coerce")

            log.info(f"Content generated for {brand}: {content.get('topic_summary', '?')}")
            return _clean_content_dict(content)

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

    forza_launch = is_forza_launch_mode()
    # Programmatic pick — Claude no longer chooses the Forza topic. Excludes
    # anything posted OR suggested in the last 7 days, so a rejected suggestion
    # does not come back tomorrow. Claude only writes the angle around this
    # locked topic.
    forza_locked_topic = _pick_forza_topic() if forza_launch else None

    forza_instruction = (
        f"""For Forza (LAUNCH MODE — first 30 days from {FORZA_LAUNCH_STARTED}): Do NOT search news. Do NOT react to current events. Forza is a brand-new venture — every post must PROMOTE A SERVICE, surface a real operator pain point, or argue for an operating philosophy.

THE FORZA TOPIC FOR TODAY IS LOCKED. You do not pick it. You write the angle for this exact topic:

  TOPIC: {forza_locked_topic}

Return this string verbatim in the "topic" field of the forza suggestion. In the "angle" field, write 2-3 sentences explaining WHY this is the right post to push today, what specific pain point or business reality it speaks to, and the concrete hook the carousel will lead with. Frame Forza posts around obvious company bottlenecks (slow follow-up, CRM bloat, founder-as-bottleneck, leads going cold, paying for unused SaaS features) and how the Forza infrastructure removes them. Show endless possibility — what a fully wired business actually looks like, with numbers where they help.
"""
        if forza_launch
        else
        "For Forza: search global operator / AI-in-business / service-business / founder-ops news — McKinsey, HBR, Bain, a16z, service-business benchmarks, BDR/SDR economics, hiring vs systems trade-offs."
    )

    prompt = f"""Today is {today} (Dubai time).

You manage FOUR brands. EACH BRAND HAS ITS OWN SEARCH UNIVERSE. Do NOT default Holdings or ListR to Dubai property news — that was the bug we just fixed.

══ NUCASSA REAL ESTATE (nucassa.com) ══
Audience: HNW global buyers of Dubai luxury residential. Individual investors, end-users.
Search universe: Dubai/UAE residential property news. DLD transactions, CBRE/JLL/Knight Frank/Savills, Gulf News property, Arabian Business property, Zawya, Bloomberg Middle East property.
On-brand topics: prices, transactions, areas, off-plan vs ready, golden visa, neighbourhood data.
OFF-BRAND for RE: institutional/SPV/family-office (Holdings), commission disruption (ListR), commercial/office, anything outside UAE.

══ NUCASSA HOLDINGS (nucassa.holdings) ══
Audience: Institutional investors, family offices, private bank allocators. $1M minimum ticket. They speak SPVs, ring-fencing, fixed-income vs equity.
Search universe: INSTITUTIONAL real estate alternatives + family office news. NOT Dubai property news. Sources: PERE, IPE Real Assets, WSJ Pro Private Markets, Reuters Wealth, Bloomberg Family Office, Preqin, FSRA/ADGM regulatory news, DIFC/ADGM fund flows, sovereign allocation trends, private RE fund raises, fixed-income credit alternatives.
On-brand topics: ADGM SPV mechanics, DBS Singapore custody, three-year cycle structuring, fixed-income note vs equity participation, Platinum multi-investor option, UAE as institutional capital destination vs London/Singapore/Cayman, family office migration to ADGM.
OFF-BRAND for Holdings: Dubai property listings, neighbourhood guides, area spotlights, "buy a Dubai apartment", individual unit prices, DLD transaction tickers as the subject, retail/budget content, short-term flip pitches.

══ LISTR (listr.ae) ══
Audience: UAE property buyers/sellers/agents. Fee-aware, tech-savvy.
Search universe: Real estate marketplace + commission disruption news. Compass/Zillow/Redfin outcomes, fee-model shifts, secondary-market direct-sale stories, RERA updates, DLD secondary transaction data, prop-tech raises, AI-driven property matching.
On-brand topics: 1% seller commission vs 2% traditional, direct buyer-seller flow, title-deed verification, AI-WhatsApp matching, 90% agent commission split, fee-comparison case studies.
OFF-BRAND for ListR: institutional/SPV (Holdings), Dubai luxury lifestyle as the lead angle (RE), mortgages/financing, property management.

══ FORZA (forzasystems.ai) ══
{forza_instruction}

Suggest ONE topic per brand with a clear angle. Each suggestion lives strictly inside its own brand universe — if Holdings angle reads like an RE post, you've drifted off-brand and must re-pick.

OUTPUT ALL FOUR BRANDS. Do not skip Forza."""

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
                    # GG decision 2026-05-05: topic suggestion is internal
                    # (GG reviews before turning into a post) — Haiku
                    # sufficient. Final carousel/post generation calls in
                    # this file stay on Sonnet.
                    "model": "claude-haiku-4-5-20251001",
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
                parsed = json.loads(raw[start:end])

                # Enforce the locked Forza topic — if Claude paraphrased or
                # invented (e.g. "Revenue Infrastructure — five layers"), we
                # overwrite with the topic we actually picked. Then log every
                # suggestion so tomorrow's pick excludes it.
                for s in parsed.get("suggestions", []) or []:
                    brand = s.get("brand", "")
                    if brand == "forza" and forza_locked_topic:
                        if (s.get("topic") or "").strip() != forza_locked_topic:
                            log.info(
                                f"[morning_suggestions] forza topic override — Claude returned "
                                f"'{s.get('topic')}', forcing locked topic '{forza_locked_topic}'"
                            )
                            s["topic"] = forza_locked_topic
                    topic = (s.get("topic") or "").strip()
                    if brand and topic:
                        _log_suggestion(brand, topic)

                return _clean_content_dict(parsed)

            return {"error": "No JSON in response"}

    except Exception as e:
        log.error(f"Morning suggestions failed: {e}")
        return {"error": str(e)}
