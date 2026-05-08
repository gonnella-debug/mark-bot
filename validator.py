"""
Startup environment validator. Each bot's `schemas.py` imports this and
calls `validate_environment(...)` before any background task starts. Any
failure produces a list of strings; the caller raises RuntimeError so the
failure routes through Sentry → Alex → Telegram.

Conservative tonight: env-var presence + placeholder detection only.
Airtable schema diffs and external API health checks are stubbed to []
with a BACKLOG note — they need GG-awake rollout.
"""
from __future__ import annotations

import logging
import os
from typing import Iterable

log = logging.getLogger(__name__)

PLACEHOLDER_FRAGMENTS = (
    "your_", "your-", "yourkey", "xxx", "todo", "change_me", "changeme",
    "placeholder", "example", "<", ">", "${", "{{", "dummy", "fake",
    "__pending", "replace_", "insert_", "sample_key",
)


def _looks_placeholder(val: str) -> bool:
    lv = val.lower()
    return any(frag in lv for frag in PLACEHOLDER_FRAGMENTS)


def validate_env_vars(required: Iterable[str]) -> list[str]:
    """Returns a list of failure strings for unset / placeholder env vars."""
    failures: list[str] = []
    for name in required:
        v = os.environ.get(name)
        if v is None:
            failures.append(f"env var {name!r} is unset")
            continue
        if not v.strip():
            failures.append(f"env var {name!r} is empty")
            continue
        if _looks_placeholder(v):
            failures.append(f"env var {name!r} has a placeholder value")
    return failures


def validate_airtable_schemas(schemas: dict) -> list[str]:
    """Placeholder. Real impl would fetch each base's meta via Airtable
    API and diff declared fields against live schema. Out of scope for
    the 2026-04-23 overnight run — tracked in BACKLOG."""
    return []


def validate_external_apis(apis: Iterable[str]) -> list[str]:
    """Placeholder. Real impl would run cheap GET /ping-style probes.
    Out of scope for 2026-04-23 overnight run."""
    return []


def validate_environment(
    required_env_vars: Iterable[str],
    airtable_schemas: dict | None = None,
    external_apis: Iterable[str] | None = None,
) -> list[str]:
    """Combines all validators. Returns a flat list of failure strings."""
    failures: list[str] = []
    failures.extend(validate_env_vars(required_env_vars))
    failures.extend(validate_airtable_schemas(airtable_schemas or {}))
    failures.extend(validate_external_apis(external_apis or []))
    return failures


# ─── Phase 0 render-time validation (2026-05-08) ──────────────────────────
# Pre-screenshot HTML lint. Hard-fails on regressions that the audit
# identified as the worst visual failures. NOT a full visual validator —
# this is containment, designed to keep the worst patterns from re-shipping
# while the full composition rebuild is planned.
#
# Hooked into renderer.render_carousel just before the temp-file write.
# A non-empty failure list raises in the caller so no PNG is produced.

CANVAS_REQUIRED = ("width: 1080px; height: 1350px",)
CANVAS_FORBIDDEN = ("width: 1080px; height: 1080px", "1080,1080", "1080, 1080")

# Tokens whose mere presence in a Forza HTML render is a regression.
# Substring match; no quote-style assumption (CSS uses both single + double).
FORZA_BANNED_TOKENS = (
    ("Cinzel",            "Cinzel typeface — Roman-inscription serif, off-brand for Forza ops register"),
    ("Cormorant",         "Cormorant Garamond — boutique-luxury serif, off-brand for Forza"),
    ("Playfair",          "Playfair Display — overused luxury serif, off-brand for Forza"),
    # Disabled blueprint hero fingerprints — substrings that exist ONLY in
    # the archived _forza_cover_blueprint__disabled function. Each tied to
    # the AI-brain visual specifically (central hex + orbital rings + 4
    # outer nodes labelled REVENUE/OPS/BRAND/PEOPLE).
    ("FORZA OS",          "Disabled blueprint hero text 'FORZA OS · v1' — AI-brain hero restored"),
    ("coreglow",          "Disabled blueprint SVG gradient id 'coreglow' — central halo hero restored"),
    ("goldwire",          "Disabled blueprint SVG gradient id 'goldwire' — orbital wires restored"),
    (">OPS</text>",       "Disabled blueprint node label '>OPS<' (single-word, blueprint-only) — AI-brain hero restored"),
    (">PEOPLE</text>",    "Disabled blueprint node label '>PEOPLE<' (single-word, blueprint-only) — AI-brain hero restored"),
)

# Tokens whose presence in ANY brand render is a regression.
GLOBAL_BANNED_TOKENS = (
    ("Montserrat",        "Montserrat font — Canva-tier default, banned globally"),
)


def validate_render_html(html: str, brand: str) -> list[str]:
    """Returns a list of failures for a single rendered slide HTML.
    Empty list = passes. Caller treats non-empty as a hard fail and refuses
    to screenshot."""
    failures: list[str] = []
    if not isinstance(html, str) or not html:
        return ["render produced empty HTML"]

    # 1. Canvas dimensions — slide must be 1080×1350, never 1080×1080.
    if not any(req in html for req in CANVAS_REQUIRED):
        failures.append(
            "canvas missing required '1080×1350' declaration "
            "(slide must be IG-correct 4:5 aspect)"
        )
    for forbidden in CANVAS_FORBIDDEN:
        if forbidden in html:
            failures.append(
                f"canvas contains forbidden '{forbidden}' "
                "(slide must be 1080×1350 not 1080×1080)"
            )
            break

    # 2. Global banned tokens — apply to every brand.
    for tok, reason in GLOBAL_BANNED_TOKENS:
        if tok in html:
            failures.append(f"banned token '{tok}' present — {reason}")

    # 3. Forza-specific banned tokens.
    if (brand or "").lower() == "forza":
        for tok, reason in FORZA_BANNED_TOKENS:
            if tok in html:
                failures.append(f"forza-banned token '{tok}' present — {reason}")

    return failures


def validate_carousel_composition(slide_html_list: list[str]) -> list[str]:
    """Carousel-level checks. Run after each slide passes individual
    validate_render_html. Catches patterns that only emerge across slides."""
    failures: list[str] = []
    if not slide_html_list:
        return failures

    # Repeated centered composition — if every slide in the carousel uses
    # `text-align: center` on a `.content` block, the carousel reads as a
    # title-card stack. Cover + CTA centered is fine; if all body slides
    # are also centered, flag it.
    centered_count = sum(
        1 for h in slide_html_list
        if "text-align: center" in h and ".content" in h
    )
    if len(slide_html_list) >= 4 and centered_count == len(slide_html_list):
        failures.append(
            f"all {len(slide_html_list)} slides use centered .content composition "
            "(repeated-centering regression — at least body slides should pull "
            "to the canvas edge)"
        )
    return failures


class RenderValidationError(RuntimeError):
    """Raised by the renderer when validate_render_html returns failures.
    Halts the screenshot before a bad PNG is produced."""
    pass
