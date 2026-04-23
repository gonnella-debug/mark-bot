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
