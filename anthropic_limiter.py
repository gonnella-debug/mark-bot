"""
Anthropic call-rate + daily-spend guard.

Usage pattern for each bot:

    from anthropic_limiter import limiter, AnthropicBudgetExceeded
    ...
    await limiter.check()  # raises AnthropicBudgetExceeded when capped
    # ... your normal httpx.AsyncClient().post(...) call ...
    limiter.record()

Or as a context manager:

    async with limiter.guarded():
        # call
        pass

Config (env vars):
  ANTHROPIC_LIMITER_ENFORCE=true     enforce (raise); default is monitor-only
  ANTHROPIC_MINUTE_CAP=60            per-minute cap
  ANTHROPIC_DAILY_CAP=3000           per-day cap
  ANTHROPIC_SERVICE_NAME=<slug>      service slug tagged on alerts
  ALEX_SENTRY_WEBHOOK_URL=<url>      Alex's /sentry-webhook full URL
                                     (with ?secret=...). If set, cap-hit
                                     events route through Alex → Telegram.

State file: $DATA_DIR/anthropic_usage.json (DATA_DIR = /data when present).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

log = logging.getLogger(__name__)


class AnthropicBudgetExceeded(RuntimeError):
    """Raised when the per-service Anthropic cap is exceeded in enforce mode."""


class AnthropicLimiter:
    def __init__(self) -> None:
        self.service = os.getenv("ANTHROPIC_SERVICE_NAME", "unknown")
        self.minute_cap = int(os.getenv("ANTHROPIC_MINUTE_CAP", "60"))
        self.daily_cap = int(os.getenv("ANTHROPIC_DAILY_CAP", "3000"))
        self.enforce = os.getenv("ANTHROPIC_LIMITER_ENFORCE", "").lower() == "true"
        self.alex_webhook = os.getenv("ALEX_SENTRY_WEBHOOK_URL", "")
        data_dir = "/data" if os.path.isdir("/data") and os.access("/data", os.W_OK) else "."
        self.state_file = os.path.join(data_dir, "anthropic_usage.json")
        self._recent: list[float] = []  # epoch seconds for last-minute sliding window
        self._lock = asyncio.Lock()
        self._last_alert_for: dict[str, float] = {}

    # ── state persistence ────────────────────────────────────────────────
    def _load(self) -> dict:
        try:
            with open(self.state_file) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save(self, data: dict) -> None:
        try:
            tmp = self.state_file + ".tmp"
            with open(tmp, "w") as f:
                json.dump(data, f)
            os.replace(tmp, self.state_file)
        except OSError as e:
            log.warning(f"anthropic_limiter: state save failed: {e}")

    def _today(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _today_count(self) -> int:
        return self._load().get(self._today(), {}).get(self.service, 0)

    def _bump_today(self) -> int:
        data = self._load()
        day = data.setdefault(self._today(), {})
        day[self.service] = day.get(self.service, 0) + 1
        # prune beyond 60 days
        keep = sorted(data.keys())[-60:]
        pruned = {k: data[k] for k in keep}
        self._save(pruned)
        return day[self.service]

    # ── rate logic ────────────────────────────────────────────────────────
    def _prune_minute_window(self) -> None:
        cutoff = time.time() - 60
        self._recent = [t for t in self._recent if t >= cutoff]

    def _minute_count(self) -> int:
        self._prune_minute_window()
        return len(self._recent)

    # ── alerting ──────────────────────────────────────────────────────────
    async def _alert(self, kind: str, msg: str) -> None:
        # Throttle: one alert per kind per 30 min.
        now = time.time()
        last = self._last_alert_for.get(kind, 0)
        if now - last < 1800:
            return
        self._last_alert_for[kind] = now
        log.error(f"anthropic_limiter[{self.service}] {kind}: {msg}")
        if not self.alex_webhook:
            return
        payload = {
            "action": "created",
            "data": {
                "issue": {
                    "id": f"anthropic-limiter-{self.service}-{kind}-{int(now)}",
                    "shortId": f"ANTHROPIC-{self.service.upper()}",
                    "title": f"Anthropic cap {kind} — {self.service}",
                    "level": "error",
                    "permalink": "",
                    "metadata": {"type": "AnthropicBudget", "value": msg},
                    "project": {"name": self.service, "slug": self.service},
                    "firstSeen": datetime.now(timezone.utc).isoformat(),
                    "count": "1",
                    "userCount": 0,
                }
            },
        }
        try:
            import httpx  # local import: only needed on alert
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(self.alex_webhook, json=payload)
        except Exception as e:
            log.warning(f"anthropic_limiter: alert webhook failed: {e}")

    # ── public API ────────────────────────────────────────────────────────
    async def check(self) -> None:
        """Call before every Anthropic request. Raises when capped (enforce
        mode) or logs + alerts (monitor mode)."""
        async with self._lock:
            minute = self._minute_count()
            daily = self._today_count()

            if daily >= self.daily_cap:
                msg = f"daily cap hit: {daily}/{self.daily_cap} for {self.service}"
                await self._alert("daily", msg)
                if self.enforce:
                    raise AnthropicBudgetExceeded(msg)

            if minute >= self.minute_cap:
                msg = f"minute cap hit: {minute}/{self.minute_cap} for {self.service}"
                await self._alert("minute", msg)
                if self.enforce:
                    raise AnthropicBudgetExceeded(msg)

    async def record(self) -> None:
        """Call after a successful Anthropic request to advance counters."""
        async with self._lock:
            self._recent.append(time.time())
            self._bump_today()

    @asynccontextmanager
    async def guarded(self):
        """Context manager: `async with limiter.guarded():` runs check + record."""
        await self.check()
        try:
            yield
        finally:
            await self.record()


# module-level singleton; safe because it's stateless until first call
limiter = AnthropicLimiter()


async def anthropic_post(client, **kwargs):
    """Drop-in replacement for `client.post("https://api.anthropic.com/v1/messages", ...)`.
    Adds per-service check-before / record-after around the call. Preserves
    all original kwargs (headers, json, timeout, etc.)."""
    await limiter.check()
    r = await client.post("https://api.anthropic.com/v1/messages", **kwargs)
    await limiter.record()
    return r


# ── sync variants for bots using the anthropic SDK directly ──────────────────
# Same check/record logic but without asyncio — used by forza-bot whose reply()
# path is sync and calls `client.messages.create(...)` via the SDK.

def _check_sync() -> None:
    limiter._prune_minute_window()
    minute = len(limiter._recent)
    daily = limiter._today_count()
    if daily >= limiter.daily_cap:
        msg = f"daily cap hit (sync): {daily}/{limiter.daily_cap} for {limiter.service}"
        log.error(f"anthropic_limiter[{limiter.service}] daily: {msg}")
        if limiter.enforce:
            raise AnthropicBudgetExceeded(msg)
    elif minute >= limiter.minute_cap:
        msg = f"minute cap hit (sync): {minute}/{limiter.minute_cap} for {limiter.service}"
        log.warning(f"anthropic_limiter[{limiter.service}] minute: {msg}")
        if limiter.enforce:
            raise AnthropicBudgetExceeded(msg)


def _record_sync() -> None:
    limiter._recent.append(time.time())
    limiter._bump_today()


def anthropic_sdk_call(callable_, *args, **kwargs):
    """Sync wrapper for SDK-based callers (e.g. anthropic.Anthropic.messages.create).
    Usage: `resp = anthropic_sdk_call(client.messages.create, model=..., messages=...)`."""
    _check_sync()
    r = callable_(*args, **kwargs)
    _record_sync()
    return r
