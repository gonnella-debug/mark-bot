# SESSION LOG — Mark renderer

> One entry per change session. Newest at the top. Each entry is `date / commit (or "uncommitted") / scope` then a body explaining what shipped.
> Pair with `POSTMORTEM.md` (incidents) and `RENDERER_FORENSIC_AUDIT.md` (architectural problems).

---

## 2026-05-08 · uncommitted · Phase 0 — renderer emergency containment

### Scope
Mark renderer only. No posting flow, scheduler, social API, or `content_brain.py` changes.
Goal: stop the worst visual failures identified in `RENDERER_FORENSIC_AUDIT.md` without rebuilding the renderer architecture. Containment patch, not a rewrite.

### What changed

**1 · Canvas dimensions — 1080×1080 → 1080×1350 (4:5 IG-correct).**
- `renderer.py:168, 169` — `_base_css()` body + slide dimensions.
- `renderer.py:543` — `_cta_editorial` slide dimensions.
- `renderer.py:2005` — institutional CTA slide dimensions.
- `renderer.py:2040, 2103` — Playwright viewports in `render_html_to_png` and `render_carousel`.
- `templates/render.js:8, 29` — Playwright viewports in the standalone screenshot script.

  Verified: rendered PNGs are now 1080×1350 (PIL-confirmed across 8 sample exports).

**2 · Forza AI-brain blueprint variant disabled.**
- `renderer.py:FORZA_COVER_VARIANTS` — `"blueprint"` removed from the random-pick pool. The audit flagged the blueprint hero (central glowing hex labelled `FORZA OS · v1` + four orbital rings + four nodes labelled REVENUE / OPS / BRAND / PEOPLE) as the canonical "AI-brain" off-brand visual.
- `renderer.py:_forza_cover_blueprint` — function body replaced with a single delegate to `_forza_cover_monolith`, so any direct caller still gets a valid cover.
- `renderer.py:_forza_cover_blueprint__disabled` — original implementation archived (not called from anywhere in the live module). Restored only when the hero is rebuilt without the orbital / halo / hex-core decoration.

  Live Forza covers now rotate across `monolith / flow / topology` (3 variants, was 4).

**3 · Cinzel + Cormorant Garamond removed from active Forza paths.**
- `renderer.py:_forza_common_head` — font import switched from `Cinzel + Cormorant Garamond` to `Inter + JetBrains Mono`. All `font-family: 'Cinzel'` and `font-family: 'Cormorant Garamond'` declarations updated. Headline `text-shadow: 0 4px 40px rgba(0,0,0,0.6)` removed. Italic-em luxury accent removed. Round `.swipe .dot` `border-radius: 50%` removed (square indicator).
- `renderer.py:generate_forza_cta_slide` — same font swap. The radial `.gold-arc` halo (a `radial-gradient` covering 150% of the canvas at 8% gold opacity) removed entirely. The italic Cormorant CTA copy is now upright Inter.
- `renderer.py:_forza_cover_monolith` — gold floor labels switched from `Cinzel` to `Inter`. The 80px gold drop-shadow halo on `.monolith` SVG removed.
- `renderer.py:_forza_cover_flow` — `.node` `box-shadow: 0 8px 40px rgba(0,0,0,0.6), inset 0 0 0 1px rgba(230,195,120,0.15)` removed. `.node .idx` and `.node .label` switched from `Cinzel` to `JetBrains Mono` / `Inter`. The 12px gold glow on `.pipe::after` removed; round dot replaced with square.
- `renderer.py:_forza_cover_topology` — `.summit` switched from `Cinzel` to `JetBrains Mono`.

  Forza now reads as institutional/operations register (Palantir / pit-wall neighborhood) instead of "Gladiator-DVD luxury" Cinzel + Cormorant. Production validator (see Fix 7) hard-fails if Cinzel / Cormorant / Playfair appears in any Forza render.

**4 · Montserrat removed as global default font.**
- `renderer.py:_base_css()` — Google Fonts import switched from Montserrat to Inter. Body `font-family` switched from `Montserrat, sans-serif` to `Inter, system-ui, sans-serif`.
- `renderer.py:generate_cta_slide.brand-name` — explicit `font-family: 'Montserrat'` on the brand wordmark switched to Inter.

  Montserrat is now absent from every active code path. Validator hard-fails if any rendered HTML reintroduces it.

**5 · Diagonal stripe + light-streak overlay removed from default usage.**
- `renderer.py:_stripe_bg_html()` — function body now returns empty string. Previously it injected three divs (`stripe-bg + streak1 + streak2`) onto every institutional `data` and `insight` slide, giving the entire feed the same diagonal-stripe template look.
- `_stripe_bg_css()` left in place (orphaned selectors are harmless and would help any backwards-compat caller).

  Inner Holdings/RE/ListR slides now render on flat warm-black `#0a0a0a` instead of the stripe overlay.

**6 · Centered-everything reduced on institutional body slides.**
- `renderer.py:generate_data_slide` (institutional default branch) — `.content` switched from `text-align: center` to `text-align: left`. Padding adjusted from `80px 70px` to `110px 90px`. Bullet rows switched from all-caps to sentence case. Logo moved from `bottom: 40px; left: 50%; transform: translateX(-50%)` to `bottom: 40px; right: 70px` (corner, not center).
- `renderer.py:generate_insight_slide` (institutional default branch) — same treatment. Divider switched from a centered gold `linear-gradient` to a 120px solid brass hairline, left-anchored.
- `renderer.py:generate_cover_slide` and `generate_cta_slide` left centered (covers and CTAs are the natural exceptions per `creative-system/master-rules/hook_hierarchy_rules.md` § 1).

  Across a 5-slide carousel, the rhythm is now: centered cover → left-aligned data → left-aligned insight → left-aligned data → centered CTA. The "all slides identical centered" template signal is broken.

**7 · `validator.py` — render-time HTML lint.**
- `validator.py` — added `validate_render_html(html, brand)` and `validate_carousel_composition(slide_html_list)`. Hard-fail on:
  - Canvas not `1080×1350` (or contains `1080×1080`).
  - Any brand: Montserrat font in HTML.
  - Forza only: Cinzel, Cormorant, or Playfair fonts.
  - Forza only: blueprint hero fingerprints (`FORZA OS`, `coreglow`, `goldwire`, `>OPS</text>`, `>PEOPLE</text>`).
- Carousel-level warning: every slide uses `text-align: center` on `.content` (regression to all-centered template).
- New `RenderValidationError` exception type.
- `renderer.py:render_carousel` — wired in. Pre-screenshot validation runs before each slide's temp-file write; on failure, raises `RenderValidationError` so the screenshot never executes. Carousel-level check runs after the loop; results stored in `visuals_used["composition_warnings"]` for caller logging.

  Smoke-tested: the archived `_forza_cover_blueprint__disabled` correctly fails validation; live `monolith / flow / topology` variants pass; Holdings cover/data/insight/cta all pass.

### What was tested

Headless Playwright render of two carousels (Forza + Holdings, 4 slides each). Saved to `/tmp/mark_phase0_samples/`.

Verified:
- `forza_slide_{1..4}.png` — 1080×1350.
- `holdings_slide_{1..4}.png` — 1080×1350.
- Forza HTML contains zero matches for `Cinzel`, `Cormorant`, `FORZA OS`, `orbital`, `Montserrat`.
- Holdings HTML contains zero matches for `Montserrat`. Inner data/insight slides contain zero `<div class="stripe-bg">` or `<div class="streak1">` divs.
- Holdings data slide CSS contains `text-align: left` (was `text-align: center`).
- Carousel-level composition validator returned no warnings (rhythm is no longer all-centered).

### Visual verdict (sample inspection)

- **Forza data slide:** dramatically less template-like. Flat warm-black canvas, left-aligned `LIVE TELEMETRY / INBOUND INFRASTRUCTURE` headline in Inter, square bullets, sentence-case body, logo bottom-right at low opacity. Reads as Linear/Stripe-press-adjacent, not Canva.
- **Forza cover (flow variant):** retained — the box-and-line `INPUT → ENGINE → OUTPUT` flow diagram is a legitimate operations visual; chrome (corner brackets, FORZASYSTEMS.AI footer, page indicator) is unchanged. Photo-behind composition unchanged.
- **Holdings data slide:** identical structural improvement — left-aligned, no diagonal stripes, no light streaks, logo corner-mounted. Reads as institutional, not broker-Instagram.
- **Holdings cover:** still uses the existing skyline/luxury photo library. Out of Phase 0 scope (photo curation requires the new `templates/holdings_covers/` library to be built per `creative-system/holdings/IMAGE_DIRECTION.md` § 7).

### What was NOT changed (deliberate)

- `content_brain.py` — untouched. Slide schema (`headline_gold` / `headline_white`) unchanged. Topic selection unchanged.
- `mark_bot_final.py` and `mark_v2_brain.py` — untouched. Posting flow, scheduler, Telegram approval workflow, social-API uploaders all unchanged.
- The four-brand `BRAND_ASSETS` palette — untouched. Three brands still share `#C9A06C` gold; this is a structural palette problem outside the Phase 0 scope. Validator does not flag it.
- Photo libraries (`templates/backgrounds/`, `templates/forza_covers/`) — not re-curated. Random round-robin tagging in `_init_backgrounds()` left in place. The Burj Khalifa cover from the audit is still in the library.
- Editorial / dashboard archetype paths — untouched (Playfair Display + IBM Plex Mono still load there). Only the institutional default and Forza paths were patched.
- The four hand-crafted templates at `templates/holdings_slide{1..4}.html` and `templates/test_slide{1..3,5}.html` — untouched. They are not used by the production renderer (confirmed in audit) and remain as historical artifacts.

### Files modified

- `renderer.py` — 9 distinct edit regions.
- `validator.py` — extended with render validation (~80 new lines).
- `templates/render.js` — viewport dimensions.

### Files created

- `SESSION_LOG.md` — this file.

### What's next (decide — not yet committed)

This was a containment patch. It does NOT solve the deeper architectural problems documented in `RENDERER_FORENSIC_AUDIT.md` Parts II–III: schema-encodes-colours, archetypes-are-font-swaps, photo-tags-are-random, three-brands-share-one-gold, validator-only-checks-env-vars (now partially addressed for output, but no contrast / type-role / palette-isolation checks).

GG decision required:
- **Option A — Ship Phase 0 now, observe.** Run a few real carousels through review. If the visual lift is meaningful, defer the full composition-engine rebuild and iterate on smaller fixes.
- **Option B — Proceed to full composition-engine rebuild** per `MARK_REBUILD_PLAN.md` and `RENDERER_FORENSIC_AUDIT.md` Part IV (composition primitives + visual layer stack + 12-column grid + named grade pipeline + type-role system + editorial primitives).

Phase 0 outputs are in `/tmp/mark_phase0_samples/` for review. Nothing has been auto-posted.
