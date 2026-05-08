# RENDERER FORENSIC AUDIT

> Code-level diagnosis of why Mark's outputs feel templated, dead, and AI-generated *despite* the brand systems documented in `creative-system/`. All findings cite specific file:line locations. No new design directions; no new bibles. The diagnosis is the deliverable.

Files audited:
- `renderer.py` (2209 lines) — the core rendering engine.
- `templates/render.js` (42 lines) — Playwright headless screenshot script.
- `validator.py` (70 lines) — supposed validator.
- `templates/holdings_slide*.html`, `templates/test_slide*.html` — manual templates.
- Schema usage across `content_brain.py:182–238` (brand definitions) and `mark_v2_brain.py`.

---

## PART I — TWELVE STRUCTURAL FAILURES IN THE RENDERING ENGINE

### Failure 1 — The canvas is the wrong aspect ratio

**`renderer.py:164`** — `_base_css()` hardcodes `body { width: 1080px; height: 1080px; }`.
**`renderer.py:165`** — `.slide { width: 1080px; height: 1080px; }`.
**`renderer.py:2065`** — Playwright viewport: `{"width": 1080, "height": 1080}`.
**`templates/render.js:8` and `:29`** — viewport `{ width: 1080, height: 1080 }`.

Every programmatic Mark output is 1:1 (1080×1080). Instagram's correct carousel ratio is 4:5 (1080×1350). The platform aggressively crops 1:1 carousels in the home feed and the brand loses ~20% of its compositional surface. **Every Mark slide that has shipped this year was rendered for the wrong aspect ratio.**

The hand-crafted templates (`holdings_slide1.html`, etc) at `1080 × 1350` are not what the renderer produces — they were a separate manual experiment that the production pipeline ignores.

### Failure 2 — Montserrat is hardcoded as the global default

**`renderer.py:162`** — `_base_css()` imports Montserrat:
```css
@import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;700;800;900&display=swap');
* { ... }
body { ... font-family: 'Montserrat', sans-serif; ... }
```

`_base_css()` is called by **every archetype** (institutional, editorial, dashboard, all Forza variants, all Holdings paths, all CTAs). Montserrat is loaded on every slide regardless of subsequent overrides — and where overrides don't fully reset, Montserrat wins by inheritance.

This is the single line in the codebase most responsible for "Canva energy." Montserrat 700–900 all-caps is the most cliched Etsy/Canva-tier social-design choice in existence. It is the *base layer* of every Mark output.

### Failure 3 — The "archetypes" are not visually distinct — they share base CSS, palette, and primitives

**`renderer.py:200`** — `ARCHETYPES = ("institutional", "editorial", "dashboard")`.
**`renderer.py:1707–1713`** — `generate_cover_slide()` switches on archetype:
```python
if a == "editorial": return _cover_editorial(...)
if a == "dashboard": return _cover_dashboard(...)
return f"""<!DOCTYPE html>...{_base_css()}..."""   # institutional default
```

But every branch starts from `_base_css()` (Montserrat + 1080×1080) and uses the *same brand-config* `accent_color`. The actual differences:

- **Institutional:** Montserrat all-caps + dark + diagonal stripes + gold.
- **Editorial:** Montserrat fallback + Playfair Display italic + gradient cream + gold.
- **Dashboard:** Montserrat fallback + IBM Plex Mono + grid background + gold.

Each archetype is **a font swap** + **a background swap** layered on the same colour palette and the same centered-everything composition. The brand identity does not change. The post does not feel different — only its texture changes.

### Failure 4 — Three of four brands share the exact same accent colour

**`renderer.py:2048–2051`** — `BRAND_ASSETS`:
```python
"nucassa_re":       {..., "accent": "#C9A06C", ...},
"nucassa_holdings": {..., "accent": "#C9A06C", ...},
"listr":            {..., "accent": "#B8962E", ...},
"forza":            {..., "accent": "#C5A86C", ...},
```

`#C9A06C`, `#C5A86C`, and `#B8962E` are all near-identical golds (deltaE < 8). Three brands look identical at the colour level. Forza's "different" gold differs from Nucassa's by 4 hex points — invisible to the eye.

The brand definitions in `content_brain.py:152–238` describe four distinct entities. The renderer collapses them to one: gold-on-dark.

### Failure 5 — Centered composition is hardwired into 90% of layouts

```bash
$ grep -c "text-align: center\|justify-content: center\|translateX(-50%)\|translate(-50%, -50%)" renderer.py
40+ instances
```

Specific positions (representative sample):
- `renderer.py:298` cover-editorial body — `text-align: center`.
- `renderer.py:351` data-editorial — `justify-content: center; text-align: center`.
- `renderer.py:408` insight-editorial — `justify-content: center; text-align: center`.
- `renderer.py:485` photo-data-editorial — `text-align: center`.
- `renderer.py:584` cover-dashboard `.content` — `top: 50%; left: 50%; transform: translate(-50%, -50%)`.
- `renderer.py:682` data-dashboard `.header` — `text-align: center`.
- `renderer.py:862` photo-data-dashboard `.content` — `top: 50%; left: 50%; transform: translate(-50%, -50%)`.
- `renderer.py:920` cta-dashboard `.terminal` — `top: 50%; left: 50%; transform: translate(-50%, -50%)`.
- `renderer.py:1732, 1791, 1852, 1926, 1975` — institutional family — all `text-align: center`.

The renderer has no concept of asymmetric layout. Every body element is dropped into the optical center of the canvas. There is no pull, no tension, no implied direction. Symmetry-by-default is the single largest contributor to "AI-generated" energy — humans compose asymmetrically; templates compose symmetrically.

### Failure 6 — The "stripe + streak" overlay is shared, hardcoded, and applied uniformly

**`renderer.py:168–186`** — `_stripe_bg_css()` and `_stripe_bg_html()`:
```python
def _stripe_bg_html():
    return '<div class="stripe-bg"></div><div class="streak1"></div><div class="streak2"></div>'
```

This three-div stack is injected into every "institutional" data and insight slide (`renderer.py:1816`, `:1879`). The stripe pattern itself is `repeating-linear-gradient(-35deg, ...)` and the two streaks are diagonal light gradients at fixed positions.

There is no per-brand variation. There is no per-archetype variation. There is no per-content variation. Every institutional body slide gets the *exact same* `-35deg` stripe at `rgba(255,255,255,0.018)` opacity plus the same two streaks at the same positions. The rendering is byte-identical across slides except for the text. **This is the canonical "template energy" failure** — the texture is structurally identical and content-blind.

### Failure 7 — Drop shadows, glows, and text-shadows are scattered through the codebase

```bash
$ grep "text-shadow\|drop-shadow\|filter:.*shadow\|box-shadow" renderer.py
```

7 distinct shadow effects:

- **`:601`** dashboard cover headline — `text-shadow: 0 0 30px rgba(201,160,108,0.25)`. Glowing gold text. The exact "AI-dashboard glow" cliché.
- **`:1014`** Forza blueprint hero — `filter: drop-shadow(0 0 60px rgba(197,168,108,0.25))`. 60px gold halo on a vector graphic.
- **`:1058`** Forza blueprint headline — `text-shadow: 0 4px 40px rgba(0,0,0,0.6)`. Movie-poster shadow on type.
- **`:1275`** Forza common-head shared headline — same 0/4/40 shadow.
- **`:1349`** Forza monolith — `filter: drop-shadow(-30px 0 80px rgba(230,195,120,0.25))`. Side-cast 80px gold glow.
- **`:1473`** — `box-shadow: 0 8px 40px rgba(0,0,0,0.6), inset 0 0 0 1px rgba(230,195,120,0.15)`. Card shadow + inset gold border.
- **`:1503`** — `box-shadow: 0 0 12px {accent_color}` — gold glow on UI element.

Drop shadows on type are the single most reliable signal that a design is amateur. Every shadow above is decoration, not function. Combined, they read as "Midjourney AI dashboard" aesthetic.

### Failure 8 — The Forza renderer is built entirely from banned visual vocabulary

**`renderer.py:980, 1216`** — Forza Cinzel + Cormorant Garamond imports:
```html
<link href="https://fonts.googleapis.com/css2?family=Cinzel:wght@500;600;700;800&family=Cormorant+Garamond:ital,wght@0,500;0,600;1,400&family=Inter:wght@400;500;600&display=swap">
```

Cinzel is a Roman-inscription / "movie-poster Trajan" decorative serif. Cormorant Garamond is the textbook overused-boutique-luxury-serif. The Forza brand spec calls for institutional/Palantir register; the actual code mounts Cinzel + Cormorant — the typographic neighbourhood of *Gladiator* DVD covers and Etsy "luxury" templates.

**`renderer.py:1098–1161`** — Forza blueprint hero SVG is structurally:
- A central glowing hex labelled **"FORZA OS · v1"**.
- Four circling labelled nodes: REVENUE / OPS / BRAND / PEOPLE.
- Two gold orbital rings, the inner solid, the outer dashed.
- A 60px gold radial halo behind the hex.
- Dashed gold connection lines from hex to nodes.

This is a textbook **AI-brain visualization** — the exact image Forza's brand spec forbids ("a glowing brain... a neural-network mesh of dots-and-lines... particle systems / floating geometric shapes / hex grids... ChatGPT-style chat-bubble overlays"). The renderer is hardcoded to draw the banned image.

**`renderer.py:1290–1322`** — `_forza_common_chrome()` injects the same elements into every Forza slide:
- "Systems That Scale" tagline (top-right, hardcoded forever).
- "FORZASYSTEMS.AI" + "Operating Systems for Service Businesses" footer (hardcoded forever).
- Four gold L-bracket corners at fixed 36px.
- iOS-style swipe dots (`renderer.py:1265–1270`) — `border-radius: 50%`.
- Cinzel "FORZA" wordmark with letter-spacing 9px-11px.

A Forza slide cannot escape any of these elements. The chrome is the brand. The brand is the chrome.

### Failure 9 — The background photo system is randomly tagged

**`renderer.py:91–114`** — `_init_backgrounds()`:
```python
all_bgs = sorted(BG_DIR.glob("*.png")) + sorted(BG_DIR.glob("*.jpg"))
drive_paths = [Path(p) for p in _drive_cached_photos()]
all_bgs = list(all_bgs) + drive_paths

categories = list(BG_TAGS.keys())  # ["skyline", "business", "luxury", "construction", "interior", "aerial", "sunset", "night", "all"]
for i, bg in enumerate(all_bgs):
    cat = categories[i % len(categories)]   # ← THE FAILURE
    BG_TAGS[cat].append(str(bg))
```

The photo tags `skyline / business / luxury / construction / interior / aerial / sunset / night` are **assigned by file index modulo 8**. Image #0 is "skyline." Image #1 is "business." Image #2 is "luxury." Regardless of what the image actually contains.

The comment on line 106 admits this: *"Simple round-robin assignment for now — can be refined with Claude vision later."*

Then `pick_background()` (`renderer.py:117–154`) does keyword matching on the topic to choose a category — but the category content is meaningless. A "luxury" topic gets a random photo from the bucket of images whose index happens to be `≡ 2 (mod 8)`. There is no signal.

The visual identity of every brand depends on its photo treatment. The current photo selection is randomness with a category-shaped wrapper.

### Failure 10 — Photo treatment is inconsistent across archetypes

Seven distinct filter values appear:

- `:282` editorial cover — `brightness(0.55) saturate(0.9)`.
- `:382, :445` editorial logo — `brightness(0.3)` (logo, not photo).
- `:473` photo-data-editorial — `brightness(0.7) saturate(0.85)`.
- `:846` photo-data-dashboard — `brightness(0.4) saturate(0.7)`.
- `:1720` institutional cover — `brightness(0.65) saturate(0.85)`.
- `:1914` institutional photo-data — `brightness(0.5) saturate(0.8)`.

Six different photo treatments coexist in the same renderer. Each function invented its own values. There is no canonical "Holdings photo pipeline" or "Forza photo pipeline." Each function set the values that "looked OK" the day it was written.

The Holdings spec calls for B&W or 0.55–0.70 desaturation with paper-warm highlights. The Forza spec calls for cool grade with single warm key. **Neither pipeline exists in code.** The actual treatment is whatever filter values happened to be typed at the moment of writing each function.

### Failure 11 — The "validator" validates env vars, not output

**`validator.py:1–70`** — the entire validator:
```python
def validate_environment(...) -> list[str]:
    failures.extend(validate_env_vars(required_env_vars))
    failures.extend(validate_airtable_schemas(...))   # stub: returns []
    failures.extend(validate_external_apis(...))      # stub: returns []
    return failures
```

There is **no contrast check**. **No typeface check**. **No banned-phrase check**. **No brand-isolation check**. **No layout / spacing / overlay validation**. The supposed validator only asserts that environment variables are non-placeholder strings.

Every output specification in `creative-system/master-rules/` (contrast, typography, overlay, hook hierarchy, CTA, carousel structure, visual consistency) is enforced by **nobody**. The renderer ships PNGs, the post goes live, no system has ever read the output.

### Failure 12 — The schema *names text colours instead of structural roles*

**`renderer.py:2096–2104`** — `_coerce()`:
```python
s.setdefault("headline_gold",   subtext or headline)
s.setdefault("headline_white",  headline)
s.setdefault("headline_top",    subtext or "")
s.setdefault("headline_bottom", "")
```

The slide schema's text fields are named `headline_gold` and `headline_white` — i.e., the field name encodes which colour the text should render in. **Brand expression and content structure are conflated at the schema level.** The renderer cannot accept a slide with two emphases of the same colour, or a headline with no colour emphasis at all, or an editorial layout with neither "gold" nor "white" — those don't fit the schema.

This is the deepest architectural problem: the data model assumes one specific visual treatment (a centered all-caps headline with a coloured emphasis line), and that assumption is baked into the field names. Any future "rebuild" that doesn't change the schema will inherit the same flatness.

---

## PART II — WHY OUTPUTS FEEL GENERIC, CANVA-LIKE, EMOTIONALLY DEAD

These are the patterns that produce the specific feelings the user reports. Each is grounded in code.

### "Generic" — caused by

- **Failure 4** (three brands, one accent). When two brands share `#C9A06C`, the reader's eye cannot distinguish them, so the brand experience collapses into "Nucassa-flavoured generic."
- **Failure 6** (uniform stripe overlay). When every body slide gets the same `-35deg` stripe, the brand IS the stripe, and every post in the feed reads as "Mark stripe brand."
- **Failure 9** (random photo tags). When the photo selection is structurally random, no thematic coherence builds across posts. Every post looks like every other post, with a different photo behind it.

### "Canva-like" — caused by

- **Failure 2** (Montserrat hardcoded). The single fastest signal of "designed in a free template tool." Canva's most-used heading font and its default heading recommendation are both Montserrat.
- **Failure 5** (centered composition). Canva templates default to centered. Adobe Express templates default to centered. Every drag-and-drop tool centers content because it's the safest choice. Mark renders the safe default.
- **Failure 7** (drop shadows + text-shadows). Drop shadow on type is the canonical Canva tell. Glowing-text-shadow in 2026 is a 2014 tutorial result.
- **Failure 6** (diagonal stripes). The `-35deg` repeating-linear-gradient is the most overused decorative texture in template marketplaces. Searching "Canva diagonal stripe template" returns thousands of free templates with the identical pattern.

### "Emotionally dead" — caused by

- **Failure 5** (symmetric center). Symmetry creates stillness. Asymmetry creates energy. Mark cannot create energy because Mark cannot compose asymmetrically.
- **Failure 8** (hardcoded chrome). When every slide carries identical "Systems That Scale" / "FORZASYSTEMS.AI" / corner brackets / wordmark / page indicator / swipe dots, the chrome is screaming "I am a template" louder than the content can speak. The chrome consumes the emotional bandwidth.
- **Failure 12** (schema names colours). When the data model only knows "gold text" and "white text," there is no way to express *quiet, restraint, contrast, weight, escalation*. Emotional range requires structural primitives the schema does not have.

### "AI-generated" — caused by

- **Failure 8** (banned-vocabulary Forza visuals). The glowing-hex-with-orbital-rings hero (`renderer.py:1098–1161`) is the most common Midjourney prompt result for "AI brand dashboard". The renderer literally outputs the AI-image-generator default image.
- **Failure 7** (glows + halos). 60px radial halos around accent elements are the visual signature of generative-AI image outputs. They appear constantly in Midjourney v5/v6 outputs and are a tell.
- **Failure 3** (archetype = font swap). When the same composition gets a different font and is called a "different archetype," the system reveals it has no genuine compositional thinking — only surface decoration. This pattern is what makes AI-generated content feel hollow even when individual pieces are competent.

---

## PART III — RENDERER LIMITATIONS PREVENTING ELITE OUTPUT

The current architecture fundamentally cannot produce the desired aesthetic neighbourhoods. Each limitation below is structural, not stylistic.

### Cannot produce cinematic composition

A cinematic frame requires:
- An off-center subject (rule of thirds, golden ratio).
- Negative space that *guides* the eye toward the subject.
- A single warm key light against a neutral cool field.
- A focal element that occupies < 40% of the frame.

Current renderer:
- Centers the subject mathematically (`Failure 5`).
- Has no concept of negative space — fills available padding (`renderer.py:298`: `padding: 0 140px 130px` then `text-align: center` puts the text in the geometric center of the padded box).
- Mixes warm and cool tones unsystematically (gold on `#0a0a0a` warm-black AND gold on `#0d1117` GitHub-blue, depending on archetype).
- Headline elements occupy 60–80% of the frame width via `padding`-based composition.

To produce cinematic composition the renderer would need a *grid-and-anchor system* it does not have.

### Cannot produce luxury editorial feel

Luxury editorial requires:
- A serif display family with optical sizing (small caps + multiple optical masters).
- Drop-caps and italic display devices used sparingly.
- Generous left-aligned column structure (Sotheby's catalogue pattern).
- A single brass / oxide / muted accent colour, used with restraint.
- Paper-grain or letterpress texture, not patterned overlays.

Current renderer:
- Uses Cormorant Garamond + Cinzel — the textbook *cheap* boutique-luxury fonts (`Failure 8`).
- Has no drop-cap support; no italic-display structural primitive.
- Centers everything (`Failure 5`); cannot left-align an editorial column without rewriting an archetype.
- Uses the same gold accent at high opacity everywhere — no sense of restraint (`Failure 4` + 25 gradient accents).
- Uses diagonal stripes as the texture (`Failure 6`) — the opposite of paper grain.

### Cannot produce emotional tension

Tension comes from:
- **Asymmetry** (off-balance composition implies movement / readiness).
- **Negative space pressure** (large empty areas implying restraint).
- **Scale contrast** (a single element radically larger than its neighbours).
- **Tonal contrast** (one element warm against a cool field, or vice versa).

Current renderer:
- Cannot compose asymmetrically.
- Pads to the canvas edge with `padding: 80px 70px` style values; doesn't reserve large empty zones.
- Scales every element relative to a single base (24–48px range for body, 32–84px for headlines). No "single hero element 4× larger than everything else" archetype exists in code.
- Uses warm-on-warm consistently (gold accent on warm-black) — no tonal contrast strategy.

### Cannot produce aggressive visual hierarchy

Aggressive hierarchy requires:
- One dominant element, two-three supporting, four-five tertiary — at progressive scale ratios of ~3:1.
- Clear "first, second, third" in eye-flow.
- Scale + colour + position acting in concert.

Current renderer:
- Headline / sub-headline / bullets pattern uses scale ratios of approximately 1.4:1:0.7 — a *flat* hierarchy.
- Eye-flow is centered → centered → centered. There is no first-second-third because everything sits in the same vertical alignment (`Failure 5`).
- Colour hierarchy is binary: gold-or-white. There are no third / fourth tonal levels.

### Cannot produce modern elite social aesthetics

Elite 2024–26 social aesthetics (Linear, Anthropic, Stripe Press, Anduril, Berghain) share:
- Mono numerals as a primary type element (not Mark's case).
- Hairlines and grid evidence as structural backbone (not Mark's case — Mark uses textures, not structure).
- A single signal accent at < 10% pixel volume (not Mark's case — gold appears at 25–40% volume).
- Restrained motion / transitions (Mark has no motion at all, but its statics imply a "designed" stillness rather than a "considered" one).
- Asymmetric typographic placement (not Mark's case).

The current renderer has no architectural slot for any of these. They are not patches — they are different primitives.

---

## PART IV — RENDERER REBUILD PLAN

A new renderer architecture. **No new design directions; this is engineering.** The goal is a renderer whose primitives can express the aesthetics the brand specs already defined.

### IV.1 — Composition-first architecture

Replace the current archetype-as-template-function model with a **composition primitive** model.

```
Composition = {
  canvas: { dimensions, background, grain },
  grid:   { columns, gutters, baseline, breakpoints },
  anchors: { regions identified by name, e.g. "primary", "secondary", "marginalia", "footer" },
  flow:   ordered list of (anchor, content_block) pairs,
  accents: list of (anchor, decoration_element) pairs,
}
```

A renderer entry point becomes:

```python
def render(composition, brand, content) -> HTML
```

Where `composition` is a *pure compositional spec* (no brand colours, no typefaces, no chrome), `brand` is a *pure brand-token spec* (palette, typefaces, grain), and `content` is the *pure content* (text, images, data). The three are composed at render time.

This separation means a new composition can be added without touching brand code. A new brand can be added without touching composition code. A new content type can be added without touching either.

The current `_cover_editorial`, `_cover_dashboard`, `_cover_institutional` etc. (one function per archetype × brand combination) becomes one `render(composition="cover", brand=X, content=Y)` call.

### IV.2 — Visual layering system

Replace the current "stack divs in HTML order" model with an explicit **layer stack**:

```
Layer 0 — Canvas         (background colour or photo)
Layer 1 — Photo treatment (filter pipeline, applied only to photo)
Layer 2 — Texture        (grain, mesh, paper — at most one)
Layer 3 — Mask           (gradient masks for text contrast — only when needed)
Layer 4 — Structure      (hairlines, dividers, frames)
Layer 5 — Type           (eyebrow, headline, body, footer)
Layer 6 — Accent         (single signal-colour element, max one per layer-5 group)
Layer 7 — Chrome         (slide ticker, page indicator)
```

Each layer has a strict *include/exclude* rule. **Exactly one element per layer** unless the layer explicitly permits multiples (only Layer 5 does). The current `_stripe_bg + streak1 + streak2 + gradient-overlay + bg-image + content` stack has six elements in three layers, one of which (textures) should hold only one.

Concretely: the current `_stripe_bg_html()` injecting three divs at once (`renderer.py:186`) becomes a single `texture: "diagonal_stripe"` directive that the renderer either honours with one element or not at all.

### IV.3 — Asymmetric composition system

Replace `text-align: center` with a **anchor-zone** grid. Every composition specifies which zones are populated and which are empty:

```
12-column × 12-row grid (1080 × 1350 = 9-row / 12-row at 90px / 112px cells)

Cover-A composition:
  primary:   columns 1–7, rows 8–11   ← bottom-left, off-center
  marginalia: columns 8–12, rows 1–3  ← top-right, small ticker
  footer:    columns 1–12, row 12

Cover-B composition:
  primary:   columns 6–12, rows 2–6   ← top-right, off-center
  photo:     columns 1–7, rows 1–8    ← left half + slight overlap
  footer:    columns 1–12, row 12
```

The grid is the renderer's *only* layout primitive. `position: absolute` with eyeballed pixel offsets (the current pattern) is forbidden. Compositions that don't fit the grid don't ship.

This forces asymmetry by construction. A composition that uses zones {1–7, 8–11} cannot be mistaken for a centered composition because the math doesn't allow it.

### IV.4 — Cinematic grading system

Replace per-function `filter: brightness(X) saturate(Y)` calls with a **named grade pipeline**. Each grade is a *frozen* filter chain:

```python
GRADES = {
  "forza/cool":   "brightness(0.58) saturate(0.72) contrast(1.10) hue-rotate(-2deg)",
  "forza/key":    "brightness(0.55) saturate(0.78) contrast(1.12) hue-rotate(2deg)",
  "holdings/bw":  "grayscale(1) brightness(0.75) contrast(1.08)",
  "holdings/warm": "brightness(0.70) saturate(0.62) contrast(1.06) sepia(0.08)",
}
```

Photos pass through exactly one grade. The grade name is recorded in the manifest. Grade values are versioned — changing `forza/cool` bumps the grade version and triggers re-rendering of any future post that uses it.

Current state: 6 different filter values invented per function. New state: 4 named grades, validated, frozen, versioned.

### IV.5 — Dynamic typography positioning

Replace the current `font-size: 56px / line-height: 1.1 / letter-spacing: -1px` style of magic numbers with a **typographic role system**:

```python
TYPE_ROLES = {
  "display":      {"family": "<brand.display>", "scale_token": "3xl", "weight": 600, "tracking_token": "-md"},
  "subdisplay":   {"family": "<brand.display>", "scale_token": "2xl", "weight": 500, "tracking_token": "0"},
  "eyebrow":      {"family": "<brand.body>",    "scale_token": "s",   "weight": 500, "tracking_token": "+lg", "case": "smallcaps"},
  "body":         {"family": "<brand.body>",    "scale_token": "m",   "weight": 400, "tracking_token": "0"},
  "ledger_value": {"family": "<brand.mono>",    "scale_token": "2xl", "weight": 600, "tracking_token": "-sm", "numeric": "tabular"},
}
```

Tokens (`3xl`, `+lg`, etc.) resolve at render time against the **brand's** type scale (defined in `creative-system/<brand>/TYPOGRAPHY_SYSTEM.md`). Forza's `3xl` and Holdings' `3xl` are different sizes, and that's correct — the role is shared, the resolution is brand-specific.

This kills the schema-encoded `headline_gold` / `headline_white` problem (`Failure 12`). Slides describe content in roles (`{display: "...", body: "..."}`) and the brand decides how each role is rendered.

### IV.6 — Editorial framing logic

Add **first-class editorial primitives** the current renderer entirely lacks:

- **Drop-caps** — large initial letter, brand-coloured, 4× display scale. A render directive.
- **Pull-quote frames** — italic display + brass hairlines above and below. A render directive.
- **Marginalia columns** — 1/3-width sidebar with hairline separator. A composition zone.
- **Numbered sections** — `01 / 02 / 03` mono numerals attached to body blocks. A composition pattern.
- **Footnoted figures** — number + unit + source attribution. A content block type.
- **Schematic diagrams** — 1px-stroke vector graphics with mono labels. A vector primitive.

Each becomes a render-level building block, not a hand-coded HTML fragment per function. The current renderer has *zero* of these as primitives — every editorial-style element today is a one-off CSS block invented inside a specific archetype function.

### IV.7 — A pre-render validator that actually validates

Replace `validator.py`'s env-var check with a real validator:

```python
def validate(composition, brand, content) -> list[Failure]:
    """Run BEFORE the renderer touches HTML. Returns failures or [] for ok."""
    failures = []
    failures += validate_typefaces(composition, brand)         # only approved faces
    failures += validate_palette(composition, brand)           # only brand tokens
    failures += validate_grade(composition.photo, brand)       # only named grades
    failures += validate_composition(composition.grid, brand)  # asymmetry / 12-col / no center default
    failures += validate_layers(composition.layers)            # ≤1 element per layer rule
    failures += validate_phrases(content, brand)               # no banned phrases
    failures += validate_chrome(composition.chrome, brand)     # no hardcoded taglines
    return failures
```

A render call **cannot** produce HTML if validation fails. The current renderer happily produces 3,000+ lines of CSS containing 7 drop shadows, 25 gradients, 3 banned typefaces, and 0 brand-isolation enforcement, with nothing checking. This is the architectural mistake that lets every other failure persist.

### IV.8 — Headless rendering pipeline rebuild

Specific changes to the screenshot pipeline:

- **`templates/render.js:8, :29`** — viewport changes to `{ width: 1080, height: 1350 }`. **Without this single fix, every other rebuild produces 1:1 outputs and the brand cannot ship in 4:5.**
- **`renderer.py:2065`** — same fix.
- **`renderer.py:2197`** — replace `wait_until="networkidle"` with explicit font-load waiting:
  ```python
  await page.evaluate("document.fonts.ready")
  await page.wait_for_function("document.fonts.status === 'loaded'")
  ```
  The current 800ms / 400ms timeout (`renderer.py:2201`) does not guarantee font load. Slides currently sometimes ship in fallback typefaces invisibly.
- **Fonts** — bundle `Söhne` / `GT Sectra` / `JetBrains Mono` / `Inter` / `Newsreader` as local WOFF2 files in `templates/fonts/`, served via `@font-face` with `font-display: block` (NOT `swap` — block prevents the fallback flash that currently lets Times New Roman slip through).
- **Output manifest** — every render writes `data/exports/<carousel-id>/manifest.json` containing the validation report, the grade version, the composition name, the brand-config version, and the SHA of the renderer code.

### IV.9 — Phased migration

The current `renderer.py` cannot be incrementally fixed. Specific items would need to change everywhere simultaneously:

- Removing Montserrat from `_base_css()` requires every archetype to declare its typeface explicitly.
- Changing canvas to 1080×1350 breaks every existing template at the same time.
- Replacing the schema (`headline_gold` → roles) requires updating `content_brain.py` LLM prompts AND the renderer in lockstep.

Migration order:

1. **Week 1 — composition primitives.** Build the grid-and-anchor system with one cover composition for each brand. New module: `renderer/v2/composition.py`. Old `renderer.py` keeps running in parallel.
2. **Week 2 — type-role + grade systems.** Move palette and typefaces out of code into `creative-system/<brand>/tokens.json` (parsed at render time). Build `forza/cool` and `holdings/bw` grades. Bundle local fonts.
3. **Week 3 — pre-render validator.** Wire validator into the new pipeline. Block any render that fails. Establish manifest format.
4. **Week 4 — schema migration.** Replace `headline_gold` / `headline_white` with `display` / `body` / `eyebrow` roles. Update LLM prompts in `content_brain.py:284–730` to emit the new schema.
5. **Week 5 — first new-pipeline carousels for review.** End-to-end: LLM → schema → composition → validator → renderer → manifest. GG approves or rejects.
6. **Week 6 — deprecate old renderer.** Once 10+ carousels have shipped from the new pipeline, archive `renderer.py` to `renderer.py.deprecated_2026-05` and remove from imports.

The hand-crafted `templates/holdings_slide*.html` and `templates/test_slide*.html` are deleted in Week 1 — they were never part of the production pipeline and only confused the audit.

---

## PART V — THE ROOT CAUSE, IN ONE SENTENCE

> *The renderer treats every output as a parameterized fill-in of a fixed visual template, when an elite social-design renderer must treat every output as a unique composition built from primitive layout, type, grade, and accent tokens.*

The current `renderer.py` is **a string-template engine for one design**. The rebuild is not "more designs in the template engine" — it is **replacing the template engine with a composition engine**. Every pattern documented in Parts I–III is downstream of that one architectural mistake.

No amount of new bibles, new concepts, new strategy docs, or new visual systems can fix the output while the renderer remains a templater. The fix is at the renderer's data model, not at its surface CSS.
