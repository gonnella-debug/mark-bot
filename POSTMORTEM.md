# Postmortem log — mark-bot

Root-cause history. One entry per incident. `class:` field first so
`grep -h '^class:' POSTMORTEM.md` across the stack surfaces systemic
patterns.

Classes: `schema-drift` | `silent-failure` | `integration-wiring` | `deploy-config` | `external-api`

---

class: integration-wiring
date: 2026-04-16
commit: 952753c
symptom: rendered content approved by GG never appeared in the Drive brand folders; posting log showed success but Drive was empty.
cause: the Drive-save call was stubbed out during an earlier refactor and never re-wired; the render path returned "saved" after uploading to nowhere.
fix: restore the Drive save in the render flow; confirm file ID in the return.
repeat-guard: every "saved" claim the bot emits must be backed by an ID from the destination system; never claim success on a stub.

---

class: external-api
date: 2026-04-01
commit: 54b6b38, 2eeec9e, 27b26e0
symptom: Drive uploads 403 Forbidden / 404 Not Found against brand folders even though the service account had access.
cause: (1) OAuth wasn't refreshing — token was static in code; (2) shared drives require `supportsAllDrives=true` on the request; (3) `drive.file` scope doesn't see folders the SA didn't create — needed the full drive scope.
fix: all three applied; SA uses a refresh token from env, full drive scope, all calls include `supportsAllDrives=true`.
repeat-guard: any Google Drive call against shared drives: full scope + supportsAllDrives=true + refresh-token flow. No exceptions.

---

class: external-api
date: 2026-04-04
commit: 7c178d0, 2a5bfc8
symptom: Instagram publish to Holdings returned error 9007 "media not ready" intermittently, then stopped posting at all.
cause: Meta's async media pipeline races the publish call — the media container isn't always ready by the time publish fires; IG also transiently 500s.
fix: retry with backoff on both 500 and 9007; small delay between container-create and publish.
repeat-guard: Meta's graph API is eventually-consistent — never assume a resource is usable immediately after create; retry with jitter.

---

class: schema-drift
date: 2026-04-07
commit: 29d5b0f
symptom: Alex reported "Mark posted 0 today" after every Railway restart, even when Mark had actually posted.
cause: posting_log lived in-memory only; a restart wiped it.
fix: persist posting_log to `/data` (Railway volume) on every write.
repeat-guard: any stateful flag/log that another bot reads goes on `/data`; in-memory is for caches only.

---

class: silent-failure
date: 2026-04-01
commit: 07ba44b
symptom: Claude API call for reference-image analysis failed because the MIME type didn't match the image bytes.
cause: code sent all reference images as PNG regardless of actual format; Claude rejected JPEG bytes labelled image/png.
fix: sniff the first bytes to detect PNG vs JPEG and set the correct mime.
repeat-guard: when sending binary to a strict API, detect the format from the bytes, don't assume from the URL.

---

class: external-api
date: 2026-04-04
commit: 6b6b7c5
symptom: Unsplash image URLs 404'd mid-render, breaking the final composition.
cause: short-form Unsplash URLs redirect to CDN, and their TTL is shorter than expected — they go stale.
fix: use the long-form photo IDs (`/photos/<id>/raw`) verified at fetch time.
repeat-guard: for third-party image sources, verify 200 before committing to them in a render.

---

class: deploy-config
date: 2026-04-01
commit: 0e20926
symptom: Mark's Docker build was 10× larger than needed and slower to deploy.
cause: Playwright was still in the Dockerfile after rendering moved to Pillow.
fix: removed Playwright from the Dockerfile.
repeat-guard: when a dependency is stripped out of runtime code, strip it out of the build the same day.

---
