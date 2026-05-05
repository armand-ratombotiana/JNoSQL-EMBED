---
name: UI-Driven Development Loop
description: Every engine feature must include Web Console validation hook with real-time metrics, interactive testing, and sub-300KB payload
type: feedback
---

## UI-Driven Development Loop

**Why:** SPEC.md mandates the Web Console as the PRIMARY interactive test harness. Every engine feature must be validated through the UI, not just unit tests. This ensures features are both functional and usable.

**How to apply:**
- Every STEP output must include `=== UI VALIDATION HOOK ===` section
- Include exact browser interaction steps (click, type, observe)
- Define HTMX targets and Alpine.js state changes
- Provide mock payloads and expected responses
- Set latency targets (TTFB <150ms, 60fps interactions)
- Validate accessibility (WCAG AA, ARIA labels, keyboard nav)
- Measure payload size (<300KB total, zero external runtime deps)

**Console tech stack:**
- Vanilla JS + HTMX + Alpine.js (zero-build)
- Inlined utility CSS
- SSE for real-time metrics (not polling)
- System fonts (Inter, JetBrains Mono)
- Light/dark theme toggle

**Before Git commit:**
- Interactive validation passes in browser
- SSE feed shows real-time metrics
- Payload <300KB, TTFB <150ms
- Accessibility audit passes (keyboard nav, ARIA, contrast)
