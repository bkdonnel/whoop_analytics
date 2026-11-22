# Whoop Analytics Pipeline

A personal data engineering portfolio project: an automated pipeline that ingests Whoop biometric data (recovery, sleep, strain, workouts) into Databricks, models it, and surfaces it through a publicly viewable dashboard — with a planned phase 2 that joins it against Garmin-tracked surf session data.

## Why this project exists

Built as a standalone portfolio piece to demonstrate **data engineering** skills to recruiters/hiring managers — distinct from an existing Databricks bootcamp capstone (AI Job Hunting Copilot), a surf-logging mobile app, and a personal AI music-production agent. Those three already exist; this project was scoped from scratch to specifically showcase DE concepts without overlapping them.

## Differentiation strategy

The goal isn't to out-analyze Whoop's own app — Whoop has a full data science team behind its recovery/strain scoring, and trying to "discover better metrics" than they already surface is a losing game. The differentiator here is the **engineering**, not the insight:

- Incremental daily ingestion (not a one-time historical dump)
- Backfill and reprocessing logic
- Idempotency (safe to re-run without duplicating data)
- Data quality tests
- Orchestration with retries/alerting (Databricks Jobs)

On top of that engineering foundation, 1-2 genuinely novel derived metrics that Whoop's app doesn't surface are a bonus, not the main event — e.g.:
- **ACWR** (acute:chronic workload ratio) — a real sports-science overtraining indicator
- A personal rolling-percentile baseline, instead of Whoop's population-normed scores

The dashboard itself is also part of the engineering story (decided 2026-11-21, see `CLAUDE.md`'s "Dashboard architecture" section): rather than embedding Databricks' own AI/BI Dashboard tool, it's a standalone app with zero runtime dependency on Databricks compute — the pipeline exports computed data once a day, and the dashboard serves entirely from that export. "Built a custom analytics app end-to-end" is the intended framing, not "wired up a vendor BI tool."

## Data sources

### Phase 1 (MVP): Whoop API
- Already have Whoop developer API access (used on a prior personal project)
- Pulls: recovery score, HRV, resting heart rate, SpO2, skin temp, sleep stages/performance/debt, respiratory rate, workout strain, heart rate zones, daily cycle/strain
- OAuth2, scopes: `read:cycles`, `read:sleep`, `read:recovery`, `offline` (for refresh tokens)
- Historical backfill: observed behavior from third-party integrators suggests an initial sync pulls somewhere between 30-180 days of history on first connect (not an officially documented limit — verify against `developer.whoop.com` directly during build)

### Phase 2 (stretch goal): Garmin Instinct 2 Solar — surf sessions
- Confirmed: the watch has a selectable "Surf" activity profile
- Confirmed: Garmin's **Personal API** is real and self-service — any Garmin Connect account holder can generate their own read-only API key directly from account settings, no business application or approval needed (this is separate from the broader Garmin Connect Developer Program, which has had partner/business-tier access paused for parts of 2026)
- Returns activity data as JSON + full `.FIT` files (GPS track, heart rate, cadence, power, laps) and health data as JSON
- **Starting from zero**: no surf sessions have been recorded on the watch yet, so this data has no history to backfill — recording needs to start now, in parallel with Phase 1 build, so there's enough volume to analyze later
- Question to answer once there's data: *does Whoop recovery/strain predict surf session quality?* — something Whoop's own app has no way to know, since it has no visibility into surf performance

### Investigated and ruled out: Lazy Surfer app
- Looked into whether the Lazy Surfer surf-logging app could be a session-data source via a documented MCP server (`lazysurfer-mcp-server`)
- The app's public **read-only** API (`backend.lazysurfer.app`) is real and verified — tested directly, returns live NOAA buoy/tide/forecast data. Could be used later as a minor conditions-enrichment detail (wave height, period, wind, tide) if wanted, but not for session data.
- The documented **MCP server** for pulling personal session data (`addSession`/`getSessions`) could not be verified — the npm package it depends on does not exist on the npm registry, and no corresponding GitHub repo was found. Documentation describes it, but it does not appear to actually be live. Not relied upon.

### Considered, not currently in scope
- **Google Calendar** (meeting load vs. next-day recovery) — calendar in question is a work calendar; shelved for now due to potential corporate API/OAuth restrictions. Could revisit as a future phase.
- **Spotify API** — interesting precisely because its "recently played" endpoint only returns a rolling last-50-track window with no full historical backfill, which would force the same "poll regularly or lose history forever" ingestion pattern as a live source. A good phase-3 candidate if more source diversity is wanted later.
- **Open-Meteo weather archive** — free, no API key, true historical depth back to 1940. Possible environmental-correlation enrichment (temperature, humidity, barometric pressure, daylight length vs. sleep/recovery).
- **GitHub API** — personal commit history vs. recovery, mostly for the novelty/narrative value in a job-search-portfolio context.
- **Strava** — only relevant if actively used to log workouts; not currently a fit.

## Stack

- **Databricks** (Spark/Delta pipeline + Databricks Jobs for orchestration) — chosen deliberately to reuse existing Databricks access, while keeping the emphasis intentionally different from the existing bootcamp capstone (that project is agent/RAG-focused; this one is ingestion/orchestration-focused) so the two don't read as repetitive to a reviewer. Databricks' job is ingestion/transformation only — it does not serve the dashboard.
- **Next.js on Vercel** — the dashboard frontend, in `/dashboard` in this same repo. Deployed continuously (via Vercel's GitHub integration); fetches data server-side from Cloudflare R2 on each request/revalidation, never directly from Databricks.
- **Cloudflare R2** — holds the daily JSON export (full Gold-layer history, not just pre-computed windows) that the dashboard reads. S3-compatible API, zero egress fees. Private bucket; only the Next.js server and the Databricks export step hold credentials.

## Recruiter accessibility (hard requirement)

The finished project must be viewable by a recruiter/hiring manager with no friction — no login, no account creation.

- **Databricks Apps cannot be made public** — confirmed via Databricks' own community support that Apps can only be shared with authenticated Databricks users, even on Free Edition. Not usable for this requirement.
- **Databricks AI/BI Dashboards can be embedded for public, login-free viewing** via a service-principal + token-server "external embedding" mechanism — this was the original plan, but was dropped 2026-11-21 in favor of a standalone app (see `CLAUDE.md`'s "Dashboard architecture" section for the reasoning: distrust of the embedding mechanism actually being frictionless, and not wanting a live Databricks SQL warehouse as a runtime dependency for every dashboard view). Kept here as a decision record, not the current plan.
- **Current plan**: the dashboard is a standalone Next.js app on Vercel, with zero runtime dependency on Databricks. Public/login-free access falls out of this for free — there's no Databricks-side gate to solve at all, since Vercel serves the app directly to anyone with the URL.

## Build sequencing

1. **Phase 1 (MVP)**: Whoop ingestion pipeline — incremental daily pulls, Delta tables, data quality checks, Databricks Jobs orchestration with retries/alerting, derived metrics (ACWR, rolling baselines).
2. **Dashboard data export**: a new Databricks Job task, after the Gold-layer refresh, exports Gold (+ run-metadata for Pipeline Health) as JSON to a private Cloudflare R2 bucket, then triggers Vercel on-demand revalidation — fully automated, part of the same daily unattended run.
3. **Dashboard frontend**: a Next.js app (`/dashboard` in this repo), deployed on Vercel, fetching that export server-side and rendering the five sections defined in `CLAUDE.md`.
4. **Phase 2 (stretch)**: Start logging surf sessions on the Garmin now; once enough sessions accumulate, pull via Garmin's Personal API and join against Whoop recovery/strain data to test the recovery-vs-surf-quality question.
5. **Possible phase 3**: Additional source (Spotify's no-backfill ingestion pattern, or Open-Meteo weather archive) if more source diversity is wanted.

No hard deadline, but the explicit priority is finishing at a reasonable pace rather than maximizing scope.
