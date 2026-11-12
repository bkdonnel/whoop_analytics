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

- **Databricks** (Spark/Delta pipeline + Databricks Jobs for orchestration + AI/BI Dashboards) — chosen deliberately to reuse existing Databricks access, while keeping the emphasis intentionally different from the existing bootcamp capstone (that project is agent/RAG-focused; this one is ingestion/orchestration-focused) so the two don't read as repetitive to a reviewer.

## Recruiter accessibility (hard requirement)

The finished project must be viewable by a recruiter/hiring manager with no friction — no login, no account creation.

- **Databricks Apps cannot be made public** — confirmed via Databricks' own community support that Apps can only be shared with authenticated Databricks users, even on Free Edition. Not usable for this requirement.
- **Databricks AI/BI Dashboards *can* be embedded for fully public, login-free viewing**, via the "external embedding" mechanism: a service principal plus a small, separately-hosted token server that mints scoped access tokens, letting anyone load the dashboard with zero Databricks account. This is the path chosen for this project — more engineering lift than a static README, but it's also a legitimate, distinct engineering artifact in its own right (most candidates don't bother solving "make my internal analytics dashboard public").

## Build sequencing

1. **Phase 1 (MVP)**: Whoop ingestion pipeline — incremental daily pulls, Delta tables, data quality checks, Databricks Jobs orchestration with retries/alerting, derived metrics (ACWR, rolling baselines), AI/BI dashboard.
2. **Public dashboard access**: service principal + token server for external embedding, deployed publicly (hosting TBD — a free tier like Render/Vercel/Fly.io is the likely target).
3. **Phase 2 (stretch)**: Start logging surf sessions on the Garmin now; once enough sessions accumulate, pull via Garmin's Personal API and join against Whoop recovery/strain data to test the recovery-vs-surf-quality question.
4. **Possible phase 3**: Additional source (Spotify's no-backfill ingestion pattern, or Open-Meteo weather archive) if more source diversity is wanted.

No hard deadline, but the explicit priority is finishing at a reasonable pace rather than maximizing scope.
