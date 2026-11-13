# Implementation Plan

Step-by-step build sequence for the Whoop Analytics Pipeline. See `README.md` for project scope/strategy and `CLAUDE.md` for the finalized dashboard/metrics design this plan implements. Phases are ordered so each one produces something runnable/testable before the next starts.

## Phase 0 — Project setup

1. ✅ `git init` the repo, commit `README.md`, `CLAUDE.md`, `IMPLEMENTATION.md`. (Done 2026-11-11 — not yet pushed to a GitHub remote; no remote created.)
2. ✅ Create a `.gitignore` (Python venv/`.env`/notebook checkpoints/Databricks CLI config/OS/editor noise). (Done 2026-11-11.)
3. ⬜ Push to a GitHub remote (create the remote repo first — none exists yet under this name).
4. ⬜ Register a Whoop developer app at `developer.whoop.com` — OAuth2 client ID/secret, redirect URI, scopes `read:cycles`, `read:sleep`, `read:recovery`, `read:workout`, `read:profile`, `offline`. (Requires your own Whoop developer account — not automatable.)
5. ⬜ Create/confirm the Databricks workspace (Free Edition or existing), and a Unity Catalog catalog + schema for this project (e.g. `whoop_analytics.bronze/silver/gold`). (Requires your own Databricks account.)
6. ⬜ Store secrets (Whoop client ID/secret, refresh token, Databricks tokens) in a Databricks secret scope — never in the repo or notebooks directly.

## Phase 1 — Whoop OAuth + raw ingestion (Bronze)

1. Build the OAuth2 flow: initial authorization-code exchange (one-time, manual/local), then automated refresh-token exchange for all subsequent runs.
2. Write an ingestion job that pulls, per endpoint (recovery, sleep, cycle, workout):
   - Full historical backfill on first run (pull until the API returns no more pages — confirm actual available history against the live API rather than assuming the 30-180 day estimate in the README).
   - Incremental daily pulls afterward, using each record's `updated_at`/cursor to avoid re-pulling the full history every run.
3. Land raw API responses as-is into Bronze Delta tables (one table per endpoint), including an ingestion timestamp and the source payload, before any transformation.
4. Make ingestion idempotent: upsert (`MERGE INTO`) on the record's natural key (e.g. `cycle_id`, `sleep_id`), not append-only — reruns and backfills must not duplicate rows.
5. Confirm manually: run the job twice in a row and verify row counts don't change on the second run.

## Phase 2 — Orchestration

1. Wrap the Bronze ingestion in a Databricks Job (scheduled daily).
2. Add retry policy (e.g. 3 retries with backoff) for transient API/network failures.
3. Add failure alerting (email or Slack webhook) on job failure after retries exhaust.
4. Add a run-metadata table (job run id, start/end time, rows ingested, success/failure) — this becomes the data source for the Pipeline Health panel in Phase 6.

## Phase 3 — Data quality checks

1. Define and implement checks per Bronze table: schema conformance, null checks on required fields, value-range checks (e.g. recovery score 0-100, strain 0-21), duplicate-key checks.
2. Run checks as a step in the Databricks Job immediately after ingestion; fail/alert the job (or quarantine bad rows) rather than silently passing bad data downstream.
3. Log check results (pass/fail per check, per run) to the same run-metadata table from Phase 2 — this is the "data-quality test pass rate" input for Pipeline Health.

## Phase 4 — Silver layer (cleaned/conformed)

1. Transform Bronze → Silver: flatten nested JSON fields, cast types, standardize timestamps to a single timezone, rename fields to consistent naming.
2. Conform grain per domain: one row per cycle/day (recovery, strain), one row per sleep session, one row per workout.
3. Re-run the Phase 3 quality checks against Silver as well, since transformation can introduce its own bugs.

## Phase 5 — Gold layer (derived metrics)

Build each metric as its own transformation, per the definitions locked in `CLAUDE.md`:

1. **ACWR** — 7-day rolling average / 28-day rolling average of daily Strain score, per day.
2. **Personal rolling-percentile baseline** — for each metric of interest (start with Recovery, HRV, RHR), compute today's percentile rank against its own trailing 60-day window.
3. **Lagged correlation** — compute rolling-window Pearson correlation (and significance) for: Strain(day N) vs Recovery(N+1), and Recovery(N) vs same-day Strain. Store both the coefficient and enough underlying data to generate the scatter chart + caption later.
4. **Recovery momentum** — 7-day MA vs 28-day MA of Recovery score, plus a derived state (`rising`/`declining`/`flat`) from the crossover.
5. Validate each derived metric against a manual spot-check (hand-compute ACWR for one known week and confirm it matches).

## Phase 6 — Dashboard (AI/BI Dashboard)

Build the five sections defined in `CLAUDE.md`, in this order:

1. **Pipeline Health** — build first; it only needs the Phase 2/3 run-metadata table, and proves the dashboard-to-Gold(ish) connection works before tackling harder visuals. Tiles: last sync freshness, % days complete, QA pass rate.
2. **Today** — snapshot tiles per domain (Recovery, Sleep, Strain, latest Workout) plus the momentum state.
3. **Trends** — rolling charts (7/30/90-day) per domain, with the 60-day percentile baseline overlaid.
4. **Derived Insights** — ACWR chart, lagged-correlation scatter + generated caption, momentum MA-crossover chart + generated caption.
5. Write the caption-generation logic (short template filling in the actual computed numbers, e.g. "strain above X preceded a Y-point recovery drop, r = Z") as a small function/notebook step feeding the dashboard's text widgets, not hand-written text.

## Phase 7 — Public access

1. Create a Databricks service principal scoped read-only to the dashboard's underlying data.
2. Build the small token server (mints short-lived scoped access tokens for the external-embedding flow) — pick a minimal framework (e.g. FastAPI/Flask).
3. Deploy the token server to a free-tier host (Render/Vercel/Fly.io — confirm which still offers a free tier at build time).
4. Embed the AI/BI Dashboard via the external-embedding mechanism on a simple static page (or directly via the token server's own served page) and confirm it loads with zero Databricks login in a private/incognito browser session.

## Phase 8 — End-to-end validation

1. Let the daily job run unattended for at least a week; confirm no manual intervention was needed and Pipeline Health reflects reality.
2. Intentionally break something (e.g. revoke a token, feed a malformed record) to confirm retries/alerting/quarantine actually fire.
3. Recruiter-readability pass: have someone unfamiliar with the project view the public dashboard cold and confirm the Derived Insights captions are understandable without extra explanation.

## Phase 9 — Phase 2 (stretch): Garmin surf data

1. Generate a Garmin Personal API key; confirm actual response shape for activity JSON + FIT files for a Surf-profile activity.
2. Start logging real surf sessions now (watch "Surf" profile), including the self-rated feel score (1-5) at save time — needed before any of this phase is useful.
3. Build Garmin ingestion following the same Bronze → Silver pattern as Whoop (raw JSON landing, idempotent upsert, FIT-file parsing for session-level feel/RPE fields 192/193 — verify field meaning against your own exported files first).
4. Once enough sessions exist (define a minimum, e.g. 20+), join Garmin surf sessions against same-day/prior-day Whoop Silver data on date, and build the predictor/target model described in `CLAUDE.md` (recovery, sleep performance, ACWR → self-rated feel).
5. Add the Surf section to the dashboard once the model/correlation is stable enough to present, following the same chart + caption convention as Derived Insights.
