# Implementation Plan

Step-by-step build sequence for the Whoop Analytics Pipeline. See `README.md` for project scope/strategy and `CLAUDE.md` for the finalized dashboard/metrics design this plan implements. Phases are ordered so each one produces something runnable/testable before the next starts.

## Phase 0 — Project setup

1. ✅ `git init` the repo, commit `README.md`, `CLAUDE.md`, `IMPLEMENTATION.md`. (Done 2026-11-11 — not yet pushed to a GitHub remote; no remote created.)
2. ✅ Create a `.gitignore` (Python venv/`.env`/notebook checkpoints/Databricks CLI config/OS/editor noise). (Done 2026-11-11.)
3. ✅ Push to a GitHub remote. (Done 2026-11-13 — public repo at `github.com/bkdonnel/whoop_analytics`.)
4. ✅ Register a Whoop developer app at `developer.whoop.com` — OAuth2 client ID/secret, redirect URI `http://localhost:8080/callback`, scopes `recovery`, `cycles`, `sleep`, `workout`, `profile`, `body_measurement`. (Done 2026-11-13. Credentials stored locally in gitignored `.env`, not yet moved to a Databricks secret scope.)
5. ✅ Create/confirm the Databricks workspace and Unity Catalog schemas for this project. (Done 2026-11-13 — workspace `dbc-7b106152-caf3.cloud.databricks.com` is a shared bootcamp metastore (`tabular`); no `CREATE CATALOG` permission there, so the project uses three schemas under the shared `bootcamp_students` catalog instead of a dedicated `whoop_analytics` catalog: `bootcamp_students.bryan_whoop_bronze`, `.bryan_whoop_silver`, `.bryan_whoop_gold`. Wherever this plan says "Bronze/Silver/Gold table," read it as "a table in the corresponding `bryan_whoop_*` schema.")
6. ✅ Store secrets (Whoop client ID/secret, refresh token, Databricks tokens) in a Databricks secret scope — never in the repo or notebooks directly. (Done 2026-11-13 — secret scope `bryan_whoop_analytics` holds `whoop_client_id`, `whoop_client_secret`, `whoop_redirect_uri`. Local `.env` can stay as a dev fallback but notebooks/jobs should read via `dbutils.secrets.get(scope="bryan_whoop_analytics", key=...)`. Refresh token and Databricks tokens to be added here once the OAuth flow in Phase 1 produces one.)

## Phase 1 — Whoop OAuth + raw ingestion (Bronze)

Built as Databricks notebooks (not a local Python package/Databricks Connect) — this account has no personal classic cluster and only a serverless SQL warehouse, and notebooks get `spark`/`dbutils` natively on whatever compute they're attached to (serverless notebook compute, confirmed available). Developed in a Databricks Repo synced to GitHub. The one-time OAuth authorization-code exchange runs as a local script instead (`scripts/whoop_authorize.py`), since its `localhost:8080` redirect only resolves in a local browser session, not inside the workspace.

1. ✅ Build the OAuth2 flow: initial authorization-code exchange (one-time, manual/local), then automated refresh-token exchange for all subsequent runs. (Done 2026-11-15. Whoop rotates the refresh token on every refresh call — confirmed empirically, a run that doesn't persist the new one breaks the very next run with a 400 on `/oauth/oauth2/token`. The ingestion notebook persists the rotated token back to the secret scope after every refresh via `databricks.sdk.WorkspaceClient()`'s notebook-native auth, which needed no personal access token — convenient, since this account cannot create one at all (`databricks tokens create` fails with "User does not have permission to use tokens"; the user has hit this same restriction on a prior project, so treat it as account-level, not a one-off). Phase 7's planned service principal for public dashboard embedding may hit the same wall — confirm before building it.)
2. ✅ Write an ingestion notebook (`notebooks/01_whoop_ingest_bronze.py`) that pulls, per endpoint (recovery, sleep, cycle, workout):
   - ✅ Full historical backfill on first run, paginating via `nextToken` until exhausted. (Done 2026-11-15 — actual available history goes back to 2022-12-25, ~4 years, not the README's guessed 30-180 days.)
   - ✅ Incremental pulls afterward, using an `ingestion_state` Delta table (one row per endpoint, last successful pull time) with a 1-day overlap, rather than a per-record `updated_at` cursor.
   - ✅ Handle Whoop's API rate limit: hit a 429 on first live run (sleep endpoint, ~17 pages in); added retry-with-backoff (honors `Retry-After`, else exponential) plus a small inter-page delay.
3. ✅ Land raw API responses as-is into Bronze Delta tables (one table per endpoint: `bootcamp_students.bryan_whoop_bronze.{recovery,cycle,sleep,workout}`), each with `natural_key`, `raw_payload` (full JSON string), and `ingested_at`.
4. ✅ Make ingestion idempotent: `MERGE INTO` on the record's natural key (`cycle_id` for recovery, `id` for cycle/sleep/workout — recovery has no key of its own, confirmed against live payloads) — reruns and backfills don't duplicate rows.
5. ✅ Confirmed manually (2026-11-15): after an initial run partially failed (429 mid-sleep-backfill) and was re-run, row counts across all four Bronze tables were sane with no duplicate/null natural keys — recovery 1352, cycle 1396, sleep 1373, workout 1431 rows, `ingestion_state` 4 rows (one per endpoint). Spot-checked a sample recovery payload against the live API shape.

## Phase 2 — Orchestration

Job definition checked into the repo as `jobs/whoop_bronze_ingestion_daily.json` (infrastructure-as-code, created via `databricks jobs create --json @jobs/whoop_bronze_ingestion_daily.json`) rather than built click-by-click in the UI.

1. ✅ Wrap the Bronze ingestion in a Databricks Job (scheduled daily). (Done 2026-11-15 — job id `468100317206602`, daily at 9:00 AM `America/Los_Angeles` (`0 0 9 * * ?`), chosen to run late enough that a normal night's sleep/recovery is synced and scored by Whoop. No cluster spec needed — confirmed the scheduled Job runs on serverless compute, same as the interactive notebook.)
2. ✅ Add retry policy (e.g. 3 retries with backoff) for transient API/network failures. (Done 2026-11-15 — `max_retries: 3`, `min_retry_interval_millis: 120000` on the task. Separate from the inline per-page 429 backoff already in the notebook itself, which handles Whoop's rate limit without needing a whole-job retry.)
3. ✅ Add failure alerting (email or Slack webhook) on job failure after retries exhaust. (Done 2026-11-15 — email to `bryankdonnelly@comcast.net` via the job's native `email_notifications.on_failure`, no custom code.)
4. ✅ Add a run-metadata table (job run id, start/end time, rows ingested, success/failure) — this becomes the data source for the Pipeline Health panel in Phase 6. (Done 2026-11-15 — `bootcamp_students.bryan_whoop_bronze.run_metadata`, one row per run: `job_id`/`run_id` (passed into the notebook via Job `base_parameters` referencing `{{job.id}}`/`{{job.run_id}}`, read via `dbutils.widgets`), `started_at`/`finished_at`, `status`, `error_message`, and a per-endpoint row count. The notebook re-raises on failure after writing the row, so the Job task itself fails and triggers its own retries/email — a swallowed exception would otherwise make a broken run look green. Verified end-to-end with an unattended `databricks jobs run-now` test: `SUCCESS`, real job/run ids recorded, correct small incremental counts.)

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
