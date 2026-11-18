# CLAUDE.md

## Project

Whoop Analytics Pipeline — see `README.md` for full project scope, differentiation strategy, data sources, stack, and build sequencing, and `IMPLEMENTATION.md` for the phased build plan and current progress. As of 2026-11-18, Phase 0 (project setup) through Phase 5 (Gold layer) are all complete. Code is pushed to a public GitHub remote (`github.com/bkdonnel/whoop_analytics`), a Whoop developer app is registered, and the Databricks workspace/schemas/secret scope are set up (see `IMPLEMENTATION.md` for the shared-metastore schema layout this required). Ingestion/transformation is one DLT pipeline (`pipelines/whoop_dlt_ingestion_pipeline.json`) spanning three library files: `notebooks/01_whoop_fetch_bronze.py` calls the Whoop API (OAuth, full backfill, incremental pulls, 429 backoff) and lands raw NDJSON into a Unity Catalog Volume; `pipelines/whoop_dlt_ingestion.py` (Auto Loader + tiered `@dlt.expect`/`@dlt.expect_or_fail` expectations + `apply_changes` upsert) turns that into the final `recovery`/`cycle`/`sleep`/`workout` Bronze tables; `pipelines/whoop_dlt_silver.py` flattens those into typed, consistently-named Silver tables (`recovery_silver`/`cycle_silver`/`sleep_silver`/`workout_silver` in `bryan_whoop_silver`), re-running the same tiered expectations against the flattened columns; `pipelines/whoop_dlt_gold.py` computes the derived metrics locked in this file's Dashboard content design section — ACWR, a 60-day rolling-percentile baseline, lagged Strain↔Recovery correlation, and Recovery momentum — into `bryan_whoop_gold`. It runs **unattended, daily at 9:00 AM America/Los_Angeles**, via a scheduled two-task Databricks Job (`jobs/whoop_bronze_ingestion_daily.json`: fetch, then DLT refresh) with retries and failure email alerting — verified end-to-end against live data, including the expectations correctly catching a genuine Whoop data oddity without blocking the pipeline, the Silver layer's own expectations catching three real Bronze→Silver field/type mismatches during its first deploy, and the Gold layer's derived metrics checking out as physiologically sane (e.g. Strain→next-day-Recovery correlation consistently negative, as sports science predicts), not just error-free. No dashboard code exists yet; Phase 6 (dashboard) is next.

## Dashboard content design (decided 2026-11-11)

Core principle: every metric on the dashboard should be something Whoop's native app doesn't already surface. This is the organizing criterion for metric selection project-wide, not just the "1-2 bonus metrics" framing in the README — plain trend charts of raw Whoop metrics are *not* differentiated on their own, since Whoop's app already supports custom-range trend views with overlays.

### Phase 1 — Whoop metrics

- All four domains in scope from the start (Recovery, Sleep, Strain, Workouts), shallow headline + trend per domain initially. Deepen later rather than gold-plating one domain before the pipeline proves out end-to-end.
- Two time views per domain: a "Today" snapshot and rolling trend charts (7/30/90-day).
- No manual/exogenous logging (alcohol, travel, stress, etc.). Whoop's public API v2 doesn't expose Journal/behavior-tag data anyway (confirmed via third-party developer docs — Terra, Thryve — as of 2026), and scope is deliberately limited to automated Whoop/Garmin ingestion only.
- Derived/novel metrics:
  - **ACWR** — computed on Whoop's daily Strain score (not a custom HR-based load model), 7-day acute / 28-day chronic ratio.
  - **Personal rolling-percentile baseline** — 60-day trailing window, replacing Whoop's population-normed scoring.
  - **Lagged correlation** — Strain(day N) → Recovery(N+1), and Recovery(N) → same-day Strain capacity. Start at 1-day lag; candidates for later: Sleep performance → next-day HRV/RHR, respiratory-rate trend → downstream recovery dip.
  - **Recovery-score momentum** — 7-day MA vs 28-day MA crossover, on Recovery score only (not HRV, not a composite — keeps it legible).
- **Pipeline-health panel**, visible on the public dashboard (not just internal logging): last successful sync freshness, % of days with complete data, data-quality test pass rate. Deliberately excludes raw row/record counts (not legible to a viewer without more context).

### Phase 2 — Surf (stretch, once Garmin session volume exists)

- Target/outcome metric: self-rated **"feel" score (1-5)**, logged on Garmin per activity at save time — this is the thing Whoop structurally cannot see, which is the point of this phase's question. (FIT session fields 192/193 carry this and RPE; undocumented by Garmin but confirmed present via the FIT SDK changelog (added in profile spec 21.141.00) and third-party exports. Verify field values against your own exported FIT files before relying on them, since the "feel" field may still change from a short/int to an enum.)
- Predictor candidates to test (not pre-ranked — let the model surface what matters): morning recovery score, prior-night sleep performance, cumulative strain/ACWR heading into the session.

### Dashboard structure

Five sections: **Today / Trends / Derived Insights / Pipeline Health / Surf** (Surf added once Phase 2 has data). "Derived Insights" groups ACWR, lagged correlation, and momentum together specifically so the uniqueness story is legible to a recruiter skimming the dashboard, rather than buried inside Trends.

### Presentation convention

Correlation and momentum results are shown as a **chart + a short generated caption** summarizing the finding in plain language (e.g., "Over your last 90 days, strain above 14 was followed by a recovery drop of ~8 points the next day, r = 0.42"). Never a bare stat tile alone, never chart-only.


# Coding Standards & Conventions

## General Python
- Use functional, declarative programming — avoid classes where possible
- Prefer iteration and modularization over code duplication
- Use descriptive variable names with auxiliary verbs (e.g., `is_active`, `has_permission`)
- Use lowercase with underscores for directories and files (e.g., `routers/user_routes.py`)
- Favor named exports for routes and utility functions
- Use the Receive an Object, Return an Object (RORO) pattern
- Use `def` for pure/synchronous functions and `async def` for asynchronous operations
- Type hints required on all function signatures
- Prefer Pydantic `BaseModel` over raw dictionaries for input validation
- No emojis or symbols in any Python files

## Code Style
- Avoid unnecessary curly braces in conditional statements
- Omit curly braces for single-line conditionals
- Use concise one-line syntax for simple conditionals (e.g., `if condition: do_something()`)

## File Structure
Each module should follow this order:
1. Exported router
2. Sub-routes
3. Utilities
4. Static content
5. Types (models, schemas)

## FastAPI
- Use functional components (plain functions) and Pydantic models for validation and response schemas
- Use declarative route definitions with explicit return type annotations
- Prefer lifespan context managers over `@app.on_event("startup")` / `@app.on_event("shutdown")`
- Use middleware for logging, error monitoring, and performance optimization
- Use `HTTPException` for expected errors modeled as specific HTTP responses
- Use middleware for unexpected errors, logging, and error monitoring
- Refer to FastAPI docs for Data Models, Path Operations, and Middleware best practices

## Database & Async
- Use async functions for all I/O-bound tasks (database calls, external API requests)
- Minimize blocking I/O operations — async required for all DB and external requests
- Preferred async DB libraries: `asyncpg` or `aiomysql`
- Use SQLAlchemy 2.0 for ORM features when needed

## Performance
- Implement caching for static and frequently accessed data (Redis or in-memory)
- Use lazy loading for large datasets and substantial API responses
- Optimize data serialization/deserialization with Pydantic
