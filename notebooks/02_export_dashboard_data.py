# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "boto3",
# ]
# ///
# MAGIC %md
# MAGIC # Dashboard data export
# MAGIC
# MAGIC Final task in the daily job (after `dlt_refresh`). Exports the Gold-layer
# MAGIC derived metrics plus the Silver-layer Sleep/Workout trend data and Bronze's
# MAGIC `run_metadata`/DLT event log as JSON, uploads it to a private Cloudflare R2
# MAGIC bucket, then triggers Vercel's on-demand revalidation so the dashboard
# MAGIC picks up fresh data within seconds. See CLAUDE.md's "Dashboard architecture"
# MAGIC section for why: zero runtime Databricks dependency for the dashboard, full
# MAGIC history exported (not pre-baked 7/30/90-day windows) so later chart/caption
# MAGIC changes are pure frontend work.
# MAGIC
# MAGIC Gold only computes derived insights (ACWR, baseline, correlation, momentum)
# MAGIC — it never touches Sleep/Workout, so this notebook also reads
# MAGIC `sleep_silver`/`workout_silver` directly for those two domains' Today/Trends
# MAGIC data, rather than adding passthrough Gold tables that would just copy Silver.
# MAGIC
# MAGIC Requires, in secret scope `bryan_whoop_analytics`: `r2_access_key_id`,
# MAGIC `r2_secret_access_key`, `r2_account_id`, `r2_bucket_name` (all set directly
# MAGIC via the Databricks CLI, not by any notebook). `vercel_revalidate_url` and
# MAGIC `vercel_revalidate_secret` don't exist yet — Phase 7 hasn't deployed the
# MAGIC Next.js app — so the revalidation step logs and skips until Phase 7 adds
# MAGIC them, rather than failing the job over a dependency that doesn't exist yet.

# COMMAND ----------

from __future__ import annotations

import json

import boto3
import httpx
from pyspark.sql import Row

# COMMAND ----------

SECRET_SCOPE = "bryan_whoop_analytics"
BRONZE_SCHEMA = "bootcamp_students.bryan_whoop_bronze"
SILVER_SCHEMA = "bootcamp_students.bryan_whoop_silver"
GOLD_SCHEMA = "bootcamp_students.bryan_whoop_gold"
PIPELINE_ID = "7d08b214-8dda-480c-9b2f-6205a8496e68"

# (gold table name, export key, column to order the export by)
GOLD_EXPORTS: list[tuple[str, str, str]] = [
    ("cycle_acwr_gold", "cycle_acwr", "cycle_date"),
    ("recovery_baseline_gold", "recovery_baseline", "cycle_date"),
    ("strain_next_day_recovery_gold", "strain_next_day_recovery", "cycle_date"),
    ("recovery_same_day_strain_gold", "recovery_same_day_strain", "cycle_date"),
    ("recovery_momentum_gold", "recovery_momentum", "cycle_date"),
]

# (export key, query) — plain domain trend data Gold never computes
SILVER_TREND_QUERIES: dict[str, str] = {
    "sleep": f"""
        SELECT
            sleep_id, cycle_id, sleep_start_at, sleep_end_at, is_nap,
            respiratory_rate, sleep_performance_percentage,
            sleep_consistency_percentage, sleep_efficiency_percentage
        FROM {SILVER_SCHEMA}.sleep_silver
        WHERE score_state = 'SCORED'
        ORDER BY sleep_start_at
    """,
    "workout": f"""
        SELECT
            workout_id, workout_start_at, workout_end_at, sport_name, strain,
            average_heart_rate, max_heart_rate, kilojoule, distance_meter
        FROM {SILVER_SCHEMA}.workout_silver
        WHERE score_state = 'SCORED'
        ORDER BY workout_start_at
    """,
}

# COMMAND ----------


def r2_client():
    account_id = dbutils.secrets.get(scope=SECRET_SCOPE, key="r2_account_id")
    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=dbutils.secrets.get(scope=SECRET_SCOPE, key="r2_access_key_id"),
        aws_secret_access_key=dbutils.secrets.get(scope=SECRET_SCOPE, key="r2_secret_access_key"),
        region_name="auto",
    )


def upload_rows_as_json(s3_client, bucket: str, key: str, rows: list[Row]) -> int:
    body = json.dumps([row.asDict(recursive=True) for row in rows], default=str).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    return len(rows)

# COMMAND ----------


def fetch_data_quality_summary() -> list[dict]:
    # Standard DLT event-log data-quality query (per-expectation pass/fail
    # counts), not raw event rows — the nested event payload isn't something
    # the frontend needs to re-derive, unlike the trailing-window Gold metrics.
    rows = spark.sql(f"""
        SELECT
            row_expectation.dataset AS dataset,
            row_expectation.name AS expectation,
            SUM(row_expectation.passed_records) AS passed_records,
            SUM(row_expectation.failed_records) AS failed_records
        FROM (
            SELECT explode(
                from_json(
                    `details`:flow_progress:data_quality:expectations,
                    'array<struct<name STRING, dataset STRING, passed_records BIGINT, failed_records BIGINT>>'
                )
            ) AS row_expectation
            FROM event_log('{PIPELINE_ID}')
            WHERE event_type = 'flow_progress'
        )
        GROUP BY row_expectation.dataset, row_expectation.name
        ORDER BY dataset, expectation
    """).collect()
    return [row.asDict() for row in rows]

# COMMAND ----------


def export_all(s3_client, bucket: str) -> dict[str, int]:
    counts: dict[str, int] = {}

    for table_name, export_key, order_col in GOLD_EXPORTS:
        rows = spark.sql(f"SELECT * FROM {GOLD_SCHEMA}.{table_name} ORDER BY {order_col}").collect()
        counts[export_key] = upload_rows_as_json(s3_client, bucket, f"gold/{export_key}.json", rows)

    for export_key, query in SILVER_TREND_QUERIES.items():
        rows = spark.sql(query).collect()
        counts[export_key] = upload_rows_as_json(s3_client, bucket, f"silver/{export_key}.json", rows)

    run_metadata_rows = spark.sql(
        f"SELECT * FROM {BRONZE_SCHEMA}.run_metadata ORDER BY started_at"
    ).collect()
    counts["run_metadata"] = upload_rows_as_json(
        s3_client, bucket, "pipeline_health/run_metadata.json", run_metadata_rows
    )

    dq_summary = fetch_data_quality_summary()
    s3_client.put_object(
        Bucket=bucket,
        Key="pipeline_health/data_quality.json",
        Body=json.dumps(dq_summary, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    counts["data_quality"] = len(dq_summary)

    return counts

# COMMAND ----------


def trigger_vercel_revalidation() -> dict:
    # Returns a status dict (rather than just printing) so export_dashboard_data
    # can persist it to R2 — Databricks' Jobs API doesn't expose notebook stdout
    # for serverless task runs, so a print-only signal is otherwise unobservable
    # from outside the workspace UI.
    status: dict = {"attempted": False, "ok": False, "error": None}

    try:
        revalidate_url = dbutils.secrets.get(scope=SECRET_SCOPE, key="vercel_revalidate_url")
        revalidate_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key="vercel_revalidate_secret")
    except Exception as error:
        status["error"] = f"secrets not available: {error}"
        print(status["error"] + " — skipping revalidation call.")
        return status

    status["attempted"] = True
    try:
        response = httpx.post(revalidate_url, params={"secret": revalidate_secret}, timeout=20)
        response.raise_for_status()
        status["ok"] = True
        print("Vercel on-demand revalidation triggered.")
    except Exception as error:
        status["error"] = str(error)
        print(f"Vercel revalidation call failed: {error}")

    return status

# COMMAND ----------


def export_dashboard_data() -> None:
    bucket = dbutils.secrets.get(scope=SECRET_SCOPE, key="r2_bucket_name")
    s3_client = r2_client()
    counts = export_all(s3_client, bucket)
    print(f"Dashboard export complete: {counts}")

    revalidation_status = trigger_vercel_revalidation()
    s3_client.put_object(
        Bucket=bucket,
        Key="pipeline_health/revalidation_status.json",
        Body=json.dumps(revalidation_status, default=str).encode("utf-8"),
        ContentType="application/json",
    )


export_dashboard_data()
