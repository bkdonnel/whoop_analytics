# Databricks notebook source
# DLT pipeline — reads the NDJSON files landed by notebooks/01_whoop_fetch_bronze.py
# (one file per fetch run, per endpoint) from the Volume landing zone, validates
# them with expectations, and upserts into the final Bronze tables in
# bootcamp_students.bryan_whoop_bronze. Runs in TRIGGERED mode, as the second
# task in jobs/whoop_bronze_ingestion_daily.json, after the fetch task lands
# that run's files.
#
# Structural checks (expect_or_fail) mean our own fetch step produced something
# broken, and fail the whole pipeline update. Value-range checks (expect) are
# logged but don't fail anything, since Whoop occasionally scoring something
# oddly isn't a bug we can fix by retrying — and they're scoped to exclude
# records that legitimately aren't scored yet (score_state != 'SCORED'), so a
# same-day in-progress cycle never counts as a violation.

# COMMAND ----------

import dlt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LANDING_VOLUME_PATH = "/Volumes/bootcamp_students/bryan_whoop_bronze/landing"
CHECKPOINT_BASE = f"{LANDING_VOLUME_PATH}/checkpoints"
LANDING_SCHEMA = "natural_key STRING, raw_payload STRING, ingested_at TIMESTAMP"


def _read_raw_landing(endpoint_name: str) -> DataFrame:
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/{endpoint_name}_schema")
        .schema(LANDING_SCHEMA)
        .load(f"{LANDING_VOLUME_PATH}/{endpoint_name}")
    )


def _with_score_fields(raw: DataFrame, score_field_path: str, score_column_name: str) -> DataFrame:
    return (
        raw.withColumn("score_state", F.get_json_object("raw_payload", "$.score_state"))
        .withColumn(score_column_name, F.get_json_object("raw_payload", score_field_path).cast("double"))
    )


def _apply_changes_from_validated(endpoint_name: str, validated_view_name: str) -> None:
    dlt.create_streaming_table(name=endpoint_name, table_properties={"quality": "bronze"})
    dlt.apply_changes(
        target=endpoint_name,
        source=validated_view_name,
        keys=["natural_key"],
        sequence_by="ingested_at",
    )

# COMMAND ----------
# ── RECOVERY ─────────────────────────────────────────────────────────────────


@dlt.table(name="raw_recovery", comment="Bronze: raw recovery records landed by the fetch notebook", table_properties={"quality": "bronze"})
def raw_recovery():
    return _read_raw_landing("recovery")


@dlt.view(name="recovery_validated")
@dlt.expect_or_fail("natural_key_present", "natural_key IS NOT NULL")
@dlt.expect_or_fail("score_state_present", "score_state IS NOT NULL")
@dlt.expect("recovery_score_in_range", "score_state != 'SCORED' OR (recovery_score BETWEEN 0 AND 100)")
def recovery_validated():
    return _with_score_fields(dlt.read_stream("raw_recovery"), "$.score.recovery_score", "recovery_score")


_apply_changes_from_validated("recovery", "recovery_validated")

# COMMAND ----------
# ── CYCLE ────────────────────────────────────────────────────────────────────


@dlt.table(name="raw_cycle", comment="Bronze: raw cycle records landed by the fetch notebook", table_properties={"quality": "bronze"})
def raw_cycle():
    return _read_raw_landing("cycle")


@dlt.view(name="cycle_validated")
@dlt.expect_or_fail("natural_key_present", "natural_key IS NOT NULL")
@dlt.expect_or_fail("score_state_present", "score_state IS NOT NULL")
@dlt.expect("strain_in_range", "score_state != 'SCORED' OR (strain BETWEEN 0 AND 21)")
def cycle_validated():
    return _with_score_fields(dlt.read_stream("raw_cycle"), "$.score.strain", "strain")


_apply_changes_from_validated("cycle", "cycle_validated")

# COMMAND ----------
# ── SLEEP ────────────────────────────────────────────────────────────────────


@dlt.table(name="raw_sleep", comment="Bronze: raw sleep records landed by the fetch notebook", table_properties={"quality": "bronze"})
def raw_sleep():
    return _read_raw_landing("sleep")


@dlt.view(name="sleep_validated")
@dlt.expect_or_fail("natural_key_present", "natural_key IS NOT NULL")
@dlt.expect_or_fail("score_state_present", "score_state IS NOT NULL")
@dlt.expect("sleep_performance_in_range", "score_state != 'SCORED' OR (sleep_performance_percentage BETWEEN 0 AND 100)")
def sleep_validated():
    return _with_score_fields(dlt.read_stream("raw_sleep"), "$.score.sleep_performance_percentage", "sleep_performance_percentage")


_apply_changes_from_validated("sleep", "sleep_validated")

# COMMAND ----------
# ── WORKOUT ──────────────────────────────────────────────────────────────────


@dlt.table(name="raw_workout", comment="Bronze: raw workout records landed by the fetch notebook", table_properties={"quality": "bronze"})
def raw_workout():
    return _read_raw_landing("workout")


@dlt.view(name="workout_validated")
@dlt.expect_or_fail("natural_key_present", "natural_key IS NOT NULL")
@dlt.expect_or_fail("score_state_present", "score_state IS NOT NULL")
@dlt.expect("strain_in_range", "score_state != 'SCORED' OR (strain BETWEEN 0 AND 21)")
def workout_validated():
    return _with_score_fields(dlt.read_stream("raw_workout"), "$.score.strain", "strain")


_apply_changes_from_validated("workout", "workout_validated")
