# Databricks notebook source
# DLT Silver layer — second library file in the same pipeline as
# whoop_dlt_ingestion.py (see pipelines/whoop_dlt_ingestion_pipeline.json),
# not a separate pipeline/job task. Reading a Bronze table defined in that
# file via dlt.read_stream() only works because both files are compiled into
# one pipeline graph; a separate pipeline would need to read Bronze's change
# feed across pipelines instead, which is more fragile for no benefit here.
#
# Bronze's apply_changes already guarantees one row per natural key, so each
# Silver table below is a straight 1:1 flatten of raw_payload's JSON into
# typed, consistently-named columns — no apply_changes/MERGE of its own is
# needed. Read via dlt.read() (batch), not dlt.read_stream(): Bronze tables
# are apply_changes targets that receive MERGE writes whenever Whoop revises
# an already-scored record, and a streaming read can't tolerate that (hit in
# production 2026-11-19 — recovery_silver failed with
# DELTA_SOURCE_TABLE_IGNORE_CHANGES the first time a real update, not just an
# insert, landed on Bronze). A materialized view recomputes from scratch each
# run instead of tailing an append-only log, so it handles upstream
# updates/deletes correctly — the same reason Gold reads Silver via dlt.read
# rather than dlt.read_stream. Table names carry an explicit _silver suffix (e.g.
# recovery_silver) even though they live in the bryan_whoop_silver schema,
# because a DLT pipeline's dataset names must be unique across the whole
# pipeline graph, not just within a schema, and Bronze already owns the name
# "recovery" etc.
#
# Grain per domain (CLAUDE.md Phase 4): recovery and cycle are already one
# row per cycle via Bronze's natural key; sleep and workout are one row per
# session. No reshaping needed here beyond flattening — sleep's `is_nap` flag
# is surfaced as a column so Gold-layer logic can exclude naps from
# day-level sleep metrics rather than this layer silently dropping them.
#
# Timestamps are standardized to UTC: WHOOP returns ISO-8601 with a "Z"
# suffix, which from_json/TimestampType parses natively as UTC. The original
# `timezone_offset` string is kept as its own column for anyone who later
# needs the wearer's local time.
#
# Field paths below are confirmed against this account's real payloads for
# sleep (full score struct, via a first-deploy failure log) and workout (all
# top-level/score fields except distance_meter/altitude_*, which were null on
# the sampled record and so unconfirmed either way), plus recovery_score,
# strain, and sleep_performance_percentage from Phase 1's Bronze spot-check.
# Not yet confirmed: the rest of RECOVERY_SCHEMA, and CYCLE_SCHEMA entirely
# (no cycle_silver failure/row has been inspected yet). First deploy already
# caught two real mismatches this way: WHOOP's `id` is a UUID string for
# sleep/workout but a plain integer for cycle (and for cycle_id/sleep_id
# foreign keys, which stay integer/string respectively — don't assume `id`
# typing generalizes to a FK of the same name), and workout's zone breakdown
# key is `zone_durations` (plural), not `zone_duration`. If another column
# comes back all-NULL after this deploys, check its JSON path the same way:
# `SELECT raw_payload FROM bootcamp_students.bryan_whoop_bronze.cycle LIMIT 1`.
#
# Each table's `name` is a fully-qualified catalog.schema.table name rather
# than a plain name plus a `schema=` kwarg — tried that first, but `schema=`
# on @dlt.table is a DDL column-definition string (for declaring a table's
# columns up front), not a target-schema override, and silently produced an
# invalid CREATE TABLE statement. The fully-qualified `name` is what actually
# routes a table to a schema other than the pipeline's configured default
# (multi-schema-per-pipeline, GA since March 2025).

# COMMAND ----------

import dlt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

SILVER_SCHEMA = "bootcamp_students.bryan_whoop_silver"


def _flatten(bronze_table: str, payload_schema: T.StructType) -> DataFrame:
    return (
        dlt.read(bronze_table)
        .withColumn("payload", F.from_json("raw_payload", payload_schema))
        .select("payload.*", "ingested_at")
    )

# COMMAND ----------
# ── RECOVERY ─────────────────────────────────────────────────────────────────

RECOVERY_SCHEMA = T.StructType([
    T.StructField("cycle_id", T.LongType()),
    T.StructField("sleep_id", T.StringType()),
    T.StructField("user_id", T.LongType()),
    T.StructField("created_at", T.TimestampType()),
    T.StructField("updated_at", T.TimestampType()),
    T.StructField("score_state", T.StringType()),
    T.StructField("score", T.StructType([
        T.StructField("recovery_score", T.DoubleType()),
        T.StructField("resting_heart_rate", T.DoubleType()),
        T.StructField("hrv_rmssd_milli", T.DoubleType()),
        T.StructField("spo2_percentage", T.DoubleType()),
        T.StructField("skin_temp_celsius", T.DoubleType()),
    ])),
])


@dlt.table(
    name=f"{SILVER_SCHEMA}.recovery_silver",
    comment="Silver: one row per cycle, recovery fields flattened and typed",
)
@dlt.expect_or_fail("cycle_id_present", "cycle_id IS NOT NULL")
@dlt.expect_or_fail("score_state_present", "score_state IS NOT NULL")
@dlt.expect("recovery_score_in_range", "score_state != 'SCORED' OR (recovery_score BETWEEN 0 AND 100)")
def recovery_silver():
    flattened = _flatten("recovery", RECOVERY_SCHEMA)
    return flattened.select(
        "cycle_id",
        "sleep_id",
        "user_id",
        "created_at",
        "updated_at",
        "score_state",
        F.col("score.recovery_score").alias("recovery_score"),
        F.col("score.resting_heart_rate").alias("resting_heart_rate"),
        F.col("score.hrv_rmssd_milli").alias("hrv_rmssd_milli"),
        F.col("score.spo2_percentage").alias("spo2_percentage"),
        F.col("score.skin_temp_celsius").alias("skin_temp_celsius"),
        "ingested_at",
    )

# COMMAND ----------
# ── CYCLE ────────────────────────────────────────────────────────────────────

CYCLE_SCHEMA = T.StructType([
    T.StructField("id", T.LongType()),
    T.StructField("user_id", T.LongType()),
    T.StructField("created_at", T.TimestampType()),
    T.StructField("updated_at", T.TimestampType()),
    T.StructField("start", T.TimestampType()),
    T.StructField("end", T.TimestampType()),
    T.StructField("timezone_offset", T.StringType()),
    T.StructField("score_state", T.StringType()),
    T.StructField("score", T.StructType([
        T.StructField("strain", T.DoubleType()),
        T.StructField("kilojoule", T.DoubleType()),
        T.StructField("average_heart_rate", T.IntegerType()),
        T.StructField("max_heart_rate", T.IntegerType()),
    ])),
])


@dlt.table(
    name=f"{SILVER_SCHEMA}.cycle_silver",
    comment="Silver: one row per physiological cycle/day, strain fields flattened and typed",
)
@dlt.expect_or_fail("cycle_id_present", "cycle_id IS NOT NULL")
@dlt.expect_or_fail("score_state_present", "score_state IS NOT NULL")
@dlt.expect("strain_in_range", "score_state != 'SCORED' OR (strain BETWEEN 0 AND 21)")
def cycle_silver():
    flattened = _flatten("cycle", CYCLE_SCHEMA)
    return flattened.select(
        F.col("id").alias("cycle_id"),
        "user_id",
        "created_at",
        "updated_at",
        F.col("start").alias("cycle_start_at"),
        F.col("end").alias("cycle_end_at"),
        "timezone_offset",
        "score_state",
        F.col("score.strain").alias("strain"),
        F.col("score.kilojoule").alias("kilojoule"),
        F.col("score.average_heart_rate").alias("average_heart_rate"),
        F.col("score.max_heart_rate").alias("max_heart_rate"),
        "ingested_at",
    )

# COMMAND ----------
# ── SLEEP ────────────────────────────────────────────────────────────────────

SLEEP_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("cycle_id", T.LongType()),
    T.StructField("user_id", T.LongType()),
    T.StructField("created_at", T.TimestampType()),
    T.StructField("updated_at", T.TimestampType()),
    T.StructField("start", T.TimestampType()),
    T.StructField("end", T.TimestampType()),
    T.StructField("timezone_offset", T.StringType()),
    T.StructField("nap", T.BooleanType()),
    T.StructField("score_state", T.StringType()),
    T.StructField("score", T.StructType([
        T.StructField("respiratory_rate", T.DoubleType()),
        T.StructField("sleep_performance_percentage", T.DoubleType()),
        T.StructField("sleep_consistency_percentage", T.DoubleType()),
        T.StructField("sleep_efficiency_percentage", T.DoubleType()),
        T.StructField("stage_summary", T.StructType([
            T.StructField("total_in_bed_time_milli", T.LongType()),
            T.StructField("total_awake_time_milli", T.LongType()),
            T.StructField("total_no_data_time_milli", T.LongType()),
            T.StructField("total_light_sleep_time_milli", T.LongType()),
            T.StructField("total_slow_wave_sleep_time_milli", T.LongType()),
            T.StructField("total_rem_sleep_time_milli", T.LongType()),
            T.StructField("sleep_cycle_count", T.IntegerType()),
            T.StructField("disturbance_count", T.IntegerType()),
        ])),
        T.StructField("sleep_needed", T.StructType([
            T.StructField("baseline_milli", T.LongType()),
            T.StructField("need_from_sleep_debt_milli", T.LongType()),
            T.StructField("need_from_recent_strain_milli", T.LongType()),
            T.StructField("need_from_recent_nap_milli", T.LongType()),
        ])),
    ])),
])


@dlt.table(
    name=f"{SILVER_SCHEMA}.sleep_silver",
    comment="Silver: one row per sleep session (naps included, flagged via is_nap), fields flattened and typed",
)
@dlt.expect_or_fail("sleep_id_present", "sleep_id IS NOT NULL")
@dlt.expect_or_fail("score_state_present", "score_state IS NOT NULL")
@dlt.expect("sleep_performance_in_range", "score_state != 'SCORED' OR (sleep_performance_percentage BETWEEN 0 AND 100)")
def sleep_silver():
    flattened = _flatten("sleep", SLEEP_SCHEMA)
    return flattened.select(
        F.col("id").alias("sleep_id"),
        "cycle_id",
        "user_id",
        "created_at",
        "updated_at",
        F.col("start").alias("sleep_start_at"),
        F.col("end").alias("sleep_end_at"),
        "timezone_offset",
        F.col("nap").alias("is_nap"),
        "score_state",
        F.col("score.respiratory_rate").alias("respiratory_rate"),
        F.col("score.sleep_performance_percentage").alias("sleep_performance_percentage"),
        F.col("score.sleep_consistency_percentage").alias("sleep_consistency_percentage"),
        F.col("score.sleep_efficiency_percentage").alias("sleep_efficiency_percentage"),
        F.col("score.stage_summary.total_in_bed_time_milli").alias("total_in_bed_time_milli"),
        F.col("score.stage_summary.total_awake_time_milli").alias("total_awake_time_milli"),
        F.col("score.stage_summary.total_no_data_time_milli").alias("total_no_data_time_milli"),
        F.col("score.stage_summary.total_light_sleep_time_milli").alias("total_light_sleep_time_milli"),
        F.col("score.stage_summary.total_slow_wave_sleep_time_milli").alias("total_slow_wave_sleep_time_milli"),
        F.col("score.stage_summary.total_rem_sleep_time_milli").alias("total_rem_sleep_time_milli"),
        F.col("score.stage_summary.sleep_cycle_count").alias("sleep_cycle_count"),
        F.col("score.stage_summary.disturbance_count").alias("disturbance_count"),
        F.col("score.sleep_needed.baseline_milli").alias("sleep_need_baseline_milli"),
        F.col("score.sleep_needed.need_from_sleep_debt_milli").alias("sleep_need_from_sleep_debt_milli"),
        F.col("score.sleep_needed.need_from_recent_strain_milli").alias("sleep_need_from_recent_strain_milli"),
        F.col("score.sleep_needed.need_from_recent_nap_milli").alias("sleep_need_from_recent_nap_milli"),
        "ingested_at",
    )

# COMMAND ----------
# ── WORKOUT ──────────────────────────────────────────────────────────────────

WORKOUT_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("user_id", T.LongType()),
    T.StructField("created_at", T.TimestampType()),
    T.StructField("updated_at", T.TimestampType()),
    T.StructField("start", T.TimestampType()),
    T.StructField("end", T.TimestampType()),
    T.StructField("timezone_offset", T.StringType()),
    T.StructField("sport_id", T.IntegerType()),
    T.StructField("sport_name", T.StringType()),
    T.StructField("score_state", T.StringType()),
    T.StructField("score", T.StructType([
        T.StructField("strain", T.DoubleType()),
        T.StructField("average_heart_rate", T.IntegerType()),
        T.StructField("max_heart_rate", T.IntegerType()),
        T.StructField("kilojoule", T.DoubleType()),
        T.StructField("percent_recorded", T.DoubleType()),
        T.StructField("distance_meter", T.DoubleType()),
        T.StructField("altitude_gain_meter", T.DoubleType()),
        T.StructField("altitude_change_meter", T.DoubleType()),
        T.StructField("zone_durations", T.StructType([
            T.StructField("zone_zero_milli", T.LongType()),
            T.StructField("zone_one_milli", T.LongType()),
            T.StructField("zone_two_milli", T.LongType()),
            T.StructField("zone_three_milli", T.LongType()),
            T.StructField("zone_four_milli", T.LongType()),
            T.StructField("zone_five_milli", T.LongType()),
        ])),
    ])),
])


@dlt.table(
    name=f"{SILVER_SCHEMA}.workout_silver",
    comment="Silver: one row per workout, strain/HR/zone fields flattened and typed",
)
@dlt.expect_or_fail("workout_id_present", "workout_id IS NOT NULL")
@dlt.expect_or_fail("score_state_present", "score_state IS NOT NULL")
@dlt.expect("strain_in_range", "score_state != 'SCORED' OR (strain BETWEEN 0 AND 21)")
def workout_silver():
    flattened = _flatten("workout", WORKOUT_SCHEMA)
    return flattened.select(
        F.col("id").alias("workout_id"),
        "user_id",
        "created_at",
        "updated_at",
        F.col("start").alias("workout_start_at"),
        F.col("end").alias("workout_end_at"),
        "timezone_offset",
        "sport_id",
        "sport_name",
        "score_state",
        F.col("score.strain").alias("strain"),
        F.col("score.average_heart_rate").alias("average_heart_rate"),
        F.col("score.max_heart_rate").alias("max_heart_rate"),
        F.col("score.kilojoule").alias("kilojoule"),
        F.col("score.percent_recorded").alias("percent_recorded"),
        F.col("score.distance_meter").alias("distance_meter"),
        F.col("score.altitude_gain_meter").alias("altitude_gain_meter"),
        F.col("score.altitude_change_meter").alias("altitude_change_meter"),
        F.col("score.zone_durations.zone_zero_milli").alias("zone_zero_milli"),
        F.col("score.zone_durations.zone_one_milli").alias("zone_one_milli"),
        F.col("score.zone_durations.zone_two_milli").alias("zone_two_milli"),
        F.col("score.zone_durations.zone_three_milli").alias("zone_three_milli"),
        F.col("score.zone_durations.zone_four_milli").alias("zone_four_milli"),
        F.col("score.zone_durations.zone_five_milli").alias("zone_five_milli"),
        "ingested_at",
    )
