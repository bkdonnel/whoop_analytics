# Databricks notebook source
# DLT Gold layer — third library file in the same pipeline as Bronze/Silver
# (pipelines/whoop_dlt_ingestion.py, pipelines/whoop_dlt_silver.py; wired into
# pipelines/whoop_dlt_ingestion_pipeline.json). Computes the derived metrics
# locked in CLAUDE.md's "Dashboard content design": ACWR, a personal
# 60-day rolling-percentile baseline, lagged Strain<->Recovery correlation,
# and Recovery-score momentum (7-day MA vs 28-day MA crossover).
#
# Reads Silver via dlt.read (batch), not dlt.read_stream — every metric here
# is a trailing-window calculation over the full history (rolling averages,
# percentile rank, rolling correlation), which needs an ordered view of all
# rows at once, not an incremental stream. Table sizes are small (~1500
# rows/table today), so recomputing from the full history on every pipeline
# run is cheap and far simpler than hand-rolling incremental window-function
# state.
#
# Silver tables are referenced by their fully-qualified name
# (f"{SILVER_SCHEMA}.cycle_silver", not the bare "cycle_silver") because
# that's the exact string Phase 4 registered them under (dlt.table(name=...)
# there is itself fully qualified) — see Phase 4's CLAUDE.md note on the
# schema= pitfall; this sidesteps re-testing whether DLT also resolves a
# same-pipeline dataset by its short name.
#
# All rolling windows are defined over a calendar-day index (datediff from a
# fixed epoch), not row position (rowsBetween) — WHOOP cycles are usually one
# per day but a missed-wear gap would otherwise silently shrink a "7-day"
# rowsBetween window to fewer actual days without surfacing that.
#
# Only SCORED records are used (score_state != 'SCORED' rows have null score
# fields by design — see Phase 3's expectations — and would otherwise poison
# a rolling average/correlation with nulls).

# COMMAND ----------

import dlt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_SCHEMA = "bootcamp_students.bryan_whoop_silver"
GOLD_SCHEMA = "bootcamp_students.bryan_whoop_gold"
EPOCH = F.lit("1970-01-01")


def _read_silver(table_name: str) -> DataFrame:
    return dlt.read(f"{SILVER_SCHEMA}.{table_name}").filter("score_state = 'SCORED'")


def _with_day_index(df: DataFrame, date_col: str) -> DataFrame:
    return df.withColumn("day_index", F.datediff(F.col(date_col), EPOCH))


def _cycle_dates() -> DataFrame:
    return _read_silver("cycle_silver").select(
        "cycle_id", F.to_date("cycle_start_at").alias("cycle_date")
    )

# COMMAND ----------
# ── ACWR (acute:chronic workload ratio) ────────────────────────────────────


@dlt.table(
    name=f"{GOLD_SCHEMA}.cycle_acwr_gold",
    comment="Gold: 7-day acute / 28-day chronic rolling average of daily Strain, per cycle/day",
)
def cycle_acwr_gold():
    cycle = _with_day_index(
        _read_silver("cycle_silver").withColumn("cycle_date", F.to_date("cycle_start_at")),
        "cycle_date",
    )

    acute_window = Window.orderBy("day_index").rangeBetween(-6, 0)
    chronic_window = Window.orderBy("day_index").rangeBetween(-27, 0)

    return cycle.select(
        "cycle_id",
        "cycle_date",
        "strain",
        F.avg("strain").over(acute_window).alias("acute_load_7d"),
        F.avg("strain").over(chronic_window).alias("chronic_load_28d"),
    ).withColumn("acwr", F.col("acute_load_7d") / F.col("chronic_load_28d"))

# COMMAND ----------
# ── Personal rolling-percentile baseline (60-day trailing window) ──────────
#
# percentile = (count of trailing-60-day values <= today's value) / window
# size * 100. Higher is "better" for recovery_score/hrv; for RHR, lower is
# physiologically better, so a high RHR percentile here means "higher than
# usual," not "healthier" — Phase 6's caption logic needs to say that
# explicitly for RHR rather than reusing the recovery/HRV phrasing as-is.


def _with_60d_percentile(df: DataFrame, value_col: str, out_col: str) -> DataFrame:
    window_60d = Window.orderBy("day_index").rangeBetween(-59, 0)
    windowed_col = f"_{value_col}_60d_window"
    with_values = df.withColumn(windowed_col, F.collect_list(value_col).over(window_60d))
    return with_values.withColumn(
        out_col,
        F.expr(f"size(filter({windowed_col}, x -> x <= {value_col})) / size({windowed_col}) * 100"),
    ).drop(windowed_col)


@dlt.table(
    name=f"{GOLD_SCHEMA}.recovery_baseline_gold",
    comment="Gold: Recovery score, HRV, and RHR each as a percentile rank against their own trailing 60-day window",
)
def recovery_baseline_gold():
    recovery = _with_day_index(
        _read_silver("recovery_silver").join(_cycle_dates(), "cycle_id"),
        "cycle_date",
    )

    recovery = _with_60d_percentile(recovery, "recovery_score", "recovery_score_percentile_60d")
    recovery = _with_60d_percentile(recovery, "hrv_rmssd_milli", "hrv_percentile_60d")
    recovery = _with_60d_percentile(recovery, "resting_heart_rate", "rhr_percentile_60d")

    return recovery.select(
        "cycle_id",
        "cycle_date",
        "recovery_score",
        "recovery_score_percentile_60d",
        "hrv_rmssd_milli",
        "hrv_percentile_60d",
        "resting_heart_rate",
        "rhr_percentile_60d",
    )

# COMMAND ----------
# ── Lagged correlation: Strain(day N) -> Recovery(day N+1) ─────────────────


@dlt.table(
    name=f"{GOLD_SCHEMA}.strain_next_day_recovery_gold",
    comment="Gold: Strain(day N) paired with Recovery(day N+1), plus a trailing 90-day rolling Pearson correlation",
)
def strain_next_day_recovery_gold():
    strain_by_day = _cycle_dates().join(
        _read_silver("cycle_silver").select("cycle_id", "strain"), "cycle_id"
    )
    recovery_by_day = _read_silver("recovery_silver").join(_cycle_dates(), "cycle_id").select(
        F.date_sub("cycle_date", 1).alias("cycle_date"), "recovery_score"
    )

    paired = strain_by_day.join(recovery_by_day, "cycle_date").withColumnRenamed(
        "recovery_score", "next_day_recovery_score"
    )
    paired = _with_day_index(paired, "cycle_date")

    rolling_90d = Window.orderBy("day_index").rangeBetween(-89, 0)
    return paired.withColumn(
        "rolling_90d_correlation", F.corr("strain", "next_day_recovery_score").over(rolling_90d)
    ).select("cycle_id", "cycle_date", "strain", "next_day_recovery_score", "rolling_90d_correlation")

# COMMAND ----------
# ── Same-day: Recovery(day N) -> Strain(day N) ──────────────────────────────


@dlt.table(
    name=f"{GOLD_SCHEMA}.recovery_same_day_strain_gold",
    comment="Gold: Recovery(day N) paired with same-day Strain(day N), plus a trailing 90-day rolling Pearson correlation",
)
def recovery_same_day_strain_gold():
    paired = _with_day_index(
        _read_silver("recovery_silver")
        .join(_cycle_dates(), "cycle_id")
        .join(_read_silver("cycle_silver").select("cycle_id", "strain"), "cycle_id")
        .select("cycle_id", "cycle_date", "recovery_score", "strain"),
        "cycle_date",
    )

    rolling_90d = Window.orderBy("day_index").rangeBetween(-89, 0)
    return paired.withColumn(
        "rolling_90d_correlation", F.corr("recovery_score", "strain").over(rolling_90d)
    ).select("cycle_id", "cycle_date", "recovery_score", "strain", "rolling_90d_correlation")

# COMMAND ----------
# ── Recovery-score momentum (7-day MA vs 28-day MA crossover) ──────────────

MOMENTUM_FLAT_BAND_POINTS = 2.0  # |ma_7d - ma_28d| within this band reports as "flat"


@dlt.table(
    name=f"{GOLD_SCHEMA}.recovery_momentum_gold",
    comment="Gold: Recovery score 7-day MA vs 28-day MA, and a rising/declining/flat momentum state",
)
def recovery_momentum_gold():
    recovery = _with_day_index(
        _read_silver("recovery_silver").join(_cycle_dates(), "cycle_id"),
        "cycle_date",
    )

    ma_7d_window = Window.orderBy("day_index").rangeBetween(-6, 0)
    ma_28d_window = Window.orderBy("day_index").rangeBetween(-27, 0)

    with_mas = recovery.select(
        "cycle_id",
        "cycle_date",
        "recovery_score",
        F.avg("recovery_score").over(ma_7d_window).alias("ma_7d"),
        F.avg("recovery_score").over(ma_28d_window).alias("ma_28d"),
    )

    return with_mas.withColumn(
        "momentum_state",
        F.when(F.col("ma_7d") - F.col("ma_28d") > MOMENTUM_FLAT_BAND_POINTS, "rising")
        .when(F.col("ma_28d") - F.col("ma_7d") > MOMENTUM_FLAT_BAND_POINTS, "declining")
        .otherwise("flat"),
    )
