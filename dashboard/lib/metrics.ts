import type {
  CycleAcwrRow,
  DataQualityRow,
  RecoveryMomentumRow,
  RecoverySameDayStrainRow,
  RunMetadataRow,
  StrainNextDayRecoveryRow,
} from "./types";

export type TrendWindowDays = 7 | 30 | 90;

export function trimToWindowDays<T>(
  rows: T[],
  windowDays: TrendWindowDays,
  dateOf: (row: T) => string
): T[] {
  if (rows.length === 0) return rows;
  const lastDate = new Date(dateOf(rows[rows.length - 1]));
  const cutoff = new Date(lastDate);
  cutoff.setDate(cutoff.getDate() - (windowDays - 1));
  return rows.filter((row) => new Date(dateOf(row)) >= cutoff);
}

export type FreshnessStatus = "good" | "warning" | "critical";

export type Freshness = {
  lastSuccessAt: Date | null;
  hoursSinceSuccess: number | null;
  status: FreshnessStatus;
  label: string;
};

export function computeFreshness(runMetadata: RunMetadataRow[]): Freshness {
  const lastSuccess = runMetadata.filter((row) => row.status === "SUCCESS").at(-1);

  if (!lastSuccess) {
    return { lastSuccessAt: null, hoursSinceSuccess: null, status: "critical", label: "No successful sync recorded" };
  }

  const lastSuccessAt = new Date(lastSuccess.finished_at);
  const hoursSinceSuccess = (Date.now() - lastSuccessAt.getTime()) / (1000 * 60 * 60);

  // The job runs once daily; a healthy gap is well under 48h, two missed
  // days (>60h) is a real problem rather than normal day-to-day jitter.
  const status: FreshnessStatus = hoursSinceSuccess <= 36 ? "good" : hoursSinceSuccess <= 60 ? "warning" : "critical";

  const label =
    hoursSinceSuccess < 1
      ? "Synced less than an hour ago"
      : hoursSinceSuccess < 24
        ? `Synced ${Math.round(hoursSinceSuccess)}h ago`
        : `Synced ${Math.round(hoursSinceSuccess / 24)}d ago`;

  return { lastSuccessAt, hoursSinceSuccess, status, label };
}

export function pctDaysComplete(cycleAcwr: CycleAcwrRow[], windowDays = 90): number {
  const recentDays = trimToWindowDays(cycleAcwr, windowDays as TrendWindowDays, (row) => row.cycle_date);
  const distinctDays = new Set(recentDays.map((row) => row.cycle_date)).size;
  return Math.min(100, (distinctDays / windowDays) * 100);
}

export type QualitySummary = {
  passRate: number;
  totalPassed: number;
  totalFailed: number;
  worst: DataQualityRow | null;
};

export function summarizeDataQuality(dataQuality: DataQualityRow[]): QualitySummary {
  const totalPassed = dataQuality.reduce((sum, row) => sum + row.passed_records, 0);
  const totalFailed = dataQuality.reduce((sum, row) => sum + row.failed_records, 0);
  const total = totalPassed + totalFailed;
  const passRate = total === 0 ? 100 : (totalPassed / total) * 100;

  const worst = dataQuality
    .filter((row) => row.passed_records + row.failed_records > 0)
    .sort((a, b) => a.passed_records / (a.passed_records + a.failed_records) - b.passed_records / (b.passed_records + b.failed_records))
    .at(0) ?? null;

  return { passRate, totalPassed, totalFailed, worst };
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

export function describeStrainNextDayRecovery(rows: StrainNextDayRecoveryRow[], windowDays = 90): string {
  const recent = trimToWindowDays(rows, windowDays as TrendWindowDays, (row) => row.cycle_date);
  if (recent.length < 14) return "Not enough history yet to describe this relationship.";

  const r = recent.at(-1)?.rolling_90d_correlation ?? null;
  const strainMedian = median(recent.map((row) => row.strain));
  const highStrain = recent.filter((row) => row.strain > strainMedian);
  const lowStrain = recent.filter((row) => row.strain <= strainMedian);
  const highMean = average(highStrain.map((row) => row.next_day_recovery_score));
  const lowMean = average(lowStrain.map((row) => row.next_day_recovery_score));
  const drop = lowMean - highMean;

  if (r === null) return "Not enough history yet to describe this relationship.";

  const direction = drop > 0 ? "drop" : "rise";
  return `Over your last ${windowDays} days, strain above ${strainMedian.toFixed(1)} was followed by a recovery ${direction} of ~${Math.abs(drop).toFixed(1)} points the next day, r = ${r.toFixed(2)}.`;
}

export function describeRecoverySameDayStrain(rows: RecoverySameDayStrainRow[], windowDays = 90): string {
  const recent = trimToWindowDays(rows, windowDays as TrendWindowDays, (row) => row.cycle_date);
  if (recent.length < 14) return "Not enough history yet to describe this relationship.";

  const r = recent.at(-1)?.rolling_90d_correlation ?? null;
  if (r === null) return "Not enough history yet to describe this relationship.";

  const strength = Math.abs(r) < 0.1 ? "little to no" : Math.abs(r) < 0.3 ? "a weak" : "a moderate";
  return `Over your last ${windowDays} days, same-day Recovery and Strain show ${strength} relationship (r = ${r.toFixed(2)}) — Recovery reflects your capacity going into the day, not how hard you end up pushing.`;
}

export function describeMomentum(rows: RecoveryMomentumRow[]): string {
  const latest = rows.at(-1);
  if (!latest) return "Not enough history yet to describe recovery momentum.";

  const diff = latest.ma_7d - latest.ma_28d;
  if (latest.momentum_state === "flat") {
    return `Your 7-day recovery average (${latest.ma_7d.toFixed(1)}) is close to your 28-day average (${latest.ma_28d.toFixed(1)}) — recovery has been flat recently.`;
  }
  const trendWord = latest.momentum_state === "rising" ? "above" : "below";
  const verb = latest.momentum_state === "rising" ? "trending up" : "trending down";
  return `Your 7-day recovery average (${latest.ma_7d.toFixed(1)}) is ${Math.abs(diff).toFixed(1)} points ${trendWord} your 28-day average (${latest.ma_28d.toFixed(1)}) — recovery is ${verb}.`;
}

function average(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

export function formatCompactNumber(value: number): string {
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 }).format(value);
}
