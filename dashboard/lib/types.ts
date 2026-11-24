export type CycleAcwrRow = {
  cycle_id: number;
  cycle_date: string;
  strain: number;
  acute_load_7d: number;
  chronic_load_28d: number;
  acwr: number;
};

export type RecoveryBaselineRow = {
  cycle_id: number;
  cycle_date: string;
  recovery_score: number;
  recovery_score_percentile_60d: number;
  hrv_rmssd_milli: number;
  hrv_percentile_60d: number;
  resting_heart_rate: number;
  rhr_percentile_60d: number;
};

export type StrainNextDayRecoveryRow = {
  cycle_id: number;
  cycle_date: string;
  strain: number;
  next_day_recovery_score: number;
  rolling_90d_correlation: number | null;
};

export type RecoverySameDayStrainRow = {
  cycle_id: number;
  cycle_date: string;
  recovery_score: number;
  strain: number;
  rolling_90d_correlation: number | null;
};

export type MomentumState = "rising" | "declining" | "flat";

export type RecoveryMomentumRow = {
  cycle_id: number;
  cycle_date: string;
  recovery_score: number;
  ma_7d: number;
  ma_28d: number;
  momentum_state: MomentumState;
};

export type SleepRow = {
  sleep_id: string;
  cycle_id: number;
  sleep_start_at: string;
  sleep_end_at: string;
  is_nap: boolean;
  respiratory_rate: number;
  sleep_performance_percentage: number;
  sleep_consistency_percentage: number;
  sleep_efficiency_percentage: number;
};

export type WorkoutRow = {
  workout_id: string;
  workout_start_at: string;
  workout_end_at: string;
  sport_name: string;
  strain: number;
  average_heart_rate: number;
  max_heart_rate: number;
  kilojoule: number;
  distance_meter: number | null;
};

export type RunStatus = "SUCCESS" | "FAILURE";

export type RunMetadataRow = {
  job_id: string;
  run_id: string;
  started_at: string;
  finished_at: string;
  status: RunStatus;
  error_message: string | null;
  recovery_count: number;
  cycle_count: number;
  sleep_count: number;
  workout_count: number;
};

export type DataQualityRow = {
  dataset: string;
  expectation: string;
  passed_records: number;
  failed_records: number;
};

export type DashboardData = {
  cycleAcwr: CycleAcwrRow[];
  recoveryBaseline: RecoveryBaselineRow[];
  strainNextDayRecovery: StrainNextDayRecoveryRow[];
  recoverySameDayStrain: RecoverySameDayStrainRow[];
  recoveryMomentum: RecoveryMomentumRow[];
  sleep: SleepRow[];
  workout: WorkoutRow[];
  runMetadata: RunMetadataRow[];
  dataQuality: DataQualityRow[];
};
