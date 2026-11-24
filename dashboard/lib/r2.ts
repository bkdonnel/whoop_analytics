import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { cacheLife, cacheTag } from "next/cache";
import type {
  CycleAcwrRow,
  DashboardData,
  DataQualityRow,
  RecoveryBaselineRow,
  RecoveryMomentumRow,
  RecoverySameDayStrainRow,
  RunMetadataRow,
  SleepRow,
  StrainNextDayRecoveryRow,
  WorkoutRow,
} from "./types";

const DASHBOARD_DATA_TAG = "dashboard-data";

function r2Client(): S3Client {
  return new S3Client({
    endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    region: "auto",
    credentials: {
      accessKeyId: process.env.R2_ACCESS_KEY_ID!,
      secretAccessKey: process.env.R2_SECRET_ACCESS_KEY!,
    },
  });
}

async function fetchJsonFromR2<T>(key: string): Promise<T> {
  const client = r2Client();
  const response = await client.send(
    new GetObjectCommand({ Bucket: process.env.R2_BUCKET_NAME!, Key: key })
  );
  const body = await response.Body!.transformToString();
  return JSON.parse(body) as T;
}

async function getDashboardData(): Promise<DashboardData> {
  "use cache";
  cacheTag(DASHBOARD_DATA_TAG);
  cacheLife("max");

  const [
    cycleAcwr,
    recoveryBaseline,
    strainNextDayRecovery,
    recoverySameDayStrain,
    recoveryMomentum,
    sleep,
    workout,
    runMetadata,
    dataQuality,
  ] = await Promise.all([
    fetchJsonFromR2<CycleAcwrRow[]>("gold/cycle_acwr.json"),
    fetchJsonFromR2<RecoveryBaselineRow[]>("gold/recovery_baseline.json"),
    fetchJsonFromR2<StrainNextDayRecoveryRow[]>("gold/strain_next_day_recovery.json"),
    fetchJsonFromR2<RecoverySameDayStrainRow[]>("gold/recovery_same_day_strain.json"),
    fetchJsonFromR2<RecoveryMomentumRow[]>("gold/recovery_momentum.json"),
    fetchJsonFromR2<SleepRow[]>("silver/sleep.json"),
    fetchJsonFromR2<WorkoutRow[]>("silver/workout.json"),
    fetchJsonFromR2<RunMetadataRow[]>("pipeline_health/run_metadata.json"),
    fetchJsonFromR2<DataQualityRow[]>("pipeline_health/data_quality.json"),
  ]);

  return {
    cycleAcwr,
    recoveryBaseline,
    strainNextDayRecovery,
    recoverySameDayStrain,
    recoveryMomentum,
    sleep,
    workout,
    runMetadata,
    dataQuality,
  };
}

export { DASHBOARD_DATA_TAG, getDashboardData };
