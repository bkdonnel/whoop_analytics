"use client";

import { useState } from "react";

import { TrendLineChart } from "@/components/charts/TrendLineChart";
import { trimToWindowDays, type TrendWindowDays } from "@/lib/metrics";
import type { DashboardData } from "@/lib/types";

const WINDOWS: TrendWindowDays[] = [7, 30, 90];

function DomainCard({ title, caption, children }: { title: string; caption?: string; children: React.ReactNode }) {
  return (
    <div className="rounded-xl p-4" style={{ background: "var(--surface-1)", border: "1px solid var(--border)" }}>
      <h3 className="text-sm font-medium" style={{ color: "var(--text-primary)" }}>
        {title}
      </h3>
      {children}
      {caption && (
        <p className="mt-2 text-xs" style={{ color: "var(--text-muted)" }}>
          {caption}
        </p>
      )}
    </div>
  );
}

export function Trends({ data }: { data: DashboardData }) {
  const [windowDays, setWindowDays] = useState<TrendWindowDays>(30);

  const recovery = trimToWindowDays(data.recoveryBaseline, windowDays, (row) => row.cycle_date);
  const strain = trimToWindowDays(data.cycleAcwr, windowDays, (row) => row.cycle_date);
  const sleep = trimToWindowDays(data.sleep, windowDays, (row) => row.sleep_start_at);
  const workout = trimToWindowDays(data.workout, windowDays, (row) => row.workout_start_at);

  const latestRecovery = recovery.at(-1);

  return (
    <section>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h2 className="text-lg font-semibold" style={{ color: "var(--text-primary)" }}>
          Trends
        </h2>
        <div className="flex gap-1 rounded-lg p-1" style={{ background: "var(--surface-1)", border: "1px solid var(--border)" }}>
          {WINDOWS.map((w) => (
            <button
              key={w}
              onClick={() => setWindowDays(w)}
              className="rounded-md px-3 py-1 text-sm font-medium transition-colors"
              style={{
                background: windowDays === w ? "var(--series-1)" : "transparent",
                color: windowDays === w ? "#ffffff" : "var(--text-secondary)",
              }}
            >
              {w}d
            </button>
          ))}
        </div>
      </div>

      <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-2">
        <DomainCard title="Recovery score" caption="Raw score with your trailing 60-day percentile rank overlaid">
          <TrendLineChart
            data={recovery}
            xKey="cycle_date"
            series={[
              { dataKey: "recovery_score", label: "Recovery score", color: "var(--series-1)" },
              { dataKey: "recovery_score_percentile_60d", label: "60d percentile", color: "var(--series-4)" },
            ]}
          />
        </DomainCard>

        <DomainCard
          title="HRV"
          caption={
            latestRecovery
              ? `Currently ${latestRecovery.hrv_percentile_60d.toFixed(0)}th percentile of your trailing 60 days`
              : undefined
          }
        >
          <TrendLineChart
            data={recovery}
            xKey="cycle_date"
            yUnit="ms"
            series={[{ dataKey: "hrv_rmssd_milli", label: "HRV (ms)", color: "var(--series-3)" }]}
          />
        </DomainCard>

        <DomainCard
          title="Resting heart rate"
          caption={
            latestRecovery
              ? `Currently ${latestRecovery.rhr_percentile_60d.toFixed(0)}th percentile — higher means more elevated than usual, not necessarily better`
              : undefined
          }
        >
          <TrendLineChart
            data={recovery}
            xKey="cycle_date"
            yUnit="bpm"
            series={[{ dataKey: "resting_heart_rate", label: "RHR (bpm)", color: "var(--series-7)" }]}
          />
        </DomainCard>

        <DomainCard title="Strain">
          <TrendLineChart
            data={strain}
            xKey="cycle_date"
            yDecimals={1}
            series={[{ dataKey: "strain", label: "Strain", color: "var(--series-2)" }]}
          />
        </DomainCard>

        <DomainCard title="Sleep performance">
          <TrendLineChart
            data={sleep}
            xKey="sleep_start_at"
            yUnit="%"
            series={[{ dataKey: "sleep_performance_percentage", label: "Sleep performance", color: "var(--series-6)" }]}
          />
        </DomainCard>

        <DomainCard title="Workout strain" caption="One point per logged workout, not daily">
          <TrendLineChart
            data={workout}
            xKey="workout_start_at"
            yDecimals={1}
            series={[{ dataKey: "strain", label: "Workout strain", color: "var(--series-5)" }]}
          />
        </DomainCard>
      </div>
    </section>
  );
}
