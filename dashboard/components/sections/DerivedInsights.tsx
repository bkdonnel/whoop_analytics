import { ScatterCorrelationChart } from "@/components/charts/ScatterCorrelationChart";
import { TrendLineChart } from "@/components/charts/TrendLineChart";
import {
  describeMomentum,
  describeRecoverySameDayStrain,
  describeStrainNextDayRecovery,
} from "@/lib/metrics";
import type { DashboardData } from "@/lib/types";

function InsightCard({ title, caption, children }: { title: string; caption: string; children: React.ReactNode }) {
  return (
    <div className="rounded-xl p-4" style={{ background: "var(--surface-1)", border: "1px solid var(--border)" }}>
      <h3 className="text-sm font-medium" style={{ color: "var(--text-primary)" }}>
        {title}
      </h3>
      {children}
      <p className="mt-2 text-sm" style={{ color: "var(--text-secondary)" }}>
        {caption}
      </p>
    </div>
  );
}

export function DerivedInsights({ data }: { data: DashboardData }) {
  const strainToRecoveryCaption = describeStrainNextDayRecovery(data.strainNextDayRecovery);
  const recoveryToStrainCaption = describeRecoverySameDayStrain(data.recoverySameDayStrain);
  const momentumCaption = describeMomentum(data.recoveryMomentum);

  return (
    <section>
      <h2 className="text-lg font-semibold" style={{ color: "var(--text-primary)" }}>
        Derived insights
      </h2>
      <p className="mt-1 text-sm" style={{ color: "var(--text-secondary)" }}>
        Metrics Whoop&apos;s own app doesn&apos;t surface — computed from the full history, not a fixed window.
      </p>

      <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-2">
        <InsightCard title="Acute:chronic workload ratio (ACWR)" caption="7-day acute load divided by 28-day chronic load — a sports-science overtraining indicator, not something Whoop's app surfaces directly.">
          <TrendLineChart
            data={data.cycleAcwr}
            xKey="cycle_date"
            yDecimals={2}
            series={[{ dataKey: "acwr", label: "ACWR", color: "var(--series-2)" }]}
          />
        </InsightCard>

        <InsightCard title="Recovery momentum" caption={momentumCaption}>
          <TrendLineChart
            data={data.recoveryMomentum}
            xKey="cycle_date"
            series={[
              { dataKey: "ma_7d", label: "7-day avg", color: "var(--series-1)" },
              { dataKey: "ma_28d", label: "28-day avg", color: "var(--series-2)" },
            ]}
          />
        </InsightCard>

        <InsightCard title="Strain → next-day recovery" caption={strainToRecoveryCaption}>
          <ScatterCorrelationChart
            data={data.strainNextDayRecovery.map((row) => ({ x: row.strain, y: row.next_day_recovery_score }))}
            xLabel="Strain"
            yLabel="Next-day recovery"
            color="var(--series-1)"
          />
        </InsightCard>

        <InsightCard title="Same-day recovery vs. strain" caption={recoveryToStrainCaption}>
          <ScatterCorrelationChart
            data={data.recoverySameDayStrain.map((row) => ({ x: row.recovery_score, y: row.strain }))}
            xLabel="Recovery"
            yLabel="Same-day strain"
            color="var(--series-7)"
          />
        </InsightCard>
      </div>
    </section>
  );
}
