import { connection } from "next/server";

import { Meter } from "@/components/charts/Meter";
import { StatTile } from "@/components/charts/StatTile";
import { computeFreshness, pctDaysComplete, summarizeDataQuality } from "@/lib/metrics";
import type { DashboardData } from "@/lib/types";

export async function PipelineHealth({ data }: { data: DashboardData }) {
  await connection(); // freshness depends on the current wall clock, so this part of the page is request-time, not static
  const freshness = computeFreshness(data.runMetadata);
  const completeness = pctDaysComplete(data.cycleAcwr, 90);
  const quality = summarizeDataQuality(data.dataQuality);

  return (
    <section>
      <h2 className="text-lg font-semibold" style={{ color: "var(--text-primary)" }}>
        Pipeline health
      </h2>
      <p className="mt-1 text-sm" style={{ color: "var(--text-secondary)" }}>
        How reliably this dashboard&apos;s data pipeline is running — not what the data says about me.
      </p>
      <div className="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-3">
        <StatTile
          label="Last successful sync"
          value={freshness.label}
          caption={freshness.status === "critical" ? "Overdue — check the Databricks job" : undefined}
        />
        <Meter
          label="Days with complete data (90d)"
          value={completeness}
          valueLabel={`${completeness.toFixed(0)}%`}
          severity={completeness >= 90 ? "good" : completeness >= 75 ? "warning" : "critical"}
        />
        <Meter
          label="Data quality test pass rate"
          value={quality.passRate}
          valueLabel={`${quality.passRate.toFixed(1)}%`}
          severity={quality.passRate >= 99 ? "good" : quality.passRate >= 95 ? "warning" : "critical"}
        />
      </div>
    </section>
  );
}
