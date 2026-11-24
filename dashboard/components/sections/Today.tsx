import { Badge } from "@/components/charts/Badge";
import { StatTile } from "@/components/charts/StatTile";
import type { DashboardData, MomentumState } from "@/lib/types";

const MOMENTUM_LABEL: Record<MomentumState, string> = {
  rising: "Recovery rising",
  declining: "Recovery declining",
  flat: "Recovery flat",
};

const MOMENTUM_TONE: Record<MomentumState, "good" | "critical" | "neutral"> = {
  rising: "good",
  declining: "critical",
  flat: "neutral",
};

export function Today({ data }: { data: DashboardData }) {
  const recovery = data.recoveryBaseline.at(-1);
  const strain = data.cycleAcwr.at(-1);
  const sleep = data.sleep.at(-1);
  const workout = data.workout.at(-1);
  const momentum = data.recoveryMomentum.at(-1);

  return (
    <section>
      <div className="flex items-center justify-between gap-3">
        <h2 className="text-lg font-semibold" style={{ color: "var(--text-primary)" }}>
          Today
        </h2>
        {momentum && <Badge label={MOMENTUM_LABEL[momentum.momentum_state]} tone={MOMENTUM_TONE[momentum.momentum_state]} />}
      </div>
      <div className="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <StatTile
          label="Recovery"
          value={recovery ? `${recovery.recovery_score.toFixed(0)}%` : "—"}
          caption={recovery ? `${recovery.recovery_score_percentile_60d.toFixed(0)}th percentile, trailing 60 days` : undefined}
        />
        <StatTile
          label="Strain"
          value={strain ? strain.strain.toFixed(1) : "—"}
          caption={strain ? `ACWR ${strain.acwr.toFixed(2)}` : undefined}
        />
        <StatTile
          label="Sleep performance"
          value={sleep ? `${sleep.sleep_performance_percentage.toFixed(0)}%` : "—"}
        />
        <StatTile
          label="Latest workout"
          value={workout ? workout.sport_name : "—"}
          caption={workout ? `Strain ${workout.strain.toFixed(1)}` : undefined}
        />
      </div>
    </section>
  );
}
