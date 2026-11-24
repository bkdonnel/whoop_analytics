type Delta = {
  value: number;
  isGood: boolean;
  suffix?: string;
};

type StatTileProps = {
  label: string;
  value: string;
  delta?: Delta;
  sparkline?: number[];
  sparklineColor?: string;
  caption?: string;
};

function Sparkline({ values, color }: { values: number[]; color: string }) {
  if (values.length < 2) return null;

  const width = 96;
  const height = 28;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;

  const points = values
    .map((value, index) => {
      const x = (index / (values.length - 1)) * width;
      const y = height - ((value - min) / range) * height;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");

  return (
    <svg width={width} height={height} className="shrink-0" aria-hidden="true">
      <polyline points={points} fill="none" stroke={color} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

export function StatTile({ label, value, delta, sparkline, sparklineColor = "var(--series-1)", caption }: StatTileProps) {
  return (
    <div
      className="flex flex-col gap-2 rounded-xl p-4"
      style={{ background: "var(--surface-1)", border: "1px solid var(--border)" }}
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-sm" style={{ color: "var(--text-secondary)" }}>
            {label}
          </p>
          <p className="mt-1 text-2xl font-semibold" style={{ color: "var(--text-primary)" }}>
            {value}
          </p>
          {delta && (
            <p
              className="mt-1 text-sm font-medium"
              style={{ color: delta.isGood ? "var(--status-good)" : "var(--status-critical)" }}
            >
              {delta.value > 0 ? "+" : ""}
              {delta.value}
              {delta.suffix ?? ""}
            </p>
          )}
        </div>
        {sparkline && <Sparkline values={sparkline} color={sparklineColor} />}
      </div>
      {caption && (
        <p className="text-xs" style={{ color: "var(--text-muted)" }}>
          {caption}
        </p>
      )}
    </div>
  );
}
