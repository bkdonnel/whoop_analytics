type MeterProps = {
  label: string;
  value: number;
  valueLabel: string;
  severity: "good" | "warning" | "critical";
};

const SEVERITY_COLOR: Record<MeterProps["severity"], string> = {
  good: "var(--status-good)",
  warning: "var(--status-warning)",
  critical: "var(--status-critical)",
};

export function Meter({ label, value, valueLabel, severity }: MeterProps) {
  const pct = Math.max(0, Math.min(100, value));

  return (
    <div
      className="flex flex-col gap-2 rounded-xl p-4"
      style={{ background: "var(--surface-1)", border: "1px solid var(--border)" }}
    >
      <div className="flex items-baseline justify-between">
        <p className="text-sm" style={{ color: "var(--text-secondary)" }}>
          {label}
        </p>
        <p className="text-xl font-semibold" style={{ color: "var(--text-primary)" }}>
          {valueLabel}
        </p>
      </div>
      <div className="h-2 w-full overflow-hidden rounded-full" style={{ background: "var(--meter-track)" }}>
        <div
          className="h-full rounded-full"
          style={{ width: `${pct}%`, background: SEVERITY_COLOR[severity] }}
        />
      </div>
    </div>
  );
}
