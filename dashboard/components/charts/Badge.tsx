type BadgeProps = {
  label: string;
  tone: "good" | "warning" | "critical" | "neutral";
};

const TONE_COLOR: Record<BadgeProps["tone"], string> = {
  good: "var(--status-good)",
  warning: "var(--status-warning)",
  critical: "var(--status-critical)",
  neutral: "var(--text-muted)",
};

export function Badge({ label, tone }: BadgeProps) {
  const color = TONE_COLOR[tone];
  return (
    <span
      className="inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium"
      style={{ background: "var(--surface-1)", border: `1px solid ${color}`, color: "var(--text-primary)" }}
    >
      <span className="inline-block size-2 rounded-full" style={{ background: color }} aria-hidden="true" />
      {label}
    </span>
  );
}
