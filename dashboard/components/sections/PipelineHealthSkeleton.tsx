export function PipelineHealthSkeleton() {
  return (
    <section>
      <h2 className="text-lg font-semibold" style={{ color: "var(--text-primary)" }}>
        Pipeline health
      </h2>
      <p className="mt-1 text-sm" style={{ color: "var(--text-secondary)" }}>
        How reliably this dashboard&apos;s data pipeline is running — not what the data says about me.
      </p>
      <div className="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-3">
        {[0, 1, 2].map((i) => (
          <div
            key={i}
            className="h-24 animate-pulse rounded-xl"
            style={{ background: "var(--surface-1)", border: "1px solid var(--border)" }}
          />
        ))}
      </div>
    </section>
  );
}
