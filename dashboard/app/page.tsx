import { Suspense } from "react";

import { DerivedInsights } from "@/components/sections/DerivedInsights";
import { PipelineHealth } from "@/components/sections/PipelineHealth";
import { PipelineHealthSkeleton } from "@/components/sections/PipelineHealthSkeleton";
import { Today } from "@/components/sections/Today";
import { Trends } from "@/components/sections/Trends";
import { getDashboardData } from "@/lib/r2";

export default async function Home() {
  const data = await getDashboardData();

  return (
    <main className="mx-auto flex w-full max-w-5xl flex-1 flex-col gap-10 px-4 py-10 sm:px-6">
      <header>
        <h1 className="text-2xl font-semibold" style={{ color: "var(--text-primary)" }}>
          Whoop Analytics
        </h1>
        <p className="mt-1 text-sm" style={{ color: "var(--text-secondary)" }}>
          A personal data-engineering pipeline — daily ingestion, data-quality checks, and derived
          metrics Whoop&apos;s own app doesn&apos;t surface. Not an official Whoop product.
        </p>
      </header>

      <Suspense fallback={<PipelineHealthSkeleton />}>
        <PipelineHealth data={data} />
      </Suspense>
      <Today data={data} />
      <Trends data={data} />
      <DerivedInsights data={data} />

      <footer className="mt-4 border-t pt-6 text-sm" style={{ borderColor: "var(--border)", color: "var(--text-muted)" }}>
        <a href="https://github.com/bkdonnel/whoop_analytics" className="underline">
          Source + pipeline writeup on GitHub
        </a>
      </footer>
    </main>
  );
}
