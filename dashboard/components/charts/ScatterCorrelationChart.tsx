"use client";

import {
  CartesianGrid,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type ScatterCorrelationChartProps = {
  data: { x: number; y: number }[];
  xLabel: string;
  yLabel: string;
  color: string;
  height?: number;
};

export function ScatterCorrelationChart({
  data,
  xLabel,
  yLabel,
  color,
  height = 220,
}: ScatterCorrelationChartProps) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <ScatterChart margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
        <CartesianGrid stroke="var(--gridline)" strokeDasharray="0" />
        <XAxis
          type="number"
          dataKey="x"
          name={xLabel}
          stroke="var(--baseline)"
          tick={{ fill: "var(--text-muted)", fontSize: 12 }}
          tickLine={false}
          axisLine={{ stroke: "var(--baseline)" }}
          label={{ value: xLabel, position: "insideBottom", offset: -2, fill: "var(--text-secondary)", fontSize: 12 }}
        />
        <YAxis
          type="number"
          dataKey="y"
          name={yLabel}
          stroke="var(--baseline)"
          tick={{ fill: "var(--text-muted)", fontSize: 12 }}
          tickLine={false}
          axisLine={false}
          width={40}
          label={{ value: yLabel, angle: -90, position: "insideLeft", fill: "var(--text-secondary)", fontSize: 12 }}
        />
        <Tooltip
          cursor={{ stroke: "var(--baseline)" }}
          contentStyle={{
            background: "var(--surface-1)",
            border: "1px solid var(--border)",
            borderRadius: 8,
            color: "var(--text-primary)",
            fontSize: 13,
          }}
        />
        <Scatter data={data} fill={color} fillOpacity={0.55} isAnimationActive={false} />
      </ScatterChart>
    </ResponsiveContainer>
  );
}
