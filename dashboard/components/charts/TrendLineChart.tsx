"use client";

import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { formatShortDate } from "@/lib/format";

export type TrendSeries = {
  dataKey: string;
  label: string;
  color: string;
};

type TrendLineChartProps = {
  data: Record<string, unknown>[];
  xKey: string;
  series: TrendSeries[];
  yUnit?: string;
  yDecimals?: number;
  height?: number;
};

export function TrendLineChart({
  data,
  xKey,
  series,
  yUnit = "",
  yDecimals = 0,
  height = 220,
}: TrendLineChartProps) {
  const formatY = (value: number) => `${value.toFixed(yDecimals)}${yUnit}`;

  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
        <CartesianGrid stroke="var(--gridline)" strokeDasharray="0" vertical={false} />
        <XAxis
          dataKey={xKey}
          tickFormatter={formatShortDate}
          stroke="var(--baseline)"
          tick={{ fill: "var(--text-muted)", fontSize: 12 }}
          tickLine={false}
          axisLine={{ stroke: "var(--baseline)" }}
          minTickGap={24}
        />
        <YAxis
          stroke="var(--baseline)"
          tick={{ fill: "var(--text-muted)", fontSize: 12 }}
          tickLine={false}
          axisLine={false}
          width={40}
          tickFormatter={formatY}
        />
        <Tooltip
          labelFormatter={(value) => formatShortDate(String(value))}
          formatter={(value, name) => [typeof value === "number" ? formatY(value) : String(value), String(name)]}
          contentStyle={{
            background: "var(--surface-1)",
            border: "1px solid var(--border)",
            borderRadius: 8,
            color: "var(--text-primary)",
            fontSize: 13,
          }}
        />
        {series.length > 1 && (
          <Legend wrapperStyle={{ fontSize: 12, color: "var(--text-secondary)" }} iconType="plainline" />
        )}
        {series.map((s) => (
          <Line
            key={s.dataKey}
            type="monotone"
            dataKey={s.dataKey}
            name={s.label}
            stroke={s.color}
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 4, stroke: "var(--surface-1)", strokeWidth: 2 }}
            isAnimationActive={false}
            connectNulls
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}
