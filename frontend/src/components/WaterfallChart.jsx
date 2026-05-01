import {
  BarChart, Bar, Cell, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, LabelList,
} from "recharts";

// Blue for positive steps, Orange for deductions/bonuses, Gray for cost, Emerald for margin
const STEP_CONFIG = [
  { key: "Blue Price", color: "#1D4ED8" },  // blue-700
  { key: "Deductions", color: "#F97316" },  // orange-500
  { key: "Invoice",    color: "#3B82F6" },  // blue-500
  { key: "Bonuses",    color: "#FB923C" },  // orange-400
  { key: "Pocket",     color: "#0EA5E9" },  // sky-500
  { key: "Std Cost",   color: "#9CA3AF" },  // gray-400
  { key: "Margin",     color: "#10B981" },  // emerald-500
];

function buildChartData(wf) {
  const { blue_price: b, deductions: d, invoice_price: inv,
          bonuses: bon, pocket_price: p, standard_cost: c,
          contribution_margin: m } = wf;

  return [
    { name: "Blue Price", base: 0,   val: b,   pct: null },
    { name: "Deductions", base: inv, val: d,   pct: `${wf.deduction_pct.toFixed(1)}%` },
    { name: "Invoice",    base: 0,   val: inv, pct: null },
    { name: "Bonuses",    base: p,   val: bon, pct: `${wf.bonus_pct.toFixed(1)}%` },
    { name: "Pocket",     base: 0,   val: p,   pct: null },
    { name: "Std Cost",   base: m,   val: c,   pct: null },
    { name: "Margin",     base: 0,   val: m,   pct: `${wf.margin_pct.toFixed(1)}%` },
  ];
}

const colorMap = Object.fromEntries(STEP_CONFIG.map((s) => [s.key, s.color]));

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  const val = payload.find((p) => p.dataKey === "val")?.value ?? 0;
  const color = colorMap[label] ?? "#374151";
  return (
    <div className="bg-white border border-gray-200 rounded-lg px-3 py-2 text-sm shadow-lg">
      <p className="font-semibold text-gray-800">{label}</p>
      <p className="font-bold" style={{ color }}>${val.toFixed(2)}</p>
    </div>
  );
}

function MetricPill({ label, value, color = "text-gray-700", bg = "bg-gray-50" }) {
  return (
    <div className={`flex flex-col items-center ${bg} border border-gray-200 rounded-lg px-3 py-2 min-w-[88px]`}>
      <span className={`text-sm font-bold ${color}`}>{value}</span>
      <span className="text-xs text-gray-400 mt-0.5">{label}</span>
    </div>
  );
}

export default function WaterfallChart({ waterfall }) {
  if (!waterfall) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400 text-sm">
        No waterfall data — apply filters or ask a question in chat.
      </div>
    );
  }

  const data = buildChartData(waterfall);

  const marginColor = waterfall.margin_pct >= 25
    ? "text-emerald-600"
    : waterfall.margin_pct >= 15
    ? "text-amber-600"
    : "text-red-600";

  return (
    <div className="flex flex-col gap-4">
      {/* Metric pills */}
      <div className="flex flex-wrap gap-2">
        <MetricPill
          label="Margin %"
          value={`${waterfall.margin_pct.toFixed(1)}%`}
          color={marginColor}
          bg={waterfall.margin_pct >= 25 ? "bg-emerald-50" : waterfall.margin_pct >= 15 ? "bg-amber-50" : "bg-red-50"}
        />
        <MetricPill label="Leakage %"    value={`${waterfall.leakage_pct.toFixed(1)}%`}   color="text-orange-600" bg="bg-orange-50" />
        <MetricPill label="Realization"  value={`${waterfall.realization_pct.toFixed(1)}%`} color="text-blue-600"  bg="bg-blue-50" />
        <MetricPill label="Pocket Price" value={`$${waterfall.pocket_price.toFixed(2)}`}   color="text-sky-700"   bg="bg-sky-50" />
        <MetricPill label="Margin $"     value={`$${(waterfall.total_margin_dollars / 1_000_000).toFixed(2)}M`} color="text-emerald-700" bg="bg-emerald-50" />
        <MetricPill label="Transactions" value={waterfall.transaction_count.toLocaleString()} color="text-gray-700" />
      </div>

      {/* Chart */}
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={data} margin={{ top: 24, right: 16, left: 10, bottom: 5 }} barCategoryGap="20%">
          <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" vertical={false} />
          <XAxis
            dataKey="name"
            tick={{ fill: "#6B7280", fontSize: 12 }}
            axisLine={false}
            tickLine={false}
          />
          <YAxis
            tick={{ fill: "#9CA3AF", fontSize: 11 }}
            axisLine={false}
            tickLine={false}
            tickFormatter={(v) => `$${v.toFixed(0)}`}
          />
          <Tooltip content={<CustomTooltip />} cursor={{ fill: "rgba(59,130,246,0.05)" }} />

          {/* Invisible base bar — positions the visible bar */}
          <Bar dataKey="base" stackId="w" fill="transparent" isAnimationActive={false} />

          {/* Visible coloured bar */}
          <Bar dataKey="val" stackId="w" radius={[4, 4, 0, 0]} isAnimationActive={true}>
            {data.map((entry) => (
              <Cell key={entry.name} fill={colorMap[entry.name]} />
            ))}
            <LabelList
              dataKey="val"
              position="top"
              formatter={(v) => `$${v.toFixed(1)}`}
              style={{ fill: "#374151", fontSize: 11, fontWeight: 600 }}
            />
          </Bar>
        </BarChart>
      </ResponsiveContainer>

      {/* Step legend */}
      <div className="flex flex-wrap gap-3 justify-center">
        {STEP_CONFIG.map(({ key, color }) => (
          <div key={key} className="flex items-center gap-1.5">
            <span className="w-2.5 h-2.5 rounded-sm inline-block flex-shrink-0" style={{ backgroundColor: color }} />
            <span className="text-xs text-gray-500">{key}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
