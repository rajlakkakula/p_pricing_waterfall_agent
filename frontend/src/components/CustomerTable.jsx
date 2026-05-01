import { useState } from "react";

const METRIC_LABELS = {
  margin_pct:      "Margin %",
  deduction_pct:   "Deduction %",
  bonus_pct:       "Bonus %",
  realization_pct: "Realization %",
};

function Badge({ severity }) {
  const cls =
    severity === "HIGH"
      ? "bg-red-100 text-red-700 border border-red-200"
      : "bg-orange-100 text-orange-700 border border-orange-200";
  return (
    <span className={`inline-block px-2 py-0.5 rounded text-xs font-semibold ${cls}`}>
      {severity}
    </span>
  );
}

function ZScore({ z }) {
  const abs = Math.abs(z);
  const color = abs >= 3 ? "text-red-600" : abs >= 2 ? "text-orange-500" : "text-gray-600";
  return (
    <span className={`font-mono font-semibold ${color}`}>
      {z > 0 ? "+" : ""}{z.toFixed(2)}
    </span>
  );
}

export default function CustomerTable({ outliers }) {
  const [showAll, setShowAll] = useState(false);

  if (!outliers) {
    return (
      <div className="flex items-center justify-center h-24 text-gray-400 text-sm">
        No outlier data loaded yet.
      </div>
    );
  }

  if (outliers.length === 0) {
    return (
      <div className="flex items-center justify-center h-24 text-emerald-600 text-sm font-medium">
        No outliers detected in this segment.
      </div>
    );
  }

  const rows = showAll ? outliers : outliers.slice(0, 20);

  return (
    <div className="flex flex-col gap-2">
      <div className="overflow-x-auto rounded-xl border border-gray-200">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-200 text-left">
              {["Severity", "Customer", "Metric", "Value", "Peer Mean", "Z-Score", "Group"].map((h) => (
                <th
                  key={h}
                  className="px-3 py-2.5 text-xs font-semibold text-gray-500 uppercase tracking-wide whitespace-nowrap"
                >
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((f, i) => {
              const rowBg =
                f.severity === "HIGH"
                  ? "bg-red-50 hover:bg-red-100"
                  : "bg-orange-50 hover:bg-orange-100";
              return (
                <tr key={i} className={`border-b border-gray-100 transition-colors ${rowBg}`}>
                  <td className="px-3 py-2"><Badge severity={f.severity} /></td>
                  <td className="px-3 py-2 font-mono text-xs text-gray-800 whitespace-nowrap">
                    {f.sold_to}
                    <span className="text-gray-400 ml-1">({f.country})</span>
                  </td>
                  <td className="px-3 py-2 text-gray-600 whitespace-nowrap">
                    {METRIC_LABELS[f.metric] ?? f.metric}
                  </td>
                  <td className="px-3 py-2 text-gray-900 font-semibold">{f.value.toFixed(1)}%</td>
                  <td className="px-3 py-2 text-gray-500">{f.peer_mean.toFixed(1)}%</td>
                  <td className="px-3 py-2"><ZScore z={f.z_score} /></td>
                  <td className="px-3 py-2 text-xs text-gray-400 font-mono">{f.volume_band}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {outliers.length > 20 && (
        <button
          onClick={() => setShowAll((s) => !s)}
          className="text-xs text-blue-600 hover:text-blue-700 font-medium self-start transition-colors"
        >
          {showAll ? "Show fewer" : `Show all ${outliers.length} outliers`}
        </button>
      )}
    </div>
  );
}
