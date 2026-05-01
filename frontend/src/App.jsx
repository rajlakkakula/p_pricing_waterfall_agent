import { useCallback, useEffect, useState } from "react";
import { getHealth, getOutliers, getTrends, getWaterfall } from "./api";
import AgentChat      from "./components/AgentChat";
import CustomerTable  from "./components/CustomerTable";
import FilterBar      from "./components/FilterBar";
import WaterfallChart from "./components/WaterfallChart";

const EMPTY_FILTERS = { country: null, pso: null, year: null, material: null };

// ── Section wrapper ────────────────────────────────────────────────────────────

function Section({ title, badge, children }) {
  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-4 flex flex-col gap-3">
      <div className="flex items-center gap-2">
        <h2 className="font-semibold text-gray-800 text-sm">{title}</h2>
        {badge && (
          <span className="text-xs bg-orange-100 text-orange-700 px-2 py-0.5 rounded-full font-medium">
            {badge}
          </span>
        )}
      </div>
      {children}
    </div>
  );
}

// ── Trend bridge mini-panel ────────────────────────────────────────────────────

function TrendPanel({ bridge }) {
  if (!bridge) return null;
  const sign = (v) => (v >= 0 ? "+" : "");
  const fmt  = (v) => `${sign(v)}$${(Math.abs(v) / 1000).toFixed(0)}K`;
  const c    = (v) => (v >= 0 ? "text-emerald-600" : "text-red-500");

  const effects = [
    { label: "Price",  val: bridge.price_effect },
    { label: "Deduct", val: bridge.deduction_effect },
    { label: "Bonus",  val: bridge.bonus_effect },
    { label: "Cost",   val: bridge.cost_effect },
    { label: "Volume", val: bridge.volume_effect },
    { label: "Mix",    val: bridge.mix_effect },
  ];

  return (
    <div className="grid grid-cols-3 gap-2">
      <div className="col-span-3 flex items-baseline gap-3">
        <span className="text-xs text-gray-500">
          {bridge.base_year} → {bridge.current_year}
        </span>
        <span className="text-xs text-gray-400">
          {bridge.base.wavg_margin_pct.toFixed(1)}% → {bridge.current.wavg_margin_pct.toFixed(1)}%
        </span>
        <span className={`text-sm font-bold ${c(bridge.total_margin_change)}`}>
          {fmt(bridge.total_margin_change)} total
        </span>
      </div>
      {effects.map(({ label, val }) => (
        <div key={label} className="bg-gray-50 border border-gray-100 rounded-lg px-2 py-1.5">
          <p className={`text-sm font-bold ${c(val)}`}>{fmt(val)}</p>
          <p className="text-xs text-gray-400">{label}</p>
        </div>
      ))}
    </div>
  );
}

// ── Header ─────────────────────────────────────────────────────────────────────

function Header({ health }) {
  return (
    <header className="flex items-center justify-between px-6 py-3 bg-white border-b border-gray-200 shadow-sm">
      <div className="flex items-center gap-3">
        <div className="w-7 h-7 rounded-lg bg-blue-600 flex items-center justify-center">
          <div className="w-3 h-3 rounded-sm bg-white opacity-90" />
        </div>
        <div>
          <span className="font-bold text-gray-900 tracking-tight">Pricing Waterfall Agent</span>
          <span className="text-xs text-gray-400 ml-2 hidden sm:inline">Filtration Industry Analytics</span>
        </div>
      </div>
      {health && (
        <div className="flex items-center gap-2 text-xs text-gray-500 bg-gray-50 border border-gray-200 rounded-full px-3 py-1">
          <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 inline-block" />
          {health.row_count?.toLocaleString()} rows · {health.data_source}
        </div>
      )}
    </header>
  );
}

// ── Main App ───────────────────────────────────────────────────────────────────

export default function App() {
  const [health,    setHealth]    = useState(null);
  const [filters,   setFilters]   = useState(EMPTY_FILTERS);
  const [waterfall, setWaterfall] = useState(null);
  const [outliers,  setOutliers]  = useState(null);
  const [bridge,    setBridge]    = useState(null);
  const [loading,   setLoading]   = useState(false);
  const [error,     setError]     = useState(null);

  useEffect(() => {
    getHealth().then(setHealth).catch(() => {});
  }, []);

  useEffect(() => { fetchDashboard(EMPTY_FILTERS); }, []);

  const fetchDashboard = useCallback(async (f) => {
    setLoading(true);
    setError(null);
    try {
      const [wfRes, outRes] = await Promise.all([
        getWaterfall(f),
        getOutliers(f),
      ]);

      if (wfRes.status === "error") {
        setError(wfRes.error);
        setWaterfall(null);
      } else {
        setWaterfall(wfRes.waterfall);
      }

      setOutliers(outRes.outliers ?? []);

      const trendRes = await getTrends(2024, 2025, {
        country: f.country, material: f.material, pso: f.pso,
      });
      setBridge(trendRes.bridge ?? null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  const handleApply = () => fetchDashboard(filters);

  const handleDashboardUpdate = useCallback(({ waterfall: wf, outliers: out }) => {
    if (wf)  setWaterfall(wf);
    if (out) setOutliers(out);
  }, []);

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900 flex flex-col">
      <Header health={health} />

      <main className="flex flex-1 overflow-hidden">
        {/* ── Left panel: Dashboard ── */}
        <div className="flex flex-col gap-4 flex-[6] p-4 overflow-y-auto">

          <FilterBar
            filters={filters}
            onChange={setFilters}
            onApply={handleApply}
            loading={loading}
          />

          {error && (
            <div className="bg-red-50 border border-red-200 rounded-xl px-4 py-3 text-sm text-red-700">
              {error}
            </div>
          )}

          <Section
            title="Price Waterfall"
            badge={waterfall ? `${waterfall.transaction_count.toLocaleString()} txns` : undefined}
          >
            <WaterfallChart waterfall={waterfall} />
          </Section>

          {bridge && (
            <Section title="Year-over-Year Margin Bridge">
              <TrendPanel bridge={bridge} />
            </Section>
          )}

          <Section
            title="Outlier Customers"
            badge={outliers ? `${outliers.length} flagged` : undefined}
          >
            <CustomerTable outliers={outliers} />
          </Section>
        </div>

        {/* ── Right panel: Chat ── */}
        <div className="flex flex-col flex-[4] border-l border-gray-200 overflow-hidden bg-white">
          <AgentChat onDashboardUpdate={handleDashboardUpdate} />
        </div>
      </main>
    </div>
  );
}
