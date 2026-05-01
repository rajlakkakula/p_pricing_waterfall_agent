const COUNTRIES  = ["USA", "Germany", "Brazil", "China", "India"];
const PSOS       = ["Americas", "EMEA", "APAC"];
const YEARS      = [2024, 2025];
const MATERIALS  = ["HYD-001", "IND-AIR-001", "PROC-001", "DUST-001"];

function Select({ label, value, onChange, options, placeholder = "All" }) {
  return (
    <div className="flex flex-col gap-1">
      <label className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
        {label}
      </label>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value || null)}
        className="bg-white border border-gray-300 text-gray-800 text-sm rounded-lg
                   px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-blue-500
                   focus:border-blue-500 transition-colors"
      >
        <option value="">{placeholder}</option>
        {options.map((o) => (
          <option key={o} value={o}>{o}</option>
        ))}
      </select>
    </div>
  );
}

export default function FilterBar({ filters, onChange, onApply, loading }) {
  const set = (key) => (val) => onChange({ ...filters, [key]: val });

  return (
    <div className="flex flex-wrap items-end gap-4 p-4 bg-white rounded-xl border border-gray-200 shadow-sm">
      <Select label="Country"  value={filters.country  ?? ""} onChange={set("country")}  options={COUNTRIES} />
      <Select label="PSO"      value={filters.pso      ?? ""} onChange={set("pso")}      options={PSOS} />
      <Select label="Year"     value={filters.year     ?? ""} onChange={set("year")}      options={YEARS} />
      <Select label="Material" value={filters.material ?? ""} onChange={set("material")} options={MATERIALS} />

      <button
        onClick={onApply}
        disabled={loading}
        className="px-5 py-1.5 bg-blue-600 hover:bg-blue-700 active:bg-blue-800
                   disabled:opacity-50 text-white text-sm font-semibold rounded-lg
                   transition-colors shadow-sm"
      >
        {loading ? "Loading…" : "Apply"}
      </button>

      <button
        onClick={() => onChange({ country: null, pso: null, year: null, material: null })}
        disabled={loading}
        className="px-5 py-1.5 bg-gray-100 hover:bg-gray-200 active:bg-gray-300
                   disabled:opacity-50 text-gray-700 text-sm font-semibold rounded-lg
                   transition-colors border border-gray-200"
      >
        Reset
      </button>
    </div>
  );
}
