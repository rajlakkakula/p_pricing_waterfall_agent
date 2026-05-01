/**
 * Fetch helpers for the Pricing Waterfall Agent API.
 * In dev, Vite proxies /api/* to http://localhost:8000.
 * In production (GitHub Pages), set VITE_API_BASE_URL to the hosted backend URL.
 */

const BASE = import.meta.env.VITE_API_BASE_URL || "/api";

async function _get(path, params = {}) {
  const qs = new URLSearchParams(
    Object.fromEntries(Object.entries(params).filter(([, v]) => v != null))
  ).toString();
  const url = qs ? `${BASE}${path}?${qs}` : `${BASE}${path}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

async function _post(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

/** @returns {Promise<{status, data_source, row_count}>} */
export const getHealth = () => _get("/health");

/**
 * @param {{country?, year?, material?, pso?, corporate_group?, sold_to?}} filters
 * @returns {Promise<AnalysisResponse>}
 */
export const getWaterfall = (filters = {}) => _get("/waterfall", filters);

/** @returns {Promise<AnalysisResponse>} */
export const getOutliers = (filters = {}) => _get("/outliers", filters);

/**
 * @param {number} baseYear
 * @param {number} currentYear
 * @param {{country?, material?, pso?}} filters
 * @returns {Promise<AnalysisResponse>}
 */
export const getTrends = (baseYear, currentYear, filters = {}) =>
  _get("/trends", { base_year: baseYear, current_year: currentYear, ...filters });

/**
 * @param {string} query  Natural language question
 * @returns {Promise<AnalysisResponse>}
 */
export const postChat = (query) => _post("/chat", { query });
