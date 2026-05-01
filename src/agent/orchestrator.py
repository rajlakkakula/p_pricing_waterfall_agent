"""Multi-step analysis coordination for the pricing waterfall agent.

Routes a ParsedIntent through the analytics pipeline:
  waterfall  → compute_waterfall
  outliers   → detect_outliers on filtered data
  trends     → compute_margin_bridge (requires base_year + current_year)
  narrative  → generate_narrative (requires waterfall)
  full_analysis → all of the above
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from src.agent.intent_parser import ParsedIntent
from src.analytics.narratives import generate_narrative
from src.analytics.outliers import OutlierFlag, detect_outliers
from src.analytics.trends import MarginBridge, compute_margin_bridge
from src.analytics.waterfall import WaterfallFilters, WaterfallResult, apply_filters, compute_waterfall


@dataclass
class AnalysisResult:
    """Aggregated output from a single agent analysis run."""

    intent: ParsedIntent
    waterfall: WaterfallResult | None = None
    outliers: list[OutlierFlag] | None = None
    bridge: MarginBridge | None = None
    narrative: str | None = None
    error: str | None = None


def _has_filters(filters: WaterfallFilters) -> bool:
    return any(
        v is not None
        for v in (
            filters.country, filters.year, filters.material,
            filters.pso, filters.corporate_group, filters.sold_to,
        )
    )


def run_analysis(df: pd.DataFrame, intent: ParsedIntent) -> AnalysisResult:
    """Coordinate the full analytics pipeline based on parsed intent.

    Args:
        df: Transaction-level DataFrame with all 15 source fields.
        intent: Structured intent from parse_intent().

    Returns:
        AnalysisResult populated according to the requested action.
        Sets result.error (and early-returns) if no data matches the filters.
    """
    result = AnalysisResult(intent=intent)
    action = intent.action

    needs_waterfall = action in ("waterfall", "full_analysis", "narrative")
    needs_outliers = action in ("outliers", "full_analysis", "narrative")
    needs_trends = (
        action in ("trends", "full_analysis")
        and intent.base_year is not None
        and intent.current_year is not None
    )
    needs_narrative = action in ("narrative", "full_analysis")

    # Waterfall — always the first step when needed
    if needs_waterfall:
        wf = compute_waterfall(df, intent.filters)
        if wf is None:
            result.error = "No transactions matched the specified filters."
            return result
        result.waterfall = wf

    # Outliers — run on filtered slice to keep peer groups contextually relevant
    if needs_outliers:
        scope = apply_filters(df, intent.filters) if _has_filters(intent.filters) else df
        result.outliers = detect_outliers(scope)

    # Margin bridge — only when both years are available
    if needs_trends:
        result.bridge = compute_margin_bridge(df, intent.base_year, intent.current_year)

    # Narrative — requires waterfall; outliers and bridge are optional enrichments
    if needs_narrative and result.waterfall is not None:
        result.narrative = generate_narrative(
            result.waterfall,
            outliers=result.outliers,
            bridge=result.bridge,
        )

    return result
