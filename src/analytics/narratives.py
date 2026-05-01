"""AI-powered narrative generation for waterfall insights via Claude API.

Calls claude-sonnet-4-6 with a cached system prompt (stable domain knowledge)
and a per-request user message containing the structured waterfall JSON.
Prompt caching on the system block saves ~90% of input token cost on repeated calls.
"""

import json

import anthropic
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.analytics.outliers import OutlierFlag
from src.analytics.trends import MarginBridge
from src.analytics.waterfall import WaterfallResult


class NarrativeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    anthropic_api_key: str


_MODEL = "claude-sonnet-4-6"

# Stable domain knowledge — marked for prompt caching.
# Any change here invalidates the cache, so keep this frozen.
_SYSTEM_PROMPT = """
You are a senior pricing analyst specializing in the filtration industry.
Your job is to interpret pricing waterfall data and produce concise, business-ready narrative insights.

## Pricing Waterfall — Domain Knowledge

The waterfall decomposes the full price journey for filtration products:

1. Blue/Jobber Price — The list or catalog price before any discounts. Starting point.
2. Deductions — Off-invoice discounts, allowances, freight charges subtracted from blue price.
3. Invoice Price — Blue Price minus Deductions. The gross price on the invoice.
4. Bonuses — Rebates, volume incentives, loyalty payments subtracted after invoicing.
5. Pocket Price — Invoice Price minus Bonuses. The actual cash received by the business.
6. Standard Cost — Fully loaded manufacturing cost per unit (materials + conversion).
7. Contribution Margin — Pocket Price minus Standard Cost. The true profitability metric.

## Key Metrics

- Margin %: Contribution Margin / Pocket Price × 100 — core profitability indicator
- Deduction %: Deductions / Blue Price × 100 — off-invoice leakage
- Bonus %: Bonuses / Invoice Price × 100 — post-invoice rebate leakage
- Realization %: Pocket Price / Blue Price × 100 — how much of list price is retained
- Leakage %: (Blue Price − Pocket Price) / Blue Price × 100 — total price erosion

## Margin Tiers (filtration industry benchmarks)

- Tier 1 (Premium): Margin > 35% — excellent
- Tier 2 (Healthy): Margin 25–35% — acceptable
- Tier 3 (Acceptable): Margin 15–25% — needs monitoring
- Tier 4 (Low): Margin 5–15% — action required
- Tier 5 (Destructive): Margin < 5% — urgent intervention

## Output Instructions

Write 3–5 paragraphs covering:
1. Overall margin health and tier positioning
2. Key leakage drivers (deductions and/or bonuses) if significant
3. Outlier customers or transactions worth investigating (if provided)
4. Year-over-year trend highlights and primary bridge effects (if provided)
5. One concrete recommendation or priority area

Be specific — cite actual numbers from the data. Use plain business English. No bullet lists, no headers.
""".strip()


def _build_payload(
    waterfall: WaterfallResult,
    outliers: list[OutlierFlag] | None,
    bridge: MarginBridge | None,
) -> str:
    """Serialize inputs to a compact JSON string for the Claude user message."""
    payload: dict = {
        "waterfall": {
            "blue_price": waterfall.blue_price,
            "deductions": waterfall.deductions,
            "invoice_price": waterfall.invoice_price,
            "bonuses": waterfall.bonuses,
            "pocket_price": waterfall.pocket_price,
            "standard_cost": waterfall.standard_cost,
            "contribution_margin": waterfall.contribution_margin,
            "margin_pct": waterfall.margin_pct,
            "deduction_pct": waterfall.deduction_pct,
            "bonus_pct": waterfall.bonus_pct,
            "realization_pct": waterfall.realization_pct,
            "leakage_pct": waterfall.leakage_pct,
            "total_qty": waterfall.total_qty,
            "transaction_count": waterfall.transaction_count,
            "total_pocket_revenue": waterfall.total_pocket_revenue,
            "total_margin_dollars": waterfall.total_margin_dollars,
        }
    }

    if outliers:
        top5 = sorted(outliers, key=lambda f: abs(f.z_score), reverse=True)[:5]
        payload["top_outliers"] = [
            {
                "sold_to": f.sold_to,
                "corporate_group": f.corporate_group,
                "metric": f.metric,
                "value": f.value,
                "peer_mean": f.peer_mean,
                "z_score": f.z_score,
                "severity": f.severity,
            }
            for f in top5
        ]

    if bridge:
        payload["yoy_bridge"] = {
            "base_year": bridge.base_year,
            "current_year": bridge.current_year,
            "base_margin_pct": bridge.base.wavg_margin_pct,
            "current_margin_pct": bridge.current.wavg_margin_pct,
            "total_margin_change": bridge.total_margin_change,
            "price_effect": bridge.price_effect,
            "deduction_effect": bridge.deduction_effect,
            "bonus_effect": bridge.bonus_effect,
            "cost_effect": bridge.cost_effect,
            "volume_effect": bridge.volume_effect,
            "mix_effect": bridge.mix_effect,
        }

    return json.dumps(payload, indent=2)


def generate_narrative(
    waterfall: WaterfallResult,
    outliers: list[OutlierFlag] | None = None,
    bridge: MarginBridge | None = None,
) -> str:
    """Generate a natural language pricing narrative via Claude.

    Args:
        waterfall: Volume-weighted waterfall metrics (required).
        outliers: Optional list of detected outlier flags — top 5 by |z| are sent.
        bridge: Optional YoY margin bridge decomposition.

    Returns:
        Narrative string (3–5 paragraphs) ready for display or email.
    """
    settings = NarrativeSettings()
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    user_content = (
        "Analyze the following pricing waterfall data and produce a narrative insight:\n\n"
        + _build_payload(waterfall, outliers, bridge)
    )

    response = client.messages.create(
        model=_MODEL,
        max_tokens=1024,
        system=[
            {
                "type": "text",
                "text": _SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": user_content}],
    )

    return next(block.text for block in response.content if block.type == "text")
