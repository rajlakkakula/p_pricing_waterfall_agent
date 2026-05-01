"""Natural language query interpretation via Claude tool use.

Parses a free-form user query into a structured ParsedIntent containing
the action to perform and any dimension filters to apply.
Uses forced tool use so the response is always machine-parseable.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass

import anthropic
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.analytics.waterfall import WaterfallFilters


class IntentParserSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    anthropic_api_key: str


_MODEL = "claude-sonnet-4-6"

ACTIONS = ("waterfall", "outliers", "trends", "full_analysis", "narrative")

_INTENT_TOOL: dict = {
    "name": "extract_intent",
    "description": (
        "Extract the analytical intent and dimension filters from a pricing waterfall query. "
        "Choose 'full_analysis' for general questions. "
        "Choose 'trends' when the user asks about year-over-year change, improvement, or bridge effects. "
        "Choose 'outliers' when asking about anomalies, unusual customers, or flagged transactions. "
        "Choose 'waterfall' when asking for margin, price, or waterfall metrics. "
        "Choose 'narrative' when asking for a written summary or narrative insight."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": list(ACTIONS),
                "description": "The type of analysis to perform.",
            },
            "country": {
                "type": "string",
                "description": "Country name filter (e.g. 'USA', 'Germany', 'Brazil'). Omit if not mentioned.",
            },
            "year": {
                "type": "integer",
                "description": "Single year filter. Omit for trend queries or if year is unspecified.",
            },
            "material": {
                "type": "string",
                "description": "Material code (e.g. 'HYD-001', 'IND-AIR-001', 'PROC-001', 'DUST-001'). Omit if not specified.",
            },
            "pso": {
                "type": "string",
                "description": "PSO business unit ('Americas', 'EMEA', 'APAC'). Omit if not specified.",
            },
            "corporate_group": {
                "type": "string",
                "description": "Parent company or corporate group name. Omit if not specified.",
            },
            "sold_to": {
                "type": "string",
                "description": "Specific customer sold_to ID. Omit if not specified.",
            },
            "base_year": {
                "type": "integer",
                "description": "Prior-year for trend comparison. Required when action is 'trends'.",
            },
            "current_year": {
                "type": "integer",
                "description": "Current year for trend comparison. Required when action is 'trends'.",
            },
        },
        "required": ["action"],
    },
}


@dataclass
class ParsedIntent:
    """Structured output of NL query parsing."""

    action: str              # one of ACTIONS
    filters: WaterfallFilters
    base_year: int | None    # for trends comparison
    current_year: int | None
    raw_query: str


def _system_prompt() -> str:
    today = datetime.date.today()
    return (
        f"You are a query parser for a filtration-industry pricing intelligence agent. "
        f"Today is {today}. The transaction data covers fiscal years 2024 and 2025. "
        f"Material codes: HYD-001 (mobile hydraulic filters), IND-AIR-001 (industrial air filters), "
        f"PROC-001 (process filtration), DUST-001 (dust collection / engine filters). "
        f"Countries available: USA, Germany, Brazil, China, India. "
        f"PSOs: 'Americas', 'EMEA', 'APAC'. "
        f"Resolve relative time references (e.g. 'last year', 'this year') to specific years using today's date. "
        f"When in doubt about the action type, use 'full_analysis'."
    )


def parse_intent(query: str) -> ParsedIntent:
    """Parse a natural language pricing query into a structured intent.

    Args:
        query: Free-form user question, e.g. "What's the margin for Germany last year?"

    Returns:
        ParsedIntent with action, WaterfallFilters, optional year pair, and raw query.
    """
    settings = IntentParserSettings()
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    response = client.messages.create(
        model=_MODEL,
        max_tokens=512,
        system=_system_prompt(),
        tools=[_INTENT_TOOL],
        tool_choice={"type": "tool", "name": "extract_intent"},
        messages=[{"role": "user", "content": query}],
    )

    tool_block = next(b for b in response.content if b.type == "tool_use")
    args: dict = tool_block.input

    filters = WaterfallFilters(
        country=args.get("country"),
        year=args.get("year"),
        material=args.get("material"),
        pso=args.get("pso"),
        corporate_group=args.get("corporate_group"),
        sold_to=args.get("sold_to"),
    )

    return ParsedIntent(
        action=args["action"],
        filters=filters,
        base_year=args.get("base_year"),
        current_year=args.get("current_year"),
        raw_query=query,
    )
