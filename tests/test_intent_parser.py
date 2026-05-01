"""Unit tests for NL intent parsing — mocks the Anthropic client."""

from unittest.mock import MagicMock, patch

import pytest

from src.agent.intent_parser import ACTIONS, ParsedIntent, _INTENT_TOOL, parse_intent
from src.analytics.waterfall import WaterfallFilters


# ── Helpers ────────────────────────────────────────────────────────────────────

def _mock_tool_response(args: dict) -> MagicMock:
    """Build a mock Anthropic response that returns a single tool_use block."""
    tool_block = MagicMock()
    tool_block.type = "tool_use"
    tool_block.input = args

    response = MagicMock()
    response.content = [tool_block]
    return response


def _call_parse_intent(query: str, tool_args: dict) -> ParsedIntent:
    """Call parse_intent with a mocked Anthropic client returning tool_args."""
    with (
        patch("src.agent.intent_parser.IntentParserSettings") as mock_settings_cls,
        patch("src.agent.intent_parser.anthropic.Anthropic") as mock_anthropic_cls,
    ):
        mock_settings_cls.return_value.anthropic_api_key = "test-key"
        client = MagicMock()
        client.messages.create.return_value = _mock_tool_response(tool_args)
        mock_anthropic_cls.return_value = client
        return parse_intent(query)


# ── Tool schema ────────────────────────────────────────────────────────────────

def test_intent_tool_name() -> None:
    assert _INTENT_TOOL["name"] == "extract_intent"


def test_intent_tool_required_action() -> None:
    assert "action" in _INTENT_TOOL["input_schema"]["required"]


def test_intent_tool_action_enum_matches_actions() -> None:
    schema_enum = set(_INTENT_TOOL["input_schema"]["properties"]["action"]["enum"])
    assert schema_enum == set(ACTIONS)


# ── parse_intent — tool call mechanics ────────────────────────────────────────

@patch("src.agent.intent_parser.anthropic.Anthropic")
@patch("src.agent.intent_parser.IntentParserSettings")
def test_parse_intent_uses_forced_tool_choice(mock_settings_cls, mock_anthropic_cls) -> None:
    mock_settings_cls.return_value.anthropic_api_key = "test-key"
    client = MagicMock()
    client.messages.create.return_value = _mock_tool_response({"action": "waterfall"})
    mock_anthropic_cls.return_value = client

    parse_intent("Show me the margin")

    call_kwargs = client.messages.create.call_args.kwargs
    assert call_kwargs["tool_choice"] == {"type": "tool", "name": "extract_intent"}


@patch("src.agent.intent_parser.anthropic.Anthropic")
@patch("src.agent.intent_parser.IntentParserSettings")
def test_parse_intent_api_key_forwarded(mock_settings_cls, mock_anthropic_cls) -> None:
    mock_settings_cls.return_value.anthropic_api_key = "sk-ant-test"
    client = MagicMock()
    client.messages.create.return_value = _mock_tool_response({"action": "waterfall"})
    mock_anthropic_cls.return_value = client

    parse_intent("Show me the waterfall")

    mock_anthropic_cls.assert_called_once_with(api_key="sk-ant-test")


# ── parse_intent — ParsedIntent construction ──────────────────────────────────

def test_parse_intent_returns_parsed_intent_type() -> None:
    result = _call_parse_intent("Show me the margin", {"action": "waterfall"})
    assert isinstance(result, ParsedIntent)


def test_parse_intent_action_extracted() -> None:
    result = _call_parse_intent("Show outliers", {"action": "outliers"})
    assert result.action == "outliers"


def test_parse_intent_raw_query_preserved() -> None:
    query = "What is the margin for Germany?"
    result = _call_parse_intent(query, {"action": "waterfall", "country": "Germany"})
    assert result.raw_query == query


def test_parse_intent_filters_country() -> None:
    result = _call_parse_intent(
        "Show margin for Germany",
        {"action": "waterfall", "country": "Germany"},
    )
    assert result.filters.country == "Germany"
    assert result.filters.year is None


def test_parse_intent_filters_pso_and_year() -> None:
    result = _call_parse_intent(
        "EMEA waterfall 2025",
        {"action": "waterfall", "pso": "EMEA", "year": 2025},
    )
    assert result.filters.pso == "EMEA"
    assert result.filters.year == 2025


def test_parse_intent_trend_years_extracted() -> None:
    result = _call_parse_intent(
        "How did margin change from 2024 to 2025?",
        {"action": "trends", "base_year": 2024, "current_year": 2025},
    )
    assert result.action == "trends"
    assert result.base_year == 2024
    assert result.current_year == 2025


def test_parse_intent_no_filters_returns_none_fields() -> None:
    result = _call_parse_intent("Give me the full analysis", {"action": "full_analysis"})
    assert result.filters.country is None
    assert result.filters.year is None
    assert result.filters.material is None
    assert result.filters.pso is None
    assert result.base_year is None
    assert result.current_year is None


def test_parse_intent_filters_is_waterfall_filters_type() -> None:
    result = _call_parse_intent("Show me the waterfall", {"action": "waterfall"})
    assert isinstance(result.filters, WaterfallFilters)


def test_parse_intent_all_filter_fields() -> None:
    result = _call_parse_intent(
        "Customer C-001 in Germany, HYD-001, Americas, Corp A, 2025",
        {
            "action": "waterfall",
            "country": "Germany",
            "year": 2025,
            "material": "HYD-001",
            "pso": "Americas",
            "corporate_group": "Corp A",
            "sold_to": "C-001",
        },
    )
    f = result.filters
    assert f.country == "Germany"
    assert f.year == 2025
    assert f.material == "HYD-001"
    assert f.pso == "Americas"
    assert f.corporate_group == "Corp A"
    assert f.sold_to == "C-001"
