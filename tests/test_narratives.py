"""Unit tests for AI narrative generation — mocks the Anthropic client."""

from unittest.mock import MagicMock, patch

import pytest

from src.analytics.narratives import _MODEL, _SYSTEM_PROMPT, _build_payload, generate_narrative
from src.analytics.outliers import OutlierFlag
from src.analytics.trends import MarginBridge, PeriodMetrics
from src.analytics.waterfall import WaterfallResult


# ── Shared fixtures ────────────────────────────────────────────────────────────

@pytest.fixture
def waterfall() -> WaterfallResult:
    return WaterfallResult(
        blue_price=100.0,
        deductions=10.0,
        invoice_price=90.0,
        bonuses=5.0,
        pocket_price=85.0,
        standard_cost=50.0,
        material_cost=30.0,
        contribution_margin=35.0,
        margin_pct=41.18,
        deduction_pct=10.0,
        bonus_pct=5.56,
        realization_pct=85.0,
        leakage_pct=15.0,
        conversion_cost=20.0,
        total_qty=1000,
        transaction_count=50,
        total_pocket_revenue=85000.0,
        total_margin_dollars=35000.0,
    )


@pytest.fixture
def outlier_flag() -> OutlierFlag:
    return OutlierFlag(
        row_idx=0,
        sold_to="C-BAD",
        corporate_group="Leaky Corp",
        country="USA",
        pso="Americas",
        material="HYD-001",
        year=2025,
        sales_qty=100.0,
        volume_band="MEDIUM",
        peer_group="HYD-001|USA|MEDIUM",
        metric="deduction_pct",
        value=45.0,
        peer_mean=10.0,
        peer_std=2.0,
        z_score=17.5,
        severity="HIGH",
        direction="high_is_bad",
    )


def _make_period(year: int) -> PeriodMetrics:
    return PeriodMetrics(
        year=year,
        wavg_blue_price=100.0,
        wavg_deductions=10.0,
        wavg_invoice_price=90.0,
        wavg_bonuses=5.0,
        wavg_pocket_price=85.0,
        wavg_standard_cost=50.0,
        wavg_margin_pct=41.18,
        total_qty=1000.0,
        total_pocket_revenue=85000.0,
        total_margin_dollars=35000.0,
        transaction_count=50,
    )


@pytest.fixture
def bridge() -> MarginBridge:
    return MarginBridge(
        base_year=2024,
        current_year=2025,
        base=_make_period(2024),
        current=_make_period(2025),
        price_effect=1000.0,
        deduction_effect=0.0,
        bonus_effect=0.0,
        cost_effect=0.0,
        volume_effect=0.0,
        mix_effect=0.0,
        total_margin_change=1000.0,
    )


def _mock_client(text: str = "Narrative text.") -> MagicMock:
    """Build a minimal mock Anthropic client that returns a text response."""
    block = MagicMock()
    block.type = "text"
    block.text = text

    response = MagicMock()
    response.content = [block]

    client = MagicMock()
    client.messages.create.return_value = response
    return client


# ── _build_payload ─────────────────────────────────────────────────────────────

def test_build_payload_waterfall_only(waterfall: WaterfallResult) -> None:
    import json
    payload = json.loads(_build_payload(waterfall, outliers=None, bridge=None))
    assert "waterfall" in payload
    assert payload["waterfall"]["blue_price"] == 100.0
    assert "top_outliers" not in payload
    assert "yoy_bridge" not in payload


def test_build_payload_includes_outliers(waterfall: WaterfallResult, outlier_flag: OutlierFlag) -> None:
    import json
    payload = json.loads(_build_payload(waterfall, outliers=[outlier_flag], bridge=None))
    assert "top_outliers" in payload
    assert payload["top_outliers"][0]["sold_to"] == "C-BAD"


def test_build_payload_includes_bridge(waterfall: WaterfallResult, bridge: MarginBridge) -> None:
    import json
    payload = json.loads(_build_payload(waterfall, outliers=None, bridge=bridge))
    assert "yoy_bridge" in payload
    assert payload["yoy_bridge"]["base_year"] == 2024
    assert payload["yoy_bridge"]["price_effect"] == 1000.0


def test_build_payload_truncates_to_top5(waterfall: WaterfallResult) -> None:
    import json
    flags = [
        OutlierFlag(
            row_idx=i, sold_to=f"C-{i}", corporate_group="Corp", country="USA",
            pso="Americas", material="HYD-001", year=2025, sales_qty=100.0,
            volume_band="MEDIUM", peer_group="HYD-001|USA|MEDIUM",
            metric="deduction_pct", value=float(10 + i), peer_mean=10.0, peer_std=1.0,
            z_score=float(i), severity="HIGH", direction="high_is_bad",
        )
        for i in range(8)
    ]
    payload = json.loads(_build_payload(waterfall, outliers=flags, bridge=None))
    assert len(payload["top_outliers"]) == 5


def test_build_payload_top5_sorted_by_abs_z(waterfall: WaterfallResult) -> None:
    """Top-5 outliers should be the five with the highest |z_score|."""
    import json
    flags = [
        OutlierFlag(
            row_idx=i, sold_to=f"C-{i}", corporate_group="Corp", country="USA",
            pso="Americas", material="HYD-001", year=2025, sales_qty=100.0,
            volume_band="MEDIUM", peer_group="HYD-001|USA|MEDIUM",
            metric="deduction_pct", value=float(i), peer_mean=5.0, peer_std=1.0,
            z_score=float(i), severity="HIGH", direction="high_is_bad",
        )
        for i in range(8)
    ]
    payload = json.loads(_build_payload(waterfall, outliers=flags, bridge=None))
    z_scores = [o["z_score"] for o in payload["top_outliers"]]
    assert z_scores == sorted(z_scores, reverse=True)


# ── generate_narrative ─────────────────────────────────────────────────────────

@patch("src.analytics.narratives.anthropic.Anthropic")
@patch("src.analytics.narratives.NarrativeSettings")
def test_generate_narrative_returns_string(
    mock_settings_cls, mock_anthropic_cls, waterfall: WaterfallResult
) -> None:
    mock_settings_cls.return_value.anthropic_api_key = "test-key"
    mock_anthropic_cls.return_value = _mock_client("This is the narrative.")

    result = generate_narrative(waterfall)
    assert isinstance(result, str)
    assert result == "This is the narrative."


@patch("src.analytics.narratives.anthropic.Anthropic")
@patch("src.analytics.narratives.NarrativeSettings")
def test_generate_narrative_uses_correct_model(
    mock_settings_cls, mock_anthropic_cls, waterfall: WaterfallResult
) -> None:
    mock_settings_cls.return_value.anthropic_api_key = "test-key"
    client = _mock_client()
    mock_anthropic_cls.return_value = client

    generate_narrative(waterfall)

    call_kwargs = client.messages.create.call_args.kwargs
    assert call_kwargs["model"] == _MODEL


@patch("src.analytics.narratives.anthropic.Anthropic")
@patch("src.analytics.narratives.NarrativeSettings")
def test_generate_narrative_system_prompt_has_cache_control(
    mock_settings_cls, mock_anthropic_cls, waterfall: WaterfallResult
) -> None:
    mock_settings_cls.return_value.anthropic_api_key = "test-key"
    client = _mock_client()
    mock_anthropic_cls.return_value = client

    generate_narrative(waterfall)

    call_kwargs = client.messages.create.call_args.kwargs
    system_block = call_kwargs["system"][0]
    assert system_block["type"] == "text"
    assert system_block["text"] == _SYSTEM_PROMPT
    assert system_block["cache_control"] == {"type": "ephemeral"}


@patch("src.analytics.narratives.anthropic.Anthropic")
@patch("src.analytics.narratives.NarrativeSettings")
def test_generate_narrative_with_outliers_and_bridge(
    mock_settings_cls, mock_anthropic_cls,
    waterfall: WaterfallResult, outlier_flag: OutlierFlag, bridge: MarginBridge,
) -> None:
    mock_settings_cls.return_value.anthropic_api_key = "test-key"
    client = _mock_client("Full narrative.")
    mock_anthropic_cls.return_value = client

    result = generate_narrative(waterfall, outliers=[outlier_flag], bridge=bridge)
    assert result == "Full narrative."

    user_message = client.messages.create.call_args.kwargs["messages"][0]["content"]
    assert "top_outliers" in user_message
    assert "yoy_bridge" in user_message


@patch("src.analytics.narratives.anthropic.Anthropic")
@patch("src.analytics.narratives.NarrativeSettings")
def test_generate_narrative_outliers_none_no_key_in_payload(
    mock_settings_cls, mock_anthropic_cls, waterfall: WaterfallResult
) -> None:
    mock_settings_cls.return_value.anthropic_api_key = "test-key"
    client = _mock_client()
    mock_anthropic_cls.return_value = client

    generate_narrative(waterfall, outliers=None, bridge=None)

    user_message = client.messages.create.call_args.kwargs["messages"][0]["content"]
    assert "top_outliers" not in user_message
    assert "yoy_bridge" not in user_message


@patch("src.analytics.narratives.anthropic.Anthropic")
@patch("src.analytics.narratives.NarrativeSettings")
def test_generate_narrative_api_key_passed_to_client(
    mock_settings_cls, mock_anthropic_cls, waterfall: WaterfallResult
) -> None:
    mock_settings_cls.return_value.anthropic_api_key = "sk-ant-test-123"
    mock_anthropic_cls.return_value = _mock_client()

    generate_narrative(waterfall)

    mock_anthropic_cls.assert_called_once_with(api_key="sk-ant-test-123")


@patch("src.analytics.narratives.anthropic.Anthropic")
@patch("src.analytics.narratives.NarrativeSettings")
def test_generate_narrative_message_role_is_user(
    mock_settings_cls, mock_anthropic_cls, waterfall: WaterfallResult
) -> None:
    mock_settings_cls.return_value.anthropic_api_key = "test-key"
    client = _mock_client()
    mock_anthropic_cls.return_value = client

    generate_narrative(waterfall)

    messages = client.messages.create.call_args.kwargs["messages"]
    assert len(messages) == 1
    assert messages[0]["role"] == "user"
