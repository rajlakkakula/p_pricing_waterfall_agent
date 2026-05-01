"""Structured response formatting for the waterfall agent API layer.

Converts AnalysisResult dataclasses into JSON-serializable dicts
ready for FastAPI response models.
"""

from __future__ import annotations

import dataclasses
from typing import Any

from src.agent.orchestrator import AnalysisResult


def format_response(result: AnalysisResult) -> dict[str, Any]:
    """Convert an AnalysisResult into a JSON-serializable response dict.

    Args:
        result: Populated AnalysisResult from run_analysis().

    Returns:
        Dict with a 'status' key ('ok' or 'error') plus all result fields.
        Nested dataclasses are recursively converted to plain dicts.
    """
    status = "error" if result.error else "ok"
    payload = dataclasses.asdict(result)
    payload["status"] = status
    return payload


def format_error(message: str) -> dict[str, Any]:
    """Build a minimal error response dict.

    Args:
        message: Human-readable error description.

    Returns:
        Dict with status='error' and the error message.
    """
    return {"status": "error", "error": message}
