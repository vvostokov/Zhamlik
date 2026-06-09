"""Traceability — decompose artifact fields to their dependencies.

Pure functions, no domain entities. Each function takes an artifact
and a field name, returns a structured dependency tree.

Every number in the system can be decomposed:
    Field → depends_on → Field → depends_on → (recursive)

Trace contract:
    {
        "field": str,
        "value": str,
        "depends_on": [TraceNode]  (optional, depth-limited)
    }
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional

from core.attribution.report_engine import AttributionReport
from core.consistency.validator import ConsistencyReport
from core.statements.models import BalanceSheet, CashFlowStatement, IncomeStatement

MAX_TRACE_DEPTH = 3


# ── Cross-artifact field registry (15B — Artifact Trace) ──

_CROSS_ARTIFACT_FIELDS: Dict[str, List[Dict[str, str]]] = {
    "net_worth": [
        {"artifact": "balance_sheet", "field": "net_worth"},
        {"artifact": "attribution", "field": "net_worth_end"},
    ],
    "delta_net_worth": [
        {"artifact": "attribution", "field": "delta_net_worth"},
        {"artifact": "consistency", "field": "bs_vs_attribution"},
    ],
    "net_pnl": [
        {"artifact": "income_statement", "field": "net_pnl"},
        {"artifact": "cash_flow", "field": "total_operating"},
        {"artifact": "consistency", "field": "pnl_vs_operating_cf"},
    ],
    "total_assets": [
        {"artifact": "balance_sheet", "field": "total_assets"},
    ],
    "total_liabilities": [
        {"artifact": "balance_sheet", "field": "total_liabilities"},
    ],
    "total_equity": [
        {"artifact": "balance_sheet", "field": "total_equity"},
    ],
    "total_revenue": [
        {"artifact": "income_statement", "field": "total_revenue"},
    ],
    "total_expenses": [
        {"artifact": "income_statement", "field": "total_expenses"},
    ],
    "income": [
        {"artifact": "attribution", "field": "income"},
        {"artifact": "income_statement", "field": "total_revenue"},
    ],
    "expenses": [
        {"artifact": "attribution", "field": "expenses"},
        {"artifact": "income_statement", "field": "total_expenses"},
    ],
    "market_effects": [
        {"artifact": "attribution", "field": "market_effects"},
    ],
    "fees": [
        {"artifact": "attribution", "field": "fees"},
    ],
    "debt_costs": [
        {"artifact": "attribution", "field": "debt_costs"},
    ],
    "contributions": [
        {"artifact": "attribution", "field": "contributions"},
    ],
    "total_operating": [
        {"artifact": "cash_flow", "field": "total_operating"},
    ],
    "total_investing": [
        {"artifact": "cash_flow", "field": "total_investing"},
    ],
    "total_financing": [
        {"artifact": "cash_flow", "field": "total_financing"},
        {"artifact": "consistency", "field": "bs_vs_pnl_plus_financing"},
    ],
    "score": [
        {"artifact": "consistency", "field": "score"},
    ],
}


@dataclass
class TraceContext:
    """Transient context bundling all artifacts for a single period.

    NOT a domain entity — exists only for the duration of one API request.
    Used to resolve cross-artifact field references (15B — Artifact Trace).

    All fields are optional — an artifact may be unavailable for the period.
    """
    period_id: str
    prev_period_id: Optional[str] = None
    balance_sheet: Optional[BalanceSheet] = None
    income_statement: Optional[IncomeStatement] = None
    cash_flow: Optional[CashFlowStatement] = None
    attribution: Optional[AttributionReport] = None
    consistency: Optional[ConsistencyReport] = None
    observations: List[Dict] = field(default_factory=list)

    def artifact_trace(self, field: str) -> Optional[List[Dict]]:
        """Which artifacts carry this field? Returns None if unknown field."""
        registry = _CROSS_ARTIFACT_FIELDS.get(field)
        if registry is None:
            return None

        resolved = []
        for entry in registry:
            artifact_key = entry["artifact"]
            # Check if the source artifact is available
            source = getattr(self, artifact_key, None)
            if source is not None:
                resolved.append({
                    "artifact": artifact_key,
                    "field": entry["field"],
                })
        return resolved if resolved else None


def _node(
    field: str,
    value: str,
    depends_on: Optional[List[Dict]] = None,
) -> Dict[str, Any]:
    return {
        "field": field,
        "value": value,
        "depends_on": depends_on or [],
    }


def _entries_summary(entries) -> List[Dict]:
    return [
        {"field": f"{e.account_code} ({e.account_name})", "value": str(e.amount), "depends_on": []}
        for e in entries
    ]


def trace_balance_sheet(
    bs: BalanceSheet, field: str, depth: int = 0
) -> Optional[Dict[str, Any]]:
    if depth > MAX_TRACE_DEPTH:
        return None

    if field == "net_worth":
        return _node(
            field="net_worth",
            value=str(bs.total_assets - bs.total_liabilities),
            depends_on=[
                trace_balance_sheet(bs, "total_assets", depth + 1),
                trace_balance_sheet(bs, "total_liabilities", depth + 1),
            ],
        )

    if field == "total_assets":
        return _node(
            field="total_assets",
            value=str(bs.total_assets),
            depends_on=_entries_summary(bs.assets),
        )

    if field == "total_liabilities":
        return _node(
            field="total_liabilities",
            value=str(bs.total_liabilities),
            depends_on=_entries_summary(bs.liabilities),
        )

    if field == "total_equity":
        return _node(
            field="total_equity",
            value=str(bs.total_equity),
            depends_on=_entries_summary(bs.equity),
        )

    return None


# ── IncomeStatement ──


def trace_income_statement(
    is_: IncomeStatement, field: str, depth: int = 0
) -> Optional[Dict[str, Any]]:
    if depth > MAX_TRACE_DEPTH:
        return None

    if field == "net_pnl":
        return _node(
            field="net_pnl",
            value=str(is_.net_pnl),
            depends_on=[
                trace_income_statement(is_, "total_revenue", depth + 1),
                trace_income_statement(is_, "total_expenses", depth + 1),
            ],
        )

    if field == "total_revenue":
        return _node(
            field="total_revenue",
            value=str(is_.total_revenue),
            depends_on=_entries_summary(is_.revenue),
        )

    if field == "total_expenses":
        return _node(
            field="total_expenses",
            value=str(is_.total_expenses),
            depends_on=_entries_summary(is_.expenses),
        )

    return None


# ── CashFlowStatement ──


def trace_cash_flow(
    cf: CashFlowStatement, field: str, depth: int = 0
) -> Optional[Dict[str, Any]]:
    if depth > MAX_TRACE_DEPTH:
        return None

    if field == "net_cash_flow":
        return _node(
            field="net_cash_flow",
            value=str(cf.net_cash_flow),
            depends_on=[
                trace_cash_flow(cf, "total_operating", depth + 1),
                trace_cash_flow(cf, "total_investing", depth + 1),
                trace_cash_flow(cf, "total_financing", depth + 1),
            ],
        )

    if field == "total_operating":
        return _node(
            field="total_operating",
            value=str(cf.total_operating),
            depends_on=_entries_summary(cf.operating),
        )

    if field == "total_investing":
        return _node(
            field="total_investing",
            value=str(cf.total_investing),
            depends_on=_entries_summary(cf.investing),
        )

    if field == "total_financing":
        return _node(
            field="total_financing",
            value=str(cf.total_financing),
            depends_on=_entries_summary(cf.financing),
        )

    return None


# ── AttributionReport ──


def trace_attribution(
    attr: AttributionReport, field: str, depth: int = 0
) -> Optional[Dict[str, Any]]:
    if depth > MAX_TRACE_DEPTH:
        return None

    if field == "delta_net_worth":
        return _node(
            field="delta_net_worth",
            value=str(attr.delta_net_worth),
            depends_on=[
                trace_attribution(attr, "contributions", depth + 1),
                trace_attribution(attr, "income", depth + 1),
                trace_attribution(attr, "expenses", depth + 1),
                trace_attribution(attr, "market_effects", depth + 1),
                trace_attribution(attr, "fees", depth + 1),
                trace_attribution(attr, "debt_costs", depth + 1),
            ],
        )

    component_fields = {
        "contributions": "contributions",
        "income": "income",
        "expenses": "expenses",
        "market_effects": "market_effects",
        "fees": "fees",
        "debt_costs": "debt_costs",
    }

    if field in component_fields:
        val = getattr(attr, component_fields[field])
        return _node(
            field=field,
            value=str(val),
        )

    if field == "accounted_change":
        return _node(
            field="accounted_change",
            value=str(attr.accounted_change),
            depends_on=[
                trace_attribution(attr, "contributions", depth + 1),
                trace_attribution(attr, "income", depth + 1),
                trace_attribution(attr, "expenses", depth + 1),
                trace_attribution(attr, "market_effects", depth + 1),
                trace_attribution(attr, "fees", depth + 1),
                trace_attribution(attr, "debt_costs", depth + 1),
            ],
        )

    return None


# ── ConsistencyReport ──


def trace_consistency(
    report: ConsistencyReport, field: str, depth: int = 0
) -> Optional[Dict[str, Any]]:
    if depth > MAX_TRACE_DEPTH:
        return None

    if field == "score":
        return _node(
            field="score",
            value=str(report.score),
            depends_on=[
                {
                    "field": c.name,
                    "value": "passed" if c.passed else "failed",
                    "depends_on": [],
                }
                for c in report.checks
            ],
        )

    if field == "all_passed":
        return _node(
            field="all_passed",
            value=str(report.all_passed),
        )

    if field == "all_passed":
        return _node(
            field="all_passed",
            value=str(report.all_passed),
            formula="all checks pass within tolerance",
            artifacts=["consistency_report"],
        )

    return None
