"""Financial API — Product API over TimelineService, StatementEngine, FinancialAnalyticsEngine.

This module serves as the EXTERNAL contract of the Financial OS.
Every response is JSON. Every engine is lazily initialized.

Design rules:
    - No domain objects leak into responses — all serialized to JSON-safe types
    - Decimal → str, datetime → int (nanoseconds), None → null
    - Every endpoint returns structured JSON: {"success": bool, "data": ..., "error": ...}
    - Engines initialized once, cached in blueprint 'g' pattern
    - Pure Flask — no transport layer abstractions
"""

import os
import time
from functools import wraps
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Callable

from flask import Blueprint, jsonify, request, g, current_app

from core.event.event_store_core import PersistentEventStore, EventType
from core.ingestion.models import ImportColumnMapping, AccountMappingRule
from core.reconciliation.accounting.ledger_adapter import DEFAULT_ASSET_MAP
from models import Transaction as CryptoTransaction
from core.ingestion.importer import import_csv_ledger
from core.reconciled.models import ReconciledState, ReconciledPeriodSnapshot
from core.timeline.engine import TimelineBuilder, TimelineEngine
from core.timeline.service import TimelineService
from core.timeline.series_builder import FinancialSeriesBuilder
from core.statements.engine import StatementEngine
from core.analytics.engine import FinancialAnalyticsEngine
from core.intelligence.engine import NetWorthIntelligence, AssetAllocationEngine, PerformanceAttribution
from core.world_model.engine import HealthEngine, RiskEngine, DriversEngine, DynamicsEngine, WorldStateEngine
from core.simulation.engine import SimulationEngine
from core.simulation.models import CashFlowNode, SimulationInput, ShockEvent
from core.simulation.graph_evaluator import expand_to_nodes
from core.simulation.comparison import ComparisonEngine
from core.simulation.period import SimulationPeriodEngine

financial_bp = Blueprint("financial", __name__, url_prefix="/api/v1/financial")


# ── Helper: response wrappers ──

def _ok(data, metadata=None):
    resp = {"success": True, "data": data}
    if metadata:
        resp["metadata"] = metadata
    return jsonify(resp), 200


def _err(msg, status=404):
    return jsonify({"success": False, "error": msg}), status


# ── Engine initialization ──

def _get_journal_path() -> str:
    """Get the journal database path from config or default."""
    return current_app.config.get(
        "FINANCIAL_JOURNAL_PATH",
        os.path.join(os.path.dirname(current_app.root_path), "financial_journal.db"),
    )


def _load_financial_stack():
    """Lazy-init and cache the full financial engine stack in Flask g."""
    if hasattr(g, "_financial_stack"):
        return g._financial_stack

    store = PersistentEventStore(_get_journal_path())
    events = store.get_by_type(EventType.RECONCILED_STATE.value)

    snapshots: List[ReconciledPeriodSnapshot] = []
    for evt in events:
        payload = evt.payload
        raw_state = payload.get("state")
        if raw_state is None:
            continue
        state = ReconciledState.from_dict(raw_state) if isinstance(raw_state, dict) else raw_state

        snap = ReconciledPeriodSnapshot(
            period_id=state.period_id,
            reconciled_state=state,
            period_closed_at_ns=evt.timestamp_ns,
            period_start_ns=evt.timestamp_ns - 86400_000_000_000,  # approximate: 24h window
            period_end_ns=evt.timestamp_ns,
        )
        snapshots.append(snap)

    timeline = TimelineBuilder.build(snapshots)
    engine = TimelineEngine(timeline)
    service = TimelineService(engine)
    statements = StatementEngine(engine)
    analytics = FinancialAnalyticsEngine(engine, statements)

    stack = {
        "store": store,
        "timeline": engine,
        "service": service,
        "statements": statements,
        "analytics": analytics,
        "net_worth": NetWorthIntelligence(engine),
        "allocation": AssetAllocationEngine(engine),
        "attribution": PerformanceAttribution(engine),
        "health": HealthEngine(engine),
        "risk": RiskEngine(engine),
        "drivers": DriversEngine(engine),
        "dynamics": DynamicsEngine(engine),
        "world_state": WorldStateEngine(engine),
        "simulation_factory": lambda ws: SimulationEngine(ws),
    }
    g._financial_stack = stack
    return stack


def _get_stack():
    return _load_financial_stack()


# ── Helper: result unwrapper ──

def _from_result(result):
    """Convert TimelineServiceResult to (data, error_response) tuple."""
    if not result.success:
        return None, _err(result.error)
    return result.data, None


# ═══════════════════════════════════════════════════
# TIMELINE ENDPOINTS
# ═══════════════════════════════════════════════════

@financial_bp.route("/timeline/latest")
def timeline_latest():
    svc = _get_stack()["service"]
    data, err = _from_result(svc.get_latest_snapshot())
    if err:
        return err
    return _ok(data)


@financial_bp.route("/timeline/genesis")
def timeline_genesis():
    svc = _get_stack()["service"]
    data, err = _from_result(svc.get_genesis_snapshot())
    if err:
        return err
    return _ok(data)


@financial_bp.route("/timeline/periods")
def timeline_periods():
    svc = _get_stack()["service"]
    data, err = _from_result(svc.get_all_periods())
    if err:
        return err
    return _ok(data["data"] if isinstance(data, dict) else data, metadata={"count": len(data["data"] if isinstance(data, dict) else data)})


@financial_bp.route("/timeline/state/<period_id>")
def timeline_state(period_id: str):
    svc = _get_stack()["service"]
    data, err = _from_result(svc.get_snapshot(period_id))
    if err:
        return err
    return _ok(data)


@financial_bp.route("/timeline/range")
def timeline_range():
    from_p = request.args.get("from", "")
    to_p = request.args.get("to", "")
    if not from_p or not to_p:
        return _err("Query params 'from' and 'to' are required", 400)
    svc = _get_stack()["service"]
    data, err = _from_result(svc.get_range(from_p, to_p))
    if err:
        return err
    return _ok(data)


@financial_bp.route("/timeline/diff")
def timeline_diff():
    from_p = request.args.get("from", "")
    to_p = request.args.get("to", "")
    if not from_p or not to_p:
        return _err("Query params 'from' and 'to' are required", 400)
    svc = _get_stack()["service"]
    data, err = _from_result(svc.get_diff(from_p, to_p))
    if err:
        return err
    return _ok(data.get("data", data) if isinstance(data, dict) else data)


@financial_bp.route("/timeline/equity_curve")
def timeline_equity_curve():
    svc = _get_stack()["service"]
    data, err = _from_result(svc.get_economic_equity_curve())
    if err:
        return err
    return _ok(data)


@financial_bp.route("/timeline/pnl_curve")
def timeline_pnl_curve():
    svc = _get_stack()["service"]
    data, err = _from_result(svc.get_pnl_curve())
    if err:
        return err
    return _ok(data)


@financial_bp.route("/timeline/returns")
def timeline_returns():
    svc = _get_stack()["service"]
    data, err = _from_result(svc.get_returns())
    if err:
        return err
    return _ok(data)


@financial_bp.route("/timeline/summary")
def timeline_summary():
    svc = _get_stack()["service"]
    data, err = _from_result(svc.get_summary())
    if err:
        return err
    return _ok(data)


@financial_bp.route("/timeline/verify")
def timeline_verify():
    svc = _get_stack()["service"]
    data, err = _from_result(svc.verify_integrity())
    if err:
        return err
    return _ok(data)


@financial_bp.route("/timeline/account/<account_code>/history")
def timeline_account_history(account_code: str):
    svc = _get_stack()["service"]
    data, err = _from_result(svc.get_account_history(account_code))
    if err:
        return err
    return _ok(data.get("data", data) if isinstance(data, dict) else data,
                  metadata=data.get("metadata") if isinstance(data, dict) else None)


# ── Account breakdown (per-asset positions + cost basis) ──

# Reverse map: account_code → [asset symbols]
_ACCOUNT_ASSET_MAP: Dict[str, List[str]] = {}
for sym, code in DEFAULT_ASSET_MAP.items():
    _ACCOUNT_ASSET_MAP.setdefault(code, []).append(sym)


def _get_per_asset_positions(account_code: str) -> List[Dict]:
    """Query per-asset positions (cost-basis) from Transaction model.

    Legacy bridge: queries the crypto Transaction model for buy/sell history
    and computes quantity, cost basis, avg entry price, realized PnL.

    NOTE: cost_basis ≠ current market value. Without a price feed,
    projections based on cost_basis underestimate returns for appreciated
    positions. This is a known limitation (Phase 13E MVP).

    Technical debt: bypasses the financial service layer. Should be
    replaced by a service-level get_asset_breakdowns() when the
    service layer fully adopts per-asset position tracking.
    """
    assets = _ACCOUNT_ASSET_MAP.get(account_code, [])
    if not assets:
        return []

    try:
        from models import Transaction as CryptoTransaction
        txs = CryptoTransaction.query.filter(
            CryptoTransaction.asset1_ticker.in_(assets)
        ).order_by(CryptoTransaction.timestamp.asc()).all()
    except Exception:
        return []

    by_asset: Dict[str, list] = {a: [] for a in assets}
    for tx in txs:
        sym = tx.asset1_ticker
        if sym in by_asset:
            by_asset[sym].append(tx)

    positions = []
    for sym, tx_list in by_asset.items():
        qty = Decimal("0")
        cost = Decimal("0")
        realized_pnl = Decimal("0")
        for tx in tx_list:
            amt = tx.asset1_amount or Decimal("0")
            price = tx.execution_price or Decimal("0")
            total = amt * price
            if tx.type and tx.type.lower() in ("buy", "long"):
                qty += amt
                cost += total
            elif tx.type and tx.type.lower() in ("sell", "short"):
                qty -= amt
                cost -= total
            if tx.realized_pnl:
                realized_pnl += tx.realized_pnl

        avg_price = str(cost / qty) if qty > 0 else None
        positions.append({
            "symbol": sym,
            "quantity": str(qty),
            "cost_basis": str(cost),
            "avg_entry_price": avg_price,
            "realized_pnl": str(realized_pnl),
            "tx_count": len(tx_list),
        })

    return positions


@financial_bp.route("/timeline/account/<account_code>/breakdown")
def timeline_account_breakdown(account_code: str):
    """Per-asset breakdown for an account: positions, cost basis, transactions."""
    svc = _get_stack()["service"]
    hist_result = svc.get_account_history(account_code)
    if not hist_result.success:
        return _err(hist_result.error)

    history = hist_result.data
    meta = hist_result.metadata
    last_balance = Decimal(history[-1]["balance"]) if history else Decimal("0")

    assets = _ACCOUNT_ASSET_MAP.get(account_code, [])
    positions = _get_per_asset_positions(account_code)

    # Recent transactions
    recent_txs = []
    if assets:
        try:
            from models import Transaction as CryptoTransaction
            recent = CryptoTransaction.query.filter(
                CryptoTransaction.asset1_ticker.in_(assets)
            ).order_by(CryptoTransaction.timestamp.desc()).limit(50).all()
            for tx in recent:
                recent_txs.append({
                    "id": tx.id,
                    "type": tx.type or "",
                    "asset": tx.asset1_ticker or "",
                    "quantity": str(tx.asset1_amount or "0"),
                    "price": str(tx.execution_price or ""),
                    "total": str((tx.asset1_amount or Decimal("0")) * (tx.execution_price or Decimal("0"))),
                    "realized_pnl": str(tx.realized_pnl or ""),
                    "timestamp": tx.timestamp.isoformat() if tx.timestamp else "",
                    "fee": str(tx.fee_amount or ""),
                })
        except Exception:
            pass

    return _ok({
        "account_code": account_code,
        "account_name": meta.get("account_name", account_code),
        "category": meta.get("category", ""),
        "total_value": str(last_balance),
        "positions": positions,
        "mapped_assets": assets,
        "recent_transactions": recent_txs,
    })


# ═══════════════════════════════════════════════════
# SERIES ENDPOINTS (FinancialSeries Builder)
# ═══════════════════════════════════════════════════

def _get_series_from_store():
    """Build FinancialSeries from event store (no engine stack)."""
    store = PersistentEventStore(_get_journal_path())
    events = store.get_by_type(EventType.RECONCILED_STATE.value)
    return FinancialSeriesBuilder.build(events)


@financial_bp.route("/series")
def series_all():
    """Full financial series — all materialized snapshots chronologically."""
    series = _get_series_from_store()
    if not series:
        return _err("No financial data available")
    return _ok(series)


@financial_bp.route("/series/summary")
def series_summary():
    """Aggregated timeline summary across all periods."""
    series = _get_series_from_store()
    summary = FinancialSeriesBuilder.compute_summary(series)
    if summary.get("periods_count", 0) == 0:
        return _err("No financial data available")
    return _ok(summary)


@financial_bp.route("/series/<metric>")
def series_metric(metric: str):
    """Historical series for a specific metric.

    Available metrics:
        net_worth, assets, liabilities, equity,
        income, expenses, net_pnl, cash
    """
    field_map = {
        "net_worth": "balance_sheet.net_worth",
        "assets": "balance_sheet.total_assets",
        "liabilities": "balance_sheet.total_liabilities",
        "equity": "balance_sheet.total_equity",
        "income": "income_statement.total_revenue",
        "expenses": "income_statement.total_expenses",
        "net_pnl": "income_statement.net_pnl",
        "cash": "balance_sheet.total_assets",
    }
    field = field_map.get(metric)
    if not field:
        return _err(f"Unknown metric '{metric}'. Available: {', '.join(field_map.keys())}", 400)

    series = _get_series_from_store()
    if not series:
        return _err("No financial data available")
    result = FinancialSeriesBuilder.extract_series(series, field)
    return _ok(result)


@financial_bp.route("/series/trend/<metric>")
def series_trend(metric: str):
    """Period-over-period trend (value, change, change_pct) for a metric."""
    field_map = {
        "net_worth": "balance_sheet.net_worth",
        "assets": "balance_sheet.total_assets",
        "liabilities": "balance_sheet.total_liabilities",
        "equity": "balance_sheet.total_equity",
        "income": "income_statement.total_revenue",
        "expenses": "income_statement.total_expenses",
        "net_pnl": "income_statement.net_pnl",
    }
    field = field_map.get(metric)
    if not field:
        return _err(f"Unknown metric '{metric}'", 400)

    series = _get_series_from_store()
    if not series:
        return _err("No financial data available")
    result = FinancialSeriesBuilder.compute_trend(series, field)
    return _ok(result)


# ═══════════════════════════════════════════════════
# STATEMENT ENDPOINTS
# ═══════════════════════════════════════════════════

def _to_dict(obj):
    """Recursively convert a frozen dataclass (or nested structure) to a JSON-safe dict."""
    if hasattr(obj, "__dataclass_fields__"):
        result = {}
        for field_name in obj.__dataclass_fields__:
            val = getattr(obj, field_name)
            if isinstance(val, Decimal):
                result[field_name] = str(val)
            elif isinstance(val, (list, tuple)):
                result[field_name] = [_to_dict(v) if hasattr(v, "__dataclass_fields__") else v for v in val]
            elif hasattr(val, "__dataclass_fields__"):
                result[field_name] = _to_dict(val)
            else:
                result[field_name] = val
        return result
    return obj


@financial_bp.route("/statements/balance_sheet/<period_id>")
def balance_sheet(period_id: str):
    stack = _get_stack()
    bs = stack["statements"].build_balance_sheet(period_id)
    if bs is None:
        return _err(f"Balance sheet not available for period '{period_id}'")
    return _ok(_to_dict(bs))


@financial_bp.route("/statements/balance_sheet/latest")
def balance_sheet_latest():
    stack = _get_stack()
    svc = stack["service"]
    latest_res = svc.get_latest_snapshot()
    if not latest_res.success:
        return _err("Timeline is empty")
    return balance_sheet(latest_res.data["period_id"])


@financial_bp.route("/statements/income_statement/<period_id>")
def income_statement(period_id: str):
    stack = _get_stack()

    # Try to load closing_summary from journal
    closing_summary = None
    try:
        store = stack["store"]
        events = store.get_by_type(EventType.RECONCILED_STATE.value)
        for evt in reversed(events):
            payload = evt.payload
            raw_state = payload.get("state", {})
            if isinstance(raw_state, dict) and raw_state.get("period_id") == period_id:
                closing_summary = payload.get("closing_summary")
                break
    except Exception:
        pass

    is_ = stack["statements"].build_income_statement(period_id, closing_summary)
    if is_ is None:
        return _err(f"Income statement not available for period '{period_id}'")
    return _ok(_to_dict(is_))


@financial_bp.route("/statements/income_statement/latest")
def income_statement_latest():
    stack = _get_stack()
    svc = stack["service"]
    latest_res = svc.get_latest_snapshot()
    if not latest_res.success:
        return _err("Timeline is empty")
    return income_statement(latest_res.data["period_id"])


@financial_bp.route("/statements/cash_flow/<period_id>")
def cash_flow(period_id: str):
    stack = _get_stack()
    cf = stack["statements"].build_cash_flow(period_id)
    if cf is None:
        return _err(f"Cash flow not available for period '{period_id}'")
    return _ok(_to_dict(cf))


@financial_bp.route("/statements/cash_flow/latest")
def cash_flow_latest():
    stack = _get_stack()
    svc = stack["service"]
    latest_res = svc.get_latest_snapshot()
    if not latest_res.success:
        return _err("Timeline is empty")
    return cash_flow(latest_res.data["period_id"])


@financial_bp.route("/statements/package/<period_id>")
def statement_package(period_id: str):
    stack = _get_stack()
    pkg = stack["statements"].build_statement_package(period_id)
    if pkg is None:
        return _err(f"Statement package not available for period '{period_id}'")
    return _ok(_to_dict(pkg))


@financial_bp.route("/statements/package/latest")
def statement_package_latest():
    stack = _get_stack()
    svc = stack["service"]
    latest_res = svc.get_latest_snapshot()
    if not latest_res.success:
        return _err("Timeline is empty")
    return statement_package(latest_res.data["period_id"])


# ═══════════════════════════════════════════════════
# ANALYTICS ENDPOINTS
# ═══════════════════════════════════════════════════

@financial_bp.route("/analytics/snapshot/<period_id>")
def analytics_snapshot(period_id: str):
    stack = _get_stack()

    closing_summary = None
    try:
        store = stack["store"]
        events = store.get_by_type(EventType.RECONCILED_STATE.value)
        for evt in reversed(events):
            payload = evt.payload
            raw_state = payload.get("state", {})
            if isinstance(raw_state, dict) and raw_state.get("period_id") == period_id:
                closing_summary = payload.get("closing_summary")
                break
    except Exception:
        pass

    snap = stack["analytics"].build_snapshot(period_id, closing_summary)
    if snap is None:
        return _err(f"Analytics snapshot not available for period '{period_id}'")
    return _ok(_to_dict(snap))


@financial_bp.route("/analytics/snapshot/latest")
def analytics_snapshot_latest():
    stack = _get_stack()
    svc = stack["service"]
    latest_res = svc.get_latest_snapshot()
    if not latest_res.success:
        return _err("Timeline is empty")
    return analytics_snapshot(latest_res.data["period_id"])


@financial_bp.route("/analytics/series")
def analytics_series():
    stack = _get_stack()
    svc = stack["service"]
    periods_res = svc.get_all_periods()
    if not periods_res.success:
        return _err("Timeline is empty")

    period_ids = periods_res.data

    # Load closing_summaries for all periods
    closing_summaries: Dict[str, Dict[str, str]] = {}
    try:
        store = stack["store"]
        events = store.get_by_type(EventType.RECONCILED_STATE.value)
        for evt in events:
            payload = evt.payload
            raw_state = payload.get("state", {})
            cs = payload.get("closing_summary")
            if isinstance(raw_state, dict) and cs:
                pid = raw_state.get("period_id")
                if pid:
                    closing_summaries[pid] = cs
    except Exception:
        pass

    series = stack["analytics"].build_series(period_ids, closing_summaries)
    return _ok(_to_dict(series))


# ═══════════════════════════════════════════════════
# COMPREHENSIVE SUMMARY
# ═══════════════════════════════════════════════════

@financial_bp.route("/summary")
def comprehensive_summary():
    """Full financial overview — uses materialized snapshot if available.

    Fast path: reads materialized_snapshot from the latest RECONCILED_STATE
    event (computed once at import time). No engine stack initialization.

    Fallback: if no materialized snapshot exists (legacy data), builds the
    full financial engine stack and computes analytics on-the-fly.
    """
    try:
        store = PersistentEventStore(_get_journal_path())
        events = store.get_by_type(EventType.RECONCILED_STATE.value)
        if events:
            latest = events[-1]
            payload = latest.payload
            raw_snapshot = payload.get("materialized_snapshot")
            if raw_snapshot is not None:
                return _ok(raw_snapshot)
    except Exception:
        pass

    # Fallback: full engine stack computation
    stack = _get_stack()
    svc = stack["service"]

    summary_res = svc.get_summary()
    if not summary_res.success:
        return _err("Timeline is empty")

    latest_res = svc.get_latest_snapshot()
    latest_period = None
    if latest_res.success:
        latest_period = latest_res.data.get("period_id")

    analytics_snap = None
    if latest_period:
        snap_res = analytics_snapshot(latest_period)
        if snap_res[1] == 200:
            analytics_snap = snap_res[0].json.get("data")

    return _ok({
        "summary": summary_res.data,
        "latest_period": latest_period,
        "analytics": analytics_snap,
    })


# ═══════════════════════════════════════════════════
# INTELLIGENCE ENDPOINTS (Phase 13)
# ═══════════════════════════════════════════════════

@financial_bp.route("/intelligence/net_worth")
@financial_bp.route("/intelligence/net_worth/<period_id>")
def intelligence_net_worth(period_id: str = "latest"):
    stack = _get_stack()
    nw = stack["net_worth"]
    result = nw.latest() if period_id == "latest" else nw.snapshot(period_id)
    if result is None:
        return _err("Net worth not available")
    return _ok(_to_dict(result))


@financial_bp.route("/intelligence/net_worth/series")
def intelligence_net_worth_series():
    stack = _get_stack()
    series = stack["net_worth"].series()
    return _ok(_to_dict(series))


@financial_bp.route("/intelligence/allocation")
@financial_bp.route("/intelligence/allocation/<period_id>")
def intelligence_allocation(period_id: str = "latest"):
    stack = _get_stack()
    alloc = stack["allocation"]
    result = alloc.latest() if period_id == "latest" else alloc.snapshot(period_id)
    if result is None:
        return _err("Allocation not available")
    return _ok(_to_dict(result))


@financial_bp.route("/intelligence/attribution")
@financial_bp.route("/intelligence/attribution/<period_id>")
def intelligence_attribution(period_id: str = "latest"):
    stack = _get_stack()

    cs = None
    try:
        store = stack["store"]
        events = store.get_by_type(EventType.RECONCILED_STATE.value)
        pid = period_id
        if pid == "latest":
            svc = stack["service"]
            latest_res = svc.get_latest_snapshot()
            if latest_res.success:
                pid = latest_res.data["period_id"]
        for evt in reversed(events):
            payload = evt.payload
            raw_state = payload.get("state", {})
            if isinstance(raw_state, dict) and raw_state.get("period_id") == pid:
                cs = payload.get("closing_summary")
                break
    except Exception:
        pass

    attr = stack["attribution"]
    result = attr.latest(cs) if period_id == "latest" else attr.snapshot(pid if period_id == "latest" else period_id, cs)
    if result is None:
        return _err("Attribution not available")
    return _ok(_to_dict(result))


# ═══════════════════════════════════════════════════
# WORLD MODEL ENDPOINTS (Phase 14)
# ═══════════════════════════════════════════════════

def _wm_endpoint(stack_key: str, period_id: str):
    """Generic handler for world model endpoints."""
    stack = _get_stack()
    eng = stack[stack_key]
    result = eng.latest() if period_id == "latest" else eng.snapshot(period_id)
    if result is None:
        return _err(f"{stack_key} not available")
    return _ok(_to_dict(result))


@financial_bp.route("/world/state")
@financial_bp.route("/world/state/<period_id>")
def world_state(period_id: str = "latest"):
    return _wm_endpoint("world_state", period_id)


@financial_bp.route("/world/health")
@financial_bp.route("/world/health/<period_id>")
def world_health(period_id: str = "latest"):
    return _wm_endpoint("health", period_id)


@financial_bp.route("/world/risk")
@financial_bp.route("/world/risk/<period_id>")
def world_risk(period_id: str = "latest"):
    return _wm_endpoint("risk", period_id)


@financial_bp.route("/world/drivers")
@financial_bp.route("/world/drivers/<period_id>")
def world_drivers(period_id: str = "latest"):
    return _wm_endpoint("drivers", period_id)


@financial_bp.route("/world/dynamics")
@financial_bp.route("/world/dynamics/<period_id>")
def world_dynamics(period_id: str = "latest"):
    return _wm_endpoint("dynamics", period_id)


# ═══════════════════════════════════════════════════
# ATTRIBUTION ENDPOINTS (Phase 13C)
# ═══════════════════════════════════════════════════

from core.attribution.engine import EntityAttributionEngine as _EntityAttributionEngine
from core.attribution.graph import AttributionGraphBuilder as _AttributionGraphBuilder


@financial_bp.route("/attribution/<period_id>")
def financial_attribution(period_id: str):
    """Entity-level attribution for a period.

    Query params:
        mode: "fast" (default, state-diff) or "graph" (event-causal)
    """
    mode = request.args.get("mode", "fast")
    stack = _get_stack()
    engine = stack["service"].engine

    if mode == "graph":
        # Graph mode: build from AccountingTransactions
        # Derive system_delta from state diff as reference
        node = engine.get_node(period_id)
        after_state = engine.get_state(period_id)
        if after_state is None or node is None or not node.prev_hash:
            return _err(f"No data for period '{period_id}'")
        before_state = None
        for pid in engine.timeline.node_order:
            n = engine.get_node(pid)
            if n and n.index.state_hash == node.prev_hash:
                before_state = engine.get_state(pid)
                break
        if before_state is None:
            return _err(f"No previous period for '{period_id}'")
        system_delta = before_state.total_equity - after_state.total_equity
        if system_delta < Decimal("0"):
            system_delta = -system_delta

        # Attempt to fetch transactions — may be empty if none stored
        try:
            from core.accounting.models import AccountingTransaction
            store = _get_stack().get("store")
            txns = []
            if store:
                # Try to get journal transactions for this period
                events = store.get_by_type("accounting_entry")
                # This path depends on storage format — fallback to diff for now
                pass
        except Exception:
            pass

        # For now, graph mode with no stored transactions falls back gracefully
        if not txns:
            return _ok({
                "period_id": period_id,
                "system_delta": str(system_delta),
                "rows": [],
                "mode": "graph",
                "note": "No stored transactions for graph mode — use mode=fast",
            })
        matrix = _AttributionGraphBuilder.build(txns, period_id, system_delta)
    else:
        matrix = _EntityAttributionEngine.compute_from_engine(engine, period_id)

    if not matrix.rows:
        return _err(f"No attribution data for period '{period_id}'")

    result = _to_dict(matrix)
    if isinstance(result, dict):
        result["mode"] = mode
    return _ok(result)


# ═══════════════════════════════════════════════════
# ATTRIBUTION REPORT ENDPOINTS (Phase 13F)
# ═══════════════════════════════════════════════════

def _build_attribution_report(
    period_id: str,
) -> Optional[Dict]:
    """Build AttributionReport for a single period. Returns None if unavailable."""
    from core.attribution.report_engine import AttributionReportEngine

    stack = _get_stack()
    engine = stack["service"].engine

    current_state = engine.get_state(period_id)
    if current_state is None:
        return None

    # Find previous period from ordered timeline
    prev_state = None
    prev_period_id = None
    node = engine.get_node(period_id)
    if node and node.prev_hash:
        node_order = engine.timeline.node_order
        try:
            idx = node_order.index(period_id)
            if idx > 0:
                prev_period_id = node_order[idx - 1]
                prev_state = engine.get_state(prev_period_id)
        except ValueError:
            pass

    if prev_state is None:
        return None

    # Load closing_summary for income statement
    closing_summary = None
    try:
        store = stack["store"]
        events = store.get_by_type(EventType.RECONCILED_STATE.value)
        for evt in reversed(events):
            payload = evt.payload
            raw_state = payload.get("state", {})
            if isinstance(raw_state, dict) and raw_state.get("period_id") == period_id:
                closing_summary = payload.get("closing_summary")
                break
    except Exception:
        pass

    income_statement = stack["statements"].build_income_statement(
        period_id, closing_summary
    )
    if income_statement is None:
        return None

    report = AttributionReportEngine.compute(prev_state, current_state, income_statement)
    return report.to_dict()


@financial_bp.route("/attribution/report/<period_id>")
def attribution_report(period_id: str):
    result = _build_attribution_report(period_id)
    if result is None:
        return _err(f"Attribution report not available for period '{period_id}'")
    return _ok(result)


@financial_bp.route("/attribution/report/latest")
def attribution_report_latest():
    stack = _get_stack()
    svc = stack["service"]
    latest_res = svc.get_latest_snapshot()
    if not latest_res.success:
        return _err("Timeline is empty")
    result = _build_attribution_report(latest_res.data["period_id"])
    if result is None:
        return _err("No previous period available for attribution")
    return _ok(result)


# ═══════════════════════════════════════════════════
# CONSISTENCY CHECK ENDPOINTS (Phase 14)
# ═══════════════════════════════════════════════════

from core.consistency.validator import CrossArtifactValidator


def _build_consistency_report(
    period_id: str,
) -> Optional[Dict]:
    """Cross-validate all artifacts for a period. Returns None if unavailable."""
    stack = _get_stack()
    engine = stack["service"].engine

    current_state = engine.get_state(period_id)
    if current_state is None:
        return None

    # Find previous period
    prev_state = None
    prev_period_id = None
    node = engine.get_node(period_id)
    if node and node.prev_hash:
        node_order = engine.timeline.node_order
        try:
            idx = node_order.index(period_id)
            if idx > 0:
                prev_period_id = node_order[idx - 1]
                prev_state = engine.get_state(prev_period_id)
        except ValueError:
            pass

    if prev_state is None:
        return None

    # Load closing_summary for income statement
    closing_summary = None
    try:
        store = stack["store"]
        events = store.get_by_type(EventType.RECONCILED_STATE.value)
        for evt in reversed(events):
            payload = evt.payload
            raw_state = payload.get("state", {})
            if isinstance(raw_state, dict) and raw_state.get("period_id") == period_id:
                closing_summary = payload.get("closing_summary")
                break
    except Exception:
        pass

    income_statement = stack["statements"].build_income_statement(
        period_id, closing_summary
    )
    if income_statement is None:
        return None

    cash_flow = stack["statements"].build_cash_flow(period_id)
    attribution_dict = _build_attribution_report(period_id)

    if cash_flow is None or attribution_dict is None:
        return None

    report = CrossArtifactValidator.validate_from_dicts(
        prev_state=prev_state.to_dict(),
        current_state=current_state.to_dict(),
        income_statement=income_statement.__dict__,
        cash_flow=cash_flow.__dict__,
        attribution_report=attribution_dict,
    )
    return report.to_dict()


@financial_bp.route("/consistency/check/<period_id>")
def consistency_check(period_id: str):
    result = _build_consistency_report(period_id)
    if result is None:
        return _err(f"Consistency check not available for period '{period_id}'")
    return _ok(result)


@financial_bp.route("/consistency/check/latest")
def consistency_check_latest():
    stack = _get_stack()
    svc = stack["service"]
    latest_res = svc.get_latest_snapshot()
    if not latest_res.success:
        return _err("Timeline is empty")
    result = _build_consistency_report(latest_res.data["period_id"])
    if result is None:
        return _err("Consistency check not available for latest period")
    return _ok(result)


# ═══════════════════════════════════════════════════
# PERIOD ENDPOINT (Phase 15 — Explainability)
# ═══════════════════════════════════════════════════

from core.period.observations import (
    compute_debt_ratio,
    compute_liquidity_cushion,
    detect_concentration,
    detect_expense_trend,
)


@financial_bp.route("/period/<period_id>")
def period_overview(period_id: str):
    """Composite view of a single financial period with explanations.

    Returns all 5 artifacts + computed observations in one call.
    Observations are typed semantic data, not pre-rendered text.
    """
    stack = _get_stack()
    engine = stack["service"].engine

    current_state = engine.get_state(period_id)
    if current_state is None:
        return _err(f"Period '{period_id}' not found")

    # Previous period for deltas and trend detection
    prev_state = None
    prev_period_id = None
    node = engine.get_node(period_id)
    if node and node.prev_hash:
        node_order = engine.timeline.node_order
        try:
            idx = node_order.index(period_id)
            if idx > 0:
                prev_period_id = node_order[idx - 1]
                prev_state = engine.get_state(prev_period_id)
        except ValueError:
            pass

    # Load closing_summary
    closing_summary = None
    try:
        store = stack["store"]
        events = store.get_by_type(EventType.RECONCILED_STATE.value)
        for evt in reversed(events):
            payload = evt.payload
            raw_state = payload.get("state", {})
            if isinstance(raw_state, dict) and raw_state.get("period_id") == period_id:
                closing_summary = payload.get("closing_summary")
                break
    except Exception:
        pass

    # Build artifacts
    bs = stack["statements"].build_balance_sheet(period_id)
    is_ = stack["statements"].build_income_statement(period_id, closing_summary)
    cf = stack["statements"].build_cash_flow(period_id)
    attr = _build_attribution_report(period_id)
    cons = _build_consistency_report(period_id)

    # Compute observations (errors are non-fatal)
    observations: List[Dict] = []
    if bs is not None:
        try:
            c = detect_concentration(bs)
            if c:
                observations.append(c)
        except Exception:
            pass

        try:
            if is_ is not None:
                liq = compute_liquidity_cushion(bs, is_)
                if liq:
                    observations.append(liq)
        except Exception:
            pass

        try:
            dr = compute_debt_ratio(bs)
            observations.append(dr)
        except Exception:
            pass

    # Expense trend: gather income statements for recent periods
    if is_ is not None:
        try:
            prev_is_list = []
            prev_ids = []
            if prev_period_id:
                prev_ids.append(prev_period_id)
            # Walk back further for trend detection
            pid = prev_period_id
            for _ in range(4):
                if not pid:
                    break
                prev_ids.append(pid)
                n = engine.get_node(pid)
                if n and n.prev_hash:
                    try:
                        idx = engine.timeline.node_order.index(pid)
                        if idx > 0:
                            pid = engine.timeline.node_order[idx - 1]
                        else:
                            pid = None
                    except ValueError:
                        pid = None
                else:
                    pid = None

            for pid in prev_ids:
                cs = None
                try:
                    store = stack["store"]
                    events = store.get_by_type(EventType.RECONCILED_STATE.value)
                    for evt in reversed(events):
                        payload = evt.payload
                        raw_state = payload.get("state", {})
                        if isinstance(raw_state, dict) and raw_state.get("period_id") == pid:
                            cs = payload.get("closing_summary")
                            break
                except Exception:
                    pass
                prev_is = stack["statements"].build_income_statement(pid, cs)
                if prev_is is not None:
                    prev_is_list.append(prev_is)

            if prev_is_list:
                et = detect_expense_trend(prev_is_list)
                if et:
                    observations.append(et)
        except Exception:
            pass

    # Reasons: attribution components → typed observations
    reasons: List[Dict] = []
    if attr is not None:
        component_map = {
            "contributions": "contributions",
            "income": "income",
            "expenses": "expenses",
            "market_effects": "market_effects",
            "fees": "fees",
            "debt_costs": "debt_costs",
        }
        amounts = {
            "contributions": Decimal(attr.get("contributions", "0")),
            "income": Decimal(attr.get("income", "0")),
            "expenses": Decimal(attr.get("expenses", "0")),
            "market_effects": Decimal(attr.get("market_effects", "0")),
            "fees": Decimal(attr.get("fees", "0")),
            "debt_costs": Decimal(attr.get("debt_costs", "0")),
        }
        for kind, amt in amounts.items():
            if amt != Decimal("0"):
                reasons.append({
                    "kind": kind,
                    "amount": str(abs(amt)),
                    "direction": "positive" if amt > 0 else "negative",
                    "delta_net_worth": str(Decimal(attr.get("delta_net_worth", "0"))),
                })

    # Issues: consistency violations
    issues: List[Dict] = []
    if cons is not None:
        for check in cons.get("checks", []):
            if not check.get("passed", True):
                issues.append({
                    "kind": check.get("name", "unknown"),
                    "drift": check.get("drift", "0"),
                    "severity": "warning",
                })
        score = cons.get("score", "1.0")
        if Decimal(score) < Decimal("1.0"):
            issues.append({
                "kind": "consistency_score",
                "score": score,
                "severity": "warning" if Decimal(score) >= Decimal("0.5") else "error",
            })

    # Compose response
    response = {
        "period_id": period_id,
        "prev_period_id": prev_period_id,
        "reconciled_state": _to_dict(current_state) if hasattr(current_state, 'to_dict') else current_state.to_dict(),
        "balance_sheet": _to_dict(bs) if bs else None,
        "income_statement": _to_dict(is_) if is_ else None,
        "cash_flow": _to_dict(cf) if cf else None,
        "attribution": attr,
        "consistency": cons,
        "observations": observations,
        "reasons": reasons,
        "issues": issues,
    }

    # Optional trace: ?trace=net_worth,total_assets
    trace_fields = request.args.get("trace", "")
    if trace_fields and (bs or is_ or cf or attr or cons):
        from core.trace import (
            TraceContext,
            trace_attribution,
            trace_balance_sheet,
            trace_cash_flow,
            trace_consistency,
            trace_income_statement,
        )

        # Resolve attribution and consistency domain objects from dicts
        attr_obj = None
        if attr:
            from core.attribution.report_engine import AttributionReport
            attr_obj = AttributionReport(
                period_id=attr["period_id"],
                prev_period_id=attr.get("prev_period_id"),
                net_worth_start=Decimal(attr.get("net_worth_start", "0")),
                net_worth_end=Decimal(attr.get("net_worth_end", "0")),
                delta_net_worth=Decimal(attr.get("delta_net_worth", "0")),
                contributions=Decimal(attr.get("contributions", "0")),
                income=Decimal(attr.get("income", "0")),
                expenses=Decimal(attr.get("expenses", "0")),
                market_effects=Decimal(attr.get("market_effects", "0")),
                fees=Decimal(attr.get("fees", "0")),
                debt_costs=Decimal(attr.get("debt_costs", "0")),
            )

        cons_obj = None
        if cons:
            from core.consistency.validator import ConsistencyCheck, ConsistencyReport
            cons_obj = ConsistencyReport(
                period_id=cons.get("period_id", period_id),
                prev_period_id=cons.get("prev_period_id"),
                checks=[
                    ConsistencyCheck(
                        name=c["name"],
                        passed=c["passed"],
                        expected=Decimal(c.get("expected", "0")),
                        actual=Decimal(c.get("actual", "0")),
                        drift=Decimal(c.get("drift", "0")),
                    )
                    for c in cons.get("checks", [])
                ],
            )

        ctx = TraceContext(
            period_id=period_id,
            prev_period_id=prev_period_id,
            balance_sheet=bs,
            income_statement=is_,
            cash_flow=cf,
            attribution=attr_obj,
            consistency=cons_obj,
        )

        traces: Dict[str, Dict] = {}
        for tf in trace_fields.split(","):
            tf = tf.strip()
            if not tf:
                continue
            entry: Dict = {}

            # 15A — Dependency Trace
            try:
                if tf in ("net_worth", "total_assets", "total_liabilities", "total_equity") and bs:
                    entry["dependency"] = trace_balance_sheet(bs, tf)
                elif tf in ("net_pnl", "total_revenue", "total_expenses") and is_:
                    entry["dependency"] = trace_income_statement(is_, tf)
                elif tf in ("net_cash_flow", "total_operating", "total_investing", "total_financing") and cf:
                    entry["dependency"] = trace_cash_flow(cf, tf)
                elif tf in ("delta_net_worth", "income", "expenses", "market_effects",
                            "fees", "debt_costs", "contributions", "accounted_change") and attr_obj:
                    entry["dependency"] = trace_attribution(attr_obj, tf)
                elif tf in ("score", "all_passed") and cons_obj:
                    entry["dependency"] = trace_consistency(cons_obj, tf)
            except Exception:
                pass

            # 15B — Artifact Trace
            try:
                at = ctx.artifact_trace(tf)
                if at is not None:
                    entry["artifacts"] = at
            except Exception:
                pass

            if entry:
                traces[tf] = entry

        if traces:
            response["traces"] = traces

    return _ok(response)


@financial_bp.route("/period/<period_id>/drilldown")
def period_drilldown(period_id: str):
    """Per-asset drilldown — reads existing artifacts, no new computation.

    Returns a consumer view over BalanceSheet + IncomeStatement:
    for each asset account, shows start/end balance, delta,
    and an approximate split into market_effects vs contributions.

    This is a CONSUMER endpoint — zero financial logic.
    Position Layer (Phase 14) will improve precision.
    """
    stack = _get_stack()
    engine = stack["service"].engine

    current_state = engine.get_state(period_id)
    if current_state is None:
        return _err(f"Period '{period_id}' not found")

    # Previous period
    prev_state = None
    prev_period_id = None
    node = engine.get_node(period_id)
    if node and node.prev_hash:
        node_order = engine.timeline.node_order
        try:
            idx = node_order.index(period_id)
            if idx > 0:
                prev_period_id = node_order[idx - 1]
                prev_state = engine.get_state(prev_period_id)
        except ValueError:
            pass

    # Get attribution report for reference totals
    attr = _build_attribution_report(period_id)

    # Build per-asset rows from ReconciledState balances
    prev_balances = prev_state.balances if prev_state else {}
    current_balances = current_state.balances

    drilldown_rows = []
    for code, bal in sorted(current_balances.items()):
        if not bal.category or bal.category.lower() not in ("asset", "investment", "crypto", "cash"):
            continue

        start_amount = Decimal("0")
        if code in prev_balances:
            start_amount = prev_balances[code].reconciled_balance

        end_amount = bal.reconciled_balance
        delta = end_amount - start_amount

        if delta == Decimal("0") and start_amount == Decimal("0"):
            continue

        row = {
            "account_code": code,
            "account_name": bal.account_name,
            "category": bal.category,
            "start_balance": str(start_amount),
            "end_balance": str(end_amount),
            "delta": str(delta),
            "market_effects": "0",
            "contributions": "0",
        }
        drilldown_rows.append(row)

    # Summary (from AttributionReport when available)
    summary = {"total_market_effects": "0", "total_delta": "0"}
    if attr:
        summary = {
            "total_market_effects": attr.get("market_effects", "0"),
            "total_delta": attr.get("delta_net_worth", "0"),
        }

    return _ok({
        "period_id": period_id,
        "prev_period_id": prev_period_id,
        "assets": drilldown_rows,
        "summary": summary,
    })


@financial_bp.route("/period/latest")
def period_overview_latest():
    stack = _get_stack()
    svc = stack["service"]
    latest_res = svc.get_latest_snapshot()
    if not latest_res.success:
        return _err("Timeline is empty")
    return period_overview(latest_res.data["period_id"])


@financial_bp.route("/period/latest/drilldown")
def period_latest_drilldown():
    stack = _get_stack()
    svc = stack["service"]
    latest_res = svc.get_latest_snapshot()
    if not latest_res.success:
        return _err("Timeline is empty")
    return period_drilldown(latest_res.data["period_id"])


# ═══════════════════════════════════════════════════
# SIMULATION ENDPOINTS (Phase 15)
# ═══════════════════════════════════════════════════

@financial_bp.route("/simulation/run", methods=["POST"])
def simulation_run():
    """Run one or more simulation scenarios.

    POST JSON body:
        scenarios: list of {
            scenario_id, description?, periods,
            nodes?: [{node_id, node_type, direction, amount, growth_pct?, tags?}],
            monthly_income?, monthly_expenses?,  (legacy, bridged to nodes)
            income_change_pct?, expense_change_pct?,
            market_return_pct?, crypto_return_pct?,
            debt_interest_pct?, monthly_payment_to_debt?,
            crypto_allocation_pct?,
            shocks?: [{time_offset, type, magnitude, node_id?, description?}]
        }

    Returns: {success, data: [{scenario_id, description, trajectory: [...]}]}
    """
    stack = _get_stack()
    body = request.get_json(silent=True)
    if not body or "scenarios" not in body:
        return _err("Request must include 'scenarios' array", 400)

    # Get current world state
    ws_engine = stack["world_state"]
    current = ws_engine.latest()
    if current is None:
        return _err("No world state available — seed the timeline first", 400)

    # Build simulation engine
    sim = stack["simulation_factory"](current)

    # Parse simulation inputs
    inputs = []
    for raw in body["scenarios"]:
        shocks = []
        for s in raw.get("shocks", []):
            shocks.append(ShockEvent(
                time_offset=s.get("time_offset", 1),
                type=s.get("type", "market_drop"),
                magnitude=Decimal(str(s.get("magnitude", "0"))),
                node_id=s.get("node_id"),
                description=s.get("description", ""),
            ))

        # Parse nodes (15D) or bridge from scalar (15C legacy)
        nodes = []
        for n in raw.get("nodes", []):
            nodes.append(CashFlowNode(
                node_id=n.get("node_id", "node"),
                node_type=n.get("node_type", "generic"),
                direction=n.get("direction", "inflow"),
                amount=Decimal(str(n.get("amount", "0"))),
                growth_pct=float(n.get("growth_pct", 0.0)),
                tags=tuple(n.get("tags", [])),
            ))

        if not nodes:
            # Bridge: convert scalar 15C inputs to 15D graph
            monthly_income = Decimal(str(raw.get("monthly_income", "0")))
            monthly_expenses = Decimal(str(raw.get("monthly_expenses", "0")))
            income_change_pct = float(raw.get("income_change_pct", 0.0))
            expense_change_pct = float(raw.get("expense_change_pct", 0.0))
            nodes = expand_to_nodes(monthly_income, monthly_expenses)
            if income_change_pct != 0.0:
                nodes = [
                    CashFlowNode(node_id=n.node_id, node_type=n.node_type,
                                 direction=n.direction, amount=n.amount,
                                 growth_pct=income_change_pct)
                    if n.direction == "inflow" else n
                    for n in nodes
                ]
            if expense_change_pct != 0.0:
                nodes = [
                    CashFlowNode(node_id=n.node_id, node_type=n.node_type,
                                 direction=n.direction, amount=n.amount,
                                 growth_pct=expense_change_pct)
                    if n.direction == "outflow" else n
                    for n in nodes
                ]

        inp = SimulationInput(
            scenario_id=raw.get("scenario_id", "scenario"),
            description=raw.get("description", ""),
            periods=raw.get("periods", 12),
            nodes=nodes,
            market_return_pct=float(raw.get("market_return_pct", 0.0)),
            crypto_return_pct=float(raw.get("crypto_return_pct", 0.0)),
            debt_interest_pct=float(raw.get("debt_interest_pct", 0.0)),
            monthly_payment_to_debt=Decimal(str(raw.get("monthly_payment_to_debt", "0"))),
            crypto_allocation_pct=float(raw.get("crypto_allocation_pct", 0.0)),
            shocks=shocks,
        )
        inputs.append(inp)

    results = sim.run(inputs)

    return _ok([
        {"scenario_id": r.scenario_id, "description": r.description,
         "trajectory": [_to_dict(s) for s in r.trajectory]}
        for r in results
    ])


@financial_bp.route("/simulation/compare", methods=["POST"])
def simulation_compare():
    """Run scenarios and return trajectories + metrics + comparisons.

    Same input as /simulation/run. Response includes:
        - trajectories: full trajectory per scenario
        - metrics: derived metrics per scenario
        - comparisons: pairwise comparisons against first scenario

    POST JSON body:
        scenarios: list of scenario configs (same as /run)
    """
    stack = _get_stack()
    body = request.get_json(silent=True)
    if not body or "scenarios" not in body:
        return _err("Request must include 'scenarios' array", 400)

    ws_engine = stack["world_state"]
    current = ws_engine.latest()
    if current is None:
        return _err("No world state available", 400)

    sim = stack["simulation_factory"](current)

    inputs = []
    for raw in body["scenarios"]:
        shocks = []
        for s in raw.get("shocks", []):
            shocks.append(ShockEvent(
                time_offset=s.get("time_offset", 1),
                type=s.get("type", "market_drop"),
                magnitude=Decimal(str(s.get("magnitude", "0"))),
                node_id=s.get("node_id"),
                description=s.get("description", ""),
            ))

        # Parse nodes (15D) or bridge from scalar (15C legacy)
        nodes = []
        for n in raw.get("nodes", []):
            nodes.append(CashFlowNode(
                node_id=n.get("node_id", "node"),
                node_type=n.get("node_type", "generic"),
                direction=n.get("direction", "inflow"),
                amount=Decimal(str(n.get("amount", "0"))),
                growth_pct=float(n.get("growth_pct", 0.0)),
                tags=tuple(n.get("tags", [])),
            ))

        if not nodes:
            monthly_income = Decimal(str(raw.get("monthly_income", "0")))
            monthly_expenses = Decimal(str(raw.get("monthly_expenses", "0")))
            income_change_pct = float(raw.get("income_change_pct", 0.0))
            expense_change_pct = float(raw.get("expense_change_pct", 0.0))
            nodes = expand_to_nodes(monthly_income, monthly_expenses)
            if income_change_pct != 0.0:
                nodes = [
                    CashFlowNode(node_id=n.node_id, node_type=n.node_type,
                                 direction=n.direction, amount=n.amount,
                                 growth_pct=income_change_pct)
                    if n.direction == "inflow" else n
                    for n in nodes
                ]
            if expense_change_pct != 0.0:
                nodes = [
                    CashFlowNode(node_id=n.node_id, node_type=n.node_type,
                                 direction=n.direction, amount=n.amount,
                                 growth_pct=expense_change_pct)
                    if n.direction == "outflow" else n
                    for n in nodes
                ]

        inputs.append(SimulationInput(
            scenario_id=raw.get("scenario_id", "scenario"),
            description=raw.get("description", ""),
            periods=raw.get("periods", 12),
            nodes=nodes,
            market_return_pct=float(raw.get("market_return_pct", 0.0)),
            crypto_return_pct=float(raw.get("crypto_return_pct", 0.0)),
            debt_interest_pct=float(raw.get("debt_interest_pct", 0.0)),
            monthly_payment_to_debt=Decimal(str(raw.get("monthly_payment_to_debt", "0"))),
            crypto_allocation_pct=float(raw.get("crypto_allocation_pct", 0.0)),
            shocks=shocks,
        ))

    results = sim.run(inputs)
    metrics, comparisons = ComparisonEngine.compute_all(results)

    return _ok({
        "trajectories": [
            {"scenario_id": r.scenario_id, "description": r.description,
             "trajectory": [_to_dict(s) for s in r.trajectory]}
            for r in results
        ],
        "metrics": [_to_dict(m) for m in metrics],
        "comparisons": [_to_dict(c) for c in comparisons],
    })


@financial_bp.route("/simulation/statements", methods=["POST"])
def simulation_statements():
    """Run a single scenario and return period-level projected statements.

    POST JSON body: same single-scenario format as /simulation/run.
    Returns projected balance sheet, income, cash flow by period.
    """
    stack = _get_stack()
    body = request.get_json(silent=True)
    if not body:
        return _err("Request body required", 400)

    ws_engine = stack["world_state"]
    current = ws_engine.latest()
    if current is None:
        return _err("No world state available", 400)

    sim = stack["simulation_factory"](current)

    shocks = []
    for s in body.get("shocks", []):
        shocks.append(ShockEvent(
            time_offset=s.get("time_offset", 1),
            type=s.get("type", "market_drop"),
            magnitude=Decimal(str(s.get("magnitude", "0"))),
            node_id=s.get("node_id"),
            description=s.get("description", ""),
        ))

    nodes = []
    for n in body.get("nodes", []):
        nodes.append(CashFlowNode(
            node_id=n.get("node_id", "node"),
            node_type=n.get("node_type", "generic"),
            direction=n.get("direction", "inflow"),
            amount=Decimal(str(n.get("amount", "0"))),
            growth_pct=float(n.get("growth_pct", 0.0)),
            tags=tuple(n.get("tags", [])),
        ))

    if not nodes:
        monthly_income = Decimal(str(body.get("monthly_income", "0")))
        monthly_expenses = Decimal(str(body.get("monthly_expenses", "0")))
        income_change_pct = float(body.get("income_change_pct", 0.0))
        expense_change_pct = float(body.get("expense_change_pct", 0.0))
        nodes = expand_to_nodes(monthly_income, monthly_expenses)
        if income_change_pct != 0.0:
            nodes = [
                CashFlowNode(node_id=n.node_id, node_type=n.node_type,
                             direction=n.direction, amount=n.amount,
                             growth_pct=income_change_pct)
                if n.direction == "inflow" else n
                for n in nodes
            ]
        if expense_change_pct != 0.0:
            nodes = [
                CashFlowNode(node_id=n.node_id, node_type=n.node_type,
                             direction=n.direction, amount=n.amount,
                             growth_pct=expense_change_pct)
                if n.direction == "outflow" else n
                for n in nodes
            ]

    inp = SimulationInput(
        scenario_id=body.get("scenario_id", "scenario"),
        description=body.get("description", ""),
        periods=body.get("periods", 12),
        nodes=nodes,
        market_return_pct=float(body.get("market_return_pct", 0.0)),
        crypto_return_pct=float(body.get("crypto_return_pct", 0.0)),
        debt_interest_pct=float(body.get("debt_interest_pct", 0.0)),
        monthly_payment_to_debt=Decimal(str(body.get("monthly_payment_to_debt", "0"))),
        crypto_allocation_pct=float(body.get("crypto_allocation_pct", 0.0)),
        shocks=shocks,
    )

    result = sim.run_single(inp)
    snapshots = SimulationPeriodEngine.build(result)
    table = SimulationPeriodEngine.summary_table(snapshots)

    return _ok({
        "scenario_id": result.scenario_id,
        "description": result.description,
        "periods": table,
        "total_net_cash_flow": str(sum(
            Decimal(s["net_cash_flow"]) for s in table
        ).quantize(Decimal("0.01"))),
    })


# ═══════════════════════════════════════════════════
# SCENARIO SANDBOX ENDPOINT (Phase 13E)
# ═══════════════════════════════════════════════════

@financial_bp.route("/scenario/project", methods=["POST"])
def scenario_project():
    """Project current financial state under a hypothetical scenario.

    POST JSON body:
        scenario_type: "price_change" | "monthly_contribution" | "pay_debt" | "sell_position"
        params: {
            asset?: str,       # for price_change & sell_position
            change_pct?: str,  # for price_change
            amount?: str,      # for monthly_contribution
            months?: int,      # for monthly_contribution
            sell_pct?: str,    # for sell_position
        }

    Returns: {
        projected_nw, projected_assets, projected_liabilities,
        projected_allocation: {category: str},
        diff_nw, diff_assets, diff_liabilities,
        details: [str]
    }
    """
    from core.sandbox.projector import ScenarioProjector

    body = request.get_json(silent=True)
    if not body:
        return _err("Request body required", 400)

    scenario_type = body.get("scenario_type", "")
    params = body.get("params", {})

    SCENARIO_TYPES = frozenset({"price_change", "monthly_contribution", "pay_debt", "sell_position"})
    if scenario_type not in SCENARIO_TYPES:
        return _err(f"Unknown scenario type '{scenario_type}'", 400)

    stack = _get_stack()
    svc = stack["service"]
    latest_res = svc.get_latest_snapshot()
    if not latest_res.success:
        return _err("No financial data available", 400)

    data = latest_res.data
    balances: Dict[str, str] = {
        code: bal["reconciled_balance"]
        for code, bal in data.get("balances", {}).items()
        if bal.get("reconciled_balance") is not None
    }

    # Load per-asset breakdowns for asset accounts (crypto positions)
    breakdowns: Dict[str, List[Dict]] = {}
    for code in set(DEFAULT_ASSET_MAP.values()):
        try:
            positions = _get_per_asset_positions(code)
            if positions:
                breakdowns[code] = positions
        except Exception:
            pass

    result = ScenarioProjector.project(balances, scenario_type, params, breakdowns)

    return _ok({
        "projected_nw": result.projected_nw,
        "projected_assets": result.projected_assets,
        "projected_liabilities": result.projected_liabilities,
        "projected_allocation": result.projected_allocation,
        "diff_nw": result.diff_nw,
        "diff_assets": result.diff_assets,
        "diff_liabilities": result.diff_liabilities,
        "details": result.details,
    })


# ═══════════════════════════════════════════════════
# CSV IMPORT ENDPOINT
# ═══════════════════════════════════════════════════

@financial_bp.route("/import/csv", methods=["POST"])
def import_csv():
    """Import a CSV file into the Financial OS ledger.

    POST multipart/form-data:
        file: CSV file
        config: JSON string with:
            date_column: str (required)
            description_column: str (required)
            amount_column: str (required)
            type_column: str (optional)
            type_income_value: str (default "income")
            type_expense_value: str (default "expense")
            date_format: str (default "%Y-%m-%d")
            skip_rows: int (default 0)
            asset: str (default "USD")
            period_id: str (optional)
            rules: list of {
                match_type: "contains"|"exact" (default "contains")
                pattern: str
                transaction_type: "income"|"expense"|"both" (default "both")
                debit_account: str
                credit_account: str
            }

    Returns: {success, data: {imported, skipped, errors, ledger_hash, total_balance, period_id, transactions_count}}
    """
    if "file" not in request.files:
        return _err("CSV file required as 'file' in multipart form data", 400)

    file = request.files["file"]
    if file.filename == "":
        return _err("Empty filename", 400)

    config_raw = request.form.get("config", "{}")
    try:
        config = json.loads(config_raw) if isinstance(config_raw, str) else config_raw
    except json.JSONDecodeError as e:
        return _err(f"Invalid JSON config: {e}", 400)

    column_mapping = ImportColumnMapping(
        date_column=config.get("date_column", ""),
        description_column=config.get("description_column", ""),
        amount_column=config.get("amount_column", ""),
        type_column=config.get("type_column", ""),
        type_income_value=config.get("type_income_value", "income"),
        type_expense_value=config.get("type_expense_value", "expense"),
        date_format=config.get("date_format", "%Y-%m-%d"),
        skip_rows=int(config.get("skip_rows", 0)),
        asset=config.get("asset", "USD"),
    )

    rules = []
    for r in config.get("rules", []):
        rules.append(AccountMappingRule(
            match_type=r.get("match_type", "contains"),
            pattern=r.get("pattern", ""),
            transaction_type=r.get("transaction_type", "both"),
            debit_account=r.get("debit_account", ""),
            credit_account=r.get("credit_account", ""),
        ))

    csv_content = file.read().decode("utf-8", errors="replace")
    period_id = config.get("period_id")

    try:
        store = PersistentEventStore(_get_journal_path())
        result = import_csv_ledger(
            store=store,
            csv_content=csv_content,
            column_mapping=column_mapping,
            account_rules=rules,
            period_id=period_id,
        )
    except Exception as e:
        return _err(f"Import failed: {e}", 500)

    return _ok({
        "imported": result.imported,
        "skipped": result.skipped,
        "errors": result.errors,
        "ledger_hash": result.ledger_hash,
        "total_balance": result.total_balance,
        "period_id": result.period_id,
        "transactions_count": result.transactions_count,
    })
