"""Unit tests for traceability functions — decompose artifact fields to sources."""

from decimal import Decimal

from core.trace import (
    trace_attribution,
    trace_balance_sheet,
    trace_cash_flow,
    trace_consistency,
    trace_income_statement,
)
from core.attribution.report_engine import AttributionReport
from core.consistency.validator import ConsistencyCheck, ConsistencyReport
from core.statements.models import BalanceSheet, CashFlowStatement, IncomeStatement, StatementEntry


def _bs(**kwargs) -> BalanceSheet:
    defaults = dict(period_id="P1", assets=[], liabilities=[], equity=[])
    defaults.update(kwargs)
    return BalanceSheet(**defaults)


def _is(**kwargs) -> IncomeStatement:
    defaults = dict(period_id="P1", revenue=[], expenses=[])
    defaults.update(kwargs)
    return IncomeStatement(**defaults)


def _cf(**kwargs) -> CashFlowStatement:
    defaults = dict(period_id="P1", operating=[], investing=[], financing=[])
    defaults.update(kwargs)
    return CashFlowStatement(**defaults)


class TestTraceBalanceSheet:

    def test_trace_net_worth(self):
        bs = _bs(
            assets=[StatementEntry("1000", "Cash", "asset", Decimal("200000"))],
            liabilities=[StatementEntry("2000", "Loan", "liability", Decimal("50000"))],
            total_assets=Decimal("200000"),
            total_liabilities=Decimal("50000"),
            total_equity=Decimal("150000"),
        )
        t = trace_balance_sheet(bs, "net_worth")
        assert t is not None
        assert t["field"] == "net_worth"
        assert t["value"] == "150000"
        assert len(t["depends_on"]) == 2

    def test_trace_total_assets(self):
        bs = _bs(
            assets=[
                StatementEntry("1000", "Cash", "asset", Decimal("50000")),
                StatementEntry("1100", "Crypto", "asset", Decimal("150000")),
            ],
            total_assets=Decimal("200000"),
        )
        t = trace_balance_sheet(bs, "total_assets")
        assert t is not None
        assert t["value"] == "200000"
        assert len(t["depends_on"]) == 2

    def test_trace_unknown_field(self):
        bs = _bs()
        assert trace_balance_sheet(bs, "nonexistent") is None


class TestTraceIncomeStatement:

    def test_trace_net_pnl(self):
        is_ = _is(total_revenue=Decimal("10000"), total_expenses=Decimal("3000"), net_pnl=Decimal("7000"))
        t = trace_income_statement(is_, "net_pnl")
        assert t is not None
        assert t["value"] == "7000"
        assert len(t["depends_on"]) == 2

    def test_trace_unknown(self):
        assert trace_income_statement(_is(), "xyz") is None


class TestTraceCashFlow:

    def test_trace_net_cash_flow(self):
        cf = _cf(total_operating=Decimal("5000"), total_investing=Decimal("-2000"), total_financing=Decimal("1000"))
        t = trace_cash_flow(cf, "net_cash_flow")
        assert t is not None
        assert t["value"] == "4000"
        assert len(t["depends_on"]) == 3

    def test_trace_operating(self):
        cf = _cf(
            operating=[StatementEntry("4000", "Revenue", "operating", Decimal("5000"))],
            total_operating=Decimal("5000"),
        )
        t = trace_cash_flow(cf, "total_operating")
        assert t is not None
        assert t["value"] == "5000"
        assert len(t["depends_on"]) == 1

    def test_trace_unknown(self):
        assert trace_cash_flow(_cf(), "xyz") is None


class TestTraceAttribution:

    def test_trace_delta_nw(self):
        attr = AttributionReport(
            period_id="P2", prev_period_id="P1",
            net_worth_start=Decimal("100000"), net_worth_end=Decimal("108200"),
            delta_net_worth=Decimal("8200"),
            contributions=Decimal("2200"), income=Decimal("5000"), expenses=Decimal("1500"),
            market_effects=Decimal("3000"), fees=Decimal("300"), debt_costs=Decimal("200"),
        )
        t = trace_attribution(attr, "delta_net_worth")
        assert t is not None
        assert t["value"] == "8200"
        assert len(t["depends_on"]) == 6

    def test_trace_component(self):
        attr = AttributionReport(
            period_id="P2", prev_period_id="P1",
            net_worth_start=Decimal("0"), net_worth_end=Decimal("5000"),
            delta_net_worth=Decimal("5000"),
            income=Decimal("5000"), expenses=Decimal("0"),
            contributions=Decimal("0"), market_effects=Decimal("0"),
            debt_costs=Decimal("0"), fees=Decimal("0"),
        )
        t = trace_attribution(attr, "income")
        assert t is not None
        assert t["value"] == "5000"

    def test_trace_accounted_change(self):
        attr = AttributionReport(
            period_id="P2", prev_period_id="P1",
            net_worth_start=Decimal("0"), net_worth_end=Decimal("8200"),
            delta_net_worth=Decimal("8200"),
            income=Decimal("5000"), expenses=Decimal("1500"),
            contributions=Decimal("2200"), market_effects=Decimal("3000"),
            debt_costs=Decimal("200"), fees=Decimal("300"),
        )
        t = trace_attribution(attr, "accounted_change")
        assert t is not None
        assert t["value"] == "8200"
        assert len(t["depends_on"]) == 6

    def test_trace_unknown(self):
        attr = AttributionReport(
            period_id="P2", prev_period_id="P1",
            net_worth_start=Decimal("0"), net_worth_end=Decimal("0"),
            delta_net_worth=Decimal("0"),
            income=Decimal("0"), expenses=Decimal("0"),
            contributions=Decimal("0"), market_effects=Decimal("0"),
            debt_costs=Decimal("0"), fees=Decimal("0"),
        )
        assert trace_attribution(attr, "xyz") is None


class TestTraceConsistency:

    def test_trace_score(self):
        report = ConsistencyReport(
            period_id="P1",
            prev_period_id=None,
            checks=[
                ConsistencyCheck(name="check_1", passed=True, expected=Decimal("100"), actual=Decimal("100"), drift=Decimal("0")),
                ConsistencyCheck(name="check_2", passed=True, expected=Decimal("200"), actual=Decimal("200"), drift=Decimal("0")),
                ConsistencyCheck(name="check_3", passed=False, expected=Decimal("300"), actual=Decimal("305"), drift=Decimal("5")),
            ],
        )
        t = trace_consistency(report, "score")
        assert t is not None
        assert t["value"] == "0.6666666666666666666666666667"  # 2/3
        assert len(t["depends_on"]) == 3

    def test_trace_all_passed(self):
        report = ConsistencyReport(period_id="P1", prev_period_id=None, checks=[])
        t = trace_consistency(report, "all_passed")
        assert t is not None
        assert t["value"] == "True"

    def test_trace_unknown(self):
        report = ConsistencyReport(period_id="P1", prev_period_id=None, checks=[])
        assert trace_consistency(report, "xyz") is None


class TestDepthLimit:

    def test_net_worth_depth_limit(self):
        bs = _bs(total_assets=Decimal("100"), total_liabilities=Decimal("30"))
        t = trace_balance_sheet(bs, "net_worth", depth=999)
        assert t is None


# ── TraceContext / Artifact Trace (15B) ──


class TestTraceContext:

    def test_artifact_trace_net_worth(self):
        from core.trace import TraceContext
        ctx = TraceContext(
            period_id="P2",
            prev_period_id="P1",
            balance_sheet=_bs(total_assets=Decimal("100")),
            attribution=None,
        )
        result = ctx.artifact_trace("net_worth")
        assert result is not None
        assert len(result) == 1
        assert result[0] == {"artifact": "balance_sheet", "field": "net_worth"}

    def test_artifact_trace_net_worth_with_both(self):
        from core.trace import TraceContext
        from core.attribution.report_engine import AttributionReport
        attr = AttributionReport(
            period_id="P2", prev_period_id="P1",
            net_worth_start=Decimal("0"), net_worth_end=Decimal("100"),
            delta_net_worth=Decimal("100"),
            income=Decimal("0"), expenses=Decimal("0"),
            contributions=Decimal("0"), market_effects=Decimal("0"),
            debt_costs=Decimal("0"), fees=Decimal("0"),
        )
        ctx = TraceContext(
            period_id="P2",
            balance_sheet=_bs(total_assets=Decimal("100")),
            attribution=attr,
        )
        result = ctx.artifact_trace("net_worth")
        assert result is not None
        assert len(result) == 2
        artifacts = {r["artifact"] for r in result}
        assert artifacts == {"balance_sheet", "attribution"}

    def test_artifact_trace_unknown_field(self):
        from core.trace import TraceContext
        ctx = TraceContext(period_id="P1")
        assert ctx.artifact_trace("nonexistent") is None

    def test_artifact_trace_delta_net_worth(self):
        from core.trace import TraceContext
        from core.consistency.validator import ConsistencyCheck, ConsistencyReport
        cons = ConsistencyReport(
            period_id="P2", prev_period_id="P1",
            checks=[
                ConsistencyCheck(name="bs_vs_attribution", passed=True,
                                 expected=Decimal("100"), actual=Decimal("100"), drift=Decimal("0")),
            ],
        )
        from core.attribution.report_engine import AttributionReport
        attr = AttributionReport(
            period_id="P2", prev_period_id="P1",
            net_worth_start=Decimal("0"), net_worth_end=Decimal("100"),
            delta_net_worth=Decimal("100"),
            income=Decimal("0"), expenses=Decimal("0"),
            contributions=Decimal("0"), market_effects=Decimal("0"),
            debt_costs=Decimal("0"), fees=Decimal("0"),
        )
        ctx = TraceContext(period_id="P2", attribution=attr, consistency=cons)
        result = ctx.artifact_trace("delta_net_worth")
        assert result is not None
        assert len(result) == 2

    def test_artifact_trace_source_missing(self):
        """Artifact returns None if none of its source artifacts are available."""
        from core.trace import TraceContext
        ctx = TraceContext(period_id="P2")
        result = ctx.artifact_trace("net_worth")
        assert result is None or len(result) == 0
