"""Unit tests for AttributionReportEngine.

Canonical invariant (13F.2):
    delta_net_worth == contributions + income - expenses + market_effects - fees - debt_costs
    reconciliation_check == 0

Test matrix:
    - Invariant: explicit formula assertion for all valid scenarios
    - Full decomposition (income + expenses + appreciation + fees + interest + contributions)
    - Edge: first period (no prev — handled by caller)
    - Edge: zero P&L, only contributions
    - Edge: only market effects
"""

from decimal import Decimal

from core.reconciled.models import ReconciledState, ReconciledBalance, ConfidenceLevel
from core.statements.models import IncomeStatement, StatementEntry
from core.attribution.report_engine import AttributionReportEngine, AttributionReport


def _rstate(period_id: str, total_assets: str, total_liabilities: str) -> ReconciledState:
    """Create a minimal ReconciledState with given assets and liabilities. NW = assets - liabilities."""
    a = Decimal(total_assets)
    l = Decimal(total_liabilities)
    return ReconciledState(
        period_id=period_id,
        balances={},
        total_assets=a,
        total_liabilities=l,
        total_equity=a - l,
        total_divergence=Decimal("0"),
        overall_confidence=ConfidenceLevel.FULLY_RECONCILED,
        fully_reconciled_count=0,
        partially_reconciled_count=0,
        uncertain_count=0,
        unverified_count=0,
        reconciled_at_ns=0,
        state_hash="",
    )


def _nw(assets: str, liabilities: str) -> str:
    """Shorthand: pass asset/liability strings, get assets - liabilities."""
    return str(Decimal(assets) - Decimal(liabilities))


def _entry(code: str, name: str, cat: str, amount: str) -> StatementEntry:
    return StatementEntry(code, name, cat, Decimal(amount))


# ── Canonical Invariant (13F.2) ──


def test_invariant_formula():
    """ΔNW ≡ contributions + income − expenses + market_effects − fees − debt_costs.

    Explicit assertion of the canonical AttributionReport formula.
    Applies to every valid period — verified with random-like components.
    """
    prev = _rstate("P1", "100000", "40000")    # NW = 60K
    curr = _rstate("P2", "118000", "40000")    # NW = 78K

    is_ = IncomeStatement(
        period_id="P2",
        revenue=[
            _entry("4300", "Salary", "Revenue", "6000"),
            _entry("4000", "TradingPnL", "Revenue", "4000"),
        ],
        expenses=[
            _entry("5510", "Food", "Expense", "2000"),
            _entry("5000", "TradingFee", "Expense", "400"),
            _entry("5300", "Interest", "Expense", "600"),
        ],
        total_revenue=Decimal("10000"),
        total_expenses=Decimal("3000"),
        net_pnl=Decimal("7000"),
    )

    r = AttributionReportEngine.compute(prev, curr, is_)

    # Canonical formula
    component_sum = (r.contributions + r.income - r.expenses
                     + r.market_effects - r.fees - r.debt_costs)
    assert r.delta_net_worth == component_sum, (
        f"ΔNW ({r.delta_net_worth}) != component_sum ({component_sum})")

    # accounted_change is the same as component_sum
    assert r.accounted_change == component_sum

    # reconciliation_check must be exactly 0
    assert r.reconciliation_check == Decimal("0"), (
        f"reconciliation_check = {r.reconciliation_check}")


def test_invariant_zero_activity():
    """No activity at all — all components zero, ΔNW = 0."""
    prev = _rstate("P1", "100000", "50000")
    curr = _rstate("P2", "100000", "50000")

    is_ = IncomeStatement(period_id="P2", revenue=[], expenses=[],
                          total_revenue=Decimal("0"), total_expenses=Decimal("0"),
                          net_pnl=Decimal("0"))

    r = AttributionReportEngine.compute(prev, curr, is_)

    assert r.delta_net_worth == Decimal("0")
    assert r.reconciliation_check == Decimal("0")
    assert r.accounted_change == Decimal("0")


# ── Complete example ──

def test_full_decomposition():
    """All components present: income, expenses, appreciation, fees, debt, contributions."""
    prev = _rstate("P1", "150000", "50000")    # NW = 100K
    curr = _rstate("P2", "158200", "50000")    # NW = 108.2K

    is_ = IncomeStatement(
        period_id="P2",
        revenue=[
            _entry("4300", "Salary", "Revenue", "5000"),
            _entry("4000", "TradingPnL", "Revenue", "3000"),
        ],
        expenses=[
            _entry("5510", "Food", "Expense", "1500"),
            _entry("5000", "TradingFee", "Expense", "300"),
            _entry("5300", "Interest", "Expense", "200"),
        ],
        total_revenue=Decimal("8000"),
        total_expenses=Decimal("2000"),
        net_pnl=Decimal("6000"),
    )

    r = AttributionReportEngine.compute(prev, curr, is_)

    assert r.net_worth_start == Decimal("100000")
    assert r.net_worth_end == Decimal("108200")
    assert r.delta_net_worth == Decimal("8200")

    # Components
    assert r.income == Decimal("5000")       # Salary
    assert r.expenses == Decimal("1500")     # Food
    assert r.market_effects == Decimal("3000")  # TradingPnL
    assert r.fees == Decimal("300")          # TradingFee
    assert r.debt_costs == Decimal("200")  # Interest

    # Contributions = ΔNW - net_pnl = 8200 - 6000 = 2200
    assert r.contributions == Decimal("2200"), f"Expected 2200, got {r.contributions}"

    # Reconciliation
    assert r.reconciliation_check == Decimal("0"), f"Check: {r.reconciliation_check}"


def test_only_contributions():
    """No P&L activity, only external capital added."""
    prev = _rstate("P1", "100000", "50000")    # NW = 50K
    curr = _rstate("P2", "110000", "50000")    # NW = 60K

    is_ = IncomeStatement(period_id="P2", revenue=[], expenses=[],
                          total_revenue=Decimal("0"), total_expenses=Decimal("0"),
                          net_pnl=Decimal("0"))

    r = AttributionReportEngine.compute(prev, curr, is_)

    assert r.income == Decimal("0")
    assert r.expenses == Decimal("0")
    assert r.market_effects == Decimal("0")
    assert r.contributions == Decimal("10000")  # ΔNW = 10000
    assert r.reconciliation_check == Decimal("0")


def test_only_market_effects():
    """NW change comes entirely from asset appreciation."""
    prev = _rstate("P1", "100000", "50000")    # NW = 50K
    curr = _rstate("P2", "115000", "50000")    # NW = 65K

    is_ = IncomeStatement(
        period_id="P2",
        revenue=[_entry("4000", "TradingPnL", "Revenue", "15000")],
        expenses=[],
        total_revenue=Decimal("15000"),
        total_expenses=Decimal("0"),
        net_pnl=Decimal("15000"),
    )

    r = AttributionReportEngine.compute(prev, curr, is_)

    assert r.market_effects == Decimal("15000")
    assert r.income == Decimal("0")
    assert r.expenses == Decimal("0")
    assert r.contributions == Decimal("0")  # ΔNW (15000) - net_pnl (15000) = 0
    assert r.reconciliation_check == Decimal("0")


def test_net_loss():
    """Expenses exceed revenue."""
    prev = _rstate("P1", "150000", "50000")    # NW = 100K
    curr = _rstate("P2", "145000", "50000")    # NW = 95K

    is_ = IncomeStatement(
        period_id="P2",
        revenue=[_entry("4300", "Salary", "Revenue", "2000")],
        expenses=[_entry("5510", "Food", "Expense", "3000"),
                  _entry("5530", "Utilities", "Expense", "1000")],
        total_revenue=Decimal("2000"),
        total_expenses=Decimal("4000"),
        net_pnl=Decimal("-2000"),
    )

    r = AttributionReportEngine.compute(prev, curr, is_)

    assert r.delta_net_worth == Decimal("-5000")  # NW decreased
    assert r.income == Decimal("2000")
    assert r.expenses == Decimal("4000")
    assert r.market_effects == Decimal("0")
    # net_pnl = -2000, ΔNW = -5000 → contributions = -3000 (distributions)
    assert r.contributions == Decimal("-3000"), f"Got {r.contributions}"
    assert r.reconciliation_check == Decimal("0")


def test_valuation_accounts():
    """Valuation (MTM) changes contribute to market appreciation."""
    prev = _rstate("P1", "130000", "50000")    # NW = 80K
    curr = _rstate("P2", "145000", "50000")    # NW = 95K

    is_ = IncomeStatement(
        period_id="P2",
        revenue=[_entry("6000", "MTM_Gain", "Valuation", "15000")],
        expenses=[],
        total_revenue=Decimal("15000"),
        total_expenses=Decimal("0"),
        net_pnl=Decimal("15000"),
    )

    r = AttributionReportEngine.compute(prev, curr, is_)

    assert r.market_effects == Decimal("15000")
    assert r.income == Decimal("0")
    assert r.contributions == Decimal("0")
    assert r.reconciliation_check == Decimal("0")


def test_debt_interest_and_fees():
    """Fees and debt interest are separate from living expenses."""
    prev = _rstate("P1", "150000", "50000")    # NW = 100K
    curr = _rstate("P2", "154000", "50000")    # NW = 104K

    is_ = IncomeStatement(
        period_id="P2",
        revenue=[_entry("4300", "Salary", "Revenue", "6000")],
        expenses=[_entry("5000", "TradingFee", "Expense", "500"),
                  _entry("5300", "Interest", "Expense", "500"),
                  _entry("5510", "Food", "Expense", "1000")],
        total_revenue=Decimal("6000"),
        total_expenses=Decimal("2000"),
        net_pnl=Decimal("4000"),
    )

    r = AttributionReportEngine.compute(prev, curr, is_)

    assert r.income == Decimal("6000")
    assert r.expenses == Decimal("1000")    # Food
    assert r.fees == Decimal("500")         # TradingFee
    assert r.debt_costs == Decimal("500") # Interest
    assert r.market_effects == Decimal("0")
    assert r.contributions == Decimal("0")  # ΔNW (4000) - net_pnl (4000) = 0
    assert r.reconciliation_check == Decimal("0")


def test_to_dict_roundtrip():
    """AttributionReport.to_dict() produces JSON-safe dict."""
    prev = _rstate("P1", "150000", "50000")    # NW = 100K
    curr = _rstate("P2", "158200", "50000")    # NW = 108.2K

    is_ = IncomeStatement(
        period_id="P2",
        revenue=[_entry("4300", "Salary", "Revenue", "5000")],
        expenses=[_entry("5510", "Food", "Expense", "1000")],
        total_revenue=Decimal("5000"),
        total_expenses=Decimal("1000"),
        net_pnl=Decimal("4000"),
    )

    r = AttributionReportEngine.compute(prev, curr, is_)
    d = r.to_dict()

    assert d["period_id"] == "P2"
    assert d["prev_period_id"] == "P1"
    assert d["delta_net_worth"] == "8200"
    assert d["income"] == "5000"
    assert d["expenses"] == "1000"
    assert d["reconciliation_check"] == "0"
    # Contributions = 8200 - 4000 = 4200
    assert d["contributions"] == "4200"
