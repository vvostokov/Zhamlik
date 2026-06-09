"""Unit tests for CrossArtifactValidator.

Invariants tested:
    1. ΔNW(BS) ≡ Attribution.delta_net_worth
    2. ΔNW(BS) ≡ NetPnL(IS) + total_financing(CF)
    3. Attribution component sum ≡ ΔNW
    4. BS: A - L ≡ E
    5. NetPnL ≈ OperatingCF (drift tracked)
"""

from decimal import Decimal

from core.attribution.report_engine import AttributionReport
from core.consistency.validator import CrossArtifactValidator, _make_state
from core.statements.models import CashFlowStatement, IncomeStatement


def _is(
    period_id: str,
    revenue: Decimal = Decimal("0"),
    expenses: Decimal = Decimal("0"),
    net_pnl: Decimal = Decimal("0"),
) -> IncomeStatement:
    return IncomeStatement(
        period_id=period_id,
        total_revenue=revenue,
        total_expenses=expenses,
        net_pnl=net_pnl,
    )


def _cf(
    period_id: str,
    operating: Decimal = Decimal("0"),
    investing: Decimal = Decimal("0"),
    financing: Decimal = Decimal("0"),
) -> CashFlowStatement:
    return CashFlowStatement(
        period_id=period_id,
        total_operating=operating,
        total_investing=investing,
        total_financing=financing,
    )


def _attr(
    period_id: str,
    delta_nw: Decimal = Decimal("0"),
    income: Decimal = Decimal("0"),
    expenses: Decimal = Decimal("0"),
    market_eff: Decimal = Decimal("0"),
    fees: Decimal = Decimal("0"),
    debt: Decimal = Decimal("0"),
    contributions: Decimal = Decimal("0"),
    nw_start: Decimal = Decimal("0"),
    nw_end: Decimal = Decimal("0"),
) -> AttributionReport:
    return AttributionReport(
        period_id=period_id,
        prev_period_id="prev",
        net_worth_start=nw_start,
        net_worth_end=nw_end,
        delta_net_worth=delta_nw,
        contributions=contributions,
        income=income,
        expenses=expenses,
        market_effects=market_eff,
        fees=fees,
        debt_costs=debt,
    )


def test_all_checks_pass_consistent():
    """All 5 checks pass when artifacts agree."""
    prev = _make_state("150000", "50000", "100000", period_id="P1")
    curr = _make_state("158200", "50000", "108200", period_id="P2")
    is_ = _is("P2", revenue=Decimal("8000"), expenses=Decimal("2000"), net_pnl=Decimal("6000"))
    cf = _cf("P2", operating=Decimal("6000"), financing=Decimal("2200"))
    attr = _attr("P2", delta_nw=Decimal("8200"),
                 income=Decimal("5000"), expenses=Decimal("1500"),
                 market_eff=Decimal("3000"), fees=Decimal("300"),
                 debt=Decimal("200"), contributions=Decimal("2200"),
                 nw_start=Decimal("100000"), nw_end=Decimal("108200"))

    r = CrossArtifactValidator.validate(prev, curr, is_, cf, attr)
    assert r.all_passed, f"Violations: {r.violations}"
    assert r.score == Decimal("1.0")
    assert len(r.checks) == 5


def test_bs_attribution_mismatch():
    """Check 1 fails when BS and Attribution disagree on ΔNW."""
    prev = _make_state("150000", "50000", "100000", period_id="P1")
    curr = _make_state("158200", "50000", "108200", period_id="P2")
    is_ = _is("P2", net_pnl=Decimal("6000"))
    cf = _cf("P2", operating=Decimal("5800"), financing=Decimal("2200"))
    attr = _attr("P2", delta_nw=Decimal("7000"), contributions=Decimal("-400"),
                 nw_start=Decimal("100000"), nw_end=Decimal("107000"))

    r = CrossArtifactValidator.validate(prev, curr, is_, cf, attr)
    assert not r.checks[0].passed
    assert r.checks[0].name == "bs_vs_attribution"
    assert r.checks[0].drift > Decimal("0")


def test_bs_internal_mismatch():
    """Check 4 fails when A - L != E."""
    prev = _make_state("100000", "20000", "80000", period_id="P1")
    curr = _make_state("150000", "50000", "95000", period_id="P2")  # should be 100000
    is_ = _is("P2")
    cf = _cf("P2")
    attr = _attr("P2", delta_nw=Decimal("20000"), contributions=Decimal("20000"),
                 nw_start=Decimal("80000"), nw_end=Decimal("100000"))

    r = CrossArtifactValidator.validate(prev, curr, is_, cf, attr)
    c = r.checks[3]
    assert not c.passed
    assert c.drift == Decimal("5000")


def test_attribution_internal_mismatch():
    """Check 3 fails when attribution components don't sum to delta."""
    prev = _make_state("100000", "20000", "80000", period_id="P1")
    curr = _make_state("120000", "20000", "100000", period_id="P2")
    is_ = _is("P2", net_pnl=Decimal("5000"))
    cf = _cf("P2", operating=Decimal("4800"))
    # components sum(5000 income - 0 + 0 - 0 - 0 + 10000 contrib) = 15000, delta=20000
    attr = _attr("P2", delta_nw=Decimal("20000"), income=Decimal("5000"),
                 contributions=Decimal("10000"),
                 nw_start=Decimal("80000"), nw_end=Decimal("100000"))

    r = CrossArtifactValidator.validate(prev, curr, is_, cf, attr)
    c = r.checks[2]
    assert not c.passed
    assert c.drift == Decimal("5000")


def test_pnl_vs_financing_mismatch():
    """Check 2 fails when ΔNW ≠ NetPnL + Financing."""
    prev = _make_state("150000", "50000", "100000", period_id="P1")
    curr = _make_state("160000", "50000", "110000", period_id="P2")
    is_ = _is("P2", net_pnl=Decimal("6000"))
    cf = _cf("P2", financing=Decimal("0"))
    # ΔNW=10000 vs NetPnL(6000)+Financing(0)=6000 → drift=4000
    attr = _attr("P2", delta_nw=Decimal("10000"), income=Decimal("5000"),
                 expenses=Decimal("1500"), market_eff=Decimal("3000"),
                 fees=Decimal("300"), debt=Decimal("200"),
                 contributions=Decimal("4000"),
                 nw_start=Decimal("100000"), nw_end=Decimal("110000"))

    r = CrossArtifactValidator.validate(prev, curr, is_, cf, attr)
    c = r.checks[1]
    assert not c.passed
    assert c.drift == Decimal("4000")


def test_to_dict():
    """ConsistencyReport.to_dict() produces clean structure."""
    prev = _make_state("150000", "50000", "100000", period_id="P1")
    curr = _make_state("158200", "50000", "108200", period_id="P2")
    is_ = _is("P2", net_pnl=Decimal("6000"))
    cf = _cf("P2", operating=Decimal("6000"), financing=Decimal("2200"))
    attr = _attr("P2", delta_nw=Decimal("8200"), income=Decimal("5000"),
                 expenses=Decimal("1500"), market_eff=Decimal("3000"),
                 fees=Decimal("300"), debt=Decimal("200"),
                 contributions=Decimal("2200"),
                 nw_start=Decimal("100000"), nw_end=Decimal("108200"))

    r = CrossArtifactValidator.validate(prev, curr, is_, cf, attr)
    d = r.to_dict()
    assert d["period_id"] == "P2"
    assert d["prev_period_id"] == "P1"
    assert d["all_passed"] is True
    assert len(d["checks"]) == 5
    for c in d["checks"]:
        assert "name" in c
        assert "passed" in c
        assert "drift" in c


def test_zero_drift_rounding():
    """Values within tolerance pass even if not exactly equal."""
    prev = _make_state("100000", "0", "100000", period_id="P1")
    curr = _make_state("110000.01", "0", "110000", period_id="P2")
    is_ = _is("P2", net_pnl=Decimal("10000"))
    cf = _cf("P2", operating=Decimal("10000"))
    attr = _attr("P2", delta_nw=Decimal("10000"), income=Decimal("10000"),
                 nw_start=Decimal("100000"), nw_end=Decimal("110000"))

    r = CrossArtifactValidator.validate(prev, curr, is_, cf, attr)
    c = r.checks[3]
    assert c.passed, f"bs_internal should pass within tolerance, drift={c.drift}"


def test_validate_from_dicts():
    """validate_from_dicts matches validate output."""
    prev = _make_state("150000", "50000", "100000", period_id="P1")
    curr = _make_state("158200", "50000", "108200", period_id="P2")
    is_ = _is("P2", net_pnl=Decimal("6000"))
    cf = _cf("P2", operating=Decimal("6000"), financing=Decimal("2200"))
    attr = _attr("P2", delta_nw=Decimal("8200"), income=Decimal("5000"),
                 expenses=Decimal("1500"), market_eff=Decimal("3000"),
                 fees=Decimal("300"), debt=Decimal("200"),
                 contributions=Decimal("2200"),
                 nw_start=Decimal("100000"), nw_end=Decimal("108200"))

    r1 = CrossArtifactValidator.validate(prev, curr, is_, cf, attr)
    r2 = CrossArtifactValidator.validate_from_dicts(
        prev.to_dict(), curr.to_dict(),
        is_.__dict__, cf.__dict__, attr.to_dict(),
    )
    assert r1.all_passed == r2.all_passed
    assert r1.score == r2.score
    assert len(r1.checks) == len(r2.checks)
    for c1, c2 in zip(r1.checks, r2.checks):
        assert c1.passed == c2.passed
        assert c1.drift == c2.drift
