"""CrossArtifactValidator — global consistency checks across BS, IS, CF, Attribution.

Phase 14: no new computation, only validation.
Every check compares two independently-built artifacts for agreement.

Invariants implemented:
    1. ΔNW(BS) ≡ Attribution.delta_net_worth
    2. ΔNW(BS) ≡ NetPnL(IS) + total_financing(CF)
    3. Attribution components ≡ ΔNW (internal)
    4. BS internal: A - L ≡ E
    5. IS + CF: NetPnL(IS) ≈ total_operating(CF)

Design rules:
    - Pure computation — no IO, no side effects
    - Consumes domain objects (ReconciledState, IncomeStatement, etc.), not flat params
    - Every check produces a ConsistencyCheck with passed/expected/actual/drift
    - No artifact is treated as authoritative — all are cross-validated
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional

from core.attribution.report_engine import AttributionReport
from core.reconciled.models import ReconciledState
from core.statements.models import CashFlowStatement, IncomeStatement


DRIFT_TOLERANCE = Decimal("0.01")


@dataclass(frozen=True)
class ConsistencyCheck:
    """Result of a single cross-artifact invariant check."""
    name: str
    passed: bool
    expected: Decimal
    actual: Decimal
    drift: Decimal

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "passed": self.passed,
            "expected": str(self.expected),
            "actual": str(self.actual),
            "drift": str(self.drift),
        }


@dataclass(frozen=True)
class ConsistencyReport:
    """Complete consistency report for a single period.

    Checks all four fundamental artifacts for agreement at the invariant level.
    Score (0..1) measures overall consistency health.
    """
    period_id: str
    prev_period_id: Optional[str]
    checks: List[ConsistencyCheck] = field(default_factory=list)

    @property
    def score(self) -> Decimal:
        if not self.checks:
            return Decimal("1.0")
        passed = sum(1 for c in self.checks if c.passed)
        return Decimal(str(passed)) / Decimal(str(len(self.checks)))

    @property
    def all_passed(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def violations(self) -> List[ConsistencyCheck]:
        return [c for c in self.checks if not c.passed]

    def to_dict(self) -> Dict:
        return {
            "period_id": self.period_id,
            "prev_period_id": self.prev_period_id,
            "score": str(self.score),
            "all_passed": self.all_passed,
            "checks": [c.to_dict() for c in self.checks],
        }


class CrossArtifactValidator:
    """Validate consistency across all four financial artifacts.

    Inputs (domain objects):
        prev_state, current_state  — ReconciledState (BS)
        income_statement           — IncomeStatement
        cash_flow                  — CashFlowStatement
        attribution_report         — AttributionReport

    Returns ConsistencyReport with per-check results.
    """

    @staticmethod
    def validate(
        prev_state: ReconciledState,
        current_state: ReconciledState,
        income_statement: IncomeStatement,
        cash_flow: CashFlowStatement,
        attribution_report: AttributionReport,
        tolerance: Decimal = DRIFT_TOLERANCE,
    ) -> ConsistencyReport:
        nw_prev = prev_state.total_assets - prev_state.total_liabilities
        nw_curr = current_state.total_assets - current_state.total_liabilities
        delta_nw_bs = nw_curr - nw_prev

        checks: List[ConsistencyCheck] = []

        # ── 1. ΔNW(BS) ≡ Attribution.delta_net_worth ──
        drift = abs(delta_nw_bs - attribution_report.delta_net_worth)
        checks.append(ConsistencyCheck(
            name="bs_vs_attribution",
            passed=drift <= tolerance,
            expected=delta_nw_bs,
            actual=attribution_report.delta_net_worth,
            drift=drift,
        ))

        # ── 2. ΔNW(BS) ≡ NetPnL(IS) + total_financing(CF) ──
        implied_delta = income_statement.net_pnl + cash_flow.total_financing
        drift = abs(delta_nw_bs - implied_delta)
        checks.append(ConsistencyCheck(
            name="bs_vs_pnl_plus_financing",
            passed=drift <= tolerance,
            expected=delta_nw_bs,
            actual=implied_delta,
            drift=drift,
        ))

        # ── 3. Attribution components ≡ ΔNW (internal consistency) ──
        component_sum = attribution_report.accounted_change
        drift = abs(component_sum - attribution_report.delta_net_worth)
        checks.append(ConsistencyCheck(
            name="attribution_internal",
            passed=drift <= tolerance,
            expected=attribution_report.delta_net_worth,
            actual=component_sum,
            drift=drift,
        ))

        # ── 4. BS internal: A - L ≡ Equity ──
        computed_equity = current_state.total_assets - current_state.total_liabilities
        drift = abs(computed_equity - current_state.total_equity)
        checks.append(ConsistencyCheck(
            name="bs_internal",
            passed=drift <= tolerance,
            expected=computed_equity,
            actual=current_state.total_equity,
            drift=drift,
        ))

        # ── 5. IS + CF: NetPnL ≈ total_operating ──
        drift = abs(income_statement.net_pnl - cash_flow.total_operating)
        checks.append(ConsistencyCheck(
            name="pnl_vs_operating_cf",
            passed=drift <= tolerance,
            expected=income_statement.net_pnl,
            actual=cash_flow.total_operating,
            drift=drift,
        ))

        return ConsistencyReport(
            period_id=current_state.period_id,
            prev_period_id=prev_state.period_id,
            checks=checks,
        )

    @staticmethod
    def validate_from_dicts(
        prev_state: Dict,
        current_state: Dict,
        income_statement: Dict,
        cash_flow: Dict,
        attribution_report: Dict,
    ) -> ConsistencyReport:
        """Same as validate(), but takes plain dicts (JSON-deserialized)."""
        prev = ReconciledState.from_dict(prev_state)
        curr = ReconciledState.from_dict(current_state)

        is_ = IncomeStatement(
            period_id=income_statement["period_id"],
            revenue=income_statement.get("revenue", []),
            expenses=income_statement.get("expenses", []),
            total_revenue=Decimal(income_statement.get("total_revenue", "0")),
            total_expenses=Decimal(income_statement.get("total_expenses", "0")),
            net_pnl=Decimal(income_statement.get("net_pnl", "0")),
        )

        cf = CashFlowStatement(
            period_id=cash_flow["period_id"],
            previous_period_id=cash_flow.get("previous_period_id"),
            operating=cash_flow.get("operating", []),
            investing=cash_flow.get("investing", []),
            financing=cash_flow.get("financing", []),
            total_operating=Decimal(cash_flow.get("total_operating", "0")),
            total_investing=Decimal(cash_flow.get("total_investing", "0")),
            total_financing=Decimal(cash_flow.get("total_financing", "0")),
        )

        attr = AttributionReport(
            period_id=attribution_report["period_id"],
            prev_period_id=attribution_report.get("prev_period_id"),
            net_worth_start=Decimal(attribution_report.get("net_worth_start", "0")),
            net_worth_end=Decimal(attribution_report.get("net_worth_end", "0")),
            delta_net_worth=Decimal(attribution_report.get("delta_net_worth", "0")),
            income=Decimal(attribution_report.get("income", "0")),
            expenses=Decimal(attribution_report.get("expenses", "0")),
            contributions=Decimal(attribution_report.get("contributions", "0")),
            market_effects=Decimal(attribution_report.get("market_effects", "0")),
            debt_costs=Decimal(attribution_report.get("debt_costs", "0")),
            fees=Decimal(attribution_report.get("fees", "0")),
        )

        return CrossArtifactValidator.validate(prev, curr, is_, cf, attr)


def _make_state(
    assets: str, liabilities: str, equity: str,
    period_id: str = "p1",
) -> ReconciledState:
    """Helper: build a ReconciledState with given balance totals and no account-level balances."""
    from core.reconciled.models import ConfidenceLevel

    return ReconciledState(
        period_id=period_id,
        balances={},
        total_assets=Decimal(assets),
        total_liabilities=Decimal(liabilities),
        total_equity=Decimal(equity),
        total_divergence=Decimal("0"),
        overall_confidence=ConfidenceLevel.FULLY_RECONCILED,
        fully_reconciled_count=0,
        partially_reconciled_count=0,
        uncertain_count=0,
        unverified_count=0,
        reconciled_at_ns=0,
        state_hash="",
    )
