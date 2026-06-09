"""AttributionReportEngine — decompose ΔNW into causal components.

Fourth fundamental financial artifact (Phase 13F), complementary to
Balance Sheet, Income Statement, and Cash Flow Statement.

Architecture:
    ReconciledState(prev) + ReconciledState(current) + IncomeStatement
        ↓
    AttributionReportEngine.compute()
        ↓
    AttributionReport

    The report answers: "Why did net worth change from X to Y?"
    It decomposes ΔNW into income, expenses, market appreciation,
    fees, debt effects, and external contributions.

Design rules:
    - Pure computation — no IO, no side effects
    - Works with plain dicts (JSON-safe) and domain objects
    - NW computed from balance sheet identity: NW = Assets - Liabilities
    - Every component maps to specific account codes in the chart of accounts
    - Contributions are computed residually: ΔNW - net_pnl
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional

from core.accounting.models import CHART_OF_ACCOUNTS, AccountSide
from core.reconciled.models import ReconciledState
from core.statements.models import IncomeStatement, StatementEntry


# ── Account code classification ──

# Revenue accounts that represent earned income (salary, freelance, dividends)
INCOME_REVENUE = {"4200", "4300", "4400", "4410", "4420"}
# Revenue accounts from trading/investing activity
TRADING_REVENUE = {"4000", "4100"}
# Fee-type expenses (trading costs, not living costs)
FEE_EXPENSES = {"5000", "5100", "5200", "5400"}
# Interest expense on debt
DEBT_INTEREST_EXPENSE = {"5300"}
# Living/operating expenses
LIVING_EXPENSES = {"5500", "5510", "5520", "5530", "5540",
                   "5550", "5560", "5570", "5580", "5590"}
# Mark-to-market valuation changes
VALUATION_GAIN = {"6000"}
VALUATION_LOSS = {"6100"}


@dataclass(frozen=True)
class AttributionReport:
    """Why net worth changed between two financial states.

    This is the fourth fundamental financial artifact:
        Balance Sheet     → what exists
        Income Statement  → what happened
        Cash Flow         → where liquidity moved
        AttributionReport → why net worth changed

    Components sum to delta_net_worth:
        delta_net_worth = contributions + income - expenses
                        + market_effects - fees - debt_costs
    """
    period_id: str
    prev_period_id: Optional[str]

    net_worth_start: Decimal
    net_worth_end: Decimal
    delta_net_worth: Decimal

    contributions: Decimal
    income: Decimal
    expenses: Decimal
    market_effects: Decimal
    fees: Decimal
    debt_costs: Decimal

    @property
    def net_pnl(self) -> Decimal:
        return self.income - self.expenses + self.market_effects - self.fees - self.debt_costs

    @property
    def accounted_change(self) -> Decimal:
        return self.contributions + self.income - self.expenses + self.market_effects - self.fees - self.debt_costs

    @property
    def reconciliation_check(self) -> Decimal:
        return self.delta_net_worth - self.accounted_change

    def to_dict(self) -> Dict:
        return {
            "period_id": self.period_id,
            "prev_period_id": self.prev_period_id,
            "net_worth_start": str(self.net_worth_start),
            "net_worth_end": str(self.net_worth_end),
            "delta_net_worth": str(self.delta_net_worth),
            "contributions": str(self.contributions),
            "income": str(self.income),
            "expenses": str(self.expenses),
            "market_effects": str(self.market_effects),
            "fees": str(self.fees),
            "debt_costs": str(self.debt_costs),
            "accounted_change": str(self.accounted_change),
            "reconciliation_check": str(self.reconciliation_check),
        }


class AttributionReportEngine:
    """Pure engine: decompose ΔNW into causal components.

    Inputs: two ReconciledStates + IncomeStatement for the intervening period.
    Output: AttributionReport with named components.

    Components are derived from P&L account codes and balance deltas:
        Income            — Revenue accounts (Salary, Freelance, Dividend, Interest, Other)
        Expenses          — Expense accounts (Housing, Food, Transport, etc.)
        Market Appreciation — Trading PnL + Valuation changes + Funding PnL
        Fees              — Trading fees, Funding fees, Slippage, Transfer fees
        Debt Effects      — Interest expense on debt
        Contributions     — Residual (ΔNW - net_pnl): capital added or withdrawn
    """

    @staticmethod
    def compute(
        prev_state: ReconciledState,
        current_state: ReconciledState,
        income_statement: IncomeStatement,
    ) -> AttributionReport:
        # Net worth from balance sheet identity: Assets - Liabilities
        nw_start = prev_state.total_assets - prev_state.total_liabilities
        nw_end = current_state.total_assets - current_state.total_liabilities
        delta_nw = nw_end - nw_start

        income = Decimal("0")
        market_effects = Decimal("0")
        expenses = Decimal("0")
        fees = Decimal("0")
        debt_costs = Decimal("0")

        for entry in income_statement.revenue:
            if entry.account_code in INCOME_REVENUE:
                income += entry.amount
            elif entry.account_code in TRADING_REVENUE:
                market_effects += entry.amount
            elif entry.account_code in VALUATION_GAIN:
                market_effects += entry.amount
            elif entry.account_code in VALUATION_LOSS:
                market_effects -= entry.amount

        for entry in income_statement.expenses:
            if entry.account_code in LIVING_EXPENSES:
                expenses += entry.amount
            elif entry.account_code in FEE_EXPENSES:
                fees += entry.amount
            elif entry.account_code in DEBT_INTEREST_EXPENSE:
                debt_costs += entry.amount
            elif entry.account_code in VALUATION_LOSS:
                market_effects -= entry.amount

        # Contributions capture equity changes outside P&L
        net_pnl = income_statement.net_pnl
        contributions = delta_nw - net_pnl

        return AttributionReport(
            period_id=current_state.period_id,
            prev_period_id=prev_state.period_id,
            net_worth_start=nw_start,
            net_worth_end=nw_end,
            delta_net_worth=delta_nw,
            contributions=contributions,
            income=income,
            expenses=expenses,
            market_effects=market_effects,
            fees=fees,
            debt_costs=debt_costs,
        )

    @staticmethod
    def compute_from_dicts(
        prev_state: Dict,
        current_state: Dict,
        income_statement: Dict,
    ) -> AttributionReport:
        """Same as compute(), but takes plain dicts (JSON-deserialized)."""
        prev = ReconciledState.from_dict(prev_state)
        curr = ReconciledState.from_dict(current_state)
        is_ = IncomeStatement(
            period_id=income_statement["period_id"],
            revenue=[
                StatementEntry(**e) for e in income_statement.get("revenue", [])
            ],
            expenses=[
                StatementEntry(**e) for e in income_statement.get("expenses", [])
            ],
            total_revenue=Decimal(income_statement.get("total_revenue", "0")),
            total_expenses=Decimal(income_statement.get("total_expenses", "0")),
            net_pnl=Decimal(income_statement.get("net_pnl", "0")),
        )
        return AttributionReportEngine.compute(prev, curr, is_)
