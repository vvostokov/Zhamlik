# RFC: AttributionReport — Canonical Contract

## Status: ACCEPTED
## Phase: 13F (final)

---

## 1. Purpose

Define the canonical decomposition of ΔNet Worth into causal components.
This is the **fourth fundamental financial artifact** — complementary to
Balance Sheet, Income Statement, and Cash Flow Statement.

Contributions, Income, Expenses, Market Effects, Fees, Debt Costs.
These six components fully explain any change in net worth.

---

## 2. Architectural Rule

> New layers improve precision of existing artifacts.
> They do not redefine the artifacts themselves.

AttributionReport is stable from Phase 13F onward.
Later phases (Position Layer, Provenance Layer) compute its components
more precisely but never change its shape.

---

## 3. Invariant

```
delta_net_worth = contributions
                 + income
                 - expenses
                 + market_effects
                 - fees
                 - debt_costs
```

`reconciliation_check = delta_net_worth - accounted_change == 0`

---

## 4. Component Contract

### 4.1 contributions
External capital added or withdrawn.
**Source:** `ReconciledState.equity_delta − IncomeStatement.net_income`
**No Position Layer dependency.** Detected at account level.

### 4.2 income
All revenue: salary, trading, dividends, staking, interest, freelance.
**Source:** `IncomeStatement.revenue`
**Canonical value:** sum of all revenue accounts.
**Optional breakdown** (never changes the artifact):

```json
"income": { "total": "5000", "breakdown": { "salary": "4000", "dividends": "700", "staking": "300" } }
```

### 4.3 expenses
All outflows: living, trading costs, interest, fees.
**Source:** `IncomeStatement.expenses`
**Canonical value:** sum of all expense accounts.
**Optional breakdown** (never changes the artifact):

```json
"expenses": { "total": "1500", "breakdown": { "housing": "800", "food": "400", "other": "300" } }
```

### 4.4 market_effects
Net change from asset price movement, trading, currency revaluation.
**Source (Phase 13F):** `IncomeStatement.net_trading` + valuation adjustments
**Source (Phase 14+):** `realized_pnl + unrealized_pnl + fx_effects`
**Canonical value:** always a single scalar.
**Position Layer extension** (never changes the artifact):

```json
"market_effects": { "total": "4200", "breakdown": { "realized_pnl": "1200", "unrealized_pnl": "2500", "fx_effects": "500" } }
```

### 4.5 fees
Transaction fees, trading fees, transfer fees, slippage.
**Source:** `IncomeStatement.expenses` filtered by fee account codes

### 4.6 debt_costs
Interest on liabilities: loans, margin, credit cards.
**Source:** `IncomeStatement.expenses` filtered by debt account codes

---

## 5. Serialization Contract (`to_dict()`)

```json
{
  "period_id": "P2",
  "prev_period_id": "P1",
  "net_worth_start": "100000",
  "net_worth_end": "108200",
  "delta_net_worth": "8200",

  "contributions": "2000",
  "income": "5000",
  "expenses": "-1500",
  "market_effects": "4200",
  "fees": "-300",
  "debt_costs": "-200",

  "accounted_change": "8200",
  "reconciliation_check": "0"
}
```

Extensions (additive, never required):

```json
{
  "income": {
    "total": "5000",
    "breakdown": { "salary": "4000", "dividends": "700", "staking": "300" }
  },
  "market_effects": {
    "total": "4200",
    "breakdown": { "realized_pnl": "1200", "unrealized_pnl": "2500", "fx_effects": "500" }
  }
}
```

---

## 6. Phase Map

| Phase | What changes | Artifact stability |
|-------|-------------|-------------------|
| **13F** | Canonical contract defined | ✅ Stable |
| **13G** | Asset Drilldown reads artifact | ✅ Unchanged |
| **14** | Position Layer computes `market_effects.breakdown` | ✅ Unchanged |
| **15** | Provenance Layer traces field origins | ✅ Unchanged |

---

## 7. Clarifications

1. **`income` and `expenses` are flat** at top level. No operating/investment split.
   Detail lives in `.breakdown` sub-object, which is always optional.
2. **`market_effects` replaces `market_appreciation`** because it includes
   trading PnL, dividends, and all price-driven change — not just appreciation.
3. **Position Layer (Phase 14) fills `market_effects.breakdown`**, not the artifact itself.
4. **Asset Drilldown (Phase 13G) reads the artifact**, not the source data.
5. **Reconciliation is invariant** — not an observation, not a derived field.
