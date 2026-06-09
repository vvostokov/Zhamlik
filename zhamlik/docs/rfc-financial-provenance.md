# RFC: Financial Provenance — Capability Contract

## Status: DRAFT
## Domain: Zhamlik
## Prerequisite: AttributionReport (13F, canonical)

---

## 1. Purpose

Define Provenance as a **capability of the financial model**,
not an object, entity, or report.

After 13F, every artifact answers "what happened" and "why".
Provenance answers "how do you know?" — for any field,
in any artifact, at any time.

---

## 2. Architectural Rule

> Provenance is a capability, not an entity.

| Domain | Entity | Capability |
|--------|--------|------------|
| Accounting | Ledger | balances |
| Financial | Artifacts | compute |
| Trust | — | trace |

Provenance introduces no new entities.
It only requires every artifact to implement one method:

```python
def trace(self, field: str, depth: int = 3) -> Optional[Dict]:
```

---

## 3. The Atom: Dependency

There is no `TraceResult` class.
There is no `ProvenanceReport` model.

The only structural concept is a **dependency** — one field
depends on other fields, which depend on accounts,
which depend on transactions.

```
Net Worth
  depends_on: [Total Assets, Total Liabilities]

Total Assets
  depends_on: [BTC (1100), ETH (1110), Cash (1000)]

BTC (1100)
  depends_on: [TX-001, TX-002]

TX-001
  depends_on: [import: exchange.csv]
```

Each link in this chain is a `depends_on` relationship.

---

## 4. Canonical Shape

`artifact.trace(field)` returns a **recursive dependency node**:

```json
{
  "field": "net_worth",
  "value": "125000",
  "depends_on": [
    {
      "field": "total_assets",
      "value": "150000",
      "depends_on": [
        { "field": "1100 (BTC)", "value": "50000" },
        { "field": "1110 (ETH)", "value": "30000" },
        { "field": "1000 (Cash)", "value": "70000" }
      ]
    },
    {
      "field": "total_liabilities",
      "value": "25000",
      "depends_on": [
        { "field": "2000 (Loan)", "value": "25000" }
      ]
    }
  ]
}
```

That's it. No wrapper types. No metadata envelope.
Every node has exactly three fields:
- `field` — name
- `value` — current value (string, JSON-safe)
- `depends_on` — children (optional, depth-limited)

---

## 5. Artifact Capabilities

### 5.1 BalanceSheet

| Field | depends_on |
|-------|------------|
| `net_worth` | total_assets, total_liabilities |
| `total_assets` | each asset account |
| `total_liabilities` | each liability account |
| `total_equity` | each equity account |
| `<account_code>` | matching transactions (depth ≥ 3) |

### 5.2 IncomeStatement

| Field | depends_on |
|-------|------------|
| `net_pnl` | total_revenue, total_expenses |
| `total_revenue` | each revenue entry |
| `total_expenses` | each expense entry |

### 5.3 CashFlowStatement

| Field | depends_on |
|-------|------------|
| `net_cash_flow` | total_operating, total_investing, total_financing |
| `total_operating` | each operating entry |

### 5.4 AttributionReport

| Field | depends_on |
|-------|------------|
| `delta_net_worth` | contributions, income, expenses, market_effects, fees, debt_costs |
| `income` | revenue entries with INCOME_REVENUE codes |
| `market_effects` | TRADING_REVENUE + VALUATION entries |

### 5.5 ConsistencyReport

| Field | depends_on |
|-------|------------|
| `score` | each check |

---

## 6. Depth Levels

| Depth | Resolution | Example |
|-------|------------|---------|
| 1 | Direct dependencies | `net_worth → [assets, liabilities]` |
| 2 | One level of decomposition | `assets → [BTC, ETH, Cash]` |
| 3 | Account-level entries | `BTC → [TX-001, TX-002]` |
| 4 | Source-level | `TX-001 → [exchange.csv:142]` |

Default: 3. Depth 4 requires explicit opt-in.

---

## 7. What Already Exists

`core/trace.py` already implements this shape
(at depth 1-2, returning a compatible structure):

```python
trace_balance_sheet(bs, "net_worth")
→ {"field": "net_worth", "value": "125000",
   "formula": "total_assets - total_liabilities",
   "components": [...]}
```

The remaining gap is minor — rename `formula` → (remove, kept as note),
rename `components` → `depends_on`, and add transaction-level
resolution at depth 3+.

---

## 8. Comparison with Previous RFC

| Before | After |
|--------|-------|
| `TraceResult` dataclass | No entity — bare dict |
| `AccountRef`, `TransactionRef` | No ref types — flat field/value nodes |
| `SourceRef` | No source model — depth 4 is opt-in |
| `formula` as top-level field | Formula is implicit in `depends_on` shape |
| Separate "Provenance Layer" | Capability distributed across artifacts |

---

## 9. Consequences

1. **No new files.** `core/trace.py` already has the shape.
   No `provenance.py`, no `trace_models.py`.

2. **No new entities.** No `TraceResult`, `TraceNode`,
   `ProvenanceReport`, `TraceGraph`.

3. **Every artifact adds one method.** `trace(field)` returns
   a plain recursive dict. That's the full contract.

4. **UI renders the tree.** The frontend receives a recursive
   `depends_on` tree and renders it as a collapsible trace
   (no server-side formatting).

5. **Consistency is the same capability.** `consistency.trace("score")`
   returns `depends_on: [checks...]`. Same contract.

---

## 10. One-Page Summary

```
artifact.trace(field) → {
  "field": str,
  "value": str,
  "depends_on": [ ... recursive ... ]
}
```

No entities. One capability. Six artifacts.
