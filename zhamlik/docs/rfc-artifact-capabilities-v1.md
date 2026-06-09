# RFC: Artifact Capabilities v1 — `validate()` and `trace()`

## Status: DRAFT
## Domain: Zhamlik
## Prerequisites: 6 financial artifacts (model complete)

---

## 1. Purpose

Define exactly two capabilities that every artifact supports.

```
validate()   — "Can I trust the model?"
trace()      — "Show me the path to this number."
```

No `explain()`. No `provenance()`.
No separate "Capability Layer" with its own entities.
Two methods, one contract each.

---

## 2. Capability: `validate()`

Already exists in `core/consistency/validator.py`.

```python
ConsistencyReport = artifact.validate()
```

Returns checks with:
```
{name, passed, expected, actual, drift}
```

Stable. No changes needed.

---

## 3. Capability: `trace()`

One method, two modes — same return shape.

```python
artifact.trace(field: str, mode: str = "dependency", depth: int = 3)
```

### 3.1 Mode: `dependency`

**Direction:** field → what it depends on.

```
balance_sheet.trace("net_worth", mode="dependency")

→ {
    "field": "net_worth",
    "value": "125000",
    "depends_on": [
      {"field": "total_assets", "value": "150000",
       "depends_on": [{"field": "1100 (BTC)", "value": "50000"}]},
      {"field": "total_liabilities", "value": "25000",
       "depends_on": [{"field": "2000 (Loan)", "value": "25000"}]},
    ]
  }
```

This is what `core/trace.py` currently returns (with `components` → `depends_on`).

### 3.2 Mode: `lineage`

**Direction:** field → where it came from.

```
balance_sheet.trace("1100 (BTC)", mode="lineage")

→ {
    "field": "1100 (BTC)",
    "value": "50000",
    "lineage": [
      {
        "event_id": "evt_001",
        "event_type": "RECONCILED_STATE",
        "timestamp": 1700000000000000000,
        "amount": "50000",
        "source": "exchange.csv:142"
      }
    ]
  }
```

This is new. Requires Event Store access.

---

## 4. Return Shape

Both modes return the same node structure:

```python
{
    "field": str,        # field name
    "value": str,        # current value (JSON-safe)
    # Exactly one of:
    "depends_on": [...]  # dependency mode
    "lineage": [...]     # lineage mode
}
```

One node shape. Two graph directions.

---

## 5. Implementation State

| Artifact | `validate()` | `trace(mode=dependency)` | `trace(mode=lineage)` |
|----------|-------------|--------------------------|----------------------|
| BalanceSheet | ✅ | ✅ (needs `components`→`depends_on`) | ❌ |
| IncomeStatement | ✅ | ✅ (same rename) | ❌ |
| CashFlow | ✅ | ✅ (same rename) | ❌ |
| Attribution | ✅ | ✅ (same rename) | ❌ |
| Consistency | ✅ (self-validate) | ✅ | ❌ |

---

## 6. What Trace Currently Returns

`core/trace.py` today:

```json
{"field": "net_worth", "value": "125000",
 "formula": "total_assets - total_liabilities",
 "artifacts": ["balance_sheet"],
 "components": [...]}
```

What it should return after refactor:

```json
{"field": "net_worth", "value": "125000",
 "depends_on": [...]}
```

Changes: `components` → `depends_on`, remove `formula`/`artifacts`.

---

## 7. Next Steps After This RFC

1. Refactor `core/trace.py`: `components` → `depends_on`, remove formula/artifacts
2. Implement `trace(mode="lineage")` — Event Store query by account_code + source metadata
3. Update `financial_api.py` trace handler to use new shape
4. Update tests

Then consumers (AssetDrilldown, Cockpit) can be built on a stable capability layer.
