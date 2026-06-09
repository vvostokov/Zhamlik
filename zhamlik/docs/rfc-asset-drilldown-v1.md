# RFC: Asset Drilldown Contract (13G)

## Status: DRAFT
## Domain: Zhamlik
## Prerequisite: AttributionReport (13F, canonical)

---

## 1. Purpose

Define AssetDrilldown as a **consumer view** over AttributionReport.

Not a new financial artifact.
Not a computation layer.
A read-only decomposition of AttributionReport components by balance sheet account.

---

## 2. Architectural Rule

AssetDrilldown contains **zero financial logic**.

It does not compute ΔNW.
It does not classify P&L entries.
It reads attribution components and Balance Sheet accounts,
then groups them by asset.

```
AttributionReport  +  BalanceSheet (prev + current)
        ↓                    ↓
        └──── AssetDrilldown ────┘
                    ↓
            per-asset rows
```

---

## 3. Canonical Contract

```python
@dataclass(frozen=True)
class AssetDrilldownRow:
    account_code: str
    account_name: str

    # Balance sheet values
    start_balance: Decimal
    end_balance: Decimal
    delta: Decimal              # end - start

    # Attribution components mapped to this asset
    market_effects: Decimal     # price-driven change
    contributions: Decimal      # capital added / withdrawn
    income: Decimal             # dividends, staking, interest from this asset

    # Position Layer extension (Phase 14, optional)
    position_breakdown: Optional[Dict] = None
```

### 3.1 What each component means per asset

| Component | Source | Example |
|-----------|--------|---------|
| `delta` | `end_balance − start_balance` | BTC 50000 → 55000 = +5000 |
| `market_effects` | delta attributable to price change | BTC price rose → +5000 |
| `contributions` | delta attributable to capital flow | bought 0.1 BTC → +1000 |
| `income` | P&L entries tagged to this asset | ETH staking → +300 |

### 3.2 Constraints

```
delta = market_effects + contributions + income
sum(delta for all assets) = asset_side_delta(BS)

sum(market_effects) = AttributionReport.market_effects × (asset_delta / nw_delta)
```

The second constraint is approximate pre-Position Layer.
When Position Layer (14) arrives, `market_effects` splits into
`realized_pnl + unrealized_pnl + fx_effects` per asset,
but the contract does not change.

---

## 4. Serialization

```json
{
  "period_id": "P2",
  "drilldown": [
    {
      "account_code": "1100",
      "account_name": "BTC",
      "start_balance": "50000",
      "end_balance": "55000",
      "delta": "5000",
      "market_effects": "5000",
      "contributions": "0",
      "income": "0"
    },
    {
      "account_code": "1110",
      "account_name": "ETH",
      "start_balance": "30000",
      "end_balance": "32000",
      "delta": "2000",
      "market_effects": "1000",
      "contributions": "1000",
      "income": "0"
    },
    {
      "account_code": "1000",
      "account_name": "Cash",
      "start_balance": "20000",
      "end_balance": "19200",
      "delta": "-800",
      "market_effects": "0",
      "contributions": "-800",
      "income": "0"
    }
  ],
  "summary": {
    "total_asset_delta": "6200",
    "drilldown_market_effects": "6000",
    "attribution_market_effects": "6000",
    "reconciliation": "0"
  }
}
```

---

## 5. Position Layer Forward Contract

When Phase 14 arrives, `position_breakdown` is added:

```json
{
  "account_code": "1100",
  "account_name": "BTC",
  "delta": "5000",
  "market_effects": "5000",
  "position_breakdown": {
    "realized_pnl": "2000",
    "unrealized_pnl": "3000",
    "fx_effects": "0"
  }
}
```

The parent fields (`market_effects`, `delta`) do not change.
Position Layer only fills detail.

---

## 6. What AssetDrilldown is NOT

| ❌ Not this | ✅ Is this |
|-------------|-----------|
| A new financial artifact | A consumer view over two artifacts |
| Computing ΔNW from scratch | Reading AttributionReport |
| Classifying P&L entries | Grouping by account_code |
| Replacing AttributionReport | Showing which assets drove market_effects |

---

## 7. Open Questions

1. Should `income` be per-asset or should AssetDrilldown only show `market_effects` + `contributions`?
   (Income is P&L, not balance sheet — mapping to an asset is approximate.)
2. Should liabilities be included in the drilldown, or is it "assets only"?
3. How to handle `contributions` — currently computed residually at system level.
   Per-asset contributions require tracking capital flows by account.
