import React, { useEffect, useState, useCallback } from 'react'
import { useParams, useNavigate, Link } from 'react-router-dom'
import { getTimelinePeriods, getPeriod } from '../api'
import PeriodOverview from './PeriodOverview'
import PeriodChanges from './PeriodChanges'

function fmt(n) {
  if (n == null) return '—'
  const v = Number(n)
  const abs = Math.abs(v)
  if (abs >= 1_000_000) return '€' + (v / 1_000_000).toFixed(2) + 'M'
  if (abs >= 1_000) return '€' + (v / 1_000).toFixed(1) + 'K'
  return '€' + v.toFixed(2)
}

const TABS = [
  { key: 'overview', label: 'Обзор' },
  { key: 'balance_sheet', label: 'Активы и долги' },
  { key: 'income', label: 'Доходы и расходы' },
  { key: 'cash_flow', label: 'Денежный поток' },
  { key: 'changes', label: 'Изменения' },
  { key: 'attribution', label: 'Изменение капитала' },
  { key: 'consistency', label: 'Проверка данных' },
]

const CATEGORY_ICONS = {
  Asset: '●',
  Liability: '○',
  Equity: '◆',
  Revenue: '▲',
  Expense: '▼',
}

export default function PeriodExplorer() {
  const { periodId } = useParams()
  const navigate = useNavigate()
  const [periods, setPeriods] = useState([])
  const [tab, setTab] = useState('overview')
  const [period, setPeriod] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    getTimelinePeriods()
      .then(ps => {
        setPeriods(ps)
        if (!periodId && ps.length) {
          navigate(`/periods/${ps[ps.length - 1]}`, { replace: true })
        }
      })
      .catch(() => {})
  }, [])

  useEffect(() => {
    if (!periodId) return
    setLoading(true)
    getPeriod(periodId)
      .then(setPeriod)
      .catch(e => setPeriod({ error: e.message }))
      .finally(() => setLoading(false))
  }, [periodId])

  const handlePeriodSelect = useCallback((pid) => {
    navigate(`/periods/${pid}`)
  }, [navigate])

  const rs = period?.reconciled_state || {}
  const totalAssets = Number(rs.total_assets || 0)
  const totalLiabilities = Number(rs.total_liabilities || 0)
  const netWorth = totalAssets - totalLiabilities
  const attr = period?.attribution || {}
  const delta = Number(attr.delta_net_worth || 0)

  return (
    <div className="page period-page">
      <div className="period-layout">
        {/* Sidebar */}
        <div className="period-sidebar">
          <h3>Периоды</h3>
          <div className="period-list">
            {periods.map(pid => {
              const isActive = pid === periodId || (!periodId && pid === periods[periods.length - 1])
              return (
                <div
                  key={pid}
                  className={`period-item ${isActive ? 'active' : ''}`}
                  onClick={() => handlePeriodSelect(pid)}
                >
                  {pid}
                </div>
              )
            })}
          </div>
        </div>

        {/* Main */}
        <div className="period-main">
          {/* Period header */}
          <div className="period-header">
            <h2>{periodId}</h2>
            <div className="period-header-stats">
              <span className={`period-nw ${netWorth >= 0 ? 'up' : 'down'}`}>
                {fmt(netWorth)}
              </span>
              {delta !== 0 && (
                <span className={`period-delta ${delta >= 0 ? 'up' : 'down'}`}>
                  {delta >= 0 ? '↑' : '↓'} {fmt(Math.abs(delta))}
                </span>
              )}
              {period?.consistency?.score > 0 && (
                <span className="period-consistency-score">
                  Данные: {(Number(period.consistency.score) * 100).toFixed(0)}%
                </span>
              )}
            </div>
          </div>

          {/* Tabs */}
          <div className="period-tabs">
            {TABS.map(t => (
              <button
                key={t.key}
                className={`period-tab ${tab === t.key ? 'active' : ''}`}
                onClick={() => setTab(t.key)}
              >
                {t.label}
              </button>
            ))}
          </div>

          {/* Tab content */}
          <div className="period-content">
            {loading && <div className="loading">Загрузка…</div>}
            {!loading && !period && <div className="empty-state">Нет данных для этого периода</div>}
            {!loading && period && tab === 'overview' && (
              <PeriodOverview periodId={periodId} period={period} />
            )}
            {!loading && period && tab === 'balance_sheet' && (
              <RenderBalanceSheet bs={period.balance_sheet} periodId={period.period_id} />
            )}
            {!loading && period && tab === 'income' && (
              <RenderIncomeStatement is={period.income_statement} />
            )}
            {!loading && period && tab === 'cash_flow' && (
              <RenderCashFlow cf={period.cash_flow} />
            )}
            {!loading && period && tab === 'changes' && (
              <PeriodChanges periodId={periodId} period={period} />
            )}
            {!loading && period && tab === 'attribution' && (
              <RenderAttribution attr={attr} reasons={period.reasons} />
            )}
            {!loading && period && tab === 'consistency' && (
              <RenderConsistency cons={period.consistency} issues={period.issues} observations={period.observations} />
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

// ── Balance Sheet Tab ──

function RenderBalanceSheet({ bs, periodId }) {
  if (!bs) return <div className="empty-state">Balance sheet not available</div>
  return (
    <div className="statement-grid">
      <div className="statement-section">
        <h3>Активы ({fmt(Number(bs.total_assets || 0))})</h3>
        <table className="statement-table">
          <tbody>
            {(bs.assets || []).map(e => (
              <tr key={e.account_code}>
                <td className="td-icon">{CATEGORY_ICONS.Asset}</td>
                <td>
                  <Link to={`/asset/${e.account_code}/period/${periodId}`}
                    style={{ color: 'var(--accent)', textDecoration: 'none' }}>
                    {e.account_name}
                  </Link>
                </td>
                <td className="td-amount">{fmt(e.amount)}</td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr><td colSpan={2}>Итого активы</td><td className="td-amount">{fmt(Number(bs.total_assets || 0))}</td></tr>
          </tfoot>
        </table>
      </div>
      <div className="statement-section">
        <h3>Обязательства ({fmt(Number(bs.total_liabilities || 0))})</h3>
        <table className="statement-table">
          <tbody>
            {(bs.liabilities || []).map(e => (
              <tr key={e.account_code}>
                <td className="td-icon">{CATEGORY_ICONS.Liability}</td>
                <td>{e.account_name}</td>
                <td className="td-amount">{fmt(e.amount)}</td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr><td colSpan={2}>Итого обязательства</td><td className="td-amount">{fmt(Number(bs.total_liabilities || 0))}</td></tr>
          </tfoot>
        </table>
      </div>
    </div>
  )
}

// ── Income Statement Tab ──

function RenderIncomeStatement({ is }) {
  if (!is) return <div className="empty-state">Income statement not available</div>
  return (
    <div className="statement-grid">
      <div className="statement-section">
        <h3>Доходы ({fmt(Number(is.total_revenue || 0))})</h3>
        <table className="statement-table">
          <tbody>
            {(is.revenue || []).map(e => (
              <tr key={e.account_code}>
                <td className="td-icon">{CATEGORY_ICONS.Revenue}</td>
                <td>{e.account_name}</td>
                <td className="td-amount">{fmt(e.amount)}</td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr><td colSpan={2}>Итого доходов</td><td className="td-amount">{fmt(Number(is.total_revenue || 0))}</td></tr>
          </tfoot>
        </table>
      </div>
      <div className="statement-section">
        <h3>Расходы ({fmt(Number(is.total_expenses || 0))})</h3>
        <table className="statement-table">
          <tbody>
            {(is.expenses || []).map(e => (
              <tr key={e.account_code}>
                <td className="td-icon">{CATEGORY_ICONS.Expense}</td>
                <td>{e.account_name}</td>
                <td className="td-amount">{fmt(e.amount)}</td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr><td colSpan={2}>Итого расходов</td><td className="td-amount">{fmt(Number(is.total_expenses || 0))}</td></tr>
          </tfoot>
        </table>
      </div>
      <div className="statement-section net-pnl">
        <h3>Чистый результат: {fmt(Number(is.net_pnl || 0))}</h3>
      </div>
    </div>
  )
}

// ── Cash Flow Tab ──

function RenderCashFlow({ cf }) {
  if (!cf) return <div className="empty-state">Cash flow not available</div>
  return (
    <div className="statement-grid">
      <h3>Куда ушли деньги?</h3>
      <div className="statement-section">
        <h4>Поступило</h4>
        <table className="statement-table">
          <tbody>
            {(cf.operating || []).map(e => (
              <tr key={e.account_code}>
                <td className="td-icon">+</td>
                <td>{e.account_name}</td>
                <td className="td-amount">{fmt(e.amount)}</td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr><td colSpan={2}>Операционный поток</td><td className="td-amount">{fmt(Number(cf.total_operating || 0))}</td></tr>
          </tfoot>
        </table>
      </div>
      <div className="statement-section">
        <h4>Потрачено / Инвестировано</h4>
        <table className="statement-table">
          <tbody>
            {(cf.investing || []).map(e => (
              <tr key={e.account_code}>
                <td className="td-icon">−</td>
                <td>{e.account_name}</td>
                <td className="td-amount">{fmt(e.amount)}</td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr><td colSpan={2}>Инвестиционный поток</td><td className="td-amount">{fmt(Number(cf.total_investing || 0))}</td></tr>
          </tfoot>
          <tbody>
            {(cf.financing || []).map(e => (
              <tr key={e.account_code}>
                <td className="td-icon">±</td>
                <td>{e.account_name}</td>
                <td className="td-amount">{fmt(e.amount)}</td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr><td colSpan={2}>Финансовый поток</td><td className="td-amount">{fmt(Number(cf.total_financing || 0))}</td></tr>
          </tfoot>
          <tfoot className="total-row">
            <tr><td colSpan={2}>Чистый денежный поток</td><td className="td-amount">{fmt(Number(cf.net_cash_flow || 0))}</td></tr>
          </tfoot>
        </table>
      </div>
    </div>
  )
}

// ── Attribution Tab ──

const REASON_LABELS = {
  income: { label: 'Доходы' },
  expenses: { label: 'Расходы' },
  market_effects: { label: 'Рынок' },
  fees: { label: 'Комиссии' },
  debt_costs: { label: 'Проценты по долгам' },
  contributions: { label: 'Пополнения' },
}

function RenderAttribution({ attr, reasons }) {
  if (!reasons || reasons.length === 0) return <div className="empty-state">Нет данных об изменении капитала</div>

  const delta = Number(attr.delta_net_worth || 0)
  const nwStart = Number(attr.net_worth_start || 0)
  const nwEnd = Number(attr.net_worth_end || 0)

  return (
    <div className="attribution-page">
      <div className="attribution-header">
        <div className="attribution-nw">
          <span className="attribution-label">Капитал на начало</span>
          <span className="attribution-value">{fmt(nwStart)}</span>
        </div>
        <div className="attribution-arrow">→</div>
        <div className="attribution-nw">
          <span className="attribution-label">Капитал на конец</span>
          <span className="attribution-value">{fmt(nwEnd)}</span>
        </div>
        <div className="attribution-delta">
          <span className="attribution-label">Изменение</span>
          <span className={`attribution-value ${delta >= 0 ? 'up' : 'down'}`}>
            {delta >= 0 ? '+' : ''}{fmt(delta)}
          </span>
        </div>
      </div>

      <h3>Почему изменился капитал?</h3>
      <div className="attribution-components">
        {reasons.map(r => {
          const meta = REASON_LABELS[r.kind] || { label: r.kind }
          const amt = Number(r.amount)
          return (
            <div key={r.kind} className={`attribution-row ${r.direction === 'positive' ? 'positive' : 'negative'}`}>
              <span className="attribution-row-label">{meta.label}</span>
              <div className="attribution-bar-container">
                <div
                  className={`attribution-bar ${r.direction === 'positive' ? 'bar-positive' : 'bar-negative'}`}
                  style={{ width: `${Math.min(Math.abs(amt) / Math.abs(delta || 1) * 100, 100)}%` }}
                />
              </div>
              <span className="attribution-row-value">
                {r.direction === 'positive' ? '+' : '−'}{fmt(amt)}
              </span>
              <span className="attribution-row-pct">
                ({delta !== 0 ? (Math.abs(amt) / Math.abs(delta) * 100).toFixed(0) : 0}%)
              </span>
            </div>
          )
        })}
      </div>

      {attr.reconciliation_check !== undefined && Number(attr.reconciliation_check) !== 0 && (
        <div className="attribution-note">
          Предупреждение: сумма компонентов не сходится с ΔNW (расхождение: {fmt(attr.reconciliation_check)})
        </div>
      )}
    </div>
  )
}

// ── Consistency Tab ──

function RenderConsistency({ cons, issues, observations }) {
  if (!cons) return <div className="empty-state">Проверка данных не выполнена</div>

  const score = Number(cons.score || 0)
  const checks = cons.checks || []

  return (
    <div className="consistency-page">
      <div className={`consistency-score-large ${score >= 0.9 ? 'good' : score >= 0.5 ? 'warn' : 'bad'}`}>
        {(score * 100).toFixed(0)}%
      </div>
      <p className="consistency-subtitle">
        {score >= 1 ? 'Все инварианты согласованы' : 'Обнаружены расхождения'}
      </p>

      <h3>Инварианты</h3>
      <table className="consistency-table">
        <thead>
          <tr>
            <th>Проверка</th>
            <th>Статус</th>
            <th>Ожидалось</th>
            <th>Факт</th>
            <th>Отклонение</th>
          </tr>
        </thead>
        <tbody>
          {checks.map(c => (
            <tr key={c.name} className={c.passed ? 'check-passed' : 'check-failed'}>
              <td>{c.name}</td>
              <td>{c.passed ? '✓' : '✗'}</td>
              <td>{Number(c.expected || 0).toFixed(2)}</td>
              <td>{Number(c.actual || 0).toFixed(2)}</td>
              <td className={c.passed ? '' : 'drift-highlight'}>{Number(c.drift || 0).toFixed(2)}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {/* Issues */}
      {issues && issues.length > 0 && (
        <div className="consistency-issues">
          <h3>Требует внимания</h3>
          {issues.map((iss, i) => (
            <div key={i} className="consistency-issue-item">
              {iss.kind === 'consistency_score'
                ? 'Согласованность данных ниже 100%'
                : `Расхождение в ${iss.kind} (${Number(iss.drift || 0).toFixed(2)})`}
            </div>
          ))}
        </div>
      )}

      {/* Observations */}
      {observations && observations.length > 0 && (
        <div className="consistency-observations">
          <h3>Наблюдения</h3>
          {observations.map((obs, i) => (
            <div key={i} className={`observation-item severity-${obs.severity || 'info'}`}>
              <ObservationText obs={obs} />
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function ObservationText({ obs }) {
  switch (obs.kind) {
    case 'concentration':
      return <span>{obs.account_name} составляет {obs.weight_pct}% активов</span>
    case 'liquidity_cushion':
      return <span>Ликвидная подушка: {obs.months} мес. расходов</span>
    case 'debt_ratio':
      return <span>Долговая нагрузка: {obs.ratio_pct}% капитала</span>
    case 'expense_growth':
      return <span>Расходы растут {obs.months} месяца подряд (+{obs.growth_pct}%)</span>
    default:
      return <span>{obs.kind}</span>
  }
}
