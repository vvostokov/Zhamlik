import React, { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { getLatestPeriod } from '../api'

const REASON_LABELS = {
  income: { label: 'Income', icon: '↑' },
  expenses: { label: 'Expenses', icon: '↓' },
  market_effects: { label: 'Market Effects', icon: '↑' },
  fees: { label: 'Fees', icon: '↓' },
  debt_costs: { label: 'Debt Costs', icon: '↓' },
  contributions: { label: 'Contributions', icon: '+' },
}

function fmt(n) {
  if (n == null) return '—'
  const v = Number(n)
  const abs = Math.abs(v)
  if (abs >= 1_000_000) return '€' + (v / 1_000_000).toFixed(2) + 'M'
  if (abs >= 1_000) return '€' + (v / 1_000).toFixed(1) + 'K'
  return '€' + v.toFixed(2)
}

function fmtSigned(n) {
  if (n == null) return '—'
  const v = Number(n)
  return (v >= 0 ? '+' : '') + fmt(Math.abs(v))
}

export default function Dashboard() {
  const [period, setPeriod] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    getLatestPeriod()
      .then(setPeriod)
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <div className="page"><div className="loading">Loading…</div></div>

  const rs = period?.reconciled_state || {}
  const totalAssets = Number(rs.total_assets || 0)
  const totalLiabilities = Number(rs.total_liabilities || 0)
  const netWorth = totalAssets - totalLiabilities

  const attr = period?.attribution || {}
  const delta = Number(attr.delta_net_worth || 0)
  const prevNw = Number(attr.net_worth_start || 0)
  const deltaPct = prevNw > 0 ? (delta / prevNw * 100) : 0

  const reasons = period?.reasons || []
  const observations = period?.observations || []
  const cons = period?.consistency || {}
  const consScore = Number(cons.score || 0)

  return (
    <div className="page dashboard-page">
      {/* Net Worth Hero */}
      <div className="dash-hero">
        <div className="dash-hero-label">Чистый капитал</div>
        <div className={`dash-hero-value ${netWorth >= 0 ? 'up' : 'down'}`}>
          {fmt(netWorth)}
        </div>
        <div className={`dash-hero-delta ${delta >= 0 ? 'up' : 'down'}`}>
          {delta >= 0 ? '↑' : '↓'} {fmtSigned(delta)}
          <span className="dash-hero-delta-pct">({deltaPct.toFixed(1)}%)</span>
          <span className="dash-hero-delta-label">за период</span>
        </div>
        <div className="dash-hero-actions">
          <Link to="/explain" className="dash-btn primary">Почему изменилось?</Link>
        </div>
      </div>

      {/* Assets vs Liabilities */}
      <div className="dash-balance-grid">
        <div className="dash-balance-card assets">
          <div className="dash-balance-label">Активы</div>
          <div className="dash-balance-value">{fmt(totalAssets)}</div>
        </div>
        <div className="dash-balance-card liabilities">
          <div className="dash-balance-label">Обязательства</div>
          <div className={`dash-balance-value ${totalLiabilities > 0 ? 'red' : ''}`}>
            {fmt(totalLiabilities)}
          </div>
        </div>
        <div className="dash-balance-card equity">
          <div className="dash-balance-label">Капитал</div>
          <div className={`dash-balance-value ${netWorth >= 0 ? 'green' : 'red'}`}>
            {fmt(netWorth)}
          </div>
        </div>
      </div>

      {/* Attribution Reasons */}
      {reasons.length > 0 && (
        <div className="dash-section">
          <h3>Почему изменился капитал?</h3>
          <div className="dash-drivers">
            {reasons.map(r => {
              const meta = REASON_LABELS[r.kind] || { label: r.kind, icon: '•' }
              const amt = Number(r.amount)
              return (
                <div key={r.kind} className={`dash-driver-item ${r.direction === 'positive' ? 'positive' : 'negative'}`}>
                  <span className="dash-driver-icon">{meta.icon}</span>
                  <span className="dash-driver-name">{meta.label}</span>
                  <span className="dash-driver-value">
                    {r.direction === 'positive' ? '+' : '-'}{fmt(amt)}
                  </span>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Observations */}
      {observations.length > 0 && (
        <div className="dash-section">
          <h3>Наблюдения</h3>
          <div className="dash-observations">
            {observations.map((obs, i) => (
              <div key={i} className={`dash-observation-item severity-${obs.severity || 'info'}`}>
                <ObservationContent obs={obs} />
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Consistency */}
      {consScore > 0 && (
        <div className="dash-section">
          <h3>Проверка данных</h3>
          <div className="dash-consistency">
            <div className={`dash-consistency-score ${consScore >= 0.9 ? 'good' : consScore >= 0.5 ? 'warn' : 'bad'}`}>
              {(consScore * 100).toFixed(0)}%
            </div>
            {consScore < 1 && period?.issues?.length > 0 && (
              <div className="dash-consistency-issues">
                {period.issues.map((iss, i) => (
                  <div key={i} className="dash-issue-item">
                    {iss.kind === 'consistency_score' ? 'Обнаружены расхождения' : `Расхождение: ${iss.kind}`}
                    {iss.drift ? ` (${Number(iss.drift).toFixed(2)})` : ''}
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Empty state */}
      {!period && (
        <div className="empty-state" style={{ padding: '60px 20px', textAlign: 'center' }}>
          <h3 style={{ fontSize: 18, marginBottom: 8 }}>Нет финансовых данных</h3>
          <p style={{ color: 'var(--text-muted)', marginBottom: 20 }}>
            Импортируйте CSV или подключите биржу, чтобы начать.
          </p>
          <Link to="/import" className="dash-btn primary" style={{ display: 'inline-block' }}>
            Импорт данных
          </Link>
        </div>
      )}
    </div>
  )
}

function ObservationContent({ obs }) {
  switch (obs.kind) {
    case 'concentration':
      return <span>Концентрация: {obs.account_name} составляет {obs.weight_pct}% активов</span>
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
