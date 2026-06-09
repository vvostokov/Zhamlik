import React from 'react'
import { useNavigate } from 'react-router-dom'

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

const REASON_LABELS = {
  income: { label: 'Доходы', icon: '↑' },
  expenses: { label: 'Расходы', icon: '↓' },
  market_effects: { label: 'Рынок', icon: '↑' },
  fees: { label: 'Комиссии', icon: '↓' },
  debt_costs: { label: 'Проценты по долгам', icon: '↓' },
  contributions: { label: 'Пополнения', icon: '+' },
}

export default function PeriodChanges({ periodId, period }) {
  const navigate = useNavigate()
  const attr = period?.attribution || {}
  const reasons = period?.reasons || []
  const observations = period?.observations || []
  const delta = Number(attr.delta_net_worth || 0)

  if (!reasons.length && !observations.length) {
    return <div className="empty-state">Нет данных об изменениях</div>
  }

  return (
    <div>
      {/* Attribution reasons — waterfall */}
      {reasons.length > 0 && (
        <div className="section">
          <h3>Почему изменился капитал?</h3>
          <div className="attribution-breakdown">
            {reasons.map(r => {
              const meta = REASON_LABELS[r.kind] || { label: r.kind, icon: '•' }
              const amt = Number(r.amount)
              const dir = r.direction === 'positive' ? 1 : -1
              return (
                <div key={r.kind} className="attribution-row">
                  <span className="attrib-label" style={{ width: 160 }}>{meta.icon} {meta.label}</span>
                  <div className="attrib-bar-wrap">
                    <div
                      className={`attrib-bar ${dir >= 0 ? 'positive' : 'negative'}`}
                      style={{ width: `${Math.min(Math.abs(amt) / Math.abs(delta || 1) * 100, 100)}%` }}
                    />
                  </div>
                  <span className={`attrib-value ${dir >= 0 ? 'green' : 'red'}`}>
                    {dir >= 0 ? '+' : '−'}{fmt(amt)}
                  </span>
                  <span className="attrib-pct" style={{ width: 50, textAlign: 'right', color: 'var(--text-muted)', fontSize: 12 }}>
                    {delta !== 0 ? (Math.abs(amt) / Math.abs(delta) * 100).toFixed(0) : 0}%
                  </span>
                </div>
              )
            })}

            {/* Total */}
            <div className="attribution-row total">
              <span className="attrib-label" style={{ width: 160 }}>Итого</span>
              <div className="attrib-bar-wrap" />
              <span className={`attrib-value ${delta >= 0 ? 'green' : 'red'}`}>
                {fmtSigned(delta)}
              </span>
              <span className="attrib-pct" style={{ width: 50 }}>100%</span>
            </div>
          </div>
        </div>
      )}

      {/* Observations */}
      {observations.length > 0 && (
        <div className="section" style={{ marginTop: 12 }}>
          <h3>Наблюдения</h3>
          {observations.map((obs, i) => (
            <div key={i} style={{ marginBottom: 4 }}>
              <ObsText obs={obs} />
            </div>
          ))}
        </div>
      )}

      {/* Narration notes */}
      {reasons.length > 0 && (
        <div className="section" style={{ marginTop: 12 }}>
          <h3>Выводы</h3>
          <Narration reasons={reasons} delta={delta} />
        </div>
      )}
    </div>
  )
}

function ObsText({ obs }) {
  switch (obs.kind) {
    case 'concentration':
      return <span>⚠ {obs.account_name} составляет {obs.weight_pct}% активов</span>
    case 'liquidity_cushion':
      return <span>💧 Ликвидная подушка: {obs.months} мес. расходов</span>
    case 'debt_ratio':
      return <span>📊 Долговая нагрузка: {obs.ratio_pct}% капитала</span>
    case 'expense_growth':
      return <span>📈 Расходы растут {obs.months} месяца подряд (+{obs.growth_pct}%)</span>
    default:
      return <span>{obs.kind}</span>
  }
}

function Narration({ reasons, delta }) {
  const positive = reasons.filter(r => r.direction === 'positive').sort((a, b) => Number(b.amount) - Number(a.amount))
  const negative = reasons.filter(r => r.direction === 'negative').sort((a, b) => Number(b.amount) - Number(a.amount))
  const topPositive = positive[0]
  const topNegative = negative[0]

  return (
    <div className="narration" style={{ lineHeight: 1.8 }}>
      <p>
        Капитал <strong>{delta >= 0 ? 'вырос' : 'снизился'}</strong> на <strong>{fmt(Math.abs(delta))}</strong>.
      </p>
      {topPositive && (
        <p>
          Основной положительный фактор — <strong>{REASON_LABELS[topPositive.kind]?.label || topPositive.kind}</strong>:
          {' '}+{fmt(Number(topPositive.amount))}.
        </p>
      )}
      {topNegative && (
        <p>
          Основной отрицательный фактор — <strong>{REASON_LABELS[topNegative.kind]?.label || topNegative.kind}</strong>:
          {' '}−{fmt(Number(topNegative.amount))}.
        </p>
      )}
    </div>
  )
}
