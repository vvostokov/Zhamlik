import React, { useEffect, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { getDrilldown, getLatestDrilldown } from '../api'

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

export default function AssetDrilldown() {
  const { accountCode, periodId } = useParams()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    const fetchData = periodId ? getDrilldown(periodId) : getLatestDrilldown()
    fetchData
      .then(setData)
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [periodId])

  if (loading) return <div className="loading">Загрузка данных…</div>
  if (error) return <div className="empty-state">Ошибка: {error}</div>
  if (!data) return <div className="empty-state">Нет данных</div>

  const { assets } = data

  // If accountCode given, filter to that asset, else show all
  const asset = accountCode
    ? assets.find(a => a.account_code === accountCode)
    : null

  if (accountCode && !asset) {
    return <div className="empty-state">Актив {accountCode} не найден</div>
  }

  const rows = asset ? [asset] : (assets || [])

  if (rows.length === 0) {
    return <div className="empty-state">Нет активов для отображения</div>
  }

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <Link to={`/periods/${data.period_id}`} style={{ fontSize: 13, color: 'var(--accent)' }}>
          ← Период {data.period_id}
        </Link>
      </div>

      {!accountCode && (
        <div className="drilldown-summary" style={{ display: 'flex', gap: 24, marginBottom: 16, padding: 12, background: 'var(--bg-card)', borderRadius: 8 }}>
          <div>
            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>Активов</div>
            <div style={{ fontSize: 20, fontWeight: 700 }}>{assets.length}</div>
          </div>
        </div>
      )}

      {rows.map(a => {
        const delta = Number(a.delta)
        return (
          <div key={a.account_code}
            className="drilldown-card"
            style={{
              marginBottom: 8, padding: 16, borderRadius: 8,
              background: 'var(--bg-card)',
              border: delta >= 0 ? '1px solid rgba(34,197,94,0.2)' : '1px solid rgba(239,68,68,0.2)',
            }}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <div>
                <div style={{ fontWeight: 700, fontSize: 16 }}>{a.account_name}</div>
                <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                  {a.account_code} · {a.category}
                </div>
              </div>
              <div style={{ textAlign: 'right' }}>
                <div style={{ fontWeight: 700, fontSize: 18 }}>{fmt(a.end_balance)}</div>
                <div style={{ fontSize: 13, color: delta >= 0 ? '#22c55e' : '#ef4444' }}>
                  {fmtSigned(delta)}
                </div>
              </div>
            </div>

            <div style={{ marginTop: 12, paddingTop: 12, borderTop: '1px solid var(--border)', fontSize: 13 }}>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                <div>
                  <div style={{ color: 'var(--text-muted)' }}>Начало периода</div>
                  <div style={{ fontSize: 15 }}>{fmt(a.start_balance)}</div>
                </div>
                <div>
                  <div style={{ color: 'var(--text-muted)' }}>Изменение</div>
                  <div style={{ fontSize: 15, color: delta >= 0 ? '#22c55e' : '#ef4444' }}>{fmtSigned(delta)}</div>
                </div>
              </div>
            </div>
          </div>
        )
      })}
    </div>
  )
}
