const API_BASE = '/api/v1/financial'

export async function get(path) {
  const res = await fetch(`${API_BASE}${path}`)
  const data = await res.json()
  if (!data.success) throw new Error(data.error || 'Request failed')
  return data.data
}

export async function getSummary() {
  const res = await get('/summary')
  return res
}

export async function getLatestSnapshot() {
  return get('/timeline/latest')
}

export async function getTimelinePeriods() {
  return get('/timeline/periods')
}

export async function getTimelineState(periodId) {
  return get(`/timeline/state/${periodId}`)
}

export async function getBalanceSheet(periodId) {
  return get(`/statements/balance_sheet/${periodId}`)
}

export async function getIncomeStatement(periodId) {
  return get(`/statements/income_statement/${periodId}`)
}

export async function getEquityCurve() {
  return get('/timeline/equity_curve')
}

export async function getDiff(fromPeriod, toPeriod) {
  return get(`/timeline/diff?from=${fromPeriod}&to=${toPeriod}`)
}

export async function getTimelineSummary() {
  return get('/timeline/summary')
}

export async function getAnalyticsSnapshot(periodId) {
  return get(`/analytics/snapshot/${periodId}`)
}

export async function getCashFlow(periodId) {
  return get(`/statements/cash_flow/${periodId}`)
}

export async function getNetWorth(periodId) {
  const p = periodId || 'latest'
  return get(`/intelligence/net_worth/${p}`)
}

export async function getNetWorthSeries() {
  return get('/intelligence/net_worth/series')
}

export async function getAllocation(periodId) {
  const p = periodId || 'latest'
  return get(`/intelligence/allocation/${p}`)
}

export async function getAttribution(periodId) {
  const p = periodId || 'latest'
  return get(`/intelligence/attribution/${p}`)
}

export async function getWorldState(periodId) {
  const p = periodId || 'latest'
  return get(`/world/state/${p}`)
}

export async function getWorldHealth(periodId) {
  const p = periodId || 'latest'
  return get(`/world/health/${p}`)
}

export async function getWorldRisk(periodId) {
  const p = periodId || 'latest'
  return get(`/world/risk/${p}`)
}

export async function getWorldDrivers(periodId) {
  const p = periodId || 'latest'
  return get(`/world/drivers/${p}`)
}

export async function getWorldDynamics(periodId) {
  const p = periodId || 'latest'
  return get(`/world/dynamics/${p}`)
}

export async function runSimulation(scenarios) {
  const res = await fetch(`${API_BASE}/simulation/run`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ scenarios }),
  })
  const data = await res.json()
  if (!data.success) throw new Error(data.error || 'Simulation failed')
  return data.data
}

export async function compareSimulation(scenarios) {
  const res = await fetch(`${API_BASE}/simulation/compare`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ scenarios }),
  })
  const data = await res.json()
  if (!data.success) throw new Error(data.error || 'Comparison failed')
  return data.data
}

export async function runSimulationStatements(scenario) {
  const res = await fetch(`${API_BASE}/simulation/statements`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(scenario),
  })
  const data = await res.json()
  if (!data.success) throw new Error(data.error || 'Statements failed')
  return data.data
}

export async function importCsv(file, config) {
  const formData = new FormData()
  formData.append('file', file)
  formData.append('config', JSON.stringify(config))
  const res = await fetch(`${API_BASE}/import/csv`, {
    method: 'POST',
    body: formData,
  })
  const data = await res.json()
  if (!data.success) throw new Error(data.error || 'Import failed')
  return data.data
}

// ── Series API ──

export async function getSeries() {
  return get('/series')
}

export async function getSeriesSummary() {
  return get('/series/summary')
}

export async function getSeriesMetric(metric) {
  return get(`/series/${metric}`)
}

export async function getSeriesTrend(metric) {
  return get(`/series/trend/${metric}`)
}

// ── Account History ──

export async function getAccountHistory(accountCode) {
  return get(`/timeline/account/${accountCode}/history`)
}

// ── Entity Attribution (Phase 13C) ──

export async function getEntityAttribution(periodId, mode = 'fast') {
  return get(`/attribution/${periodId}?mode=${mode}`)
}

// ── Account Breakdown (Phase 13D) ──

export async function getAccountBreakdown(accountCode) {
  return get(`/timeline/account/${accountCode}/breakdown`)
}

// ── Scenario Projection (Phase 13E) ──

export async function post(path, body) {
  const res = await fetch(`${API_BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  const data = await res.json()
  if (!data.success) throw new Error(data.error || 'Request failed')
  return data.data
}

export async function projectScenario(scenarioType, params = {}) {
  return post('/scenario/project', { scenario_type: scenarioType, params })
}

// ── Period API (Phase 13H — Period-Centric UI) ──

export async function getPeriod(periodId, traceFields) {
  let path = `/period/${periodId}`
  if (traceFields) path += `?trace=${traceFields}`
  return get(path)
}

export async function getLatestPeriod(traceFields) {
  let path = '/period/latest'
  if (traceFields) path += `?trace=${traceFields}`
  return get(path)
}

// ── Asset Drilldown (Phase 13G) ──

export async function getDrilldown(periodId) {
  return get(`/period/${periodId}/drilldown`)
}

export async function getLatestDrilldown() {
  return get('/period/latest/drilldown')
}
