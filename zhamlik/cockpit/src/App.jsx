import React from 'react'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { useWebSocket } from './hooks/useWebSocket'
import Navigation from './components/Navigation'
import Dashboard from './pages/Dashboard'
import SimulationPage from './pages/Simulation'
import WorldStatePage from './pages/WorldState'
import HealthPage from './pages/Health'
import RiskPage from './pages/Risk'
import DriversPage from './pages/Drivers'
import DynamicsPage from './pages/Dynamics'
import NetWorthPage from './pages/NetWorth'
import AllocationPage from './pages/Allocation'
import AttributionPage from './pages/Attribution'
import BalanceSheetPage from './pages/BalanceSheet'
import IncomeStatementPage from './pages/IncomeStatement'
import CashFlowPage from './pages/CashFlow'
import TimelineExplorer from './pages/TimelineExplorer'
import PeriodExplorer from './pages/PeriodExplorer'
import EquityCurve from './pages/EquityCurve'
import DiffViewer from './pages/DiffViewer'
import ImportPage from './pages/Import'
import AssetDrilldown from './pages/AssetDrilldown'
import WhyDidMyNWChange from './pages/WhyDidMyNWChange'
import ScenarioSandbox from './pages/ScenarioSandbox'

/* Re-export existing operational cockpit */
import PortfolioPanel from './components/PortfolioPanel'
import PositionsGrid from './components/PositionsGrid'
import OrdersPanel from './components/OrdersPanel'
import OrderForm from './components/OrderForm'
import EventTimeline from './components/EventTimeline'
import RuntimeStatus from './components/RuntimeStatus'
import ReplayControls from './components/ReplayControls'

function OperationsPage({ connected, connectionError, subscribe, send }) {
  return (
    <div className="page">
      <header className="page-header">
        <h2>Operations</h2>
        <RuntimeStatus subscribe={subscribe} connected={connected} send={send} />
      </header>
      {connectionError && (
        <div className="banner error">WebSocket connection failed — reconnect in progress</div>
      )}
      <ReplayControls send={send} connected={connected} />
      <main className="ops-grid">
        <div className="grid-left">
          <PortfolioPanel subscribe={subscribe} />
          <PositionsGrid subscribe={subscribe} />
          <OrdersPanel subscribe={subscribe} send={send} />
        </div>
        <div className="grid-right">
          <OrderForm send={send} connected={connected} />
          <EventTimeline subscribe={subscribe} />
        </div>
      </main>
    </div>
  )
}

export default function App() {
  const { connected, connectionError, subscribe, send } = useWebSocket()

  return (
    <BrowserRouter basename="/cockpit">
      <div className="app-layout">
        <Navigation connected={connected} />
        <div className="content-area">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/simulation" element={<SimulationPage />} />
            <Route path="/world" element={<WorldStatePage />} />
            <Route path="/health" element={<HealthPage />} />
            <Route path="/risk" element={<RiskPage />} />
            <Route path="/drivers" element={<DriversPage />} />
            <Route path="/dynamics" element={<DynamicsPage />} />
            <Route path="/net-worth" element={<NetWorthPage />} />
            <Route path="/allocation" element={<AllocationPage />} />
            <Route path="/attribution" element={<AttributionPage />} />
            <Route path="/balance-sheet" element={<BalanceSheetPage />} />
            <Route path="/income-statement" element={<IncomeStatementPage />} />
            <Route path="/cash-flow" element={<CashFlowPage />} />
            <Route path="/periods" element={<PeriodExplorer />} />
            <Route path="/periods/:periodId" element={<PeriodExplorer />} />
            <Route path="/asset/:accountCode" element={<AssetDrilldown />} />
            <Route path="/asset/:accountCode/period/:periodId" element={<AssetDrilldown />} />
            <Route path="/explain" element={<WhyDidMyNWChange />} />
            <Route path="/scenario" element={<ScenarioSandbox />} />
            <Route path="/timeline" element={<TimelineExplorer />} />
            <Route path="/equity-curve" element={<EquityCurve />} />
            <Route path="/diff" element={<DiffViewer />} />
            <Route path="/import" element={<ImportPage />} />
            <Route path="/operations" element={
              <OperationsPage
                connected={connected}
                connectionError={connectionError}
                subscribe={subscribe}
                send={send}
              />
            } />
          </Routes>
        </div>
      </div>
    </BrowserRouter>
  )
}
