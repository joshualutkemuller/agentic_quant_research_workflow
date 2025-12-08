# Consumer Quant Blueprint – 2025-12-07

## Investor Profile
- Risk tolerance: moderate | Horizon: 10 years
- Objective: Build a diversified portfolio with disciplined risk controls while keeping fees low.

## Portfolio Snapshot
- Total value: $32,700
- Allocation by asset class:
  - bonds: 24.5%
  - cash: 15.3%
  - equities: 54.7%
  - real_assets: 5.5%

### Top Positions
- AAPL: 29.1% of portfolio ($9,500)
- VTI: 25.7% of portfolio ($8,400)
- AGG: 24.5% of portfolio ($8,000)
- CASH: 15.3% of portfolio ($5,000)
- GLD: 5.5% of portfolio ($1,800)

## Scenario Diagnostics
- Herfindahl concentration index: 0.237

### Stress Tests
- equity_shock: Equities down 15%, bonds up 2%, gold up 5% | P&L: $-2,435 (-7.4%)
- rate_spike: Rates +150 bps, bonds fall, equities wobble | P&L: $-1,141 (-3.5%)
- inflation_surprise: Inflation surprise boosts commodities and hurts bonds | P&L: $-1,052 (-3.2%)

### 12-Month Projection
- Month 1: $33,567
- Month 2: $34,437
- Month 3: $35,310
- Month 4: $36,186
- Month 5: $37,066
- Month 6: $37,948
- Month 7: $38,834
- Month 8: $39,723
- Month 9: $40,615
- Month 10: $41,510
- Month 11: $42,408
- Month 12: $43,310

## Action Plan
- Add approximately $3,355 in equities to move toward 65% target.
- Trim approximately $3,365 in cash to move toward 5% target.
- Reduce AAPL which is 29.1% of the portfolio (above 25% limit).
- Reduce VTI which is 25.7% of the portfolio (above 25% limit).
- Prioritize tax-advantaged accounts for bond exposure.
- Favor low-cost ETFs unless single-name conviction is high.

## Application Modules
- Broker CSV/API ingestion
- Holdings normalization and cost basis tracking
- Factor-lite analytics (allocation, concentration, beta proxies)
- Scenario builder for retail-friendly what-if analysis
- Goal tracking with monthly contribution planning
- Action queue connected to brokerage/trading APIs

## App Delivery Plan
### iOS Experience
- App name: Everyday Quant
- Primary flows:
  - Onboarding with brokerage link and optional CSV upload
  - Portfolio overview: weights, trends, and projected value
  - Action center: rebalance suggestions and goal tracking
- Widgets/notifications:
  - Home Screen portfolio tile with daily P&L
  - Lock screen push for stress alerts and rebalance nudges
- Delivery pipeline:
  - Fastlane lanes for build/test/distribute
  - TestFlight beta with crash + performance telemetry
  - App Store release notes sourced from GitHub releases

### Backend & Data
- Hosting: FastAPI service on Azure App Service with managed PostgreSQL
- Services:
  - portfolio-api: CRUD for portfolios, holdings, and user targets | Stack: FastAPI + SQLAlchemy + pydantic
  - analytics-worker: Async jobs for projections, stress tests, and report generation | Stack: Azure Container Apps + Redis queue
- API contracts:
  - GET /api/v1/portfolio – Return latest holdings, allocation weights, and diagnostics | Response: { holdings: [...], weights: {..}, herfindahl_index: float }
  - POST /api/v1/actions/accept – User accepts or defers a suggested action | Response: { status: 'queued', action_id: str }
  - POST /api/v1/imports/broker – Ingest brokerage export or Plaid payload | Response: { status: 'received', job_id: str }
- Data schema:
  - portfolio_snapshots:
    - snapshot_id (uuid)
    - user_id (uuid)
    - as_of (date)
    - total_value (numeric)
    - weights (jsonb)
  - holdings:
    - holding_id (uuid)
    - snapshot_id (uuid)
    - symbol (text)
    - asset_class (text)
    - quantity (numeric)
    - price (numeric)
    - value (numeric)
  - actions:
    - action_id (uuid)
    - user_id (uuid)
    - type (text)
    - payload (jsonb)
    - status (text)
    - created_at (timestamptz)

### Web Experience
- URL: https://app.everydayquant.com
- Auth: OAuth + device login with email/SMS MFA
- Pages:
  - Landing: value prop, screenshots, and waitlist
  - Dashboard: mirror of iOS overview with responsive charts
  - Report viewer: latest quant blueprint and PDF/CSV exports