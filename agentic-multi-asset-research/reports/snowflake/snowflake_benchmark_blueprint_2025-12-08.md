# Snowflake Benchmark Analytics Blueprint – 2025-12-08

## Connection profile
- Warehouse: ANALYTICS_WH
- Role: BENCHMARK_ANALYST
- Default database.schema: CAPITAL_MARKETS.BENCHMARKS

## Tables
### BENCHMARK_MASTER
- Universe of benchmarks and ETFs with currency and region tags.
- Grain: one row per benchmark_id
- Primary keys:
  - benchmark_id
- Columns:
  - benchmark_id (string)
  - benchmark_name (string)
  - currency (string)
  - region (string)
  - provider (string)
  - inception_date (date)

### BENCHMARK_RETURNS
- Daily performance in local currency with FX alignment columns.
- Grain: one row per benchmark_id per trade_date
- Primary keys:
  - benchmark_id
  - trade_date
- Columns:
  - trade_date (date)
  - benchmark_id (string)
  - total_return_local (float)
  - fx_rate_to_usd (float)
  - vendor_batch_id (string)

### BENCHMARK_RETURNS_FX
- Daily returns normalized to USD for downstream analytics.
- Grain: one row per benchmark_id per trade_date
- Primary keys:
  - benchmark_id
  - trade_date
- Columns:
  - trade_date (date)
  - benchmark_id (string)
  - total_return_usd (float)
  - fx_rate_to_usd (float)
  - load_timestamp (timestamp)

### BENCHMARK_CONSTITUENTS
- Benchmark member list with weights and classification tags.
- Grain: one row per benchmark_id per ticker per as_of_date
- Primary keys:
  - benchmark_id
  - ticker
  - as_of_date
- Columns:
  - as_of_date (date)
  - next_rebalance_date (date)
  - benchmark_id (string)
  - ticker (string)
  - weight (float)
  - sector (string)
  - country (string)
  - currency (string)

### CONSTITUENT_FUNDAMENTALS
- Point-in-time valuation and quality metrics for constituents.
- Grain: one row per ticker per period_end_date
- Primary keys:
  - ticker
  - period_end_date
- Columns:
  - ticker (string)
  - period_end_date (date)
  - pe_ratio (float)
  - pb_ratio (float)
  - dividend_yield (float)
  - revenue_growth (float)
  - sector (string)
  - country (string)
  - source (string)

### PRICES_SNAPSHOTS
- Daily closes and volumes for any tradable constituents.
- Grain: one row per ticker per trade_date
- Primary keys:
  - ticker
  - trade_date
- Columns:
  - trade_date (date)
  - ticker (string)
  - close_price (float)
  - volume (float)
  - adjusted_close (float)
  - fx_rate_to_usd (float)
  - vendor_batch_id (string)

## Query library
### Benchmark coverage and freshness
Purpose: Check latest date per benchmark and ensure feed completeness.

```sql
SELECT
            benchmark_id,
            MAX(trade_date) AS latest_date,
            COUNT(*) AS rows_loaded
          FROM CAPITAL_MARKETS.BENCHMARKS.BENCHMARK_RETURNS
          GROUP BY 1
          ORDER BY latest_date DESC;
```

### Daily performance with FX normalization
Purpose: Pull daily total return with currency conversion into a uniform base.

```sql
SELECT
            r.trade_date,
            r.benchmark_id,
            m.benchmark_name,
            r.total_return_local,
            r.fx_rate_to_usd,
            (r.total_return_local * COALESCE(r.fx_rate_to_usd, 1)) AS total_return_usd
          FROM CAPITAL_MARKETS.BENCHMARKS.BENCHMARK_RETURNS r
          JOIN CAPITAL_MARKETS.BENCHMARKS.BENCHMARK_MASTER m USING (benchmark_id)
          WHERE r.trade_date BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
          ORDER BY r.trade_date, r.benchmark_id;
```

### Monthly performance and drawdowns
Purpose: Resample to month-end, compute rolling return, and drawdown path for dashboards.

```sql
WITH monthlies AS (
            SELECT
              DATE_TRUNC('month', trade_date) AS month,
              benchmark_id,
              EXP(SUM(LN(1 + total_return_usd))) - 1 AS monthly_return
            FROM CAPITAL_MARKETS.BENCHMARKS.BENCHMARK_RETURNS_FX
            GROUP BY 1, 2
          ),
          nav_path AS (
            SELECT
              month,
              benchmark_id,
              SUM(monthly_return) OVER (PARTITION BY benchmark_id ORDER BY month) AS cum_return
            FROM monthlies
          )
          SELECT
            *,
            cum_return - MAX(cum_return) OVER (PARTITION BY benchmark_id ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS drawdown
          FROM nav_path
          ORDER BY month, benchmark_id;
```

### Constituent weights with sector and country tilt
Purpose: Surface benchmark composition for attribution tiles.

```sql
SELECT
            c.as_of_date,
            c.benchmark_id,
            m.benchmark_name,
            c.ticker,
            c.weight,
            c.sector,
            c.country
          FROM CAPITAL_MARKETS.BENCHMARKS.BENCHMARK_CONSTITUENTS c
          JOIN CAPITAL_MARKETS.BENCHMARKS.BENCHMARK_MASTER m USING (benchmark_id)
          WHERE c.as_of_date = (SELECT MAX(as_of_date) FROM CAPITAL_MARKETS.BENCHMARKS.BENCHMARK_CONSTITUENTS)
          ORDER BY c.benchmark_id, c.weight DESC;
```

### Fundamental snapshot by benchmark
Purpose: Aggregate valuation and quality ratios using the latest constituent fundamentals.

```sql
WITH latest_fundamentals AS (
            SELECT
              f.ticker,
              f.period_end_date,
              f.pe_ratio,
              f.pb_ratio,
              f.dividend_yield,
              f.revenue_growth,
              ROW_NUMBER() OVER (PARTITION BY f.ticker ORDER BY f.period_end_date DESC) AS rn
            FROM CAPITAL_MARKETS.BENCHMARKS.CONSTITUENT_FUNDAMENTALS f
          )
          SELECT
            c.benchmark_id,
            m.benchmark_name,
            SUM(c.weight * lf.pe_ratio) AS weighted_pe,
            SUM(c.weight * lf.pb_ratio) AS weighted_pb,
            SUM(c.weight * lf.dividend_yield) AS weighted_dividend_yield,
            SUM(c.weight * lf.revenue_growth) AS weighted_revenue_growth
          FROM CAPITAL_MARKETS.BENCHMARKS.BENCHMARK_CONSTITUENTS c
          JOIN latest_fundamentals lf ON c.ticker = lf.ticker AND lf.rn = 1
          JOIN CAPITAL_MARKETS.BENCHMARKS.BENCHMARK_MASTER m USING (benchmark_id)
          WHERE c.as_of_date = (SELECT MAX(as_of_date) FROM CAPITAL_MARKETS.BENCHMARKS.BENCHMARK_CONSTITUENTS)
          GROUP BY 1, 2
          ORDER BY benchmark_id;
```

### Attribution-ready joined fact table
Purpose: Produce a wide fact table combining returns, weights, and fundamentals for BI tools.

```sql
SELECT
            r.trade_date,
            r.benchmark_id,
            m.benchmark_name,
            c.ticker,
            c.weight,
            r.total_return_usd,
            f.pe_ratio,
            f.revenue_growth,
            f.sector,
            f.country
          FROM CAPITAL_MARKETS.BENCHMARKS.BENCHMARK_RETURNS_FX r
          JOIN CAPITAL_MARKETS.BENCHMARKS.BENCHMARK_CONSTITUENTS c
            ON r.benchmark_id = c.benchmark_id
            AND r.trade_date BETWEEN c.as_of_date AND COALESCE(c.next_rebalance_date, r.trade_date)
          LEFT JOIN CAPITAL_MARKETS.BENCHMARKS.CONSTITUENT_FUNDAMENTALS f ON c.ticker = f.ticker AND f.period_end_date = (
            SELECT MAX(period_end_date) FROM CAPITAL_MARKETS.BENCHMARKS.CONSTITUENT_FUNDAMENTALS f2 WHERE f2.ticker = f.ticker
          )
          JOIN CAPITAL_MARKETS.BENCHMARKS.BENCHMARK_MASTER m USING (benchmark_id)
          WHERE r.trade_date >= DATEADD(month, -6, CURRENT_DATE());
```

### Data quality checks
Purpose: Detect gaps, nulls, or outlier returns before publishing dashboards.

```sql
SELECT
            'missing_constituent_weights' AS check_name,
            COUNT(*) AS issue_count
          FROM CAPITAL_MARKETS.BENCHMARKS.BENCHMARK_CONSTITUENTS
          WHERE weight IS NULL OR weight <= 0
          UNION ALL
          SELECT 'extreme_returns', COUNT(*)
          FROM CAPITAL_MARKETS.BENCHMARKS.BENCHMARK_RETURNS_FX
          WHERE ABS(total_return_usd) > 0.3
          UNION ALL
          SELECT 'stale_fundamentals', COUNT(*)
          FROM CAPITAL_MARKETS.BENCHMARKS.CONSTITUENT_FUNDAMENTALS
          WHERE period_end_date < DATEADD(month, -18, CURRENT_DATE());
```

## Dashboard starters
- Benchmark overview: 1M/3M/6M/1Y returns, drawdown sparkline, latest price vs. SMA.
- Composition: top 10 constituents, sector and country treemaps, weight drift vs. target.
- Quality & valuation: weighted P/E, P/B, dividend yield, revenue growth trends.
- Attribution: return contribution by sector, country, and factor proxies.
- Data health: feed freshness by benchmark, null/zero weight checks, extreme return monitors.