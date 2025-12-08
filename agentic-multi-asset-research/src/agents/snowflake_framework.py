from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import yaml
from loguru import logger


@dataclass
class SnowflakeFrameworkConfig:
  schema_path: Path
  output_dir: Path


class SnowflakeSchemaAgent:
  """Load the Snowflake table schema and surface a structured view for other agents."""

  def __init__(self, config: SnowflakeFrameworkConfig):
    self.config = config
    with open(config.schema_path, "r", encoding="utf-8") as f:
      self.schema_doc = yaml.safe_load(f)

  def connection_profile(self) -> Dict[str, Any]:
    return self.schema_doc.get("connection", {})

  def tables(self) -> Dict[str, Dict[str, Any]]:
    return self.schema_doc.get("tables", {})

  def describe_tables(self) -> List[Dict[str, Any]]:
    tables = []
    for name, meta in self.tables().items():
      tables.append(
        {
          "slug": name,
          "name": meta.get("name", name),
          "description": meta.get("description", ""),
          "grain": meta.get("grain", ""),
          "primary_keys": meta.get("primary_keys", []),
          "columns": meta.get("columns", []),
        }
      )
    logger.info("Loaded {} Snowflake tables from schema", len(tables))
    return tables

  def fully_qualified_name(self, table_slug: str) -> str:
    tables = self.tables()
    meta = tables.get(table_slug, {})
    name = meta.get("name", table_slug)
    conn = self.connection_profile()
    database = conn.get("database", "").upper()
    schema = conn.get("schema", "").upper()
    return f"{database}.{schema}.{name}"


class SnowflakeQueryAgent:
  """Generate reusable SQL snippets and dashboards from the provided schema."""

  def __init__(self, schema_agent: SnowflakeSchemaAgent):
    self.schema_agent = schema_agent

  def _table(self, slug: str) -> str:
    return self.schema_agent.fully_qualified_name(slug)

  def build_queries(self) -> List[Dict[str, str]]:
    queries = [
      {
        "name": "Benchmark coverage and freshness",
        "purpose": "Check latest date per benchmark and ensure feed completeness.",
        "sql": f"""
          SELECT
            benchmark_id,
            MAX(trade_date) AS latest_date,
            COUNT(*) AS rows_loaded
          FROM {self._table('benchmark_returns')}
          GROUP BY 1
          ORDER BY latest_date DESC;
        """,
      },
      {
        "name": "Daily performance with FX normalization",
        "purpose": "Pull daily total return with currency conversion into a uniform base.",
        "sql": f"""
          SELECT
            r.trade_date,
            r.benchmark_id,
            m.benchmark_name,
            r.total_return_local,
            r.fx_rate_to_usd,
            (r.total_return_local * COALESCE(r.fx_rate_to_usd, 1)) AS total_return_usd
          FROM {self._table('benchmark_returns')} r
          JOIN {self._table('benchmark_master')} m USING (benchmark_id)
          WHERE r.trade_date BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
          ORDER BY r.trade_date, r.benchmark_id;
        """,
      },
      {
        "name": "Monthly performance and drawdowns",
        "purpose": "Resample to month-end, compute rolling return, and drawdown path for dashboards.",
        "sql": f"""
          WITH monthlies AS (
            SELECT
              DATE_TRUNC('month', trade_date) AS month,
              benchmark_id,
              EXP(SUM(LN(1 + total_return_usd))) - 1 AS monthly_return
            FROM {self._table('benchmark_returns_fx')}
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
        """,
      },
      {
        "name": "Constituent weights with sector and country tilt",
        "purpose": "Surface benchmark composition for attribution tiles.",
        "sql": f"""
          SELECT
            c.as_of_date,
            c.benchmark_id,
            m.benchmark_name,
            c.ticker,
            c.weight,
            c.sector,
            c.country
          FROM {self._table('benchmark_constituents')} c
          JOIN {self._table('benchmark_master')} m USING (benchmark_id)
          WHERE c.as_of_date = (SELECT MAX(as_of_date) FROM {self._table('benchmark_constituents')})
          ORDER BY c.benchmark_id, c.weight DESC;
        """,
      },
      {
        "name": "Fundamental snapshot by benchmark",
        "purpose": "Aggregate valuation and quality ratios using the latest constituent fundamentals.",
        "sql": f"""
          WITH latest_fundamentals AS (
            SELECT
              f.ticker,
              f.period_end_date,
              f.pe_ratio,
              f.pb_ratio,
              f.dividend_yield,
              f.revenue_growth,
              ROW_NUMBER() OVER (PARTITION BY f.ticker ORDER BY f.period_end_date DESC) AS rn
            FROM {self._table('constituent_fundamentals')} f
          )
          SELECT
            c.benchmark_id,
            m.benchmark_name,
            SUM(c.weight * lf.pe_ratio) AS weighted_pe,
            SUM(c.weight * lf.pb_ratio) AS weighted_pb,
            SUM(c.weight * lf.dividend_yield) AS weighted_dividend_yield,
            SUM(c.weight * lf.revenue_growth) AS weighted_revenue_growth
          FROM {self._table('benchmark_constituents')} c
          JOIN latest_fundamentals lf ON c.ticker = lf.ticker AND lf.rn = 1
          JOIN {self._table('benchmark_master')} m USING (benchmark_id)
          WHERE c.as_of_date = (SELECT MAX(as_of_date) FROM {self._table('benchmark_constituents')})
          GROUP BY 1, 2
          ORDER BY benchmark_id;
        """,
      },
      {
        "name": "Attribution-ready joined fact table",
        "purpose": "Produce a wide fact table combining returns, weights, and fundamentals for BI tools.",
        "sql": f"""
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
          FROM {self._table('benchmark_returns_fx')} r
          JOIN {self._table('benchmark_constituents')} c
            ON r.benchmark_id = c.benchmark_id
            AND r.trade_date BETWEEN c.as_of_date AND COALESCE(c.next_rebalance_date, r.trade_date)
          LEFT JOIN {self._table('constituent_fundamentals')} f ON c.ticker = f.ticker AND f.period_end_date = (
            SELECT MAX(period_end_date) FROM {self._table('constituent_fundamentals')} f2 WHERE f2.ticker = f.ticker
          )
          JOIN {self._table('benchmark_master')} m USING (benchmark_id)
          WHERE r.trade_date >= DATEADD(month, -6, CURRENT_DATE());
        """,
      },
      {
        "name": "Data quality checks",
        "purpose": "Detect gaps, nulls, or outlier returns before publishing dashboards.",
        "sql": f"""
          SELECT
            'missing_constituent_weights' AS check_name,
            COUNT(*) AS issue_count
          FROM {self._table('benchmark_constituents')}
          WHERE weight IS NULL OR weight <= 0
          UNION ALL
          SELECT 'extreme_returns', COUNT(*)
          FROM {self._table('benchmark_returns_fx')}
          WHERE ABS(total_return_usd) > 0.3
          UNION ALL
          SELECT 'stale_fundamentals', COUNT(*)
          FROM {self._table('constituent_fundamentals')}
          WHERE period_end_date < DATEADD(month, -18, CURRENT_DATE());
        """,
      },
    ]

    logger.info("Prepared {} Snowflake queries for the blueprint", len(queries))
    return queries

  def dashboard_ideas(self) -> List[str]:
    ideas = [
      "Benchmark overview: 1M/3M/6M/1Y returns, drawdown sparkline, latest price vs. SMA.",
      "Composition: top 10 constituents, sector and country treemaps, weight drift vs. target.",
      "Quality & valuation: weighted P/E, P/B, dividend yield, revenue growth trends.",
      "Attribution: return contribution by sector, country, and factor proxies.",
      "Data health: feed freshness by benchmark, null/zero weight checks, extreme return monitors.",
    ]
    logger.info("Enumerated {} dashboard ideas", len(ideas))
    return ideas


class SnowflakeBlueprintWriter:
  def __init__(self, schema_agent: SnowflakeSchemaAgent, query_agent: SnowflakeQueryAgent):
    self.schema_agent = schema_agent
    self.query_agent = query_agent

  def build(self, as_of: date) -> Dict[str, Any]:
    tables = self.schema_agent.describe_tables()
    connection = self.schema_agent.connection_profile()
    queries = self.query_agent.build_queries()
    dashboards = self.query_agent.dashboard_ideas()

    return {
      "as_of": str(as_of),
      "connection": connection,
      "tables": tables,
      "queries": queries,
      "dashboards": dashboards,
    }

  def to_markdown(self, blueprint: Dict[str, Any]) -> str:
    lines: List[str] = [f"# Snowflake Benchmark Analytics Blueprint – {blueprint['as_of']}", ""]

    conn = blueprint.get("connection", {})
    if conn:
      lines.append("## Connection profile")
      if conn.get("warehouse"):
        lines.append(f"- Warehouse: {conn['warehouse']}")
      if conn.get("role"):
        lines.append(f"- Role: {conn['role']}")
      db = conn.get("database")
      schema = conn.get("schema")
      if db and schema:
        lines.append(f"- Default database.schema: {db}.{schema}")
      lines.append("")

    lines.append("## Tables")
    for table in blueprint.get("tables", []):
      lines.append(f"### {table['name']}")
      if table.get("description"):
        lines.append(f"- {table['description']}")
      if table.get("grain"):
        lines.append(f"- Grain: {table['grain']}")
      if table.get("primary_keys"):
        lines.append("- Primary keys:")
        for key in table["primary_keys"]:
          lines.append(f"  - {key}")
      if table.get("columns"):
        lines.append("- Columns:")
        for column in table["columns"]:
          lines.append(f"  - {column}")
      lines.append("")

    lines.append("## Query library")
    for query in blueprint.get("queries", []):
      lines.append(f"### {query['name']}")
      lines.append(f"Purpose: {query['purpose']}")
      lines.append("")
      lines.append("```sql")
      lines.append(query["sql"].strip())
      lines.append("```")
      lines.append("")

    lines.append("## Dashboard starters")
    for idea in blueprint.get("dashboards", []):
      lines.append(f"- {idea}")

    return "\n".join(lines)


class SnowflakeAnalyticsFramework:
  def __init__(self, config: SnowflakeFrameworkConfig):
    self.config = config
    self.schema_agent = SnowflakeSchemaAgent(config)
    self.query_agent = SnowflakeQueryAgent(self.schema_agent)
    self.writer = SnowflakeBlueprintWriter(self.schema_agent, self.query_agent)
    self.config.output_dir.mkdir(parents=True, exist_ok=True)

  def build(self, as_of: date) -> Path:
    blueprint = self.writer.build(as_of=as_of)
    markdown = self.writer.to_markdown(blueprint)
    output_path = self.config.output_dir / f"snowflake_benchmark_blueprint_{as_of}.md"
    output_path.write_text(markdown, encoding="utf-8")
    logger.info("Wrote Snowflake benchmark blueprint to {}", output_path)
    return output_path
