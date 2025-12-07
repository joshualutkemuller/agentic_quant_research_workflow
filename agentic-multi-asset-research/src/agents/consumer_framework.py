from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
import yaml
from loguru import logger


@dataclass
class ConsumerFrameworkConfig:
  blueprint_path: Path
  output_dir: Path


class ConsumerDataAgent:
  def __init__(self, blueprint: Dict[str, Any]):
    self.blueprint = blueprint

  def load_portfolio(self) -> Dict[str, Any]:
    holdings = pd.DataFrame(self.blueprint["portfolio"]["holdings"])
    holdings["value"] = holdings["quantity"] * holdings["price"]
    holdings["weight"] = holdings["value"] / holdings["value"].sum()

    logger.info("Loaded consumer portfolio with {} positions", len(holdings))

    return {
      "holdings": holdings,
      "total_value": holdings["value"].sum(),
      "profile": self.blueprint.get("user_profile", {}),
    }


class ConsumerAnalyticsAgent:
  def __init__(self, blueprint: Dict[str, Any]):
    self.blueprint = blueprint

  def summarize_allocation(self, holdings: pd.DataFrame) -> Dict[str, Any]:
    allocation = holdings.groupby("asset_class")["value"].sum()
    weights = (allocation / allocation.sum()).to_dict()

    top_positions = holdings.sort_values("value", ascending=False).head(5)[
      ["symbol", "asset_class", "value", "weight"]
    ].to_dict(orient="records")

    herfindahl = float(((holdings["weight"]) ** 2).sum())

    logger.info("Computed allocation across {} asset classes", len(allocation))

    return {
      "weights": weights,
      "top_positions": top_positions,
      "herfindahl_index": herfindahl,
    }

  def run_stress_tests(self, holdings: pd.DataFrame) -> List[Dict[str, Any]]:
    scenarios = []
    for name, scenario in self.blueprint.get("stress_tests", {}).items():
      shocks = scenario.get("shocks", {})
      shocked_values = []
      for _, row in holdings.iterrows():
        shock = shocks.get(row["asset_class"], shocks.get("default", 0.0))
        shocked_values.append(row["value"] * (1 + shock))

      scenario_value = sum(shocked_values)
      current_value = holdings["value"].sum()
      pnl = scenario_value - current_value
      pnl_pct = pnl / current_value if current_value != 0 else 0.0

      scenarios.append(
        {
          "name": name,
          "description": scenario.get("description", ""),
          "pnl": pnl,
          "pnl_pct": pnl_pct,
        }
      )

    logger.info("Ran {} consumer stress tests", len(scenarios))
    return scenarios

  def project_growth(self, holdings: pd.DataFrame, months: int = 12) -> List[Dict[str, Any]]:
    expected_returns = self.blueprint.get("expected_returns", {})
    monthly_contribution = self.blueprint.get("action_templates", {}).get("monthly_contribution", 0)
    projection = []

    total = holdings["value"].sum()
    for month in range(1, months + 1):
      monthly_return = 0.0
      for _, row in holdings.iterrows():
        asset_return = expected_returns.get(row["asset_class"], 0.0)
        monthly_return += row["weight"] * (asset_return / 12)

      total = total * (1 + monthly_return) + monthly_contribution
      projection.append({"month": month, "projected_value": total})

    logger.info("Built {}-month projection for consumer portfolio", months)
    return projection


class ConsumerActionAgent:
  def __init__(self, blueprint: Dict[str, Any]):
    self.blueprint = blueprint

  def generate_rebalance_actions(self, allocation: Dict[str, float], total_value: float) -> List[str]:
    targets = self.blueprint.get("policy_targets", {})
    threshold = self.blueprint.get("action_templates", {}).get("rebalance_threshold", 0.0)
    actions: List[str] = []

    for asset_class, target in targets.items():
      current = allocation.get(asset_class, 0.0)
      drift = current - target
      if abs(drift) >= threshold:
        direction = "Trim" if drift > 0 else "Add"
        dollar = abs(drift) * total_value
        actions.append(
          f"{direction} approximately ${dollar:,.0f} in {asset_class} to move toward {target:.0%} target."
        )

    return actions

  def generate_concentration_actions(self, top_positions: List[Dict[str, Any]]) -> List[str]:
    max_single = self.blueprint.get("action_templates", {}).get("max_single_position", 1.0)
    actions: List[str] = []
    for pos in top_positions:
      if pos["weight"] > max_single:
        actions.append(
          f"Reduce {pos['symbol']} which is {pos['weight']:.1%} of the portfolio (above {max_single:.0%} limit)."
        )
    return actions

  def build_action_plan(self, allocation: Dict[str, Any], analytics: Dict[str, Any], total_value: float) -> List[str]:
    actions = []
    actions.extend(self.generate_rebalance_actions(allocation, total_value))
    actions.extend(self.generate_concentration_actions(analytics.get("top_positions", [])))
    actions.extend(self.blueprint.get("action_templates", {}).get("notes", []))
    return actions


class ConsumerApplicationAgent:
  def __init__(self, blueprint: Dict[str, Any]):
    self.blueprint = blueprint

  def assemble_blueprint(self, as_of: date, data_state: Dict[str, Any], analytics: Dict[str, Any], projection: List[Dict[str, Any]], stress_tests: List[Dict[str, Any]], actions: List[str]) -> Dict[str, Any]:
    profile = data_state.get("profile", {})

    return {
      "as_of": str(as_of),
      "user_profile": profile,
      "portfolio": {
        "total_value": data_state.get("total_value", 0.0),
        "top_positions": analytics.get("top_positions", []),
        "weights": analytics.get("weights", {}),
      },
      "analytics": {
        "herfindahl_index": analytics.get("herfindahl_index"),
        "stress_tests": stress_tests,
        "projection": projection,
      },
      "actions": actions,
      "modules": [
        "Broker CSV/API ingestion",
        "Holdings normalization and cost basis tracking",
        "Factor-lite analytics (allocation, concentration, beta proxies)",
        "Scenario builder for retail-friendly what-if analysis",
        "Goal tracking with monthly contribution planning",
        "Action queue connected to brokerage/trading APIs",
      ],
    }

  def to_markdown(self, app_blueprint: Dict[str, Any]) -> str:
    lines = [f"# Consumer Quant Blueprint – {app_blueprint['as_of']}", ""]

    profile = app_blueprint.get("user_profile", {})
    if profile:
      lines.append("## Investor Profile")
      lines.append(
        f"- Risk tolerance: {profile.get('risk_tolerance', 'n/a')} | Horizon: {profile.get('investment_horizon_years', 'n/a')} years"
      )
      if profile.get("objective"):
        lines.append(f"- Objective: {profile['objective']}")
      lines.append("")

    lines.append("## Portfolio Snapshot")
    lines.append(f"- Total value: ${app_blueprint['portfolio'].get('total_value', 0):,.0f}")
    lines.append("- Allocation by asset class:")
    for asset_class, weight in app_blueprint["portfolio"].get("weights", {}).items():
      lines.append(f"  - {asset_class}: {weight:.1%}")
    lines.append("")

    lines.append("### Top Positions")
    for pos in app_blueprint["portfolio"].get("top_positions", []):
      lines.append(f"- {pos['symbol']}: {pos['weight']:.1%} of portfolio (${pos['value']:,.0f})")
    lines.append("")

    lines.append("## Scenario Diagnostics")
    lines.append(f"- Herfindahl concentration index: {app_blueprint['analytics'].get('herfindahl_index'):.3f}")
    lines.append("")
    lines.append("### Stress Tests")
    for scenario in app_blueprint["analytics"].get("stress_tests", []):
      lines.append(
        f"- {scenario['name']}: {scenario.get('description', '')} | P&L: ${scenario['pnl']:,.0f} ({scenario['pnl_pct']:.1%})"
      )
    lines.append("")

    lines.append("### 12-Month Projection")
    for point in app_blueprint["analytics"].get("projection", []):
      lines.append(f"- Month {point['month']}: ${point['projected_value']:,.0f}")
    lines.append("")

    lines.append("## Action Plan")
    for action in app_blueprint.get("actions", []):
      lines.append(f"- {action}")
    lines.append("")

    lines.append("## Application Modules")
    for module in app_blueprint.get("modules", []):
      lines.append(f"- {module}")

    return "\n".join(lines)


class ConsumerQuantFramework:
  def __init__(self, config: ConsumerFrameworkConfig):
    self.config = config
    with open(config.blueprint_path, "r", encoding="utf-8") as f:
      self.blueprint = yaml.safe_load(f)

    self.data_agent = ConsumerDataAgent(self.blueprint)
    self.analytics_agent = ConsumerAnalyticsAgent(self.blueprint)
    self.action_agent = ConsumerActionAgent(self.blueprint)
    self.application_agent = ConsumerApplicationAgent(self.blueprint)

    self.config.output_dir.mkdir(parents=True, exist_ok=True)

  def build(self, as_of: date) -> Path:
    data_state = self.data_agent.load_portfolio()
    analytics = self.analytics_agent.summarize_allocation(data_state["holdings"])
    stress_tests = self.analytics_agent.run_stress_tests(data_state["holdings"])
    projection = self.analytics_agent.project_growth(data_state["holdings"], months=12)
    actions = self.action_agent.build_action_plan(analytics.get("weights", {}), analytics, data_state["total_value"])

    app_blueprint = self.application_agent.assemble_blueprint(
      as_of=as_of,
      data_state=data_state,
      analytics=analytics,
      projection=projection,
      stress_tests=stress_tests,
      actions=actions,
    )

    markdown = self.application_agent.to_markdown(app_blueprint)
    output_path = self.config.output_dir / f"consumer_quant_blueprint_{as_of}.md"
    output_path.write_text(markdown, encoding="utf-8")

    logger.info("Wrote consumer quant blueprint to {}", output_path)
    return output_path
