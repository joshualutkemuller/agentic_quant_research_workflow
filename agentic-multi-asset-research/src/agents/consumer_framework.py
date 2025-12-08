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

  def derive_app_delivery_plan(self) -> Dict[str, Any]:
    targets = self.blueprint.get("application_targets", {})

    ios_plan = {
      "app_name": targets.get("ios", {}).get("app_name", ""),
      "primary_flows": targets.get("ios", {}).get("primary_flows", []),
      "widgets": targets.get("ios", {}).get("widgets", []),
      "ci_cd": targets.get("ios", {}).get("ci_cd", []),
    }

    backend_plan = {
      "services": targets.get("backend", {}).get("services", []),
      "api_contracts": targets.get("backend", {}).get("api_contracts", []),
      "data_schema": targets.get("backend", {}).get("data_schema", {}),
      "hosting": targets.get("backend", {}).get("hosting", ""),
    }

    web_plan = {
      "url": targets.get("web", {}).get("url", ""),
      "pages": targets.get("web", {}).get("pages", []),
      "auth": targets.get("web", {}).get("auth", ""),
    }

    return {
      "ios": ios_plan,
      "backend": backend_plan,
      "web": web_plan,
    }

  def assemble_blueprint(self, as_of: date, data_state: Dict[str, Any], analytics: Dict[str, Any], projection: List[Dict[str, Any]], stress_tests: List[Dict[str, Any]], actions: List[str]) -> Dict[str, Any]:
    profile = data_state.get("profile", {})
    delivery_plan = self.derive_app_delivery_plan()

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
      "app_delivery": delivery_plan,
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

    delivery = app_blueprint.get("app_delivery", {})
    if any(delivery.values()):
      lines.append("")
      lines.append("## App Delivery Plan")

      ios_plan = delivery.get("ios", {})
      if ios_plan:
        lines.append("### iOS Experience")
        if ios_plan.get("app_name"):
          lines.append(f"- App name: {ios_plan['app_name']}")
        if ios_plan.get("primary_flows"):
          lines.append("- Primary flows:")
          for flow in ios_plan["primary_flows"]:
            lines.append(f"  - {flow}")
        if ios_plan.get("widgets"):
          lines.append("- Widgets/notifications:")
          for widget in ios_plan["widgets"]:
            lines.append(f"  - {widget}")
        if ios_plan.get("ci_cd"):
          lines.append("- Delivery pipeline:")
          for step in ios_plan["ci_cd"]:
            lines.append(f"  - {step}")

      backend_plan = delivery.get("backend", {})
      if backend_plan:
        lines.append("")
        lines.append("### Backend & Data")
        if backend_plan.get("hosting"):
          lines.append(f"- Hosting: {backend_plan['hosting']}")
        if backend_plan.get("services"):
          lines.append("- Services:")
          for service in backend_plan["services"]:
            name = service.get("name")
            description = service.get("description", "")
            stack = service.get("stack", "")
            line = f"  - {name}: {description}" if description else f"  - {name}"
            if stack:
              line += f" | Stack: {stack}"
            lines.append(line)
        if backend_plan.get("api_contracts"):
          lines.append("- API contracts:")
          for api in backend_plan["api_contracts"]:
            method = api.get("method", "GET")
            path = api.get("path", "")
            purpose = api.get("purpose", "")
            response = api.get("response", "")
            lines.append(f"  - {method} {path} – {purpose} | Response: {response}")
        if backend_plan.get("data_schema"):
          lines.append("- Data schema:")
          for table, fields in backend_plan["data_schema"].items():
            lines.append(f"  - {table}:")
            for field in fields:
              lines.append(f"    - {field}")

      web_plan = delivery.get("web", {})
      if web_plan:
        lines.append("")
        lines.append("### Web Experience")
        if web_plan.get("url"):
          lines.append(f"- URL: {web_plan['url']}")
        if web_plan.get("auth"):
          lines.append(f"- Auth: {web_plan['auth']}")
        if web_plan.get("pages"):
          lines.append("- Pages:")
          for page in web_plan["pages"]:
            lines.append(f"  - {page}")

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
