from __future__ import annotations
from datetime import date
from pathlib import Path

from src.agents.data_agent import DataAgent, DataAgentConfig
from src.agents.model_agent import ModelAgent, ModelAgentConfig
from src.agents.insight_agent import InsightAgent, InsightAgentConfig
from src.agents.dashboard_agent import DashboardAgent, DashboardAgentConfig


def run_monthly(as_of: date, repo_root: Path) -> None:
  config_dir = repo_root / "config"
  reports_dir = repo_root / "reports" / "monthly"

  data_agent = DataAgent(DataAgentConfig(datasources_path=config_dir / "datasources.yaml"))
  model_agent = ModelAgent(ModelAgentConfig(models_config_path=config_dir / "models.yaml"))
  insight_agent = InsightAgent(InsightAgentConfig(output_dir=reports_dir))
  dashboard_agent = DashboardAgent(DashboardAgentConfig(dashboards_config_path=config_dir / "dashboards.yaml"))

  data_bundle = data_agent.load_cross_asset_returns(as_of_date=as_of)
  model_results = model_agent.run_models(data_bundle)

  summary_path = insight_agent.create_daily_summary(as_of=as_of, data_bundle=data_bundle, model_results=model_results)
  dashboard_agent.publish_monthly_feeds(data_bundle, model_results)

  print(f"Monthly SAA review written to {summary_path}")
