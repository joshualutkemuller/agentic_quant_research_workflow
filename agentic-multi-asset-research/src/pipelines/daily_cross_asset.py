from __future__ import annotations
from datetime import date
from pathlib import Path

from src.agents.data_agent import DataAgent, DataAgentConfig
from src.agents.model_agent import ModelAgent, ModelAgentConfig
from src.agents.insight_agent import InsightAgent, InsightAgentConfig
from src.agents.dashboard_agent import DashboardAgent, DashboardAgentConfig
from src.agents.github_agent import GitHubAgent, GitHubAgentConfig


def run_daily(as_of: date, repo_root: Path, github_repo: str | None = None) -> None:
  config_dir = repo_root / "config"
  reports_dir = repo_root / "reports" / "daily"

  data_agent = DataAgent(DataAgentConfig(datasources_path=config_dir / "datasources.yaml"))
  model_agent = ModelAgent(ModelAgentConfig(models_config_path=config_dir / "models.yaml"))
  insight_agent = InsightAgent(InsightAgentConfig(output_dir=reports_dir))
  dashboard_agent = DashboardAgent(DashboardAgentConfig(dashboards_config_path=config_dir / "dashboards.yaml"))

  github_agent = GitHubAgent(GitHubAgentConfig(repo=github_repo or "", token_env_var="GITHUB_TOKEN"))

  data_bundle = data_agent.load_cross_asset_returns(as_of_date=as_of)
  model_results = model_agent.run_models(data_bundle)
  summary_path = insight_agent.create_daily_summary(as_of=as_of, data_bundle=data_bundle, model_results=model_results)
  dashboard_agent.publish_daily_feeds(data_bundle, model_results)

  for asset_class, stats in data_bundle["diagnostics"].items():
    if stats["coverage"] < 0.8:
      github_agent.log_data_quality_issue(as_of, asset_class, stats["coverage"])

  print(f"Daily summary written to {summary_path}")
