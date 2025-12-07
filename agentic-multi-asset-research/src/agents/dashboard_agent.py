from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any
from pathlib import Path
import yaml
from loguru import logger

from src.tools.dashboard_io import write_dashboard_feeds


@dataclass
class DashboardAgentConfig:
  dashboards_config_path: Path


class DashboardAgent:
  def __init__(self, config: DashboardAgentConfig):
    self.config = config
    with open(config.dashboards_config_path, "r") as f:
      self._cfg = yaml.safe_load(f)

  def publish_daily_feeds(self, data_bundle: Dict[str, Any], model_results: Dict[str, Any]) -> None:
    logger.info("Publishing daily dashboard feeds")
    dashboards_cfg = self._cfg["dashboards"]
    daily_cfg = dashboards_cfg.get("daily_overview", {})
    write_dashboard_feeds(daily_cfg, data_bundle, model_results)

  def publish_weekly_feeds(self, data_bundle: Dict[str, Any], model_results: Dict[str, Any]) -> None:
    logger.info("Publishing weekly dashboard feeds")
    dashboards_cfg = self._cfg["dashboards"]
    weekly_cfg = dashboards_cfg.get("weekly_review", {})
    write_dashboard_feeds(weekly_cfg, data_bundle, model_results)

  def publish_monthly_feeds(self, data_bundle: Dict[str, Any], model_results: Dict[str, Any]) -> None:
    logger.info("Publishing monthly dashboard feeds")
    dashboards_cfg = self._cfg["dashboards"]
    monthly_cfg = dashboards_cfg.get("monthly_saa", {})
    write_dashboard_feeds(monthly_cfg, data_bundle, model_results)
