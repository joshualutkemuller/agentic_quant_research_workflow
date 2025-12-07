from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any
from pathlib import Path
import yaml
from loguru import logger

from src.tools.factor_timing_models import run_factor_timing_models
from src.tools.saa_optimizer import run_saa_optimizer


@dataclass
class ModelAgentConfig:
  models_config_path: Path


class ModelAgent:
  def __init__(self, config: ModelAgentConfig):
    self.config = config
    with open(config.models_config_path, "r") as f:
      self._cfg = yaml.safe_load(f)

  def run_models(self, data_bundle: Dict[str, Any]) -> Dict[str, Any]:
    logger.info("Running factor timing and SAA models")
    results: Dict[str, Any] = {}

    if self._cfg.get("factor_timing", {}).get("enabled", False):
      results["factor_timing"] = run_factor_timing_models(
        standardized_returns=data_bundle["standardized"],
        config=self._cfg["factor_timing"],
      )

    if self._cfg.get("saa_optimizer", {}).get("enabled", False):
      results["saa"] = run_saa_optimizer(
        standardized_returns=data_bundle["standardized"],
        config=self._cfg["saa_optimizer"],
      )

    results["risk_metrics"] = {
      "volatility": {},
      "correlations": {},
    }

    return results
