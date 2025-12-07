from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any
from loguru import logger
import yaml
from pathlib import Path

from src.tools.snowflake_io import SnowflakeClient
from src.tools.price_return_engine import compute_standard_returns


@dataclass
class DataAgentConfig:
  datasources_path: Path


class DataAgent:
  def __init__(self, config: DataAgentConfig):
    self.config = config
    with open(config.datasources_path, "r") as f:
      self._cfg = yaml.safe_load(f)
    self._snowflake_client = SnowflakeClient.from_env(self._cfg["connections"]["snowflake"])

  def load_cross_asset_returns(self, as_of_date) -> Dict[str, Any]:
    """
    Load and standardize cross-asset benchmark data for a given as-of date.
    Returns a dict with raw and standardized frames and basic diagnostics.
    """
    logger.info(f"Loading cross-asset returns for {as_of_date}")
    benchmarks_cfg = self._cfg["benchmarks"]

    raw_frames = {}
    for asset_class, meta in benchmarks_cfg.items():
      universe = meta["universe"]
      raw_frames[asset_class] = self._snowflake_client.fetch_benchmark_returns(
        universe=universe,
        as_of_date=as_of_date,
        frequency=meta["frequency"],
      )

    standardized = compute_standard_returns(raw_frames)
    diagnostics = self._compute_data_diagnostics(standardized, benchmarks_cfg)

    return {
      "raw": raw_frames,
      "standardized": standardized,
      "diagnostics": diagnostics,
    }

  @staticmethod
  def _compute_data_diagnostics(standardized: Dict[str, Any], benchmarks_cfg: Dict[str, Any]) -> Dict[str, Any]:
    diagnostics = {}
    for asset_class, df in standardized.items():
      expected = len(benchmarks_cfg[asset_class]["universe"])
      actual = df["BENCHMARK_ID"].nunique() if not df.empty else 0
      coverage = actual / expected if expected > 0 else 1.0
      diagnostics[asset_class] = {
        "expected": expected,
        "actual": actual,
        "coverage": coverage,
      }
    return diagnostics
