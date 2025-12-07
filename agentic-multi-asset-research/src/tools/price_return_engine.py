from __future__ import annotations
from typing import Dict
import pandas as pd


def compute_standard_returns(raw_frames: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
  """Standardize different asset-class frames into a common schema."""
  standardized: Dict[str, pd.DataFrame] = {}
  for asset_class, df in raw_frames.items():
    if df is None or df.empty:
      standardized[asset_class] = df
      continue
    tmp = df.copy()
    if "DAILY_RETURN" in tmp.columns:
      tmp = tmp.rename(columns={"DAILY_RETURN": "RETURN"})
    standardized[asset_class] = tmp[["AS_OF_DATE", "BENCHMARK_ID", "RETURN"]]
  return standardized
