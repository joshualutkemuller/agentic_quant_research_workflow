from __future__ import annotations
from typing import Dict, Any
from pathlib import Path
import json
from loguru import logger


def write_dashboard_feeds(cfg: Dict[str, Any], data_bundle: Dict[str, Any], model_results: Dict[str, Any]) -> None:
  outputs = cfg.get("outputs", [])
  for output in outputs:
    file_path = Path(output["file"])
    file_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Writing dashboard feed: {file_path}")
    payload = {
      "meta": {
        "description": output.get("description", ""),
      },
      "data": {
        "diagnostics": data_bundle.get("diagnostics", {}),
        "factor_timing": model_results.get("factor_timing", {}),
        "saa": model_results.get("saa", {}),
      },
    }
    with open(file_path, "w", encoding="utf-8") as f:
      json.dump(payload, f, default=str)
