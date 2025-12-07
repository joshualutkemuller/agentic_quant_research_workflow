from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any
from loguru import logger
from datetime import date
from pathlib import Path


@dataclass
class InsightAgentConfig:
  output_dir: Path


class InsightAgent:
  def __init__(self, config: InsightAgentConfig):
    self.config = config
    self.config.output_dir.mkdir(parents=True, exist_ok=True)

  def create_daily_summary(self, as_of: date, data_bundle: Dict[str, Any], model_results: Dict[str, Any]) -> Path:
    """Produces a concise markdown summary for the daily snapshot."""
    logger.info(f"Creating daily summary for {as_of}")
    diagnostics = data_bundle["diagnostics"]
    factor_timing = model_results.get("factor_timing", {})

    output_path = self.config.output_dir / f"summary_{as_of}.md"
    with open(output_path, "w", encoding="utf-8") as f:
      f.write(f"# Daily Cross-Asset Summary – {as_of}\n\n")

      f.write("## Data Coverage\n\n")
      for asset_class, stats in diagnostics.items():
        f.write(
          f"- **{asset_class.capitalize()}**: coverage {stats['coverage']:.1%} "
          f"({stats['actual']} of {stats['expected']} benchmarks)\n"
        )

      if factor_timing:
        f.write("\n## Factor Timing Highlights\n\n")
        f.write(
          "Summary of factor-timing model outputs for key asset classes and factors. "
          "This section can be expanded with tables and specific signals.\n"
        )
      else:
        f.write("\n## Factor Timing\n\n")
        f.write("Factor timing models are disabled in the current configuration.\n")

      f.write("\n## Notes\n\n")
      f.write("- All metrics are preliminary and subject to data vendor revisions.\n")

    return output_path
