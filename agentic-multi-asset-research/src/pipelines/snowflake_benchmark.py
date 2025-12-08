from __future__ import annotations
from datetime import date
from pathlib import Path

from src.agents.snowflake_framework import SnowflakeAnalyticsFramework, SnowflakeFrameworkConfig


def run_snowflake_benchmark(as_of: date, repo_root: Path) -> Path:
  """Build a Snowflake benchmark analytics blueprint and return the markdown path."""
  config = SnowflakeFrameworkConfig(
    schema_path=repo_root / "config" / "snowflake" / "schema.yaml",
    output_dir=repo_root / "reports" / "snowflake",
  )

  framework = SnowflakeAnalyticsFramework(config)
  return framework.build(as_of=as_of)
