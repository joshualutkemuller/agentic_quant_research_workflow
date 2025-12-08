from __future__ import annotations
from datetime import date
from pathlib import Path

from src.agents.consumer_framework import ConsumerFrameworkConfig, ConsumerQuantFramework


def run_consumer_quant(as_of: date, repo_root: Path) -> Path:
  """Build a consumer-friendly quant blueprint and return the markdown path."""
  config = ConsumerFrameworkConfig(
    blueprint_path=repo_root / "config" / "consumer" / "blueprint.yaml",
    output_dir=repo_root / "reports" / "consumer",
  )

  framework = ConsumerQuantFramework(config)
  return framework.build(as_of=as_of)
