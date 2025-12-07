from __future__ import annotations
import argparse
from datetime import datetime, date
from pathlib import Path
from loguru import logger

from src.pipelines.consumer_quant import run_consumer_quant
from src.pipelines.daily_cross_asset import run_daily
from src.pipelines.monthly_saa_review import run_monthly
from src.pipelines.weekly_factor_deepdive import run_weekly


def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(description="Agentic multi-asset research runner")
  parser.add_argument(
    "--pipeline",
    choices=["daily", "weekly", "monthly", "consumer"],
    required=True,
    help="Which pipeline to run",
  )
  parser.add_argument(
    "--as-of",
    type=str,
    default=None,
    help="As-of date in YYYY-MM-DD format (defaults to today)",
  )
  parser.add_argument(
    "--repo-root",
    type=str,
    default=None,
    help="Override repository root path (defaults to current working directory)",
  )
  parser.add_argument(
    "--github-repo",
    type=str,
    default=None,
    help='GitHub repo in "owner/name" format, e.g. "user/agentic-multi-asset-research"',
  )
  return parser.parse_args()


def main() -> None:
  args = parse_args()
  as_of = date.today() if args.as_of is None else datetime.strptime(args.as_of, "%Y-%m-%d").date()
  repo_root = Path(args.repo_root or ".").resolve()

  logger.info(f"Running pipeline={args.pipeline} as_of={as_of} repo_root={repo_root}")

  if args.pipeline == "daily":
    run_daily(as_of=as_of, repo_root=repo_root, github_repo=args.github_repo)
  elif args.pipeline == "weekly":
    run_weekly(as_of=as_of, repo_root=repo_root)
  elif args.pipeline == "monthly":
    run_monthly(as_of=as_of, repo_root=repo_root)
  elif args.pipeline == "consumer":
    run_consumer_quant(as_of=as_of, repo_root=repo_root)


if __name__ == "__main__":
  main()
