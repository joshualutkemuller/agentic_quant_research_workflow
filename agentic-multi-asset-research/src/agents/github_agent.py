from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from datetime import date
import os
import requests
from loguru import logger


@dataclass
class GitHubAgentConfig:
  repo: str  # e.g. "user/agentic-multi-asset-research"
  token_env_var: str = "GITHUB_TOKEN"


class GitHubAgent:
  def __init__(self, config: GitHubAgentConfig):
    self.config = config
    token = os.getenv(config.token_env_var)
    if not token:
      logger.warning("No GitHub token found; GitHub integration will be disabled.")
    self._token = token

  def _headers(self) -> dict:
    return {
      "Authorization": f"token {self._token}",
      "Accept": "application/vnd.github+json",
    }

  def create_issue(self, title: str, body: str) -> Optional[int]:
    if not self._token:
      return None
    logger.info(f"Creating GitHub issue: {title}")
    url = f"https://api.github.com/repos/{self.config.repo}/issues"
    resp = requests.post(url, json={"title": title, "body": body}, headers=self._headers())
    if resp.status_code != 201:
      logger.error(f"Failed to create issue: {resp.status_code} {resp.text}")
      return None
    return resp.json().get("number")

  def log_data_quality_issue(self, as_of: date, asset_class: str, coverage: float) -> None:
    title = f"Data quality alert: {asset_class} coverage {coverage:.1%} on {as_of}"
    body = (
      f"Data coverage for {asset_class} fell below the threshold on {as_of}.\n\n"
      f"- Coverage: {coverage:.1%}\n\n"
      "Please review data ingestion and vendor feeds."
    )
    self.create_issue(title, body)
