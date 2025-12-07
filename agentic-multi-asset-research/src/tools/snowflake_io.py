from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List
import os
from loguru import logger
import snowflake.connector
import pandas as pd


@dataclass
class SnowflakeClient:
  account: str
  user: str
  warehouse: str
  role: str
  database: str
  schema: str

  @classmethod
  def from_env(cls, cfg: Dict) -> "SnowflakeClient":
    return cls(
      account=os.getenv("SNOWFLAKE_ACCOUNT", cfg["account"]),
      user=os.getenv("SNOWFLAKE_USER", cfg["user"]),
      warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", cfg["warehouse"]),
      role=os.getenv("SNOWFLAKE_ROLE", cfg["role"]),
      database=cfg["database"],
      schema=cfg["schema"],
    )

  def _conn(self):
    return snowflake.connector.connect(
      account=self.account,
      user=self.user,
      warehouse=self.warehouse,
      role=self.role,
      database=self.database,
      schema=self.schema,
    )

  def fetch_benchmark_returns(self, universe: List[str], as_of_date, frequency: str) -> pd.DataFrame:
    logger.info(f"Fetching {frequency} returns for {len(universe)} benchmarks on {as_of_date}")
    universe_str = ",".join(f"'{u}'" for u in universe)
    query = f"""
      SELECT AS_OF_DATE, BENCHMARK_ID, DAILY_RETURN
      FROM BENCHMARK_RETURNS
      WHERE BENCHMARK_ID IN ({universe_str})
        AND AS_OF_DATE <= %s
    """
    with self._conn() as conn:
      df = pd.read_sql(query, conn, params=[as_of_date])
    return df
