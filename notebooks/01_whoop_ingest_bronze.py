# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Whoop Bronze ingestion
# MAGIC
# MAGIC Pulls recovery, cycle, sleep, and workout data from the Whoop v2 API and lands
# MAGIC it as raw JSON into Bronze Delta tables, idempotently (MERGE on natural key).
# MAGIC First run per endpoint does a full backfill (no `start` bound); later runs
# MAGIC pull incrementally from the endpoint's last successful pull, via the
# MAGIC `ingestion_state` table.
# MAGIC
# MAGIC Requires, in secret scope `bryan_whoop_analytics`: `whoop_client_id`,
# MAGIC `whoop_client_secret`, `whoop_refresh_token` (all set by
# MAGIC `scripts/whoop_authorize.py`). Whoop rotates the refresh token on every
# MAGIC refresh call (confirmed empirically, not just third-party speculation), so
# MAGIC this notebook persists the new one back to the secret scope immediately
# MAGIC after every refresh, using `databricks.sdk.WorkspaceClient()`'s built-in
# MAGIC notebook-native authentication (no personal access token needed or
# MAGIC obtainable on this account anyway, per IMPLEMENTATION.md).
# MAGIC
# MAGIC Note on structure: CLAUDE.md's file-structure convention orders a module as
# MAGIC utilities, then static content, then types last. Notebooks execute cell by
# MAGIC cell top to bottom, so types/config must appear before the utilities that use
# MAGIC them; the final cell (last) is the actual entry point, which is the closest
# MAGIC equivalent to that convention's "exported" top-of-file element.

# COMMAND ----------

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import httpx
from databricks.sdk import WorkspaceClient
from pydantic import BaseModel

# COMMAND ----------

SECRET_SCOPE = "bryan_whoop_analytics"
BRONZE_SCHEMA = "bootcamp_students.bryan_whoop_bronze"
API_BASE_URL = "https://api.prod.whoop.com/developer"
TOKEN_URL = "https://api.prod.whoop.com/oauth/oauth2/token"
PULL_PAGE_LIMIT = 25
INCREMENTAL_OVERLAP = timedelta(days=1)
MAX_RETRIES_PER_PAGE = 5
BASE_RETRY_DELAY_SECONDS = 2.0
INTER_PAGE_DELAY_SECONDS = 0.3

# COMMAND ----------


class WhoopOAuthConfig(BaseModel):
    client_id: str
    client_secret: str
    refresh_token: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    expires_in: int
    scope: str
    token_type: str


class WhoopEndpointConfig(BaseModel):
    endpoint_name: str
    endpoint_path: str
    table_name: str
    natural_key_field: str


ENDPOINTS: list[WhoopEndpointConfig] = [
    WhoopEndpointConfig(
        endpoint_name="recovery",
        endpoint_path="/v2/recovery",
        table_name=f"{BRONZE_SCHEMA}.recovery",
        natural_key_field="cycle_id",
    ),
    WhoopEndpointConfig(
        endpoint_name="cycle",
        endpoint_path="/v2/cycle",
        table_name=f"{BRONZE_SCHEMA}.cycle",
        natural_key_field="id",
    ),
    WhoopEndpointConfig(
        endpoint_name="sleep",
        endpoint_path="/v2/activity/sleep",
        table_name=f"{BRONZE_SCHEMA}.sleep",
        natural_key_field="id",
    ),
    WhoopEndpointConfig(
        endpoint_name="workout",
        endpoint_path="/v2/activity/workout",
        table_name=f"{BRONZE_SCHEMA}.workout",
        natural_key_field="id",
    ),
]

# COMMAND ----------


def ensure_bronze_tables() -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_SCHEMA}.ingestion_state (
            endpoint_name STRING,
            last_pulled_at TIMESTAMP
        ) USING DELTA
    """)
    for endpoint in ENDPOINTS:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {endpoint.table_name} (
                natural_key STRING,
                raw_payload STRING,
                ingested_at TIMESTAMP
            ) USING DELTA
        """)

# COMMAND ----------


def load_oauth_config() -> WhoopOAuthConfig:
    return WhoopOAuthConfig(
        client_id=dbutils.secrets.get(scope=SECRET_SCOPE, key="whoop_client_id"),
        client_secret=dbutils.secrets.get(scope=SECRET_SCOPE, key="whoop_client_secret"),
        refresh_token=dbutils.secrets.get(scope=SECRET_SCOPE, key="whoop_refresh_token"),
    )


async def refresh_access_token(config: WhoopOAuthConfig) -> TokenResponse:
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(
            TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": config.refresh_token,
                "client_id": config.client_id,
                "client_secret": config.client_secret,
            },
        )
    response.raise_for_status()
    return TokenResponse.model_validate(response.json())


async def persist_rotated_refresh_token(new_refresh_token: str) -> None:
    workspace_client = WorkspaceClient()
    await asyncio.to_thread(
        workspace_client.secrets.put_secret,
        scope=SECRET_SCOPE,
        key="whoop_refresh_token",
        string_value=new_refresh_token,
    )

# COMMAND ----------


async def fetch_page_with_retry(client: httpx.AsyncClient, url: str, query: dict[str, str | int]) -> dict:
    for attempt in range(MAX_RETRIES_PER_PAGE):
        response = await client.get(url, params=query)
        is_rate_limited = response.status_code == 429
        if not is_rate_limited:
            response.raise_for_status()
            return response.json()

        retry_after = response.headers.get("Retry-After")
        delay = float(retry_after) if retry_after else BASE_RETRY_DELAY_SECONDS * (2 ** attempt)
        print(f"Rate limited (429) on {url}, retrying in {delay:.0f}s (attempt {attempt + 1}/{MAX_RETRIES_PER_PAGE})...")
        await asyncio.sleep(delay)

    raise RuntimeError(f"Exceeded {MAX_RETRIES_PER_PAGE} retries after repeated 429s for {url}")


async def pull_all_records(endpoint: WhoopEndpointConfig, access_token: str, start: datetime | None) -> list[dict]:
    records: list[dict] = []
    next_token: str | None = None
    headers = {"Authorization": f"Bearer {access_token}"}

    async with httpx.AsyncClient(timeout=30, headers=headers) as client:
        while True:
            query: dict[str, str | int] = {"limit": PULL_PAGE_LIMIT}
            if start is not None:
                query["start"] = start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            if next_token:
                query["nextToken"] = next_token

            payload = await fetch_page_with_retry(client, f"{API_BASE_URL}{endpoint.endpoint_path}", query)

            records.extend(payload.get("records", []))
            next_token = payload.get("next_token")
            has_more_pages = bool(next_token)
            if not has_more_pages:
                break
            await asyncio.sleep(INTER_PAGE_DELAY_SECONDS)

    return records

# COMMAND ----------


def land_records_to_bronze(endpoint: WhoopEndpointConfig, records: list[dict], ingested_at: datetime) -> int:
    if not records:
        return 0

    rows = [
        {
            "natural_key": str(record.get(endpoint.natural_key_field)),
            "raw_payload": json.dumps(record),
            "ingested_at": ingested_at,
        }
        for record in records
    ]

    staging_df = spark.createDataFrame(rows)
    staging_df.createOrReplaceTempView("whoop_staging_records")

    spark.sql(f"""
        MERGE INTO {endpoint.table_name} AS target
        USING whoop_staging_records AS source
        ON target.natural_key = source.natural_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    return len(rows)

# COMMAND ----------


def read_last_pulled_at(endpoint_name: str) -> datetime | None:
    rows = spark.sql(
        f"SELECT last_pulled_at FROM {BRONZE_SCHEMA}.ingestion_state WHERE endpoint_name = :endpoint_name",
        args={"endpoint_name": endpoint_name},
    ).collect()
    return rows[0]["last_pulled_at"] if rows else None


def write_last_pulled_at(endpoint_name: str, pulled_at: datetime) -> None:
    spark.sql(
        f"""
        MERGE INTO {BRONZE_SCHEMA}.ingestion_state AS target
        USING (SELECT :endpoint_name AS endpoint_name, :pulled_at AS last_pulled_at) AS source
        ON target.endpoint_name = source.endpoint_name
        WHEN MATCHED THEN UPDATE SET target.last_pulled_at = source.last_pulled_at
        WHEN NOT MATCHED THEN INSERT *
        """,
        args={"endpoint_name": endpoint_name, "pulled_at": pulled_at},
    )

# COMMAND ----------


async def run_ingestion_for_endpoint(endpoint: WhoopEndpointConfig, access_token: str) -> int:
    last_pulled_at = read_last_pulled_at(endpoint.endpoint_name)
    is_backfill = last_pulled_at is None
    start = None if is_backfill else last_pulled_at - INCREMENTAL_OVERLAP

    records = await pull_all_records(endpoint, access_token, start)
    ingested_at = datetime.now(timezone.utc)
    landed_count = land_records_to_bronze(endpoint, records, ingested_at)
    write_last_pulled_at(endpoint.endpoint_name, ingested_at)

    pull_kind = "backfill" if is_backfill else "incremental"
    print(f"{endpoint.endpoint_name}: {pull_kind} pull landed {landed_count} records")
    return landed_count

# COMMAND ----------


async def run_bronze_ingestion() -> None:
    ensure_bronze_tables()

    config = load_oauth_config()
    tokens = await refresh_access_token(config)
    await persist_rotated_refresh_token(tokens.refresh_token)

    for endpoint in ENDPOINTS:
        await run_ingestion_for_endpoint(endpoint, tokens.access_token)


# Databricks notebook cells run on an active asyncio event loop, so top-level
# `await` works here. If this specific runtime/compute raises a SyntaxError or
# RuntimeError on it, replace this line with: asyncio.run(run_bronze_ingestion())
await run_bronze_ingestion()
