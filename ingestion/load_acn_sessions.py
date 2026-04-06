import json
import os
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_batch, Json

from db_utils import get_connection

load_dotenv()

BASE_URL = "https://ev.caltech.edu/api/v1/"
TOKEN = os.getenv("ACN_API_TOKEN")
SITE = os.getenv("ACN_SITE", "caltech")


def fetch_all_sessions(site: str, where_clause: str | None = None):
    url = f"{BASE_URL}sessions/{site}"
    params = {}
    if where_clause:
        params["where"] = where_clause

    all_items = []

    while url:
        response = requests.get(url, params=params if "sessions/" in url else None, auth=(TOKEN, ""))
        response.raise_for_status()
        data = response.json()

        items = data.get("_items", [])
        all_items.extend(items)

        next_link = data.get("_links", {}).get("next", {}).get("href")
        if next_link:
            url = urljoin(BASE_URL, next_link)
            params = None
        else:
            url = None

    return all_items


def transform_item(item: dict):
    return {
        "acn_id": item.get("_id"),
        "session_id": item.get("sessionID"),
        "user_id": item.get("userID"),
        "station_id": item.get("stationID"),
        "space_id": item.get("spaceID"),
        "site_id": item.get("siteID"),
        "cluster_id": item.get("clusterID"),
        "connection_time_raw": item.get("connectionTime"),
        "disconnect_time_raw": item.get("disconnectTime"),
        "done_charging_time_raw": item.get("doneChargingTime"),
        "timezone": item.get("timezone"),
        "kwh_delivered": item.get("kWhDelivered"),
        "user_inputs_json": item.get("userInputs"),
        "json_payload": item,
        "ingested_at": datetime.now(timezone.utc),
    }


def load_to_postgres(rows: list[dict]):
    insert_sql = """
        INSERT INTO raw.charging_sessions (
            acn_id,
            session_id,
            user_id,
            station_id,
            space_id,
            site_id,
            cluster_id,
            connection_time_raw,
            disconnect_time_raw,
            done_charging_time_raw,
            timezone,
            kwh_delivered,
            user_inputs_json,
            json_payload,
            ingested_at
        ) VALUES (
            %(acn_id)s,
            %(session_id)s,
            %(user_id)s,
            %(station_id)s,
            %(space_id)s,
            %(site_id)s,
            %(cluster_id)s,
            %(connection_time_raw)s,
            %(disconnect_time_raw)s,
            %(done_charging_time_raw)s,
            %(timezone)s,
            %(kwh_delivered)s,
            %(user_inputs_json)s,
            %(json_payload)s,
            %(ingested_at)s
        )
        ON CONFLICT (acn_id) DO UPDATE SET
            session_id = EXCLUDED.session_id,
            user_id = EXCLUDED.user_id,
            station_id = EXCLUDED.station_id,
            space_id = EXCLUDED.space_id,
            site_id = EXCLUDED.site_id,
            cluster_id = EXCLUDED.cluster_id,
            connection_time_raw = EXCLUDED.connection_time_raw,
            disconnect_time_raw = EXCLUDED.disconnect_time_raw,
            done_charging_time_raw = EXCLUDED.done_charging_time_raw,
            timezone = EXCLUDED.timezone,
            kwh_delivered = EXCLUDED.kwh_delivered,
            user_inputs_json = EXCLUDED.user_inputs_json,
            json_payload = EXCLUDED.json_payload,
            ingested_at = EXCLUDED.ingested_at;
    """

    formatted_rows = []
    for row in rows:
        formatted_rows.append({
            **row,
            "user_inputs_json": Json(row["user_inputs_json"]),
            "json_payload": Json(row["json_payload"]),
        })

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, insert_sql, formatted_rows, page_size=100)


def main():
    where_clause = (
        'connectionTime>="Wed, 1 May 2019 00:00:00 GMT" '
        'and connectionTime<="Sat, 1 Jun 2019 00:00:00 GMT"'
    )

    print(f"Fetching ACN sessions for site={SITE} ...")
    items = fetch_all_sessions(SITE, where_clause=where_clause)
    print(f"Fetched {len(items)} sessions")

    rows = [transform_item(item) for item in items]
    load_to_postgres(rows)

    print(f"Loaded {len(rows)} rows into raw.charging_sessions")


if __name__ == "__main__":
    main()