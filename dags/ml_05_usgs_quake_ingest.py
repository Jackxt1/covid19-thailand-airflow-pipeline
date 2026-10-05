"""
ml_05_usgs_quake_ingest.py
==========================
ดึงข้อมูลแผ่นดินไหวทั่วโลกจาก USGS FDSN Event API เข้า PostgreSQL

ต่างจาก pipeline โควิดตรงที่แหล่งข้อมูลเป็น API ไม่ใช่ไฟล์ จึงออกแบบเป็น
incremental ETL เต็มรูปแบบ

- schedule รายเดือน + catchup=True ให้ Airflow ไล่ backfill ย้อนหลังเองทีละเดือน
- แต่ละ DAG run รับผิดชอบข้อมูลเฉพาะเดือนของตัวเอง (data_interval)
- เขียนแบบ idempotent: ลบช่วงเวลาของเดือนนั้นทิ้งก่อนแล้วค่อยใส่ใหม่
  รันซ้ำเดือนไหนกี่ครั้งข้อมูลก็ไม่ซ้ำ และแก้ข้อมูลย้อนหลังได้ด้วยการ clear task

ข้อจำกัดของ API ที่ออกแบบรับไว้แล้ว
- ขอข้อมูลช่วงยาว ๆ ทีเดียวไม่ได้ เซิร์ฟเวอร์ตอบ 503/504 (ทดสอบแล้ว)
  จึงยิงทีละเดือน ซึ่งมีราว 6,500-16,500 เหตุการณ์
- หนึ่ง request ได้สูงสุด 20,000 แถว จึงมีการแบ่งหน้าด้วย limit/offset รองรับ
  เดือนที่มี aftershock ถี่เป็นพิเศษ
"""

import csv
import io
import time as time_module
from datetime import datetime, timedelta

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# -----------------------------------------------------------------
# Config
# -----------------------------------------------------------------

default_args = {
    "owner": "workshop2",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

POSTGRES_CONN_ID = "postgres_target"
TABLE = "quake_events"

API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

# เพดานของ FDSN ต่อหนึ่ง request
PAGE_SIZE = 20000

# จำนวนแถวต่อหนึ่งรอบ INSERT
CHUNK_SIZE = 5000

REQUEST_TIMEOUT = 180
MAX_ATTEMPTS = 4

# เริ่ม backfill ปี 2015 ได้ข้อมูลราว 1.4 ล้านเหตุการณ์
# ถ้าอยากได้มากกว่านี้ เลื่อนปีให้เก่าลง (ถึง 2000 จะได้ราว 2.8 ล้าน)
BACKFILL_START = datetime(2015, 1, 1)


# -----------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------

def _to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_timestamp(value):
    """USGS ส่งเวลามาเป็น ISO 8601 ลงท้าย Z"""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        pass
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None


def _fetch_page(start_iso, end_iso, offset):
    """
    ขอข้อมูลหนึ่งหน้าจาก API พร้อม retry

    API ตอบ 204 เมื่อไม่มีข้อมูลในช่วงนั้น ซึ่งไม่ใช่ข้อผิดพลาด
    ส่วน 503/504 เป็นอาการเซิร์ฟเวอร์ล้า ให้รอแล้วลองใหม่
    """
    params = {
        "format": "csv",
        "starttime": start_iso,
        "endtime": end_iso,
        "orderby": "time-asc",
        "limit": PAGE_SIZE,
        "offset": offset,
    }

    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(API_URL, params=params, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            last_error = f"ต่อ API ไม่ได้: {exc}"
        else:
            if response.status_code == 204:
                return []
            if response.status_code == 200:
                return list(csv.DictReader(io.StringIO(response.text)))
            last_error = f"API ตอบ {response.status_code}: {response.text[:200]}"

        wait = 10 * attempt
        print(f"  ลองใหม่ครั้งที่ {attempt}/{MAX_ATTEMPTS} ใน {wait} วินาที — {last_error}")
        time_module.sleep(wait)

    raise RuntimeError(f"ดึงข้อมูลไม่สำเร็จหลังลอง {MAX_ATTEMPTS} ครั้ง — {last_error}")


# -----------------------------------------------------------------
# Tasks
# -----------------------------------------------------------------

def create_tables(**kwargs):
    """สร้างตารางถ้ายังไม่มี — ปลอดภัยที่จะรันทุก DAG run"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            event_id    TEXT PRIMARY KEY,
            occurred_at TIMESTAMP NOT NULL,
            latitude    DOUBLE PRECISION,
            longitude   DOUBLE PRECISION,
            depth_km    DOUBLE PRECISION,
            magnitude   DOUBLE PRECISION,
            mag_type    TEXT,
            place       TEXT,
            event_type  TEXT,
            network     TEXT,
            status      TEXT,
            updated_at  TIMESTAMP,
            ingested_at TIMESTAMP NOT NULL DEFAULT now()
        )
        """
    )
    hook.run(f"CREATE INDEX IF NOT EXISTS {TABLE}_occurred_at_idx ON {TABLE} (occurred_at)")
    hook.run(f"CREATE INDEX IF NOT EXISTS {TABLE}_magnitude_idx ON {TABLE} (magnitude)")
    print(f"ตาราง {TABLE} พร้อมใช้งาน")


def fetch_window(**kwargs):
    """
    ดึงข้อมูลเฉพาะเดือนของ DAG run นี้ แล้วเขียนทับช่วงเวลาเดิม

    ใช้ data_interval ของ Airflow เป็นขอบเขต ทำให้ backfill ย้อนหลัง
    และรันซ้ำได้โดยไม่ต้องแก้โค้ด
    """
    from psycopg2.extras import execute_values

    window_start = kwargs["data_interval_start"]
    window_end = kwargs["data_interval_end"]
    start_iso = window_start.strftime("%Y-%m-%dT%H:%M:%S")
    end_iso = window_end.strftime("%Y-%m-%dT%H:%M:%S")

    print(f"ช่วงเวลาที่รับผิดชอบ: {start_iso} ถึง {end_iso}")

    rows = []
    offset = 1
    while True:
        page = _fetch_page(start_iso, end_iso, offset)
        print(f"  offset {offset}: ได้ {len(page):,} แถว")
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    connection = hook.get_conn()
    cursor = connection.cursor()

    try:
        # ลบของเดิมในหน้าต่างเวลานี้ก่อน เพื่อให้รันซ้ำแล้วไม่ซ้ำข้อมูล
        cursor.execute(
            f"DELETE FROM {TABLE} WHERE occurred_at >= %s AND occurred_at < %s",
            (window_start, window_end),
        )
        deleted = cursor.rowcount

        records = []
        skipped = 0
        for row in rows:
            occurred_at = _to_timestamp(row.get("time"))
            event_id = (row.get("id") or "").strip()
            if not event_id or occurred_at is None:
                skipped += 1
                continue
            records.append((
                event_id,
                occurred_at,
                _to_float(row.get("latitude")),
                _to_float(row.get("longitude")),
                _to_float(row.get("depth")),
                _to_float(row.get("mag")),
                (row.get("magType") or "").strip() or None,
                (row.get("place") or "").strip() or None,
                (row.get("type") or "").strip() or None,
                (row.get("net") or "").strip() or None,
                (row.get("status") or "").strip() or None,
                _to_timestamp(row.get("updated")),
            ))

        insert_sql = (
            f"INSERT INTO {TABLE} (event_id, occurred_at, latitude, longitude, "
            f"depth_km, magnitude, mag_type, place, event_type, network, status, "
            f"updated_at) VALUES %s ON CONFLICT (event_id) DO NOTHING"
        )

        inserted = 0
        for i in range(0, len(records), CHUNK_SIZE):
            batch = records[i:i + CHUNK_SIZE]
            execute_values(cursor, insert_sql, batch, page_size=2000)
            inserted += len(batch)

        connection.commit()
    finally:
        cursor.close()
        connection.close()

    print(
        f"เดือน {window_start:%Y-%m}: ลบของเดิม {deleted:,} แถว "
        f"ใส่ใหม่ {inserted:,} แถว (ข้ามแถวเสีย {skipped:,})"
    )
    kwargs["ti"].xcom_push(key="inserted", value=inserted)
    return inserted


def log_window(**kwargs):
    """สรุปผลของเดือนนี้ พร้อมยอดสะสมทั้งตาราง"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    inserted = kwargs["ti"].xcom_pull(task_ids="fetch_window", key="inserted") or 0

    total, first_day, last_day = hook.get_first(
        f"SELECT count(*), min(occurred_at), max(occurred_at) FROM {TABLE}"
    )

    print("-" * 56)
    print(f"เดือนนี้เพิ่มมา    : {inserted:,} เหตุการณ์")
    print(f"ยอดสะสมในตาราง   : {total:,} เหตุการณ์")
    print(f"ครอบคลุมช่วง      : {first_day} ถึง {last_day}")
    print("-" * 56)


# -----------------------------------------------------------------
# DAG
# -----------------------------------------------------------------

with DAG(
    dag_id="usgs_quake_ingest",
    description="ดึงแผ่นดินไหวทั่วโลกจาก USGS API ทีละเดือน พร้อม backfill ย้อนหลัง",
    default_args=default_args,
    start_date=BACKFILL_START,
    schedule="@monthly",
    catchup=True,          # ให้ Airflow ไล่เติมเดือนที่ยังไม่เคยรันเอง
    max_active_runs=3,     # จำกัดไม่ให้ยิง API พร้อมกันเกินไป
    max_active_tasks=3,
    tags=["usgs", "earthquake", "api", "incremental", "workshop2"],
) as dag:

    create_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )

    fetch_task = PythonOperator(
        task_id="fetch_window",
        python_callable=fetch_window,
    )

    log_task = PythonOperator(
        task_id="log_window",
        python_callable=log_window,
    )

    create_task >> fetch_task >> log_task
