"""
ml_04_covid_dashboard.py
========================
ETL Pipeline บน Apache Airflow — สรุปข้อมูลผู้ป่วยยืนยันโควิด-19 ของไทย
เพื่อเสิร์ฟให้หน้าแดชบอร์ดที่ http://localhost:8001/covid

แหล่งข้อมูล
- ไฟล์ Excel จากกรมควบคุมโรค (DDC) ที่ mount ไว้ที่ /opt/airflow/data/covid
- 5 ไฟล์ รวมประมาณ 4.26 ล้านแถว ช่วงเวลาทับซ้อนกันบางส่วน

หลักการที่ยึด
- อ่านแบบ streaming (openpyxl read_only) แล้วเขียนลง Postgres เป็นชุด ๆ
  ไม่โหลดทั้งไฟล์เข้าหน่วยความจำ — ไฟล์ขนาด 1 ล้านแถวทำให้ worker โดน OOM kill ได้
- ทุก task เขียนแบบ idempotent (สร้างตารางใหม่ก่อนโหลด) รันซ้ำกี่ครั้งข้อมูลก็ไม่ซ้ำ
- ตัด duplicate ข้ามไฟล์ด้วยเลขเคส (case_no) ซึ่งเป็นเลขรันระดับประเทศ
- หน้าเว็บอ่านจากตารางสรุปเท่านั้น ไม่แตะตารางดิบ 4 ล้านแถว
"""

import os
import re
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# -----------------------------------------------------------------
# Config
# -----------------------------------------------------------------

default_args = {
    "owner": "workshop2",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

POSTGRES_CONN_ID = "postgres_target"

DATA_DIR = "/opt/airflow/data/covid"

# โฟลเดอร์ปลายทางของไฟล์ CSV สำหรับ Power BI (host: ./exports)
EXPORT_DIR = "/opt/airflow/exports"

# ไฟล์ที่ต้องมี เรียงจากช่วงเวลาเก่าสุดไปใหม่สุด
SOURCE_FILES = [
    "confirmed-cases.xlsx",
    "confirmed-cases-since-120864.xlsx",
    "confirmed-cases-since-271064.xlsx",
    "confirmed-cases-since-120465.xlsx",
    "confirmed-cases-since-280265.xlsx",
]

RAW_TABLE = "covid_cases_raw"
CLEAN_TABLE = "covid_cases"

# จำนวนแถวต่อหนึ่งรอบการ INSERT
CHUNK_SIZE = 50000

# ขอบเขตที่ยอมรับได้ ใช้คัดแถวเสียทิ้งในขั้น clean
MIN_AGE = 0
MAX_AGE = 120
MIN_DATE = date(2020, 1, 1)
MAX_DATE = date(2023, 12, 31)

# ชื่อคอลัมน์ในไฟล์ต้นทางไม่ตรงกันทุกไฟล์ จึง map เข้าหาชื่อกลางชุดเดียว
COLUMN_ALIASES = {
    "no": "case_no",
    "no.": "case_no",
    "announce_date": "announce_date",
    "announce date": "announce_date",
    "notified date": "notified_date",
    "notified_date": "notified_date",
    "sex": "sex",
    "age": "age",
    "unit": "age_unit",
    "nationality": "nationality",
    "province_of_isolation": "province_of_isolation",
    "province of isolation": "province_of_isolation",
    "risk": "risk",
    "province_of_onset": "province_of_onset",
    "province of onset": "province_of_onset",
    "district_of_onset": "district_of_onset",
    "district of onset": "district_of_onset",
}

TARGET_COLUMNS = [
    "case_no",
    "announce_date",
    "notified_date",
    "sex",
    "age",
    "age_unit",
    "nationality",
    "province_of_isolation",
    "risk",
    "province_of_onset",
    "district_of_onset",
]

# Excel เก็บวันที่เป็นจำนวนวันนับจาก 1899-12-30 (ระบบ 1900 ของ Windows)
EXCEL_EPOCH = date(1899, 12, 30)


# -----------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------

def _normalize_header(value):
    """ทำให้ชื่อคอลัมน์เทียบกันได้ ไม่ว่าจะเว้นวรรคหรือตัวพิมพ์แบบไหน"""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().lower()


def _to_date(value):
    """แปลงค่าวันที่จาก Excel ให้เป็น date — รองรับทั้ง serial, datetime และสตริง"""
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    if isinstance(value, (int, float)):
        try:
            return EXCEL_EPOCH + timedelta(days=int(value))
        except (ValueError, OverflowError):
            return None

    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _to_int(value):
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_text(value):
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def _db_hook():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


# -----------------------------------------------------------------
# Tasks
# -----------------------------------------------------------------

def check_source_files(**kwargs):
    """ยืนยันว่าไฟล์ต้นทางครบก่อนเริ่มโหลด — ขาดไฟล์ไหนให้ fail ทันที"""
    missing = []
    found = []

    for name in SOURCE_FILES:
        path = os.path.join(DATA_DIR, name)
        if os.path.exists(path):
            size_mb = os.path.getsize(path) / (1024 * 1024)
            found.append(f"{name} ({size_mb:.1f} MB)")
        else:
            missing.append(name)

    if missing:
        raise FileNotFoundError(
            f"ไม่พบไฟล์ต้นทางใน {DATA_DIR}: {', '.join(missing)} — "
            "ตรวจ volume COVID_DATA_DIR ใน docker-compose.yaml"
        )

    print(f"พบไฟล์ครบ {len(found)} ไฟล์:")
    for item in found:
        print(f"  - {item}")

    return found


def create_raw_table(**kwargs):
    """สร้างตารางดิบใหม่ทุกครั้ง เพื่อให้รันซ้ำแล้วข้อมูลไม่ทับซ้อน"""
    hook = _db_hook()
    hook.run(f"DROP TABLE IF EXISTS {RAW_TABLE}")
    hook.run(
        f"""
        CREATE TABLE {RAW_TABLE} (
            case_no               BIGINT,
            announce_date         DATE,
            notified_date         DATE,
            sex                   TEXT,
            age                   INTEGER,
            age_unit              TEXT,
            nationality           TEXT,
            province_of_isolation TEXT,
            risk                  TEXT,
            province_of_onset     TEXT,
            district_of_onset     TEXT,
            source_file           TEXT NOT NULL
        )
        """
    )
    print(f"สร้างตาราง {RAW_TABLE} เรียบร้อย")


def load_raw_file(file_name, **kwargs):
    """
    อ่านไฟล์ Excel หนึ่งไฟล์แบบ streaming แล้ว INSERT ลง Postgres เป็นชุดละ CHUNK_SIZE

    ใช้ openpyxl read_only + values_only เพื่อให้หน่วยความจำคงที่
    ไม่ว่าไฟล์จะมีกี่ล้านแถว
    """
    from openpyxl import load_workbook
    from psycopg2.extras import execute_values

    path = os.path.join(DATA_DIR, file_name)
    workbook = load_workbook(path, read_only=True, data_only=True)
    sheet = workbook[workbook.sheetnames[0]]

    rows_iter = sheet.iter_rows(values_only=True)

    try:
        header_row = next(rows_iter)
    except StopIteration:
        workbook.close()
        raise ValueError(f"{file_name} ไม่มีข้อมูล")

    # หาว่าคอลัมน์กลางแต่ละตัวอยู่ตำแหน่งไหนในไฟล์นี้
    index_of = {}
    for position, raw_name in enumerate(header_row):
        key = COLUMN_ALIASES.get(_normalize_header(raw_name))
        if key and key not in index_of:
            index_of[key] = position

    missing = [c for c in ("case_no", "announce_date") if c not in index_of]
    if missing:
        workbook.close()
        raise ValueError(
            f"{file_name} ไม่มีคอลัมน์ที่จำเป็น: {missing} "
            f"(header ที่อ่านได้: {header_row})"
        )

    print(f"{file_name} -> mapping คอลัมน์: {index_of}")

    insert_sql = (
        f"INSERT INTO {RAW_TABLE} "
        f"({', '.join(TARGET_COLUMNS)}, source_file) VALUES %s"
    )

    hook = _db_hook()
    connection = hook.get_conn()
    cursor = connection.cursor()

    buffer = []
    total = 0

    def pick(row, column):
        position = index_of.get(column)
        if position is None or position >= len(row):
            return None
        return row[position]

    try:
        for row in rows_iter:
            if row is None:
                continue

            record = (
                _to_int(pick(row, "case_no")),
                _to_date(pick(row, "announce_date")),
                _to_date(pick(row, "notified_date")),
                _to_text(pick(row, "sex")),
                _to_int(pick(row, "age")),
                _to_text(pick(row, "age_unit")),
                _to_text(pick(row, "nationality")),
                _to_text(pick(row, "province_of_isolation")),
                _to_text(pick(row, "risk")),
                _to_text(pick(row, "province_of_onset")),
                _to_text(pick(row, "district_of_onset")),
                file_name,
            )

            # แถวว่างล้วน (ท้ายชีต) ข้ามไป
            if record[0] is None and record[1] is None and record[7] is None:
                continue

            buffer.append(record)

            if len(buffer) >= CHUNK_SIZE:
                execute_values(cursor, insert_sql, buffer, page_size=10000)
                connection.commit()
                total += len(buffer)
                buffer.clear()
                print(f"  {file_name}: โหลดแล้ว {total:,} แถว")

        if buffer:
            execute_values(cursor, insert_sql, buffer, page_size=10000)
            connection.commit()
            total += len(buffer)
    finally:
        cursor.close()
        connection.close()
        workbook.close()

    print(f"{file_name}: เสร็จสิ้น รวม {total:,} แถว")
    return total


def clean_transform(**kwargs):
    """
    ทำความสะอาดข้อมูลดิบแล้วเขียนลงตาราง covid_cases

    - ตัด duplicate ข้ามไฟล์ด้วย case_no (เลขรันระดับประเทศ)
    - normalize เพศให้เหลือ ชาย / หญิง / ไม่ระบุ
    - ตัดอายุนอกช่วง 0-120 และวันที่นอกช่วงข้อมูลจริงทิ้ง
    - เติม 'ไม่ระบุ' แทน null ในคอลัมน์ที่หน้าเว็บต้องใช้ group by
    """
    hook = _db_hook()

    hook.run(f"DROP TABLE IF EXISTS {CLEAN_TABLE}")
    hook.run(
        f"""
        CREATE TABLE {CLEAN_TABLE} AS
        WITH normalized AS (
            SELECT
                case_no,
                announce_date,
                notified_date,
                CASE
                    WHEN sex IS NULL THEN 'ไม่ระบุ'
                    WHEN lower(btrim(sex)) IN ('male', 'm', 'ชาย') THEN 'ชาย'
                    WHEN lower(btrim(sex)) IN ('female', 'f', 'หญิง') THEN 'หญิง'
                    ELSE 'ไม่ระบุ'
                END                                      AS sex,
                CASE
                    WHEN age BETWEEN {MIN_AGE} AND {MAX_AGE} THEN age
                    ELSE NULL
                END                                      AS age,
                -- ไฟล์ต้นทางสะกดสัญชาติเดียวกันหลายแบบ (Thailand/Thai,
                -- Burmese/Burma) ถ้าไม่รวมก่อน ตารางสรุปจะนับแยกเป็นคนละสัญชาติ
                CASE lower(btrim(COALESCE(nationality, '')))
                    WHEN ''             THEN 'ไม่ระบุ'
                    WHEN 'thai'         THEN 'Thailand'
                    WHEN 'thailand'     THEN 'Thailand'
                    WHEN 'ไทย'          THEN 'Thailand'
                    WHEN 'burma'        THEN 'Myanmar'
                    WHEN 'burmese'      THEN 'Myanmar'
                    WHEN 'myanmar'      THEN 'Myanmar'
                    WHEN 'พม่า'          THEN 'Myanmar'
                    WHEN 'cambodia'     THEN 'Cambodia'
                    WHEN 'cambodian'    THEN 'Cambodia'
                    WHEN 'กัมพูชา'       THEN 'Cambodia'
                    WHEN 'lao'          THEN 'Laos'
                    WHEN 'laos'         THEN 'Laos'
                    WHEN 'laotian'      THEN 'Laos'
                    WHEN 'laotian / lao' THEN 'Laos'
                    WHEN 'ลาว'           THEN 'Laos'
                    ELSE btrim(nationality)
                END                                                           AS nationality,
                COALESCE(NULLIF(btrim(province_of_isolation), ''), 'ไม่ระบุ') AS province,
                COALESCE(NULLIF(btrim(risk), ''), 'ไม่ระบุ')                  AS risk,
                COALESCE(NULLIF(btrim(district_of_onset), ''), 'ไม่ระบุ')     AS district,
                source_file,
                ROW_NUMBER() OVER (
                    PARTITION BY case_no
                    ORDER BY announce_date, source_file
                )                                        AS dup_rank
            FROM {RAW_TABLE}
            WHERE announce_date IS NOT NULL
              AND announce_date BETWEEN DATE '{MIN_DATE}' AND DATE '{MAX_DATE}'
              AND case_no IS NOT NULL
        )
        SELECT
            case_no,
            announce_date,
            notified_date,
            sex,
            age,
            CASE
                WHEN age IS NULL          THEN 'ไม่ระบุ'
                WHEN age < 10             THEN '0-9'
                WHEN age < 20             THEN '10-19'
                WHEN age < 30             THEN '20-29'
                WHEN age < 40             THEN '30-39'
                WHEN age < 50             THEN '40-49'
                WHEN age < 60             THEN '50-59'
                WHEN age < 70             THEN '60-69'
                ELSE '70+'
            END AS age_group,
            nationality,
            province,
            risk,
            district,
            source_file
        FROM normalized
        WHERE dup_rank = 1
        """
    )

    hook.run(f"CREATE INDEX ON {CLEAN_TABLE} (announce_date)")
    hook.run(f"CREATE INDEX ON {CLEAN_TABLE} (province)")

    raw_count = hook.get_first(f"SELECT count(*) FROM {RAW_TABLE}")[0]
    clean_count = hook.get_first(f"SELECT count(*) FROM {CLEAN_TABLE}")[0]
    dropped = raw_count - clean_count

    print(f"ดิบ {raw_count:,} แถว -> สะอาด {clean_count:,} แถว (ตัดทิ้ง {dropped:,} แถว)")

    kwargs["ti"].xcom_push(key="raw_count", value=raw_count)
    kwargs["ti"].xcom_push(key="clean_count", value=clean_count)
    return clean_count


def build_aggregates(**kwargs):
    """สร้างตารางสรุปให้หน้าเว็บอ่าน — เล็กพอที่จะ query ได้ในระดับมิลลิวินาที"""
    hook = _db_hook()

    statements = [
        # เคสรายวัน + ค่าเฉลี่ยเคลื่อนที่ 7 วัน
        f"""
        DROP TABLE IF EXISTS covid_daily;
        CREATE TABLE covid_daily AS
        WITH daily AS (
            SELECT announce_date AS day, count(*) AS cases
            FROM {CLEAN_TABLE}
            GROUP BY announce_date
        )
        SELECT
            day,
            cases,
            round(AVG(cases) OVER (
                ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 1) AS moving_avg_7d
        FROM daily
        ORDER BY day
        """,
        f"""
        DROP TABLE IF EXISTS covid_by_province;
        CREATE TABLE covid_by_province AS
        SELECT province, count(*) AS cases
        FROM {CLEAN_TABLE}
        GROUP BY province
        ORDER BY cases DESC
        """,
        f"""
        DROP TABLE IF EXISTS covid_by_age_group;
        CREATE TABLE covid_by_age_group AS
        SELECT age_group, count(*) AS cases
        FROM {CLEAN_TABLE}
        GROUP BY age_group
        """,
        f"""
        DROP TABLE IF EXISTS covid_by_sex;
        CREATE TABLE covid_by_sex AS
        SELECT sex, count(*) AS cases
        FROM {CLEAN_TABLE}
        GROUP BY sex
        ORDER BY cases DESC
        """,
        f"""
        DROP TABLE IF EXISTS covid_by_nationality;
        CREATE TABLE covid_by_nationality AS
        SELECT nationality, count(*) AS cases
        FROM {CLEAN_TABLE}
        GROUP BY nationality
        ORDER BY cases DESC
        """,
        # ตัวเลขสรุปบนหัวแดชบอร์ด เก็บเป็นแถวเดียว
        f"""
        DROP TABLE IF EXISTS covid_summary;
        CREATE TABLE covid_summary AS
        SELECT
            count(*)                        AS total_cases,
            min(announce_date)              AS first_date,
            max(announce_date)              AS last_date,
            round(avg(age) FILTER (WHERE age IS NOT NULL), 1) AS avg_age,
            count(DISTINCT province)        AS province_count,
            now()                           AS refreshed_at
        FROM {CLEAN_TABLE}
        """,
    ]

    for statement in statements:
        hook.run(statement)

    summary = hook.get_first(
        "SELECT total_cases, first_date, last_date, avg_age FROM covid_summary"
    )
    print(
        f"สรุป: {summary[0]:,} เคส | {summary[1]} ถึง {summary[2]} | "
        f"อายุเฉลี่ย {summary[3]} ปี"
    )
    return summary[0]


def data_quality_check(**kwargs):
    """ด่านตรวจคุณภาพ — ไม่ผ่านข้อไหน DAG ต้อง fail ไม่ปล่อยข้อมูลพังขึ้นหน้าเว็บ"""
    hook = _db_hook()
    problems = []

    clean_count = hook.get_first(f"SELECT count(*) FROM {CLEAN_TABLE}")[0]
    if clean_count == 0:
        problems.append("ตาราง covid_cases ว่างเปล่า")

    duplicate_count = hook.get_first(
        f"SELECT count(*) FROM (SELECT case_no FROM {CLEAN_TABLE} "
        f"GROUP BY case_no HAVING count(*) > 1) d"
    )[0]
    if duplicate_count > 0:
        problems.append(f"ยังมี case_no ซ้ำ {duplicate_count:,} ค่า")

    null_dates = hook.get_first(
        f"SELECT count(*) FROM {CLEAN_TABLE} WHERE announce_date IS NULL"
    )[0]
    if null_dates > 0:
        problems.append(f"announce_date เป็น null {null_dates:,} แถว")

    bad_age = hook.get_first(
        f"SELECT count(*) FROM {CLEAN_TABLE} "
        f"WHERE age IS NOT NULL AND (age < {MIN_AGE} OR age > {MAX_AGE})"
    )[0]
    if bad_age > 0:
        problems.append(f"อายุนอกช่วง {MIN_AGE}-{MAX_AGE} จำนวน {bad_age:,} แถว")

    daily_rows = hook.get_first("SELECT count(*) FROM covid_daily")[0]
    if daily_rows == 0:
        problems.append("ตารางสรุป covid_daily ว่างเปล่า")

    if problems:
        raise ValueError("ข้อมูลไม่ผ่านการตรวจ: " + " | ".join(problems))

    print(
        f"ผ่านการตรวจทั้งหมด — {clean_count:,} เคส, "
        f"{daily_rows:,} วัน, ไม่มี case_no ซ้ำ"
    )
    return clean_count


def export_for_powerbi(**kwargs):
    """
    เขียนไฟล์ CSV ชุด star schema ให้ Power BI import ตรงได้

    ตารางข้อเท็จจริงสรุปที่ระดับ วัน x จังหวัด x เพศ x ช่วงอายุ x สัญชาติ
    (ประมาณ 450,000 แถว) เล็กพอให้ Power BI โหลดเร็ว แต่ยังละเอียดพอ
    ให้สร้าง slicer ได้ทุกมิติที่หน้าแดชบอร์ดมี

    ทุกไฟล์เขียนเป็น UTF-8 พร้อม BOM เพื่อให้ Power BI และ Excel
    อ่านภาษาไทยถูกต้องโดยไม่ต้องเลือก encoding เอง
    """
    hook = _db_hook()
    connection = hook.get_conn()
    cursor = connection.cursor()

    os.makedirs(EXPORT_DIR, exist_ok=True)

    exports = {
        # ตารางข้อเท็จจริง — หนึ่งแถวคือหนึ่งชุดมิติ พร้อมจำนวนเคส
        "fact_covid_cases.csv": f"""
            SELECT
                announce_date AS "วันที่",
                province      AS "จังหวัด",
                sex           AS "เพศ",
                age_group     AS "ช่วงอายุ",
                nationality   AS "สัญชาติ",
                count(*)      AS "จำนวนผู้ป่วย"
            FROM {CLEAN_TABLE}
            GROUP BY announce_date, province, sex, age_group, nationality
            ORDER BY announce_date, province
        """,
        # มิติวันที่ — ครอบคลุมทุกวันในช่วงข้อมูล รวมวันที่ไม่มีเคส
        "dim_date.csv": f"""
            WITH bounds AS (
                SELECT min(announce_date) AS d1, max(announce_date) AS d2 FROM {CLEAN_TABLE}
            ), days AS (
                SELECT generate_series(d1, d2, INTERVAL '1 day')::date AS day FROM bounds
            )
            SELECT
                day                                   AS "วันที่",
                EXTRACT(YEAR FROM day)::int           AS "ปี ค.ศ.",
                EXTRACT(YEAR FROM day)::int + 543     AS "ปี พ.ศ.",
                EXTRACT(MONTH FROM day)::int          AS "เลขเดือน",
                (ARRAY['ม.ค.','ก.พ.','มี.ค.','เม.ย.','พ.ค.','มิ.ย.',
                       'ก.ค.','ส.ค.','ก.ย.','ต.ค.','พ.ย.','ธ.ค.'])
                    [EXTRACT(MONTH FROM day)::int]    AS "เดือน",
                to_char(day, 'YYYY-MM')               AS "ปีเดือน",
                EXTRACT(QUARTER FROM day)::int        AS "ไตรมาส",
                (ARRAY['จันทร์','อังคาร','พุธ','พฤหัสบดี','ศุกร์','เสาร์','อาทิตย์'])
                    [EXTRACT(ISODOW FROM day)::int]   AS "วันในสัปดาห์",
                -- เขียนเป็น TRUE/FALSE ไม่ใช่ boolean ตรง ๆ เพราะ COPY
                -- จะได้ t/f ซึ่ง Power BI อ่านเป็นข้อความ ไม่ใช่ค่าตรรกะ
                CASE WHEN EXTRACT(ISODOW FROM day) >= 6
                     THEN 'TRUE' ELSE 'FALSE' END     AS "วันหยุดสุดสัปดาห์"
            FROM days
            ORDER BY day
        """,
        # มิติจังหวัด พร้อมอันดับ ใช้ทำ top-N ใน Power BI ได้ทันที
        "dim_province.csv": """
            SELECT
                province AS "จังหวัด",
                cases    AS "ผู้ป่วยสะสม",
                ROW_NUMBER() OVER (ORDER BY cases DESC)::int AS "อันดับ"
            FROM covid_by_province
            ORDER BY cases DESC
        """,
        # มิติช่วงอายุ พร้อมลำดับ — ใช้ตั้ง Sort by column ใน Power BI
        # ไม่งั้นแกนจะเรียงตามตัวอักษรเป็น 0-9, 10-19, 20-29, 70+ ผิดลำดับ
        "dim_age_group.csv": """
            SELECT
                age_group AS "ช่วงอายุ",
                CASE age_group
                    WHEN '0-9'   THEN 1 WHEN '10-19' THEN 2 WHEN '20-29' THEN 3
                    WHEN '30-39' THEN 4 WHEN '40-49' THEN 5 WHEN '50-59' THEN 6
                    WHEN '60-69' THEN 7 WHEN '70+'   THEN 8 ELSE 9
                END       AS "ลำดับ",
                cases     AS "ผู้ป่วยสะสม"
            FROM covid_by_age_group
            ORDER BY 2
        """,
        # ค่าเฉลี่ยเคลื่อนที่ 7 วัน คำนวณมาให้แล้ว จะได้ไม่ต้องเขียน DAX เอง
        "summary_daily.csv": """
            SELECT
                day           AS "วันที่",
                cases         AS "ผู้ป่วยรายวัน",
                moving_avg_7d AS "เฉลี่ย 7 วัน"
            FROM covid_daily
            ORDER BY day
        """,
        "summary_overall.csv": """
            SELECT
                total_cases    AS "ผู้ป่วยสะสม",
                first_date     AS "วันแรกของข้อมูล",
                last_date      AS "วันสุดท้ายของข้อมูล",
                avg_age        AS "อายุเฉลี่ย",
                province_count AS "จำนวนจังหวัด",
                refreshed_at   AS "สรุปข้อมูลเมื่อ"
            FROM covid_summary
        """,
    }

    written = []
    try:
        for file_name, select_sql in exports.items():
            path = os.path.join(EXPORT_DIR, file_name)
            with open(path, "wb") as handle:
                handle.write(b"\xef\xbb\xbf")  # BOM
                cursor.copy_expert(
                    f"COPY ({select_sql}) TO STDOUT WITH CSV HEADER", handle
                )
            size_mb = os.path.getsize(path) / (1024 * 1024)
            # ลบหัวตารางออกจากการนับ จะได้เป็นจำนวนแถวข้อมูลจริง
            with open(path, "rb") as handle:
                rows = max(0, sum(1 for _ in handle) - 1)
            written.append(f"{file_name}: {rows:,} แถว ({size_mb:.1f} MB)")
            print(f"  เขียน {file_name} — {rows:,} แถว ({size_mb:.1f} MB)")
    finally:
        cursor.close()
        connection.close()

    print(f"ไฟล์สำหรับ Power BI อยู่ที่ {EXPORT_DIR} (บนเครื่อง host คือโฟลเดอร์ exports/)")
    return written


def log_result(**kwargs):
    """สรุปผลการรันไว้ใน log ให้อ่านย้อนหลังได้"""
    hook = _db_hook()
    ti = kwargs["ti"]

    raw_count = ti.xcom_pull(task_ids="clean_transform", key="raw_count")
    clean_count = ti.xcom_pull(task_ids="clean_transform", key="clean_count")

    summary = hook.get_first(
        "SELECT total_cases, first_date, last_date, avg_age, province_count "
        "FROM covid_summary"
    )
    top_province = hook.get_first(
        "SELECT province, cases FROM covid_by_province ORDER BY cases DESC LIMIT 1"
    )

    print("=" * 60)
    print("COVID-19 Dashboard Pipeline — สรุปผลการรัน")
    print("=" * 60)
    print(f"แถวดิบที่โหลด      : {raw_count:,}")
    print(f"แถวหลังทำความสะอาด : {clean_count:,}")
    print(f"ช่วงวันที่          : {summary[1]} ถึง {summary[2]}")
    print(f"อายุเฉลี่ย          : {summary[3]} ปี")
    print(f"จำนวนจังหวัด        : {summary[4]}")
    print(f"จังหวัดสูงสุด       : {top_province[0]} ({top_province[1]:,} เคส)")
    print("=" * 60)
    print("เปิดแดชบอร์ดที่ http://localhost:8001/covid")


# -----------------------------------------------------------------
# DAG
# -----------------------------------------------------------------

with DAG(
    dag_id="covid_dashboard_pipeline",
    description="ETL ข้อมูลผู้ป่วยโควิด-19 จากไฟล์ Excel ของ DDC เพื่อทำแดชบอร์ดสรุป",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,          # ข้อมูลเป็นไฟล์นิ่ง สั่งรันเองเมื่อมีไฟล์ใหม่
    catchup=False,
    max_active_tasks=3,     # จำกัดงานพร้อมกัน กัน worker กินแรมเกิน
    tags=["covid19", "etl", "dashboard", "workshop2"],
) as dag:

    check_task = PythonOperator(
        task_id="check_source_files",
        python_callable=check_source_files,
    )

    create_table_task = PythonOperator(
        task_id="create_raw_table",
        python_callable=create_raw_table,
    )

    load_tasks = [
        PythonOperator(
            task_id="load_raw__" + re.sub(r"[^0-9a-zA-Z]+", "_", name.replace(".xlsx", "")),
            python_callable=load_raw_file,
            op_kwargs={"file_name": name},
        )
        for name in SOURCE_FILES
    ]

    clean_task = PythonOperator(
        task_id="clean_transform",
        python_callable=clean_transform,
    )

    aggregate_task = PythonOperator(
        task_id="build_aggregates",
        python_callable=build_aggregates,
    )

    quality_task = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
    )

    export_task = PythonOperator(
        task_id="export_for_powerbi",
        python_callable=export_for_powerbi,
    )

    log_task = PythonOperator(
        task_id="log_result",
        python_callable=log_result,
    )

    check_task >> create_table_task >> load_tasks >> clean_task
    clean_task >> aggregate_task >> quality_task >> export_task >> log_task
