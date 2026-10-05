"""
ml_06_usgs_quake_marts.py
=========================
สร้างตารางสรุปจาก quake_events ให้หน้าเว็บและ Power BI อ่าน

แยกเป็นคนละ DAG กับตัวดึงข้อมูลโดยตั้งใจ เพราะ usgs_quake_ingest รันเดือนละครั้ง
และตอน backfill จะรันเป็นร้อยรอบ ถ้าสร้างตารางสรุปไว้ในนั้นด้วยก็จะสร้างซ้ำ
เป็นร้อยรอบโดยไม่จำเป็น DAG นี้จึงรันรวดเดียวหลัง ingest เสร็จ

ภูมิภาคของเหตุการณ์แกะจากคอลัมน์ place ของ USGS ซึ่งมีรูปแบบ
"96 km SSE of Sand Point, Alaska" จึงตัดเอาข้อความหลังคอมมาตัวสุดท้าย
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    "owner": "workshop2",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

POSTGRES_CONN_ID = "postgres_target"
SOURCE_TABLE = "quake_events"
EXPORT_DIR = "/opt/airflow/exports/quakes"

# ขนาดที่ถือว่าเป็นแผ่นดินไหวรุนแรงพอจะขึ้นแผนที่
STRONG_MAGNITUDE = 6.0


def _hook():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


# ใช้ซ้ำหลายที่ จึงดึงออกมาเป็นค่าคงที่
# USGS เขียนชื่อรัฐทั้งแบบย่อและเต็มปนกัน เช่น "14km SE of Ridgecrest, CA"
# กับ "10 km N of Hollister, California" ถ้าไม่รวมก่อน ตารางสรุปจะนับเป็นคนละภูมิภาค
REGION_ALIASES = {
    "CA": "California", "NV": "Nevada", "AK": "Alaska", "HI": "Hawaii",
    "OK": "Oklahoma", "UT": "Utah", "ID": "Idaho", "MT": "Montana",
    "WA": "Washington", "OR": "Oregon", "KS": "Kansas", "TN": "Tennessee",
    "TX": "Texas", "WY": "Wyoming", "NM": "New Mexico", "NC": "North Carolina",
    "MO": "Missouri", "CO": "Colorado", "AZ": "Arizona", "MX": "Mexico",
    "AR": "Arkansas", "AL": "Alabama", "GA": "Georgia", "SC": "South Carolina",
    "VA": "Virginia", "NY": "New York", "ME": "Maine", "NE": "Nebraska",
    "IL": "Illinois", "IN": "Indiana", "KY": "Kentucky", "PR": "Puerto Rico",
    "B.C.": "British Columbia", "CA.": "California",
}

_REGION_RAW = (
    "btrim(split_part(place, ',', array_length(string_to_array(place, ','), 1)))"
)

REGION_EXPR = (
    "COALESCE(NULLIF(CASE " + _REGION_RAW + " "
    + " ".join(f"WHEN '{code}' THEN '{name}'" for code, name in REGION_ALIASES.items())
    + " ELSE " + _REGION_RAW + " END, ''), 'ไม่ระบุ')"
)

# USGS ใส่ค่า sentinel อย่าง -9.99 และ -5 แทน "ไม่มีค่าขนาด" ไม่ใช่ขนาดจริง
# ส่วนค่าราว -2 เป็นแผ่นดินไหวจิ๋วที่เครื่องวัดสมัยใหม่จับได้จริง จึงเก็บไว้
MAGNITUDE_CLEAN_EXPR = "CASE WHEN magnitude < -3 THEN NULL ELSE magnitude END"

MAG_BAND_EXPR = f"""
    CASE
        WHEN {MAGNITUDE_CLEAN_EXPR} IS NULL THEN 'ไม่ระบุ'
        WHEN magnitude < 2      THEN 'ต่ำกว่า 2.0'
        WHEN magnitude < 3      THEN '2.0 - 2.9'
        WHEN magnitude < 4      THEN '3.0 - 3.9'
        WHEN magnitude < 5      THEN '4.0 - 4.9'
        WHEN magnitude < 6      THEN '5.0 - 5.9'
        WHEN magnitude < 7      THEN '6.0 - 6.9'
        ELSE '7.0 ขึ้นไป'
    END
"""

DEPTH_BAND_EXPR = """
    CASE
        WHEN depth_km IS NULL   THEN 'ไม่ระบุ'
        WHEN depth_km < 70      THEN 'ตื้น (น้อยกว่า 70 กม.)'
        WHEN depth_km < 300     THEN 'ปานกลาง (70 - 300 กม.)'
        ELSE 'ลึก (เกิน 300 กม.)'
    END
"""


def build_marts(**kwargs):
    """สร้างตารางสรุปทั้งชุด เล็กพอให้หน้าเว็บ query ได้ในระดับมิลลิวินาที"""
    hook = _hook()

    statements = [
        # ตารางกลางที่ตารางสรุปอื่นอ้างอิง มีคอลัมน์ที่แกะแล้ว
        f"""
        DROP TABLE IF EXISTS quake_enriched;
        CREATE TABLE quake_enriched AS
        SELECT
            event_id,
            occurred_at,
            occurred_at::date          AS occurred_on,
            date_trunc('month', occurred_at)::date AS occurred_month,
            latitude,
            longitude,
            depth_km,
            {MAGNITUDE_CLEAN_EXPR}     AS magnitude,
            mag_type,
            place,
            {REGION_EXPR}              AS region,
            {MAG_BAND_EXPR}            AS magnitude_band,
            {DEPTH_BAND_EXPR}          AS depth_band
        FROM {SOURCE_TABLE}
        WHERE event_type = 'earthquake'
        """,
        # USGS สะกดชื่อภูมิภาคเดียวกันด้วยตัวพิมพ์ต่างกัน เช่น
        # "off the coast of Oregon" กับ "Off the coast of Oregon"
        # PostgreSQL จัดกลุ่มแบบแยกตัวพิมพ์ แต่ Power BI เทียบข้อความแบบไม่สนตัวพิมพ์
        # ถ้าไม่รวมก่อน จะกลายเป็นคีย์ซ้ำในฝั่ง one ของความสัมพันธ์แล้วโหลดไม่ผ่าน
        # เลือกตัวสะกดที่พบบ่อยที่สุดเป็นตัวแทน ไม่ใช้ initcap เพราะจะได้ชื่อที่อ่านแปลก
        """
        DROP TABLE IF EXISTS region_canonical;
        CREATE TABLE region_canonical AS
        SELECT DISTINCT ON (lower(region))
            lower(region) AS region_key,
            region        AS region_label
        FROM quake_enriched
        GROUP BY lower(region), region
        ORDER BY lower(region), count(*) DESC, region
        """,
        """
        UPDATE quake_enriched e
        SET region = c.region_label
        FROM region_canonical c
        WHERE lower(e.region) = c.region_key
          AND e.region <> c.region_label
        """,
        "CREATE INDEX ON quake_enriched (occurred_month)",
        "CREATE INDEX ON quake_enriched (region)",
        f"""
        DROP TABLE IF EXISTS quake_monthly;
        CREATE TABLE quake_monthly AS
        SELECT
            occurred_month                              AS month,
            count(*)                                    AS events,
            round(avg(magnitude)::numeric, 2)           AS avg_magnitude,
            max(magnitude)                              AS max_magnitude,
            round(avg(depth_km)::numeric, 1)            AS avg_depth_km,
            count(*) FILTER (WHERE magnitude >= 5)      AS events_m5_plus
        FROM quake_enriched
        GROUP BY occurred_month
        ORDER BY occurred_month
        """,
        f"""
        DROP TABLE IF EXISTS quake_by_magnitude;
        CREATE TABLE quake_by_magnitude AS
        SELECT
            magnitude_band,
            count(*) AS events,
            CASE magnitude_band
                WHEN 'ต่ำกว่า 2.0' THEN 1 WHEN '2.0 - 2.9' THEN 2
                WHEN '3.0 - 3.9'   THEN 3 WHEN '4.0 - 4.9' THEN 4
                WHEN '5.0 - 5.9'   THEN 5 WHEN '6.0 - 6.9' THEN 6
                WHEN '7.0 ขึ้นไป'  THEN 7 ELSE 8
            END AS sort_order
        FROM quake_enriched
        GROUP BY magnitude_band
        ORDER BY 3
        """,
        f"""
        DROP TABLE IF EXISTS quake_by_region;
        CREATE TABLE quake_by_region AS
        SELECT
            region,
            count(*)                          AS events,
            max(magnitude)                    AS max_magnitude,
            round(avg(depth_km)::numeric, 1)  AS avg_depth_km
        FROM quake_enriched
        GROUP BY region
        ORDER BY events DESC
        """,
        f"""
        DROP TABLE IF EXISTS quake_by_depth;
        CREATE TABLE quake_by_depth AS
        SELECT
            depth_band,
            count(*) AS events,
            CASE depth_band
                WHEN 'ตื้น (น้อยกว่า 70 กม.)'  THEN 1
                WHEN 'ปานกลาง (70 - 300 กม.)' THEN 2
                WHEN 'ลึก (เกิน 300 กม.)'      THEN 3
                ELSE 4
            END AS sort_order
        FROM quake_enriched
        GROUP BY depth_band
        ORDER BY 3
        """,
        f"""
        DROP TABLE IF EXISTS quake_strongest;
        CREATE TABLE quake_strongest AS
        SELECT event_id, occurred_at, magnitude, depth_km, place, region, latitude, longitude
        FROM quake_enriched
        WHERE magnitude IS NOT NULL
        ORDER BY magnitude DESC, occurred_at DESC
        LIMIT 20
        """,
        # จุดสำหรับวาดแผนที่ เอาเฉพาะที่แรงพอ ไม่งั้นจุดทับกันจนอ่านไม่ออก
        f"""
        DROP TABLE IF EXISTS quake_map_points;
        CREATE TABLE quake_map_points AS
        SELECT event_id, occurred_at, latitude, longitude, magnitude, depth_km, place
        FROM quake_enriched
        WHERE magnitude >= {STRONG_MAGNITUDE}
          AND latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY occurred_at
        """,
        f"""
        DROP TABLE IF EXISTS quake_summary;
        CREATE TABLE quake_summary AS
        SELECT
            count(*)                                   AS total_events,
            min(occurred_at)                           AS first_event,
            max(occurred_at)                           AS last_event,
            max(magnitude)                             AS max_magnitude,
            round(avg(magnitude)::numeric, 2)          AS avg_magnitude,
            round(avg(depth_km)::numeric, 1)           AS avg_depth_km,
            count(DISTINCT region)                     AS region_count,
            count(*) FILTER (WHERE magnitude >= 6)     AS events_m6_plus,
            now()                                      AS refreshed_at
        FROM quake_enriched
        """,
    ]

    for statement in statements:
        hook.run(statement)

    summary = hook.get_first(
        "SELECT total_events, first_event, last_event, max_magnitude FROM quake_summary"
    )
    print(
        f"สรุป: {summary[0]:,} เหตุการณ์ | {summary[1]} ถึง {summary[2]} | "
        f"แรงสุด M{summary[3]}"
    )
    kwargs["ti"].xcom_push(key="total_events", value=summary[0])
    return summary[0]


def quality_check(**kwargs):
    """ไม่ผ่านข้อใด DAG fail ข้อมูลพังจะได้ไม่ขึ้นหน้าเว็บ"""
    hook = _hook()
    problems = []

    total = hook.get_first("SELECT count(*) FROM quake_enriched")[0]
    if total == 0:
        problems.append("ตาราง quake_enriched ว่างเปล่า")

    dup = hook.get_first(
        "SELECT count(*) FROM (SELECT event_id FROM quake_enriched "
        "GROUP BY event_id HAVING count(*) > 1) d"
    )[0]
    if dup > 0:
        problems.append(f"มี event_id ซ้ำ {dup:,} ค่า")

    bad_coord = hook.get_first(
        "SELECT count(*) FROM quake_enriched WHERE latitude IS NOT NULL "
        "AND (latitude < -90 OR latitude > 90 OR longitude < -180 OR longitude > 180)"
    )[0]
    if bad_coord > 0:
        problems.append(f"พิกัดนอกขอบเขตโลก {bad_coord:,} แถว")

    # ขอบล่าง -3 เพราะแผ่นดินไหวจิ๋วระดับ -2 กว่า ๆ วัดได้จริง
    # ส่วนค่า sentiNel ที่ต่ำกว่านั้นถูกแปลงเป็น NULL ไปแล้วตอนสร้าง quake_enriched
    bad_mag = hook.get_first(
        "SELECT count(*) FROM quake_enriched WHERE magnitude IS NOT NULL "
        "AND (magnitude < -3 OR magnitude > 10)"
    )[0]
    if bad_mag > 0:
        problems.append(f"ขนาดนอกช่วงที่เป็นไปได้ {bad_mag:,} แถว")

    # Power BI เทียบข้อความแบบไม่สนตัวพิมพ์ ถ้ามีชื่อที่ต่างกันแค่ตัวพิมพ์
    # จะกลายเป็นคีย์ซ้ำในตารางมิติแล้วโหลดโมเดลไม่ผ่าน
    case_dup = hook.get_first(
        "SELECT count(*) FROM (SELECT lower(region) FROM quake_by_region "
        "GROUP BY lower(region) HAVING count(*) > 1) d"
    )[0]
    if case_dup > 0:
        problems.append(
            f"มีชื่อภูมิภาคที่ต่างกันแค่ตัวพิมพ์ {case_dup} กลุ่ม — Power BI จะมองเป็นคีย์ซ้ำ"
        )

    months = hook.get_first("SELECT count(*) FROM quake_monthly")[0]
    if months == 0:
        problems.append("ตารางสรุปรายเดือนว่างเปล่า")

    # ข้อมูลที่ดึงมาต้องต่อเนื่อง ไม่ควรมีเดือนที่หายไปกลางช่วง
    gaps = hook.get_first(
        """
        SELECT count(*) FROM (
            SELECT generate_series(
                (SELECT min(month) FROM quake_monthly),
                (SELECT max(month) FROM quake_monthly),
                INTERVAL '1 month'
            )::date AS m
        ) s
        LEFT JOIN quake_monthly q ON q.month = s.m
        WHERE q.month IS NULL
        """
    )[0]
    if gaps > 0:
        problems.append(f"มีเดือนที่ไม่มีข้อมูลเลย {gaps} เดือน — ตรวจว่า backfill ครบหรือยัง")

    if problems:
        raise ValueError("ข้อมูลไม่ผ่านการตรวจ: " + " | ".join(problems))

    print(f"ผ่านการตรวจทั้งหมด — {total:,} เหตุการณ์ ครอบคลุม {months} เดือนต่อเนื่อง")
    return total


def export_for_powerbi(**kwargs):
    """
    เขียน CSV ชุด star schema ให้ Power BI import ตรง
    ทุกไฟล์เป็น UTF-8 พร้อม BOM เพื่อให้ภาษาไทยอ่านออกโดยไม่ต้องเลือก encoding
    """
    hook = _hook()
    connection = hook.get_conn()
    cursor = connection.cursor()

    os.makedirs(EXPORT_DIR, exist_ok=True)

    exports = {
        # ตารางข้อเท็จจริง ระดับ เดือน x ภูมิภาค x ช่วงขนาด x ช่วงความลึก
        "fact_quakes.csv": """
            SELECT
                occurred_month AS "เดือน",
                region         AS "ภูมิภาค",
                magnitude_band AS "ช่วงขนาด",
                depth_band     AS "ช่วงความลึก",
                count(*)       AS "จำนวนเหตุการณ์",
                round(avg(magnitude)::numeric, 2) AS "ขนาดเฉลี่ย",
                max(magnitude) AS "ขนาดสูงสุด",
                round(avg(depth_km)::numeric, 1)  AS "ความลึกเฉลี่ย"
            FROM quake_enriched
            GROUP BY occurred_month, region, magnitude_band, depth_band
            ORDER BY occurred_month, region
        """,
        "dim_month.csv": """
            WITH bounds AS (
                SELECT min(occurred_month) AS m1, max(occurred_month) AS m2 FROM quake_enriched
            ), months AS (
                SELECT generate_series(m1, m2, INTERVAL '1 month')::date AS month FROM bounds
            )
            SELECT
                month                               AS "เดือน",
                EXTRACT(YEAR FROM month)::int       AS "ปี ค.ศ.",
                EXTRACT(YEAR FROM month)::int + 543 AS "ปี พ.ศ.",
                EXTRACT(MONTH FROM month)::int      AS "เลขเดือน",
                (ARRAY['ม.ค.','ก.พ.','มี.ค.','เม.ย.','พ.ค.','มิ.ย.',
                       'ก.ค.','ส.ค.','ก.ย.','ต.ค.','พ.ย.','ธ.ค.'])
                    [EXTRACT(MONTH FROM month)::int] AS "เดือนไทย",
                to_char(month, 'YYYY-MM')           AS "ปีเดือน",
                EXTRACT(QUARTER FROM month)::int    AS "ไตรมาส"
            FROM months
            ORDER BY month
        """,
        "dim_region.csv": """
            SELECT
                region        AS "ภูมิภาค",
                events        AS "จำนวนเหตุการณ์",
                max_magnitude AS "ขนาดสูงสุด",
                avg_depth_km  AS "ความลึกเฉลี่ย",
                ROW_NUMBER() OVER (ORDER BY events DESC)::int AS "อันดับ"
            FROM quake_by_region
            ORDER BY events DESC
        """,
        "dim_magnitude_band.csv": """
            SELECT
                magnitude_band AS "ช่วงขนาด",
                sort_order     AS "ลำดับ",
                events         AS "จำนวนเหตุการณ์"
            FROM quake_by_magnitude
            ORDER BY sort_order
        """,
        "dim_depth_band.csv": """
            SELECT
                depth_band AS "ช่วงความลึก",
                sort_order AS "ลำดับ",
                events     AS "จำนวนเหตุการณ์"
            FROM quake_by_depth
            ORDER BY sort_order
        """,
        "summary_monthly.csv": """
            SELECT
                month           AS "เดือน",
                events          AS "จำนวนเหตุการณ์",
                avg_magnitude   AS "ขนาดเฉลี่ย",
                max_magnitude   AS "ขนาดสูงสุด",
                events_m5_plus  AS "เหตุการณ์ตั้งแต่ M5"
            FROM quake_monthly
            ORDER BY month
        """,
        "strongest_events.csv": """
            SELECT
                occurred_at AS "เวลาที่เกิด",
                magnitude   AS "ขนาด",
                depth_km    AS "ความลึก",
                place       AS "ตำแหน่ง",
                region      AS "ภูมิภาค",
                latitude    AS "ละติจูด",
                longitude   AS "ลองจิจูด"
            FROM quake_strongest
            ORDER BY magnitude DESC
        """,
    }

    written = []
    try:
        for file_name, select_sql in exports.items():
            path = os.path.join(EXPORT_DIR, file_name)
            with open(path, "wb") as handle:
                handle.write(b"\xef\xbb\xbf")
                cursor.copy_expert(f"COPY ({select_sql}) TO STDOUT WITH CSV HEADER", handle)
            with open(path, "rb") as handle:
                rows = max(0, sum(1 for _ in handle) - 1)
            size_mb = os.path.getsize(path) / (1024 * 1024)
            written.append(f"{file_name}: {rows:,} แถว")
            print(f"  เขียน {file_name} — {rows:,} แถว ({size_mb:.1f} MB)")
    finally:
        cursor.close()
        connection.close()

    print(f"ไฟล์สำหรับ Power BI อยู่ที่ {EXPORT_DIR}")
    return written


def log_result(**kwargs):
    hook = _hook()
    summary = hook.get_first(
        "SELECT total_events, first_event, last_event, max_magnitude, "
        "avg_depth_km, region_count, events_m6_plus FROM quake_summary"
    )
    top_region = hook.get_first(
        "SELECT region, events FROM quake_by_region ORDER BY events DESC LIMIT 1"
    )
    strongest = hook.get_first(
        "SELECT place, magnitude, occurred_at FROM quake_strongest "
        "ORDER BY magnitude DESC LIMIT 1"
    )

    print("=" * 60)
    print("USGS Earthquake Marts — สรุปผลการรัน")
    print("=" * 60)
    print(f"เหตุการณ์ทั้งหมด   : {summary[0]:,}")
    print(f"ช่วงเวลา           : {summary[1]} ถึง {summary[2]}")
    print(f"ขนาดสูงสุด         : M{summary[3]}")
    print(f"ความลึกเฉลี่ย      : {summary[4]} กม.")
    print(f"จำนวนภูมิภาค       : {summary[5]:,}")
    print(f"เหตุการณ์ M6 ขึ้นไป : {summary[6]:,}")
    print(f"ภูมิภาคที่พบมากสุด : {top_region[0]} ({top_region[1]:,} ครั้ง)")
    print(f"ครั้งที่แรงที่สุด    : {strongest[0]} M{strongest[1]} เมื่อ {strongest[2]}")
    print("=" * 60)
    print("เปิดแดชบอร์ดที่ http://localhost:8001/quakes")


with DAG(
    dag_id="usgs_quake_marts",
    description="สร้างตารางสรุปและไฟล์ Power BI จากข้อมูลแผ่นดินไหวที่ ingest ไว้",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,   # สั่งรันเองหลัง ingest เสร็จ
    catchup=False,
    tags=["usgs", "earthquake", "marts", "dashboard", "workshop2"],
) as dag:

    marts_task = PythonOperator(task_id="build_marts", python_callable=build_marts)
    quality_task = PythonOperator(task_id="quality_check", python_callable=quality_check)
    export_task = PythonOperator(task_id="export_for_powerbi", python_callable=export_for_powerbi)
    log_task = PythonOperator(task_id="log_result", python_callable=log_result)

    marts_task >> quality_task >> export_task >> log_task
