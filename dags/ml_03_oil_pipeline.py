"""
ml_03_oil_pipeline.py
=====================
ML Pipeline บน Apache Airflow — พยากรณ์ราคาขายปลีกแก๊สโซฮอล์ 95

แก้ไขเวอร์ชันนี้:
- Bootstrap อย่างน้อย 30 วัน
- Money Buffalo ใช้เป็นแหล่งราคาล่าสุด
- ใช้คลังข้อมูลรายวันของ Rakawannee สำหรับเติมข้อมูลย้อนหลัง
  (อ้างอิงราคาปั๊ม ปตท. ในแต่ละวัน)
- ไม่ crawl historical ทุกครั้งที่ DAG รันรายวัน
- Train อย่างน้อย 20 samples เพื่อป้องกันโมเดลเรียนจากข้อมูลน้อยเกินไป
- Holdout 5 samples
- แก้การบันทึก deploy decision ใน XCom
"""

import math
import os
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


# -----------------------------------------------------------------
# Config
# -----------------------------------------------------------------

default_args = {
    "owner": "workshop2",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

POSTGRES_CONN_ID = "postgres_target"
MODEL_NAME = "gasohol_95_thailand"

SYMBOL = "GASOHOL95"
FUEL_NAME = "Gasohol 95"
UNIT = "บาท/ลิตร"

MONEY_BUFFALO_URL = "https://www.moneybuffalo.in.th/rate/oil-price"
RAKAWANNEE_URL = "https://rakawannee.com/fuel/archive/{be_year}/{month:02d}/{day:02d}"

MIN_HISTORY_DAYS = 30
BOOTSTRAP_FETCH_DAYS = 60

LAG_WINDOW = 5
HOLDOUT_SIZE = 5
MIN_TRAIN_SAMPLES = 20

MODEL_DIR = "/opt/airflow/models/oil_models"
CURRENT_MODEL_PATH = os.path.join(MODEL_DIR, "current_model.pkl")

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS oil_price_history (
    date DATE NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    price FLOAT NOT NULL,
    inserted_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (date, symbol)
);

CREATE TABLE IF NOT EXISTS model_metrics (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    rmse FLOAT NOT NULL,
    deployed BOOLEAN NOT NULL,
    run_at TIMESTAMP DEFAULT NOW()
);
"""

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}


# -----------------------------------------------------------------
# Utility
# -----------------------------------------------------------------

def _thai_or_iso_date(value):
    """แปลงวันที่เป็น datetime.date"""
    if value is None:
        return None

    s = str(value).strip()

    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d-%m-%y",
        "%d/%m/%y",
    ):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass

    thai_months = {
        "ม.ค.": 1, "ก.พ.": 2, "มี.ค.": 3, "เม.ย.": 4,
        "พ.ค.": 5, "มิ.ย.": 6, "ก.ค.": 7, "ส.ค.": 8,
        "ก.ย.": 9, "ต.ค.": 10, "พ.ย.": 11, "ธ.ค.": 12,
        "มกราคม": 1, "กุมภาพันธ์": 2, "มีนาคม": 3,
        "เมษายน": 4, "พฤษภาคม": 5, "มิถุนายน": 6,
        "กรกฎาคม": 7, "สิงหาคม": 8, "กันยายน": 9,
        "ตุลาคม": 10, "พฤศจิกายน": 11, "ธันวาคม": 12,
    }

    m = re.match(r"^(\d{1,2})\s+([^\s]+)\s+(\d{4})$", s)
    if m and m.group(2) in thai_months:
        year = int(m.group(3))
        if year > 2400:
            year -= 543
        try:
            return datetime(year, thai_months[m.group(2)], int(m.group(1))).date()
        except ValueError:
            return None

    parsed = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return None

    date_value = parsed.date()

    # pandas อาจตีความ พ.ศ. เป็น ค.ศ. ผิดกรณี input เป็นตัวเลข 25xx
    if date_value.year > 2400:
        date_value = date_value.replace(year=date_value.year - 543)

    return date_value


def _clean_price(value):
    s = str(value).strip().replace(",", "")
    if s.lower() in {"", "-", "nan", "none", "null"}:
        return None

    m = re.search(r"\d+(?:\.\d+)?", s)
    if not m:
        return None

    price = float(m.group(0))

    # ราคาขายปลีก Gasohol 95 ที่ผิดปกติให้ตัดออก
    if not 20 <= price <= 80:
        return None

    return price


def _normalize_text(value):
    return re.sub(r"\s+", "", str(value)).lower()


def _dedupe_records(records):
    if not records:
        return pd.DataFrame(columns=["date", "price"])

    out = pd.DataFrame(records, columns=["date", "price"])
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out = out.dropna(subset=["date", "price"])

    # ถ้ามีวันซ้ำ ให้รายการที่เพิ่มทีหลังชนะ
    out = (
        out.drop_duplicates(subset=["date"], keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )
    return out


# -----------------------------------------------------------------
# Money Buffalo
# -----------------------------------------------------------------

def _fetch_moneybuffalo_history():
    """อ่านตารางย้อนหลังล่าสุดจาก Money Buffalo"""
    import requests
    from bs4 import BeautifulSoup
    from io import StringIO

    response = requests.get(
        MONEY_BUFFALO_URL,
        headers=HEADERS,
        timeout=30,
    )
    response.raise_for_status()

    print(f"Money Buffalo HTTP={response.status_code}, HTML={len(response.text):,} bytes")

    records = []

    def add_record(date_value, price_value):
        d = _thai_or_iso_date(date_value)
        p = _clean_price(price_value)
        if d is not None and p is not None:
            records.append((d, p))

    try:
        tables = pd.read_html(StringIO(response.text))
        print(f"Money Buffalo พบ HTML tables: {len(tables)}")

        for table_index, df in enumerate(tables):
            if df.empty or df.shape[1] < 2:
                continue

            print(
                f"Money Buffalo table {table_index}: "
                f"shape={df.shape}, columns={list(df.columns)}"
            )

            # 1) ถ้าหัวคอลัมน์มี Gasohol 95
            price_col = None
            for col in df.columns:
                if "gasohol95" in _normalize_text(col):
                    price_col = col
                    break

            if price_col is not None:
                date_col = df.columns[0]
                for _, row in df.iterrows():
                    add_record(row[date_col], row[price_col])
                continue

            # 2) โครงสร้างปัจจุบันของ Money Buffalo:
            # วันที่ | Unnamed:1 | ...
            # column 1 = Gasohol 95
            date_hits = sum(
                _thai_or_iso_date(v) is not None
                for v in df.iloc[:, 0].tolist()
            )

            if date_hits >= 2:
                for _, row in df.iterrows():
                    add_record(row.iloc[0], row.iloc[1])

    except Exception as exc:
        print(f"Money Buffalo read_html มีปัญหา: {exc}")

    # BeautifulSoup fallback
    if not records:
        print("Money Buffalo fallback -> BeautifulSoup")
        soup = BeautifulSoup(response.text, "html.parser")

        for table_index, table in enumerate(soup.find_all("table")):
            for tr in table.find_all("tr"):
                cells = [
                    c.get_text(" ", strip=True)
                    for c in tr.find_all(["th", "td"])
                ]

                if len(cells) >= 2 and _thai_or_iso_date(cells[0]) is not None:
                    add_record(cells[0], cells[1])

    out = _dedupe_records(records)
    print(f"Money Buffalo ได้ {len(out)} วัน")

    return out


# -----------------------------------------------------------------
# Rakawannee date archive
# -----------------------------------------------------------------

def _fetch_rakawannee_day(date_value):
    """
    ดึงราคาของ ปตท. จากหน้าประวัติรายวันของ Rakawannee

    URL ตัวอย่าง:
    /fuel/archive/2569/08/24

    ใช้สำหรับเติม historical data เพราะ Money Buffalo เปิดย้อนหลัง
    เพียงประมาณ 7 วันในหน้าปัจจุบัน
    """
    import requests
    from bs4 import BeautifulSoup

    be_year = date_value.year + 543
    url = RAKAWANNEE_URL.format(
        be_year=be_year,
        month=date_value.month,
        day=date_value.day,
    )

    try:
        response = requests.get(
            url,
            headers=HEADERS,
            timeout=20,
        )

        if response.status_code != 200:
            print(f"Rakawannee {date_value}: HTTP {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # หาแถวที่ชื่อแก๊สโซฮอล์ 95 แบบ exact
        for tr in soup.find_all("tr"):
            cells = [
                c.get_text(" ", strip=True)
                for c in tr.find_all(["th", "td"])
            ]

            if len(cells) < 2:
                continue

            fuel = re.sub(r"\s+", " ", cells[0]).strip().lower()

            if fuel in {
                "แก๊สโซฮอล์ 95",
                "แก๊สโซฮอล์95",
                "gasohol 95",
                "gasohol95",
            }:
                price = _clean_price(cells[1])
                if price is not None:
                    return price

        # fallback: regex จากข้อความ
        text = soup.get_text(" ", strip=True)
        pattern = (
            r"แก๊สโซฮอล์\s*95"
            r"(?:\s*\|\s*|\s+)"
            r"(\d+(?:\.\d+)?)"
        )
        match = re.search(pattern, text, re.IGNORECASE)

        if match:
            return _clean_price(match.group(1))

    except Exception as exc:
        print(f"Rakawannee {date_value}: {exc}")

    return None


def _fetch_rakawannee_history(days=BOOTSTRAP_FETCH_DAYS):
    """ดึงย้อนหลังตามจำนวนวันปฏิทินที่กำหนด"""
    from datetime import date

    today = datetime.now(ZoneInfo("Asia/Bangkok")).date()
    records = []

    print(
        f"Rakawannee: เริ่มดึงย้อนหลัง {days} วัน "
        f"จาก {today - timedelta(days=days - 1)} ถึง {today}"
    )

    for offset in range(days):
        d = today - timedelta(days=offset)
        price = _fetch_rakawannee_day(d)

        if price is not None:
            records.append((d, price))
            print(f"Rakawannee OK: {d} -> {price:.2f}")
        else:
            print(f"Rakawannee ไม่พบราคา: {d}")

    out = _dedupe_records(records)
    print(f"Rakawannee ได้ {len(out)} วัน")
    return out


# -----------------------------------------------------------------
# Combined history
# -----------------------------------------------------------------

def fetch_gasohol95_history(include_archive=True):
    """
    รวมข้อมูล:
    - Money Buffalo: ราคาล่าสุด
    - Rakawannee: historical รายวัน

    ถ้าวันเดียวกันมีข้อมูลจากทั้งสองแหล่ง จะให้ Money Buffalo ชนะ
    เพราะใช้เป็นแหล่งราคาล่าสุดของระบบ
    """
    money = _fetch_moneybuffalo_history()

    if not include_archive:
        return money

    archive = _fetch_rakawannee_history()

    # archive ก่อน, Money Buffalo ทับวันซ้ำ
    records = []

    for _, row in archive.iterrows():
        records.append((row["date"], row["price"]))

    for _, row in money.iterrows():
        records.append((row["date"], row["price"]))

    out = _dedupe_records(records)

    print("====================================")
    print("Gasohol 95 Historical Data")
    print("====================================")
    print(f"Money Buffalo: {len(money)} วัน")
    print(f"Rakawannee:   {len(archive)} วัน")
    print(f"รวมทั้งหมด:   {len(out)} วัน")

    if not out.empty:
        print(f"วันแรก:   {out['date'].iloc[0].date()}")
        print(f"วันล่าสุด: {out['date'].iloc[-1].date()}")
        print(f"ราคาล่าสุด: {out['price'].iloc[-1]:.2f} {UNIT}")

    print("====================================")

    return out


# -----------------------------------------------------------------
# Bootstrap
# -----------------------------------------------------------------

def bootstrap_historical_data(**kwargs):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    count = hook.get_first(
        "SELECT COUNT(*) FROM oil_price_history WHERE symbol = %s;",
        parameters=(SYMBOL,),
    )[0]

    print(
        f"มีข้อมูล {FUEL_NAME} อยู่แล้ว {count} วัน "
        f"(ต้องการอย่างน้อย {MIN_HISTORY_DAYS} วัน)"
    )

    if count >= MIN_HISTORY_DAYS:
        print("ข้อมูลครบแล้ว ข้าม bootstrap")
        return

    df = fetch_gasohol95_history(include_archive=True)

    if len(df) < MIN_HISTORY_DAYS:
        raise ValueError(
            f"แหล่งข้อมูลให้ข้อมูลเพียง {len(df)} วัน "
            f"แต่ระบบต้องการอย่างน้อย {MIN_HISTORY_DAYS} วัน"
        )

    inserted = 0

    for _, row in df.iterrows():
        hook.run(
            """
            INSERT INTO oil_price_history (date, symbol, price)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, symbol) DO NOTHING;
            """,
            parameters=(
                row["date"].date(),
                SYMBOL,
                float(row["price"]),
            ),
        )
        inserted += 1

    final_count = hook.get_first(
        "SELECT COUNT(*) FROM oil_price_history WHERE symbol = %s;",
        parameters=(SYMBOL,),
    )[0]

    if final_count < MIN_HISTORY_DAYS:
        raise ValueError(
            f"ข้อมูลย้อนหลังไม่พอ: ได้ {final_count} วัน "
            f"ต้องการอย่างน้อย {MIN_HISTORY_DAYS} วัน"
        )

    print(
        f"Bootstrap เสร็จ: พยายามเพิ่ม {inserted} แถว, "
        f"รวมในฐานข้อมูล {final_count} วัน"
    )


# -----------------------------------------------------------------
# Daily latest price
# -----------------------------------------------------------------

def fetch_latest_price(**kwargs):
    """
    หลัง bootstrap แล้วไม่ต้อง crawl historical 60 วันทุกครั้ง
    ดึงเฉพาะตารางล่าสุดของ Money Buffalo
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    df = _fetch_moneybuffalo_history()

    if df.empty:
        raise ValueError("Money Buffalo ไม่พบราคาล่าสุด")

    latest = df.iloc[-1]
    latest_date = latest["date"].date()
    latest_price = float(latest["price"])

    hook.run(
        """
        INSERT INTO oil_price_history (date, symbol, price)
        VALUES (%s, %s, %s)
        ON CONFLICT (date, symbol)
        DO UPDATE SET
            price = EXCLUDED.price,
            inserted_at = NOW();
        """,
        parameters=(latest_date, SYMBOL, latest_price),
    )

    print(
        f"บันทึกราคาล่าสุด {FUEL_NAME} "
        f"({latest_date}): {latest_price:.2f} {UNIT}"
    )


# -----------------------------------------------------------------
# Prepare training data
# -----------------------------------------------------------------

def prepare_training_data(**kwargs):
    ti = kwargs["ti"]
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    rows = hook.get_records(
        """
        SELECT date, price
        FROM oil_price_history
        WHERE symbol = %s
        ORDER BY date ASC;
        """,
        parameters=(SYMBOL,),
    )

    df = pd.DataFrame(rows, columns=["date", "price"])

    if len(df) < MIN_HISTORY_DAYS:
        raise ValueError(
            f"มีข้อมูลเพียง {len(df)} วัน "
            f"ต้องการอย่างน้อย {MIN_HISTORY_DAYS} วัน"
        )

    if len(df) < LAG_WINDOW + HOLDOUT_SIZE + 1:
        raise ValueError("ข้อมูลไม่พอสำหรับสร้าง training/holdout")

    prices = df["price"].astype(float).tolist()

    features = []
    targets = []

    for i in range(LAG_WINDOW, len(prices)):
        lags = [
            prices[i - k]
            for k in range(1, LAG_WINDOW + 1)
        ]

        moving_avg = sum(lags) / LAG_WINDOW

        features.append(lags + [moving_avg])
        targets.append(prices[i])

    last_n = prices[-LAG_WINDOW:]

    latest_lags = [
        last_n[-k]
        for k in range(1, LAG_WINDOW + 1)
    ]

    latest_features = latest_lags + [
        sum(last_n) / LAG_WINDOW
    ]

    ti.xcom_push(key="features", value=features)
    ti.xcom_push(key="targets", value=targets)
    ti.xcom_push(key="latest_features", value=latest_features)
    ti.xcom_push(
        key="latest_date",
        value=str(df["date"].iloc[-1]),
    )

    print(f"ข้อมูล {FUEL_NAME}: {len(prices)} วัน")
    print(f"สร้าง feature: {len(features)} แถว")
    print(f"feature ล่าสุด: {latest_features}")


# -----------------------------------------------------------------
# Train
# -----------------------------------------------------------------

def train_model(**kwargs):
    import joblib
    from sklearn.linear_model import LinearRegression

    ti = kwargs["ti"]

    features = ti.xcom_pull(
        task_ids="prepare_training_data",
        key="features",
    )
    targets = ti.xcom_pull(
        task_ids="prepare_training_data",
        key="targets",
    )

    if len(features) <= HOLDOUT_SIZE:
        raise ValueError("training data ไม่พอสำหรับ holdout")

    X_train = features[:-HOLDOUT_SIZE]
    y_train = targets[:-HOLDOUT_SIZE]

    X_holdout = features[-HOLDOUT_SIZE:]
    y_holdout = targets[-HOLDOUT_SIZE:]

    if len(X_train) < MIN_TRAIN_SAMPLES:
        raise ValueError(
            f"Training samples ไม่พอ: {len(X_train)} แถว "
            f"ต้องการอย่างน้อย {MIN_TRAIN_SAMPLES} แถว"
        )

    # เปลี่ยนจาก RandomForestRegressor เป็น LinearRegression เพราะ
    # RandomForest (decision tree ensemble) extrapolate นอกช่วงราคาที่
    # เคยเทรนไม่ได้ — กรอกราคาที่สูง/ต่ำผิดปกติ (เช่น 50, 100 บาท/ลิตร)
    # จะได้ค่าทำนายคงที่เท่ากันหมด เพราะ tree ไหลไปตกที่ leaf บนสุด/ล่างสุด
    # เหมือนกัน LinearRegression คำนวณเป็นสมการเชิงเส้นจริง จึงทำนายค่า
    # ที่เปลี่ยนไปตามอินพุตได้ต่อเนื่อง แม้อินพุตจะอยู่นอกช่วงข้อมูลเทรน
    model = LinearRegression()

    model.fit(X_train, y_train)

    os.makedirs(MODEL_DIR, exist_ok=True)

    run_id = (
        kwargs["run_id"]
        .replace(":", "-")
        .replace("+", "-")
    )

    candidate_path = os.path.join(
        MODEL_DIR,
        f"candidate_{run_id}.pkl",
    )

    joblib.dump(model, candidate_path)

    ti.xcom_push(
        key="candidate_model_path",
        value=candidate_path,
    )
    ti.xcom_push(key="X_holdout", value=X_holdout)
    ti.xcom_push(key="y_holdout", value=y_holdout)

    print(
        f"เทรนโมเดลเสร็จ: "
        f"train={len(X_train)}, holdout={len(X_holdout)}"
    )
    print(f"บันทึก candidate: {candidate_path}")


# -----------------------------------------------------------------
# Evaluate
# -----------------------------------------------------------------

def evaluate_model(**kwargs):
    import joblib

    ti = kwargs["ti"]

    candidate_path = ti.xcom_pull(
        task_ids="train_model",
        key="candidate_model_path",
    )

    X_holdout = ti.xcom_pull(
        task_ids="train_model",
        key="X_holdout",
    )

    y_holdout = ti.xcom_pull(
        task_ids="train_model",
        key="y_holdout",
    )

    model = joblib.load(candidate_path)

    predictions = model.predict(X_holdout)

    squared_errors = [
        (p - a) ** 2
        for p, a in zip(predictions, y_holdout)
    ]

    rmse = math.sqrt(
        sum(squared_errors) / len(squared_errors)
    )

    ti.xcom_push(key="rmse", value=rmse)

    print(
        f"RMSE {FUEL_NAME} = "
        f"{rmse:.4f} {UNIT} (ยิ่งต่ำยิ่งดี)"
    )


# -----------------------------------------------------------------
# Champion / Challenger
# -----------------------------------------------------------------

def get_previous_rmse(**kwargs):
    ti = kwargs["ti"]
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    row = hook.get_first(
        """
        SELECT rmse
        FROM model_metrics
        WHERE model_name = %s
          AND deployed = TRUE
        ORDER BY run_at DESC
        LIMIT 1;
        """,
        parameters=(MODEL_NAME,),
    )

    previous_rmse = row[0] if row else None

    ti.xcom_push(
        key="previous_rmse",
        value=previous_rmse,
    )

    if previous_rmse is None:
        print("ยังไม่มี champion ของ Gasohol 95")
    else:
        print(
            f"Champion RMSE เดิม = "
            f"{previous_rmse:.4f} {UNIT}"
        )


def decide_deploy(**kwargs):
    ti = kwargs["ti"]

    rmse = ti.xcom_pull(
        task_ids="evaluate_model",
        key="rmse",
    )

    previous_rmse = ti.xcom_pull(
        task_ids="get_previous_rmse",
        key="previous_rmse",
    )

    if previous_rmse is None or rmse < previous_rmse:
        ti.xcom_push(
            key="deploy_decision",
            value=True,
        )
        print(
            "โมเดลใหม่ดีกว่า/ยังไม่มี champion -> deploy"
        )
        return "deploy_model"

    ti.xcom_push(
        key="deploy_decision",
        value=False,
    )

    print(
        "โมเดลใหม่ไม่ดีกว่า champion -> skip"
    )

    return "skip_deploy"


def deploy_model(**kwargs):
    import shutil

    ti = kwargs["ti"]

    candidate_path = ti.xcom_pull(
        task_ids="train_model",
        key="candidate_model_path",
    )

    os.makedirs(MODEL_DIR, exist_ok=True)

    shutil.copyfile(
        candidate_path,
        CURRENT_MODEL_PATH,
    )

    print(
        f"Deploy สำเร็จ: "
        f"{candidate_path} -> {CURRENT_MODEL_PATH}"
    )


def skip_deploy(**kwargs):
    ti = kwargs["ti"]

    rmse = ti.xcom_pull(
        task_ids="evaluate_model",
        key="rmse",
    )

    previous_rmse = ti.xcom_pull(
        task_ids="get_previous_rmse",
        key="previous_rmse",
    )

    print(
        f"ข้าม deploy: "
        f"ใหม่={rmse:.4f} vs "
        f"เดิม={previous_rmse:.4f} {UNIT}"
    )


# -----------------------------------------------------------------
# Smoke test
# -----------------------------------------------------------------

def smoke_test(**kwargs):
    import joblib

    ti = kwargs["ti"]

    latest_features = ti.xcom_pull(
        task_ids="prepare_training_data",
        key="latest_features",
    )

    latest_date = ti.xcom_pull(
        task_ids="prepare_training_data",
        key="latest_date",
    )

    if not os.path.exists(CURRENT_MODEL_PATH):
        raise FileNotFoundError(
            f"ไม่พบ deployed model: {CURRENT_MODEL_PATH}"
        )

    model = joblib.load(CURRENT_MODEL_PATH)

    forecast = float(
        model.predict([latest_features])[0]
    )

    print("===== Smoke Test =====")
    print(f"ข้อมูลล่าสุด: {latest_date}")
    print(
        f"พยากรณ์ {FUEL_NAME} วันถัดไป: "
        f"{forecast:.2f} {UNIT}"
    )


# -----------------------------------------------------------------
# Log result
# -----------------------------------------------------------------

def log_result(**kwargs):
    ti = kwargs["ti"]

    hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    rmse = ti.xcom_pull(
        task_ids="evaluate_model",
        key="rmse",
    )

    deployed = ti.xcom_pull(
        task_ids="decide_deploy",
        key="deploy_decision",
    )

    deployed = bool(deployed)

    hook.run(
        """
        INSERT INTO model_metrics
            (model_name, rmse, deployed)
        VALUES (%s, %s, %s);
        """,
        parameters=(
            MODEL_NAME,
            rmse,
            deployed,
        ),
    )

    print("===== สรุปผล Gasohol 95 Pipeline =====")
    print(f"RMSE: {rmse:.4f} {UNIT}")
    print(
        f"Deploy: {'ใช่' if deployed else 'ไม่ใช่'}"
    )


# -----------------------------------------------------------------
# DAG
# -----------------------------------------------------------------

with DAG(
    dag_id="oil_price_pipeline_dag",
    default_args=default_args,
    description=(
        "พยากรณ์ราคาขายปลีก Gasohol 95 "
        "ในประเทศไทยวันถัดไป"
    ),
    schedule=None,
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=[
        "workshop2",
        "ml-pipeline",
        "oil",
        "gasohol95",
        "thailand",
    ],
) as dag:

    create_tables_task = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=CREATE_TABLES_SQL,
    )

    bootstrap_task = PythonOperator(
        task_id="bootstrap_historical_data",
        python_callable=bootstrap_historical_data,
    )

    fetch_task = PythonOperator(
        task_id="fetch_latest_price",
        python_callable=fetch_latest_price,
    )

    prepare_task = PythonOperator(
        task_id="prepare_training_data",
        python_callable=prepare_training_data,
    )

    train_task = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )

    evaluate_task = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_model,
    )

    previous_rmse_task = PythonOperator(
        task_id="get_previous_rmse",
        python_callable=get_previous_rmse,
    )

    decide_task = BranchPythonOperator(
        task_id="decide_deploy",
        python_callable=decide_deploy,
    )

    deploy_task = PythonOperator(
        task_id="deploy_model",
        python_callable=deploy_model,
    )

    skip_task = PythonOperator(
        task_id="skip_deploy",
        python_callable=skip_deploy,
    )

    smoke_test_task = PythonOperator(
        task_id="smoke_test",
        python_callable=smoke_test,
    )

    log_task = PythonOperator(
        task_id="log_result",
        python_callable=log_result,
        trigger_rule="none_failed_min_one_success",
    )

    (
        create_tables_task
        >> bootstrap_task
        >> fetch_task
        >> prepare_task
        >> train_task
        >> evaluate_task
        >> previous_rmse_task
        >> decide_task
    )

    decide_task >> deploy_task >> smoke_test_task >> log_task
    decide_task >> skip_task >> log_task
