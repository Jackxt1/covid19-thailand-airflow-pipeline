"""
oil.py — Gasohol 95 Model Router
"""

import os
from datetime import datetime

import joblib
import numpy as np
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

OIL_MODEL_PATH = "/models/oil_models/current_model.pkl"
router = APIRouter()


class PredictOilRequest(BaseModel):
    price_5_days_ago: float = Field(..., example=37.69, description="ราคาย้อนหลัง 5 วัน (บาท/ลิตร)")
    price_4_days_ago: float = Field(..., example=37.69, description="ราคาย้อนหลัง 4 วัน (บาท/ลิตร)")
    price_3_days_ago: float = Field(..., example=37.69, description="ราคาย้อนหลัง 3 วัน (บาท/ลิตร)")
    price_2_days_ago: float = Field(..., example=38.29, description="ราคาย้อนหลัง 2 วัน (บาท/ลิตร)")
    price_yesterday: float = Field(..., example=38.29, description="ราคาของเมื่อวาน (บาท/ลิตร)")


class PredictOilResponse(BaseModel):
    forecast_price: float
    unit: str
    fuel: str
    model_last_modified: str


def load_oil_model():
    if not os.path.exists(OIL_MODEL_PATH):
        raise HTTPException(
            status_code=503,
            detail=(
                f"ยังไม่มีไฟล์โมเดลที่ {OIL_MODEL_PATH} — ต้องรัน DAG "
                "oil_price_pipeline_dag ใน Airflow อย่างน้อย 1 ครั้งก่อน"
            ),
        )
    return joblib.load(OIL_MODEL_PATH)


def oil_model_file_info():
    exists = os.path.exists(OIL_MODEL_PATH)
    last_modified = None
    if exists:
        last_modified = datetime.fromtimestamp(os.path.getmtime(OIL_MODEL_PATH)).isoformat()
    return {"exists": exists, "last_modified": last_modified}


@router.post("/predict_oil", response_model=PredictOilResponse)
def predict_oil(payload: PredictOilRequest):
    """ทำนายราคาขายปลีก Gasohol 95 วันถัดไปจากราคา 5 วันล่าสุด"""
    model = load_oil_model()

    lag1 = payload.price_yesterday
    lag2 = payload.price_2_days_ago
    lag3 = payload.price_3_days_ago
    lag4 = payload.price_4_days_ago
    lag5 = payload.price_5_days_ago
    moving_avg = (lag1 + lag2 + lag3 + lag4 + lag5) / 5

    features = np.array([[lag1, lag2, lag3, lag4, lag5, moving_avg]])
    forecast = float(model.predict(features)[0])
    forecast = max(0.0, forecast)

    last_modified = datetime.fromtimestamp(os.path.getmtime(OIL_MODEL_PATH)).isoformat()

    return PredictOilResponse(
        forecast_price=round(forecast, 2),
        unit="บาท/ลิตร",
        fuel="แก๊สโซฮอล์ 95",
        model_last_modified=last_modified,
    )


OIL_FORM_HTML = """
  <hr style="margin: 32px 0; border: none; border-top: 1px solid #e5e7eb;">

  <h1>⛽ Gasohol 95 Price Model — ทำนายราคาวันถัดไป</h1>
  <p style="color:#6b7280; font-size:14px;">
    กรอกราคาขายปลีกแก๊สโซฮอล์ 95 จำนวน 5 วันล่าสุด (บาท/ลิตร)
    แล้วกด "พยากรณ์" เพื่อเรียกโมเดลล่าสุดที่ Airflow deploy ไว้
  </p>

  <div class="field">
    <label>ราคาของ 5 วันก่อน (บาท/ลิตร)</label>
    <input type="number" step="0.01" min="0" id="price_5_days_ago" value="37.69">
  </div>
  <div class="field">
    <label>ราคาของ 4 วันก่อน (บาท/ลิตร)</label>
    <input type="number" step="0.01" min="0" id="price_4_days_ago" value="37.69">
  </div>
  <div class="field">
    <label>ราคาของ 3 วันก่อน (บาท/ลิตร)</label>
    <input type="number" step="0.01" min="0" id="price_3_days_ago" value="37.69">
  </div>
  <div class="field">
    <label>ราคาของ 2 วันก่อน (บาท/ลิตร)</label>
    <input type="number" step="0.01" min="0" id="price_2_days_ago" value="38.29">
  </div>
  <div class="field">
    <label>ราคาของเมื่อวาน (บาท/ลิตร)</label>
    <input type="number" step="0.01" min="0" id="price_yesterday" value="38.29">
  </div>

  <button onclick="doPredictOil()">พยากรณ์</button>
  <div id="oil_result"></div>

<script>
async function doPredictOil() {
  const payload = {
    price_5_days_ago: parseFloat(document.getElementById('price_5_days_ago').value),
    price_4_days_ago: parseFloat(document.getElementById('price_4_days_ago').value),
    price_3_days_ago: parseFloat(document.getElementById('price_3_days_ago').value),
    price_2_days_ago: parseFloat(document.getElementById('price_2_days_ago').value),
    price_yesterday: parseFloat(document.getElementById('price_yesterday').value),
  };

  if (Object.values(payload).some(v => !Number.isFinite(v) || v < 0)) {
    document.getElementById('oil_result').style.display = 'block';
    document.getElementById('oil_result').className = 'err';
    document.getElementById('oil_result').innerHTML = '❌ กรุณากรอกราคาให้ครบและต้องไม่ติดลบ';
    return;
  }

  const resultDiv = document.getElementById('oil_result');
  resultDiv.style.display = 'block';
  resultDiv.className = '';
  resultDiv.innerHTML = 'กำลังพยากรณ์...';

  try {
    const res = await fetch('/predict_oil', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json();

    if (!res.ok) {
      resultDiv.className = 'err';
      resultDiv.innerHTML = '❌ ' + (data.detail || 'เกิดข้อผิดพลาด');
      return;
    }

    resultDiv.className = 'ok';
    resultDiv.innerHTML = `
      <div style="font-size:18px; font-weight:bold;">⛽ พยากรณ์แก๊สโซฮอล์ 95 วันถัดไป: ${data.forecast_price.toFixed(2)} บาท/ลิตร</div>
      <div class="meta">โมเดลอัปเดตล่าสุด: ${data.model_last_modified}</div>
    `;
  } catch (e) {
    resultDiv.className = 'err';
    resultDiv.innerHTML = '❌ เรียก API ไม่สำเร็จ: ' + e.message;
  }
}
</script>
"""
