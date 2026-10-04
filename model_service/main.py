"""
main.py — Gasohol 95 Price Model Serving API
================================================
FastAPI service สำหรับเสิร์ฟโมเดลพยากรณ์ราคาน้ำมัน
ที่ Airflow deploy ไว้

ทำหน้าที่:
- เสิร์ฟโมเดล Gasohol 95
- Endpoint /predict_oil มาจาก oil.py
- Endpoint /health สำหรับตรวจสอบสถานะ API และโมเดล
- หน้าเว็บสำหรับทดสอบการพยากรณ์ราคาน้ำมัน
"""

import os

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from oil import router as oil_router, oil_model_file_info, OIL_FORM_HTML
from covid import router as covid_router, covid_pipeline_status


app = FastAPI(
    title="Gasohol 95 Price Model Serving API",
    version="1.0.0"
)

# เพิ่ม endpoint /predict_oil จาก oil.py
app.include_router(oil_router)

# เพิ่มหน้าแดชบอร์ด /covid และ endpoint /api/covid/* จาก covid.py
app.include_router(covid_router)


@app.get("/health")
def health():
    """ตรวจสอบสถานะ service และไฟล์โมเดลน้ำมัน"""

    oil_info = oil_model_file_info()

    return {
        "status": "ok",
        "oil_model": oil_info,
        "covid_dashboard": covid_pipeline_status(),
    }


@app.get("/", response_class=HTMLResponse)
def test_page():
    """หน้าเว็บทดสอบโมเดล Gasohol 95"""

    page = """
<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Gasohol 95 — พยากรณ์ราคาวันถัดไป</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,600;9..144,700&family=IBM+Plex+Sans+Thai:wght@400;500;600;700&display=swap" rel="stylesheet">

<style>
  :root {
    --bg: #0F2A2C;
    --panel: #163638;
    --panel-2: #1C4144;
    --line: #2B5154;
    --ink: #F1EAD9;
    --ink-dim: #8CA6A3;
    --amber: #E8A33D;
    --amber-ink: #201404;
    --up: #7FC79A;
    --down: #E4796E;
    --down-bg: #2A1917;
    --radius: 8px;
  }

  * { box-sizing: border-box; }

  html { -webkit-text-size-adjust: 100%; }

  body {
    font-family: 'IBM Plex Sans Thai', sans-serif;
    background: var(--bg);
    background-image: radial-gradient(ellipse 900px 500px at 50% -10%, rgba(232,163,61,0.10), transparent 60%);
    color: var(--ink);
    margin: 0;
    padding: 40px 18px 64px;
    display: flex;
    justify-content: center;
  }

  .page {
    width: 100%;
    max-width: 440px;
  }

  h1 {
    font-family: 'Fraunces', serif;
    font-size: 27px;
    font-weight: 600;
    line-height: 1.25;
    letter-spacing: -0.2px;
    margin: 0 0 8px;
  }

  .lede {
    font-size: 14px;
    line-height: 1.65;
    color: var(--ink-dim);
    margin: 0 0 30px;
    max-width: 34ch;
  }

  /* ---------- gauge ---------- */

  .gauge-card {
    background: linear-gradient(180deg, var(--panel-2), var(--panel));
    border: 1px solid var(--line);
    border-radius: 14px;
    padding: 26px 22px 22px;
    margin-bottom: 22px;
  }

  .gauge-wrap {
    position: relative;
    max-width: 300px;
    margin: 0 auto;
  }

  #needleGroup, #gaugeProgress {
    transition: transform 700ms cubic-bezier(.34,1.4,.4,1), stroke-dashoffset 700ms cubic-bezier(.34,1.4,.4,1);
  }

  @media (prefers-reduced-motion: reduce) {
    #needleGroup, #gaugeProgress { transition: none; }
  }

  .gauge-scale {
    display: flex;
    justify-content: space-between;
    margin-top: -34px;
    padding: 0 6px;
    font-size: 12px;
    color: var(--ink-dim);
    font-variant-numeric: tabular-nums;
  }

  .gauge-readout {
    text-align: center;
    margin-top: 4px;
  }

  .gauge-readout .placeholder {
    font-size: 13.5px;
    color: var(--ink-dim);
    padding: 10px 0 4px;
  }

  .gauge-readout .result { display: none; }

  .price-line {
    display: flex;
    align-items: baseline;
    justify-content: center;
    gap: 8px;
  }

  .price-line .num {
    font-family: 'Fraunces', serif;
    font-size: 46px;
    font-weight: 600;
    letter-spacing: -0.5px;
    line-height: 1;
  }

  .price-line .unit {
    font-size: 13px;
    color: var(--ink-dim);
  }

  .delta {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    margin-top: 10px;
    font-size: 13px;
    padding: 4px 10px;
    border-radius: 999px;
    background: var(--panel);
  }

  .delta.up { color: var(--up); }
  .delta.down { color: var(--down); }

  .meta {
    font-size: 12px;
    color: var(--ink-dim);
    margin-top: 12px;
  }

  /* ---------- inputs ---------- */

  .field-label {
    font-size: 13.5px;
    font-weight: 600;
    margin: 0 0 12px;
  }

  .fields {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 10px;
    margin-bottom: 18px;
  }

  .field { display: flex; flex-direction: column; gap: 6px; }

  .field label {
    font-size: 12px;
    color: var(--ink-dim);
  }

  .field input {
    width: 100%;
    background: var(--panel);
    border: 1px solid var(--line);
    color: var(--ink);
    font-family: 'IBM Plex Sans Thai', sans-serif;
    font-variant-numeric: tabular-nums;
    font-size: 15px;
    padding: 10px 10px;
    border-radius: 6px;
  }

  .field input:focus-visible {
    outline: 2px solid var(--amber);
    outline-offset: 1px;
    border-color: var(--amber);
  }

  button.predict {
    width: 100%;
    padding: 15px;
    background: var(--amber);
    color: var(--amber-ink);
    border: none;
    border-radius: 8px;
    font-family: 'IBM Plex Sans Thai', sans-serif;
    font-size: 15px;
    font-weight: 600;
    cursor: pointer;
  }

  button.predict:hover { filter: brightness(1.08); }

  button.predict:focus-visible {
    outline: 2px solid var(--ink);
    outline-offset: 2px;
  }

  .alert {
    display: none;
    margin-top: 14px;
    padding: 12px 14px;
    font-size: 13.5px;
    background: var(--down-bg);
    color: var(--down);
    border-left: 3px solid var(--down);
    border-radius: 4px;
  }

  .footnote {
    font-size: 12px;
    line-height: 1.6;
    color: var(--ink-dim);
    margin-top: 28px;
    padding-top: 18px;
    border-top: 1px solid var(--line);
  }

  @media (max-width: 360px) {
    .fields { grid-template-columns: repeat(2, 1fr); }
  }
</style>
</head>

<body>
  <div class="page">

    <h1>พยากรณ์ราคา Gasohol 95</h1>
    <p class="lede">กรอกราคาขายปลีกย้อนหลัง 5 วัน แล้วโมเดลจะประเมินราคาของวันถัดไปให้</p>

    <div class="gauge-card">
      <div class="gauge-wrap">
        <svg id="gaugeSvg" viewBox="0 0 320 200" width="100%" role="img" aria-label="มาตรวัดราคาที่พยากรณ์ได้">
          <defs>
            <linearGradient id="gaugeGrad" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stop-color="#5AACA8"/>
              <stop offset="55%" stop-color="#E8A33D"/>
              <stop offset="100%" stop-color="#E4796E"/>
            </linearGradient>
          </defs>
          <path d="M40,170 A140,140 0 0 1 280,170" fill="none" stroke="#2B5154" stroke-width="14" stroke-linecap="round"/>
          <path id="gaugeProgress" d="M40,170 A140,140 0 0 1 280,170" fill="none" stroke="url(#gaugeGrad)" stroke-width="14" stroke-linecap="round"/>
          <g id="needleGroup" transform="rotate(0 160 170)">
            <line x1="160" y1="170" x2="160" y2="68" stroke="#F1EAD9" stroke-width="3" stroke-linecap="round"/>
            <circle cx="160" cy="170" r="8" fill="#F1EAD9" stroke="#163638" stroke-width="2"/>
          </g>
        </svg>
        <div class="gauge-scale">
          <span id="gaugeMin">–</span>
          <span id="gaugeMax">–</span>
        </div>
      </div>

      <div class="gauge-readout">
        <p class="placeholder" id="gaugePlaceholder">กรอกราคาด้านล่างแล้วกดพยากรณ์เพื่อดูผล</p>
        <div class="result" id="oil_readout">
          <div class="price-line">
            <span class="num" id="oil_price_value">0.00</span>
            <span class="unit">บาท/ลิตร · พรุ่งนี้</span>
          </div>
          <div id="oil_delta" class="delta"></div>
          <p class="meta" id="oil_meta"></p>
        </div>
      </div>
    </div>

    <p class="field-label">ราคาขายปลีกย้อนหลัง (บาท/ลิตร)</p>

    <div class="fields">
      <div class="field">
        <label for="price_5_days_ago">5 วันก่อน</label>
        <input type="number" step="0.01" min="0" id="price_5_days_ago" value="37.69">
      </div>
      <div class="field">
        <label for="price_4_days_ago">4 วันก่อน</label>
        <input type="number" step="0.01" min="0" id="price_4_days_ago" value="37.69">
      </div>
      <div class="field">
        <label for="price_3_days_ago">3 วันก่อน</label>
        <input type="number" step="0.01" min="0" id="price_3_days_ago" value="37.69">
      </div>
      <div class="field">
        <label for="price_2_days_ago">2 วันก่อน</label>
        <input type="number" step="0.01" min="0" id="price_2_days_ago" value="38.29">
      </div>
      <div class="field">
        <label for="price_yesterday">เมื่อวาน</label>
        <input type="number" step="0.01" min="0" id="price_yesterday" value="38.29">
      </div>
    </div>

    <button class="predict" onclick="doPredictOil()">พยากรณ์ราคาวันถัดไป</button>

    <div id="oil_error" class="alert"></div>

    <p class="footnote">โมเดลนี้เป็น Linear Regression ที่ฝึกและดีพลอยให้อัตโนมัติทุกวันผ่าน Airflow ผลลัพธ์เป็นการประมาณการเพื่อการทดสอบเท่านั้น</p>
    <p class="footnote" style="margin-top:12px"><a href="/covid" style="color:#E8A33D;text-decoration:none">ดูแดชบอร์ดโควิด-19 ประเทศไทย →</a></p>

  </div>

<script>
const ARC_LEN = Math.PI * 140;
const needleGroup = document.getElementById('needleGroup');
const gaugeProgress = document.getElementById('gaugeProgress');

function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }

function setGauge(fraction) {
  const f = clamp(fraction, 0, 1);
  const angle = (f - 0.5) * 180;
  needleGroup.setAttribute('transform', `rotate(${angle} 160 170)`);
  gaugeProgress.style.strokeDasharray = ARC_LEN;
  gaugeProgress.style.strokeDashoffset = ARC_LEN * (1 - f);
}

setGauge(0.5);
gaugeProgress.style.strokeDashoffset = ARC_LEN;

async function doPredictOil() {

  const fields = ['price_5_days_ago','price_4_days_ago','price_3_days_ago','price_2_days_ago','price_yesterday'];
  const payload = {};
  fields.forEach(id => { payload[id] = parseFloat(document.getElementById(id).value); });

  const errorBox = document.getElementById('oil_error');
  const readout = document.getElementById('oil_readout');
  const placeholder = document.getElementById('gaugePlaceholder');

  errorBox.style.display = 'none';

  if (Object.values(payload).some(v => !Number.isFinite(v) || v < 0)) {
    errorBox.style.display = 'block';
    errorBox.textContent = 'กรุณากรอกราคาให้ครบและต้องไม่ติดลบ';
    return;
  }

  try {
    const res = await fetch('/predict_oil', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    const data = await res.json();

    if (!res.ok) {
      errorBox.style.display = 'block';
      errorBox.textContent = data.detail || 'เกิดข้อผิดพลาด';
      return;
    }

    const forecast = data.forecast_price;
    const yesterday = payload.price_yesterday;
    const allValues = [...Object.values(payload), forecast];
    const min = Math.min(...allValues) - 0.5;
    const max = Math.max(...allValues) + 0.5;
    const fraction = (forecast - min) / (max - min || 1);

    setGauge(fraction);
    document.getElementById('gaugeMin').textContent = min.toFixed(2);
    document.getElementById('gaugeMax').textContent = max.toFixed(2);

    document.getElementById('oil_price_value').textContent = forecast.toFixed(2);
    document.getElementById('oil_meta').textContent = 'อัปเดตโมเดลล่าสุด ' + data.model_last_modified;

    const delta = forecast - yesterday;
    const deltaEl = document.getElementById('oil_delta');
    deltaEl.classList.remove('up', 'down');
    if (delta >= 0) {
      deltaEl.classList.add('up');
      deltaEl.textContent = '▲ เพิ่มขึ้น ' + Math.abs(delta).toFixed(2) + ' บาท จากเมื่อวาน';
    } else {
      deltaEl.classList.add('down');
      deltaEl.textContent = '▼ ลดลง ' + Math.abs(delta).toFixed(2) + ' บาท จากเมื่อวาน';
    }

    placeholder.style.display = 'none';
    readout.style.display = 'block';

  } catch (e) {
    errorBox.style.display = 'block';
    errorBox.textContent = 'เรียก API ไม่สำเร็จ: ' + e.message;
  }
}
</script>

</body>
</html>
"""

    return page
