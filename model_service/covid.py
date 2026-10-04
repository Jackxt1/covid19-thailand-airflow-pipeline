"""
covid.py — COVID-19 Thailand Dashboard Router
=============================================
อ่านตารางสรุปที่ DAG covid_dashboard_pipeline สร้างไว้ใน postgres_target
แล้วเสิร์ฟเป็น JSON ให้หน้าแดชบอร์ดที่ /covid

ทุก endpoint อ่านจากตารางสรุป ไม่แตะตารางดิบ 4 ล้านแถว
หน้าเว็บจึงโหลดเสร็จในระดับมิลลิวินาที
"""

import os
from datetime import date, datetime
from decimal import Decimal

import psycopg2
from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

router = APIRouter()

DB_SETTINGS = {
    "host": os.getenv("ETL_DB_HOST", "postgres_target"),
    "port": int(os.getenv("ETL_DB_PORT", "5432")),
    "dbname": os.getenv("ETL_DB_NAME", "etl_db"),
    "user": os.getenv("ETL_DB_USER", "etluser"),
    "password": os.getenv("ETL_DB_PASSWORD", "etlpass"),
}

PIPELINE_HINT = (
    "ยังไม่มีตารางสรุปในฐานข้อมูล — ต้องรัน DAG covid_dashboard_pipeline "
    "ใน Airflow (http://localhost:8080) ให้สำเร็จอย่างน้อย 1 ครั้งก่อน"
)


def _connect():
    try:
        return psycopg2.connect(connect_timeout=5, **DB_SETTINGS)
    except psycopg2.Error as exc:
        raise HTTPException(
            status_code=503,
            detail=f"ต่อฐานข้อมูล etl_db ไม่ได้: {exc}",
        )


def _jsonable(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _query(sql, params=None):
    """คืนผลลัพธ์เป็น list ของ dict — ตารางหายให้ตอบ 503 พร้อมวิธีแก้"""
    connection = _connect()
    try:
        with connection.cursor() as cursor:
            try:
                cursor.execute(sql, params or ())
            except psycopg2.errors.UndefinedTable:
                raise HTTPException(status_code=503, detail=PIPELINE_HINT)
            columns = [c[0] for c in cursor.description]
            return [
                {col: _jsonable(val) for col, val in zip(columns, row)}
                for row in cursor.fetchall()
            ]
    finally:
        connection.close()


def covid_pipeline_status():
    """ใช้ใน /health — บอกว่าตารางสรุปพร้อมหรือยัง"""
    try:
        connection = psycopg2.connect(connect_timeout=3, **DB_SETTINGS)
    except psycopg2.Error as exc:
        return {"ready": False, "reason": f"ต่อฐานข้อมูลไม่ได้: {exc}".strip()}

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('public.covid_summary')")
            if cursor.fetchone()[0] is None:
                return {"ready": False, "reason": PIPELINE_HINT}
            cursor.execute(
                "SELECT total_cases, first_date, last_date, refreshed_at "
                "FROM covid_summary"
            )
            row = cursor.fetchone()
    finally:
        connection.close()

    if row is None:
        return {"ready": False, "reason": PIPELINE_HINT}

    return {
        "ready": True,
        "total_cases": row[0],
        "first_date": _jsonable(row[1]),
        "last_date": _jsonable(row[2]),
        "refreshed_at": _jsonable(row[3]),
    }


# -----------------------------------------------------------------
# API
# -----------------------------------------------------------------

@router.get("/api/covid/summary")
def api_summary():
    """ตัวเลขสรุปบนหัวแดชบอร์ด"""
    rows = _query(
        "SELECT total_cases, first_date, last_date, avg_age, province_count, "
        "refreshed_at FROM covid_summary"
    )
    if not rows:
        raise HTTPException(status_code=503, detail=PIPELINE_HINT)

    summary = rows[0]
    peak = _query("SELECT day, cases FROM covid_daily ORDER BY cases DESC LIMIT 1")
    top = _query("SELECT province, cases FROM covid_by_province ORDER BY cases DESC LIMIT 1")

    summary["peak_day"] = peak[0]["day"] if peak else None
    summary["peak_cases"] = peak[0]["cases"] if peak else None
    summary["top_province"] = top[0]["province"] if top else None
    summary["top_province_cases"] = top[0]["cases"] if top else None
    return summary


@router.get("/api/covid/daily")
def api_daily():
    """เคสรายวันพร้อมค่าเฉลี่ยเคลื่อนที่ 7 วัน"""
    return _query("SELECT day, cases, moving_avg_7d FROM covid_daily ORDER BY day")


@router.get("/api/covid/provinces")
def api_provinces(limit: int = 15):
    return _query(
        "SELECT province, cases FROM covid_by_province ORDER BY cases DESC LIMIT %s",
        (limit,),
    )


@router.get("/api/covid/age-groups")
def api_age_groups():
    """เรียงตามช่วงอายุจริง ไม่ใช่เรียงตามจำนวน"""
    return _query(
        """
        SELECT age_group, cases
        FROM covid_by_age_group
        ORDER BY CASE age_group
            WHEN '0-9'   THEN 1
            WHEN '10-19' THEN 2
            WHEN '20-29' THEN 3
            WHEN '30-39' THEN 4
            WHEN '40-49' THEN 5
            WHEN '50-59' THEN 6
            WHEN '60-69' THEN 7
            WHEN '70+'   THEN 8
            ELSE 9
        END
        """
    )


@router.get("/api/covid/sex")
def api_sex():
    return _query("SELECT sex, cases FROM covid_by_sex ORDER BY cases DESC")


@router.get("/api/covid/nationalities")
def api_nationalities(limit: int = 10):
    return _query(
        "SELECT nationality, cases FROM covid_by_nationality "
        "ORDER BY cases DESC LIMIT %s",
        (limit,),
    )


# -----------------------------------------------------------------
# หน้าเว็บ
# -----------------------------------------------------------------

@router.get("/covid", response_class=HTMLResponse)
def covid_dashboard():
    return COVID_DASHBOARD_HTML


COVID_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>โควิด-19 ประเทศไทย — เส้นเวลาการระบาด</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,600&family=IBM+Plex+Sans+Thai:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  :root {
    --plane:      #0B2122;
    --surface:    #163638;
    --surface-2:  #1C4144;
    --line:       #2B5154;
    --ink:        #F1EAD9;
    --ink-dim:    #8CA6A3;
    --ink-muted:  #6E8A88;
    --c-case:     #239AAA;   /* ผู้ป่วยรายวัน — ผ่าน validator บนพื้น #163638 */
    --c-trend:    #DC6A33;   /* เฉลี่ย 7 วัน */
    --radius:     12px;
  }

  * { box-sizing: border-box; }
  html { -webkit-text-size-adjust: 100%; }

  body {
    margin: 0;
    padding: 48px 20px 72px;
    background: var(--plane);
    background-image:
      radial-gradient(ellipse 1100px 520px at 50% -14%, rgba(35,154,170,0.16), transparent 62%);
    color: var(--ink);
    font-family: 'IBM Plex Sans Thai', system-ui, -apple-system, 'Segoe UI', sans-serif;
    font-size: 15px;
    line-height: 1.6;
  }

  .page { max-width: 1120px; margin: 0 auto; }

  /* ---------- หัวเรื่อง ---------- */

  .eyebrow {
    font-size: 12px;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--c-case);
    margin: 0 0 14px;
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .eyebrow::after {
    content: '';
    flex: 1;
    height: 1px;
    background: var(--line);
  }

  h1 {
    font-family: 'Fraunces', Georgia, serif;
    font-size: clamp(30px, 4.4vw, 46px);
    font-weight: 600;
    line-height: 1.14;
    letter-spacing: -0.4px;
    margin: 0 0 14px;
  }

  .lede {
    color: var(--ink-dim);
    max-width: 62ch;
    margin: 0 0 34px;
  }

  /* ---------- การ์ด ---------- */

  .card {
    background: var(--surface);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    padding: 24px 26px 20px;
    margin-bottom: 22px;
  }

  .card-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 16px;
    flex-wrap: wrap;
    margin-bottom: 4px;
  }

  h2 {
    font-family: 'Fraunces', Georgia, serif;
    font-size: 20px;
    font-weight: 600;
    margin: 0;
    letter-spacing: -0.1px;
  }

  .card-note {
    font-size: 13px;
    color: var(--ink-muted);
    margin: 2px 0 18px;
  }

  /* ---------- legend ---------- */

  .legend {
    display: flex;
    gap: 18px;
    flex-wrap: wrap;
    font-size: 13px;
    color: var(--ink-dim);
  }
  .legend span { display: inline-flex; align-items: center; gap: 7px; }
  .swatch { width: 13px; height: 3px; border-radius: 2px; display: inline-block; }
  .swatch.block { height: 11px; width: 11px; border-radius: 3px; }

  /* ---------- แถบตัวเลขสรุป ---------- */

  .stats {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    background: var(--surface);
    overflow: hidden;
    margin-bottom: 22px;
  }
  .stat {
    padding: 20px 22px;
    border-right: 1px solid var(--line);
  }
  .stat:last-child { border-right: none; }
  .stat .label {
    font-size: 12px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--ink-muted);
    margin-bottom: 6px;
  }
  .stat .value {
    font-size: 27px;
    font-weight: 600;
    line-height: 1.15;
    font-variant-numeric: tabular-nums;
  }
  .stat .sub {
    font-size: 13px;
    color: var(--ink-dim);
    margin-top: 3px;
  }

  /* ---------- กริดสองคอลัมน์ ---------- */

  .grid-2 {
    display: grid;
    grid-template-columns: 1.15fr 1fr;
    gap: 22px;
    margin-bottom: 22px;
  }

  /* ---------- svg ---------- */

  svg { display: block; width: 100%; height: auto; overflow: visible; }
  .grid-line { stroke: var(--line); stroke-width: 1; }
  .axis-text { fill: var(--ink-muted); font-size: 12px; }
  .bar-label { fill: var(--ink-dim); font-size: 13px; }
  .bar-value { fill: var(--ink); font-size: 13px; font-variant-numeric: tabular-nums; }
  .peak-label { fill: var(--ink); font-size: 12px; font-variant-numeric: tabular-nums; }
  .peak-stem { stroke: var(--ink-muted); stroke-width: 1; stroke-dasharray: none; }

  /* ---------- tooltip ---------- */

  .chart-wrap { position: relative; }
  .tip {
    position: absolute;
    pointer-events: none;
    background: var(--plane);
    border: 1px solid var(--line);
    border-radius: 8px;
    padding: 9px 12px;
    font-size: 13px;
    line-height: 1.5;
    white-space: nowrap;
    opacity: 0;
    transition: opacity 120ms ease;
    z-index: 5;
  }
  .tip.on { opacity: 1; }
  .tip .tip-day { color: var(--ink-dim); font-size: 12px; margin-bottom: 3px; }
  .tip .tip-row { display: flex; align-items: center; gap: 7px; font-variant-numeric: tabular-nums; }

  /* ---------- ตาราง ---------- */

  table { width: 100%; border-collapse: collapse; font-size: 14px; }
  th, td { text-align: left; padding: 9px 4px; border-bottom: 1px solid var(--line); }
  th {
    font-size: 12px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--ink-muted);
    font-weight: 500;
  }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  tbody tr:last-child td { border-bottom: none; }
  .rank { color: var(--ink-muted); font-variant-numeric: tabular-nums; width: 28px; }

  /* ---------- สัดส่วนเพศ ---------- */

  .split-row { display: flex; gap: 2px; margin: 18px 0 14px; }
  .split-seg {
    height: 34px;
    border-radius: 3px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 13px;
    font-weight: 600;
    color: var(--plane);
    font-variant-numeric: tabular-nums;
    overflow: hidden;
  }
  .split-list { display: grid; gap: 9px; font-size: 14px; }
  .split-item { display: flex; align-items: center; gap: 9px; }
  .split-item .n { margin-left: auto; font-variant-numeric: tabular-nums; color: var(--ink-dim); }

  /* ---------- สถานะ ---------- */

  .state {
    border: 1px solid var(--line);
    border-radius: var(--radius);
    background: var(--surface);
    padding: 34px 28px;
    color: var(--ink-dim);
    text-align: center;
  }
  .state strong { color: var(--ink); display: block; margin-bottom: 8px; font-size: 16px; }
  .state code {
    background: var(--surface-2);
    border-radius: 4px;
    padding: 2px 6px;
    font-size: 13px;
  }

  /* ---------- ท้ายหน้า ---------- */

  footer {
    margin-top: 30px;
    padding-top: 20px;
    border-top: 1px solid var(--line);
    font-size: 13px;
    color: var(--ink-muted);
    display: flex;
    justify-content: space-between;
    gap: 16px;
    flex-wrap: wrap;
  }
  a { color: var(--c-case); text-decoration: none; }
  a:hover { text-decoration: underline; }
  a:focus-visible, [tabindex]:focus-visible {
    outline: 2px solid var(--c-trend);
    outline-offset: 3px;
    border-radius: 3px;
  }

  @media (max-width: 900px) {
    .grid-2 { grid-template-columns: 1fr; }
    .stats { grid-template-columns: repeat(2, 1fr); }
    .stat:nth-child(2) { border-right: none; }
    .stat:nth-child(1), .stat:nth-child(2) { border-bottom: 1px solid var(--line); }
  }
  @media (max-width: 520px) {
    body { padding: 32px 14px 56px; }
    .card { padding: 20px 16px 16px; }
    /* ไทยไม่มีเว้นวรรคระหว่างคำ เบราว์เซอร์จึงตัดกลางคำได้ ย่อขนาดลงให้พอดีบรรทัด */
    h1 { font-size: 25px; word-break: keep-all; }
    .stats { grid-template-columns: 1fr; }
    .stat { border-right: none; border-bottom: 1px solid var(--line); }
    .stat:last-child { border-bottom: none; }
  }
  @media (prefers-reduced-motion: reduce) {
    * { transition: none !important; animation: none !important; }
  }
</style>
</head>
<body>
<div class="page">

  <p class="eyebrow">กรมควบคุมโรค · ผู้ป่วยยืนยัน</p>
  <h1>เส้นเวลาการระบาดของโควิด-19<br>ในประเทศไทย</h1>
  <p class="lede">
    รวมผู้ป่วยยืนยันรายวันจากไฟล์เปิดของกรมควบคุมโรค ผ่าน ETL บน Apache Airflow
    ทำความสะอาด ตัดเคสซ้ำ แล้วสรุปลง PostgreSQL ทุกตัวเลขในหน้านี้อ่านจากตารางสรุปโดยตรง
  </p>

  <div id="root">
    <div class="state">กำลังโหลดข้อมูล…</div>
  </div>

  <footer>
    <span id="provenance">แหล่งข้อมูล: ไฟล์ confirmed-cases ของกรมควบคุมโรค</span>
    <span><a href="/">ไปหน้าพยากรณ์ราคา Gasohol 95</a></span>
  </footer>

</div>

<script>
const C_CASE  = '#239AAA';
const C_TREND = '#DC6A33';

const nf = new Intl.NumberFormat('th-TH');

function thaiDate(iso, withYear) {
  const d = new Date(iso + 'T00:00:00');
  return d.toLocaleDateString('th-TH', {
    day: 'numeric',
    month: 'short',
    year: withYear ? 'numeric' : undefined,
  });
}

function el(html) {
  const t = document.createElement('template');
  t.innerHTML = html.trim();
  return t.content.firstElementChild;
}

function esc(s) {
  return String(s).replace(/[&<>"]/g, c => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]
  ));
}

/* เส้นทางสี่เหลี่ยมที่มนเฉพาะปลายด้านข้อมูล */
function barPath(x, y, w, h, r, horizontal) {
  const rr = Math.max(0, Math.min(r, horizontal ? w : h));
  if (horizontal) {
    return `M${x},${y} H${x + w - rr} Q${x + w},${y} ${x + w},${y + rr} ` +
           `V${y + h - rr} Q${x + w},${y + h} ${x + w - rr},${y + h} H${x} Z`;
  }
  return `M${x},${y + h} V${y + rr} Q${x},${y} ${x + rr},${y} ` +
         `H${x + w - rr} Q${x + w},${y} ${x + w},${y + rr} V${y + h} Z`;
}

/* ---------------- กราฟเส้นเวลาการระบาด ---------------- */

function renderTimeline(daily) {
  const W = 1040, H = 360;
  const PAD = { top: 26, right: 18, bottom: 30, left: 56 };
  const plotW = W - PAD.left - PAD.right;
  const plotH = H - PAD.top - PAD.bottom;

  const maxCases = Math.max(...daily.map(d => d.cases));
  const x = i => PAD.left + (daily.length <= 1 ? 0 : (i / (daily.length - 1)) * plotW);
  const y = v => PAD.top + plotH - (v / maxCases) * plotH;

  // เส้นแกน y 4 ขั้น
  const ticks = [0, 0.25, 0.5, 0.75, 1].map(f => Math.round(maxCases * f));
  const gridMarkup = ticks.map(v => `
    <line class="grid-line" x1="${PAD.left}" y1="${y(v)}" x2="${W - PAD.right}" y2="${y(v)}"></line>
    <text class="axis-text" x="${PAD.left - 10}" y="${y(v) + 4}" text-anchor="end">${nf.format(v)}</text>
  `).join('');

  // ป้ายปีบนแกน x
  const yearMarks = [];
  let lastYear = null;
  daily.forEach((d, i) => {
    const year = d.day.slice(0, 4);
    if (year !== lastYear) {
      yearMarks.push({ i, year: Number(year) + 543 });
      lastYear = year;
    }
  });
  const yearMarkup = yearMarks.map(m => `
    <line class="grid-line" x1="${x(m.i)}" y1="${PAD.top}" x2="${x(m.i)}" y2="${PAD.top + plotH}"></line>
    <text class="axis-text" x="${x(m.i) + 6}" y="${H - 10}">พ.ศ. ${m.year}</text>
  `).join('');

  const areaPath = 'M' + x(0) + ',' + y(0) + ' ' +
    daily.map((d, i) => `L${x(i).toFixed(1)},${y(d.cases).toFixed(1)}`).join(' ') +
    ` L${x(daily.length - 1)},${y(0)} Z`;

  const trendPath = 'M' + daily
    .map((d, i) => `${x(i).toFixed(1)},${y(Number(d.moving_avg_7d) || 0).toFixed(1)}`)
    .join(' L');

  // หา 3 ยอดสูงสุดที่ห่างกันอย่างน้อย 45 วัน — บอกว่าแต่ละระลอกพีควันไหน
  const peaks = [];
  [...daily.keys()]
    .sort((a, b) => daily[b].cases - daily[a].cases)
    .forEach(i => {
      if (peaks.length >= 3) return;
      if (peaks.every(p => Math.abs(p - i) > 45)) peaks.push(i);
    });
  peaks.sort((a, b) => a - b);

  const peakMarkup = peaks.map(i => {
    const px = x(i), py = y(daily[i].cases);
    const anchor = px > W - 190 ? 'end' : 'start';
    const dx = anchor === 'end' ? -8 : 8;
    return `
      <line class="peak-stem" x1="${px}" y1="${py - 4}" x2="${px}" y2="${py - 20}"></line>
      <circle cx="${px}" cy="${py}" r="3.5" fill="${C_TREND}" stroke="var(--surface)" stroke-width="2"></circle>
      <text class="peak-label" x="${px + dx}" y="${py - 22}" text-anchor="${anchor}">
        ${nf.format(daily[i].cases)} เคส · ${esc(thaiDate(daily[i].day, true))}
      </text>`;
  }).join('');

  const svg = `
    <svg viewBox="0 0 ${W} ${H}" role="img"
         aria-label="กราฟผู้ป่วยยืนยันรายวันตั้งแต่ ${esc(thaiDate(daily[0].day, true))} ถึง ${esc(thaiDate(daily[daily.length - 1].day, true))}">
      ${gridMarkup}
      ${yearMarkup}
      <path d="${areaPath}" fill="${C_CASE}" fill-opacity="0.26"></path>
      <path d="${trendPath}" fill="none" stroke="${C_TREND}" stroke-width="2"
            stroke-linejoin="round" stroke-linecap="round"></path>
      ${peakMarkup}
      <line id="tlCross" x1="0" y1="${PAD.top}" x2="0" y2="${PAD.top + plotH}"
            stroke="var(--ink-muted)" stroke-width="1" opacity="0"></line>
      <circle id="tlDot" r="4" fill="${C_TREND}" stroke="var(--surface)" stroke-width="2" opacity="0"></circle>
      <rect id="tlHit" x="${PAD.left}" y="${PAD.top}" width="${plotW}" height="${plotH}" fill="transparent"></rect>
    </svg>`;

  const wrap = el(`<div class="chart-wrap">${svg}<div class="tip" id="tlTip"></div></div>`);

  // crosshair + tooltip
  const svgEl = wrap.querySelector('svg');
  const cross = wrap.querySelector('#tlCross');
  const dot   = wrap.querySelector('#tlDot');
  const tip   = wrap.querySelector('#tlTip');

  function hide() {
    cross.setAttribute('opacity', '0');
    dot.setAttribute('opacity', '0');
    tip.classList.remove('on');
  }

  function move(ev) {
    const box = svgEl.getBoundingClientRect();
    const scale = W / box.width;
    const sx = (ev.clientX - box.left) * scale;
    let i = Math.round(((sx - PAD.left) / plotW) * (daily.length - 1));
    i = Math.max(0, Math.min(daily.length - 1, i));
    const d = daily[i];

    cross.setAttribute('x1', x(i)); cross.setAttribute('x2', x(i));
    cross.setAttribute('opacity', '0.5');
    dot.setAttribute('cx', x(i)); dot.setAttribute('cy', y(Number(d.moving_avg_7d) || 0));
    dot.setAttribute('opacity', '1');

    tip.innerHTML =
      `<div class="tip-day">${esc(thaiDate(d.day, true))}</div>` +
      `<div class="tip-row"><span class="swatch block" style="background:${C_CASE}"></span>` +
      `ผู้ป่วยวันนั้น ${nf.format(d.cases)}</div>` +
      `<div class="tip-row"><span class="swatch" style="background:${C_TREND}"></span>` +
      `เฉลี่ย 7 วัน ${nf.format(Math.round(Number(d.moving_avg_7d) || 0))}</div>`;
    tip.classList.add('on');

    const left = (x(i) / scale) + 16;
    tip.style.left = Math.min(left, box.width - tip.offsetWidth - 8) + 'px';
    tip.style.top = '8px';
  }

  svgEl.addEventListener('mousemove', move);
  svgEl.addEventListener('mouseleave', hide);
  return wrap;
}

/* ---------------- แท่งแนวนอน: จังหวัด ---------------- */

function renderProvinces(rows) {
  const rowH = 27, labelW = 126, valueW = 72;
  const W = 520, H = rows.length * rowH + 8;
  const barW = W - labelW - valueW;
  const max = Math.max(...rows.map(r => r.cases));

  const bars = rows.map((r, i) => {
    const w = Math.max(2, (r.cases / max) * barW);
    const y = i * rowH + 6;
    return `
      <text class="bar-label" x="${labelW - 10}" y="${y + 12}" text-anchor="end">${esc(r.province)}</text>
      <path d="${barPath(labelW, y, w, 15, 4, true)}" fill="${C_CASE}">
        <title>${esc(r.province)} ${nf.format(r.cases)} เคส</title>
      </path>
      <text class="bar-value" x="${labelW + w + 9}" y="${y + 12}">${nf.format(r.cases)}</text>`;
  }).join('');

  return el(`<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="จังหวัดที่พบผู้ป่วยสูงสุด">${bars}</svg>`);
}

/* ---------------- แท่งแนวตั้ง: กลุ่มอายุ ---------------- */

function renderAgeGroups(rows) {
  const W = 460, H = 280;
  const PAD = { top: 24, right: 6, bottom: 34, left: 48 };
  const plotW = W - PAD.left - PAD.right;
  const plotH = H - PAD.top - PAD.bottom;
  const max = Math.max(...rows.map(r => r.cases));
  const slot = plotW / rows.length;
  const barW = Math.min(38, slot - 10);

  const ticks = [0, 0.5, 1].map(f => Math.round(max * f));
  const grid = ticks.map(v => {
    const y = PAD.top + plotH - (v / max) * plotH;
    return `<line class="grid-line" x1="${PAD.left}" y1="${y}" x2="${W - PAD.right}" y2="${y}"></line>
            <text class="axis-text" x="${PAD.left - 9}" y="${y + 4}" text-anchor="end">${nf.format(v)}</text>`;
  }).join('');

  const bars = rows.map((r, i) => {
    const h = Math.max(2, (r.cases / max) * plotH);
    const x = PAD.left + i * slot + (slot - barW) / 2;
    const y = PAD.top + plotH - h;
    return `
      <path d="${barPath(x, y, barW, h, 4, false)}" fill="${C_CASE}">
        <title>อายุ ${esc(r.age_group)} ปี — ${nf.format(r.cases)} เคส</title>
      </path>
      <text class="axis-text" x="${x + barW / 2}" y="${H - 12}" text-anchor="middle">${esc(r.age_group)}</text>`;
  }).join('');

  return el(`<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="ผู้ป่วยแยกตามกลุ่มอายุ">${grid}${bars}</svg>`);
}

/* ---------------- สัดส่วนเพศ ---------------- */

function renderSex(rows) {
  const total = rows.reduce((s, r) => s + r.cases, 0);
  const colors = { 'หญิง': C_CASE, 'ชาย': C_TREND, 'ไม่ระบุ': 'var(--ink-muted)' };

  const segs = rows.map(r => {
    const pct = (r.cases / total) * 100;
    const color = colors[r.sex] || 'var(--ink-muted)';
    const label = pct >= 12 ? pct.toFixed(1) + '%' : '';
    return `<div class="split-seg" style="width:${pct}%;background:${color}"
                 title="${esc(r.sex)} ${nf.format(r.cases)} เคส">${label}</div>`;
  }).join('');

  const list = rows.map(r => {
    const color = colors[r.sex] || 'var(--ink-muted)';
    const pct = ((r.cases / total) * 100).toFixed(1);
    return `<div class="split-item">
              <span class="swatch block" style="background:${color}"></span>
              ${esc(r.sex)}
              <span class="n">${nf.format(r.cases)} · ${pct}%</span>
            </div>`;
  }).join('');

  return el(`<div><div class="split-row">${segs}</div><div class="split-list">${list}</div></div>`);
}

/* ---------------- ตารางสัญชาติ ---------------- */

function renderNationalities(rows, total) {
  const body = rows.map((r, i) => `
    <tr>
      <td class="rank">${i + 1}</td>
      <td>${esc(r.nationality)}</td>
      <td class="num">${nf.format(r.cases)}</td>
      <td class="num">${((r.cases / total) * 100).toFixed(2)}%</td>
    </tr>`).join('');

  return el(`
    <table>
      <thead><tr><th></th><th>สัญชาติ</th><th class="num">ผู้ป่วย</th><th class="num">สัดส่วน</th></tr></thead>
      <tbody>${body}</tbody>
    </table>`);
}

/* ---------------- ประกอบหน้า ---------------- */

async function getJSON(url) {
  const res = await fetch(url);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.detail || ('เรียก ' + url + ' ไม่สำเร็จ'));
  return data;
}

async function build() {
  const root = document.getElementById('root');

  let summary, daily, provinces, ages, sexes, nationalities;
  try {
    [summary, daily, provinces, ages, sexes, nationalities] = await Promise.all([
      getJSON('/api/covid/summary'),
      getJSON('/api/covid/daily'),
      getJSON('/api/covid/provinces?limit=15'),
      getJSON('/api/covid/age-groups'),
      getJSON('/api/covid/sex'),
      getJSON('/api/covid/nationalities?limit=10'),
    ]);
  } catch (err) {
    root.innerHTML =
      '<div class="state"><strong>ยังไม่มีข้อมูลให้แสดง</strong>' +
      esc(err.message) + '</div>';
    return;
  }

  root.innerHTML = '';

  // 1) เส้นเวลา — ตัวหลักของหน้า
  const timeline = el(`
    <div class="card">
      <div class="card-head">
        <h2>ผู้ป่วยยืนยันรายวัน</h2>
        <div class="legend">
          <span><span class="swatch block" style="background:${C_CASE};opacity:.5"></span>รายวัน</span>
          <span><span class="swatch" style="background:${C_TREND}"></span>เฉลี่ย 7 วัน</span>
        </div>
      </div>
      <p class="card-note">จุดสีส้มคือวันที่พบผู้ป่วยสูงสุดของแต่ละระลอก · เลื่อนเมาส์บนกราฟเพื่อดูรายวัน</p>
    </div>`);
  timeline.appendChild(renderTimeline(daily));
  root.appendChild(timeline);

  // 2) ตัวเลขสรุป
  root.appendChild(el(`
    <div class="stats">
      <div class="stat">
        <div class="label">ผู้ป่วยสะสม</div>
        <div class="value">${nf.format(summary.total_cases)}</div>
        <div class="sub">${esc(thaiDate(summary.first_date, true))} – ${esc(thaiDate(summary.last_date, true))}</div>
      </div>
      <div class="stat">
        <div class="label">วันที่สูงสุด</div>
        <div class="value">${nf.format(summary.peak_cases)}</div>
        <div class="sub">${esc(thaiDate(summary.peak_day, true))}</div>
      </div>
      <div class="stat">
        <div class="label">จังหวัดสูงสุด</div>
        <div class="value" style="font-size:22px">${esc(summary.top_province)}</div>
        <div class="sub">${nf.format(summary.top_province_cases)} เคส จาก ${summary.province_count} จังหวัด</div>
      </div>
      <div class="stat">
        <div class="label">อายุเฉลี่ย</div>
        <div class="value">${summary.avg_age} <span style="font-size:15px;color:var(--ink-dim)">ปี</span></div>
        <div class="sub">จากเคสที่ระบุอายุ</div>
      </div>
    </div>`));

  // 3) จังหวัด + กลุ่มอายุ
  const row1 = el('<div class="grid-2"></div>');
  const provCard = el(`
    <div class="card">
      <div class="card-head"><h2>15 จังหวัดที่พบมากที่สุด</h2></div>
      <p class="card-note">นับตามจังหวัดที่รักษาตัว</p>
    </div>`);
  provCard.appendChild(renderProvinces(provinces));
  const ageCard = el(`
    <div class="card">
      <div class="card-head"><h2>ช่วงอายุผู้ป่วย</h2></div>
      <p class="card-note">หน่วยเป็นจำนวนเคส</p>
    </div>`);
  ageCard.appendChild(renderAgeGroups(ages));
  row1.append(provCard, ageCard);
  root.appendChild(row1);

  // 4) เพศ + สัญชาติ
  const row2 = el('<div class="grid-2"></div>');
  const sexCard = el(`
    <div class="card">
      <div class="card-head"><h2>สัดส่วนเพศ</h2></div>
      <p class="card-note">คิดจากผู้ป่วยทั้งหมดที่โหลดเข้าระบบ</p>
    </div>`);
  sexCard.appendChild(renderSex(sexes));
  const natCard = el(`
    <div class="card">
      <div class="card-head"><h2>10 สัญชาติที่พบมากที่สุด</h2></div>
      <p class="card-note">สัดส่วนเทียบผู้ป่วยสะสมทั้งหมด</p>
    </div>`);
  natCard.appendChild(renderNationalities(nationalities, summary.total_cases));
  row2.append(sexCard, natCard);
  root.appendChild(row2);

  document.getElementById('provenance').textContent =
    'แหล่งข้อมูล: ไฟล์ confirmed-cases ของกรมควบคุมโรค · สรุปล่าสุดเมื่อ ' +
    new Date(summary.refreshed_at).toLocaleString('th-TH');
}

build();
</script>
</body>
</html>
"""
