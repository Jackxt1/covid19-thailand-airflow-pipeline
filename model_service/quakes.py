"""
quakes.py — USGS Earthquake Dashboard Router
============================================
อ่านตารางสรุปที่ DAG usgs_quake_marts สร้างไว้ใน postgres_target
แล้วเสิร์ฟเป็น JSON ให้หน้าแดชบอร์ดที่ /quakes

ทุก endpoint อ่านจากตารางสรุป ไม่แตะตารางดิบ 1.8 ล้านแถว
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
    "ยังไม่มีตารางสรุปแผ่นดินไหว — ต้องรัน DAG usgs_quake_ingest ให้ดึงข้อมูลเข้ามาก่อน "
    "แล้วตามด้วย usgs_quake_marts ใน Airflow (http://localhost:8080)"
)


def _jsonable(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _query(sql, params=None):
    try:
        connection = psycopg2.connect(connect_timeout=5, **DB_SETTINGS)
    except psycopg2.Error as exc:
        raise HTTPException(status_code=503, detail=f"ต่อฐานข้อมูล etl_db ไม่ได้: {exc}")

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


def quake_pipeline_status():
    """ใช้ใน /health"""
    try:
        connection = psycopg2.connect(connect_timeout=3, **DB_SETTINGS)
    except psycopg2.Error as exc:
        return {"ready": False, "reason": f"ต่อฐานข้อมูลไม่ได้: {exc}".strip()}

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('public.quake_summary')")
            if cursor.fetchone()[0] is None:
                return {"ready": False, "reason": PIPELINE_HINT}
            cursor.execute(
                "SELECT total_events, first_event, last_event, refreshed_at FROM quake_summary"
            )
            row = cursor.fetchone()
    finally:
        connection.close()

    if row is None:
        return {"ready": False, "reason": PIPELINE_HINT}

    return {
        "ready": True,
        "total_events": row[0],
        "first_event": _jsonable(row[1]),
        "last_event": _jsonable(row[2]),
        "refreshed_at": _jsonable(row[3]),
    }


# -----------------------------------------------------------------
# API
# -----------------------------------------------------------------

@router.get("/api/quakes/summary")
def api_summary():
    rows = _query(
        "SELECT total_events, first_event, last_event, max_magnitude, avg_magnitude, "
        "avg_depth_km, region_count, events_m6_plus, refreshed_at FROM quake_summary"
    )
    if not rows:
        raise HTTPException(status_code=503, detail=PIPELINE_HINT)

    summary = rows[0]
    strongest = _query(
        "SELECT place, magnitude, occurred_at FROM quake_strongest "
        "ORDER BY magnitude DESC LIMIT 1"
    )
    top_region = _query(
        "SELECT region, events FROM quake_by_region ORDER BY events DESC LIMIT 1"
    )
    summary["strongest_place"] = strongest[0]["place"] if strongest else None
    summary["strongest_at"] = strongest[0]["occurred_at"] if strongest else None
    summary["top_region"] = top_region[0]["region"] if top_region else None
    summary["top_region_events"] = top_region[0]["events"] if top_region else None
    return summary


@router.get("/api/quakes/monthly")
def api_monthly():
    return _query(
        "SELECT month, events, avg_magnitude, max_magnitude, events_m5_plus "
        "FROM quake_monthly ORDER BY month"
    )


@router.get("/api/quakes/magnitude-bands")
def api_magnitude_bands():
    return _query(
        "SELECT magnitude_band, events FROM quake_by_magnitude ORDER BY sort_order"
    )


@router.get("/api/quakes/depth-bands")
def api_depth_bands():
    return _query("SELECT depth_band, events FROM quake_by_depth ORDER BY sort_order")


@router.get("/api/quakes/regions")
def api_regions(limit: int = 15):
    return _query(
        "SELECT region, events, max_magnitude, avg_depth_km FROM quake_by_region "
        "ORDER BY events DESC LIMIT %s",
        (limit,),
    )


@router.get("/api/quakes/strongest")
def api_strongest(limit: int = 10):
    return _query(
        "SELECT occurred_at, magnitude, depth_km, place, region "
        "FROM quake_strongest ORDER BY magnitude DESC LIMIT %s",
        (limit,),
    )


@router.get("/api/quakes/map-points")
def api_map_points():
    return _query(
        "SELECT latitude, longitude, magnitude, depth_km FROM quake_map_points "
        "ORDER BY magnitude"
    )


@router.get("/quakes", response_class=HTMLResponse)
def quake_dashboard():
    return QUAKE_DASHBOARD_HTML


QUAKE_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>แผ่นดินไหวทั่วโลก — ข้อมูลจาก USGS</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Thai:wght@300;400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  :root {
    --plane:     #0B2122;
    --surface:   #163638;
    --surface-2: #1C4144;
    --line:      #2B5154;
    --ink:       #F1EAD9;
    --ink-dim:   #8CA6A3;
    --ink-muted: #6E8A88;
    --c-main:    #239AAA;   /* ตื้น / ซีรีส์หลัก — ผ่าน validator บนพื้น #163638 */
    --c-alt:     #DC6A33;   /* ลึก / ซีรีส์รอง */
    --radius:    12px;
  }

  * { box-sizing: border-box; }
  html { -webkit-text-size-adjust: 100%; }

  body {
    margin: 0;
    padding: 48px 20px 72px;
    background: var(--plane);
    background-image: radial-gradient(ellipse 1100px 520px at 50% -14%, rgba(35,154,170,0.16), transparent 62%);
    color: var(--ink);
    font-family: 'IBM Plex Sans Thai', system-ui, -apple-system, 'Segoe UI', sans-serif;
    font-size: 15px;
    line-height: 1.6;
  }

  .page { max-width: 1180px; margin: 0 auto; }

  .eyebrow {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--c-main);
    margin: 0 0 14px;
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .eyebrow::after { content: ''; flex: 1; height: 1px; background: var(--line); }

  h1 {
    font-size: clamp(30px, 4.4vw, 46px);
    font-weight: 600;
    line-height: 1.16;
    letter-spacing: -0.3px;
    margin: 0 0 14px;
  }

  .lede { color: var(--ink-dim); max-width: 64ch; margin: 0 0 34px; }

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
  }

  h2 { font-size: 20px; font-weight: 600; margin: 0; }
  .card-note { font-size: 13px; color: var(--ink-muted); margin: 2px 0 18px; }

  .legend { display: flex; gap: 18px; flex-wrap: wrap; font-size: 13px; color: var(--ink-dim); }
  .legend span { display: inline-flex; align-items: center; gap: 7px; }
  .swatch { width: 13px; height: 3px; border-radius: 2px; display: inline-block; }
  .swatch.dot { height: 11px; width: 11px; border-radius: 50%; }

  .stats {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    background: var(--surface);
    overflow: hidden;
    margin-bottom: 22px;
  }
  .stat { padding: 20px 22px; border-right: 1px solid var(--line); }
  .stat:last-child { border-right: none; }
  .stat .label {
    font-size: 12px; letter-spacing: 0.08em; text-transform: uppercase;
    color: var(--ink-muted); margin-bottom: 6px;
  }
  .stat .value { font-size: 27px; font-weight: 600; line-height: 1.15; font-variant-numeric: tabular-nums; }
  .stat .sub { font-size: 13px; color: var(--ink-dim); margin-top: 3px; }

  .grid-2 { display: grid; grid-template-columns: 1.15fr 1fr; gap: 22px; margin-bottom: 22px; }

  svg { display: block; width: 100%; height: auto; overflow: visible; }
  .grid-line { stroke: var(--line); stroke-width: 1; }
  .axis-text { fill: var(--ink-muted); font-size: 12px; }
  .bar-label { fill: var(--ink-dim); font-size: 13px; }
  .bar-value { fill: var(--ink); font-size: 13px; font-variant-numeric: tabular-nums; }
  .peak-label { fill: var(--ink); font-size: 12px; font-variant-numeric: tabular-nums; }
  .peak-stem { stroke: var(--ink-muted); stroke-width: 1; }

  .chart-wrap { position: relative; }
  .tip {
    position: absolute; pointer-events: none;
    background: var(--plane); border: 1px solid var(--line); border-radius: 8px;
    padding: 9px 12px; font-size: 13px; line-height: 1.5; white-space: nowrap;
    opacity: 0; transition: opacity 120ms ease; z-index: 5;
  }
  .tip.on { opacity: 1; }
  .tip .tip-day { color: var(--ink-dim); font-size: 12px; margin-bottom: 3px; }
  .tip .tip-row { display: flex; align-items: center; gap: 7px; font-variant-numeric: tabular-nums; }

  table { width: 100%; border-collapse: collapse; font-size: 14px; }
  th, td { text-align: left; padding: 9px 4px; border-bottom: 1px solid var(--line); }
  th {
    font-size: 12px; letter-spacing: 0.06em; text-transform: uppercase;
    color: var(--ink-muted); font-weight: 500;
  }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  tbody tr:last-child td { border-bottom: none; }
  .rank { color: var(--ink-muted); font-variant-numeric: tabular-nums; width: 28px; }
  .mag { color: var(--c-alt); font-weight: 600; font-variant-numeric: tabular-nums; }

  .state {
    border: 1px solid var(--line); border-radius: var(--radius); background: var(--surface);
    padding: 34px 28px; color: var(--ink-dim); text-align: center;
  }
  .state strong { color: var(--ink); display: block; margin-bottom: 8px; font-size: 16px; }

  footer {
    margin-top: 30px; padding-top: 20px; border-top: 1px solid var(--line);
    font-size: 13px; color: var(--ink-muted);
    display: flex; justify-content: space-between; gap: 16px; flex-wrap: wrap;
  }
  a { color: var(--c-main); text-decoration: none; }
  a:hover { text-decoration: underline; }
  a:focus-visible { outline: 2px solid var(--c-alt); outline-offset: 3px; border-radius: 3px; }

  @media (max-width: 900px) {
    .grid-2 { grid-template-columns: 1fr; }
    .stats { grid-template-columns: repeat(2, 1fr); }
    .stat:nth-child(2) { border-right: none; }
    .stat:nth-child(1), .stat:nth-child(2) { border-bottom: 1px solid var(--line); }
  }
  @media (max-width: 520px) {
    body { padding: 32px 14px 56px; }
    .card { padding: 20px 16px 16px; }
    h1 { font-size: 25px; word-break: keep-all; }
    .stats { grid-template-columns: 1fr; }
    .stat { border-right: none; border-bottom: 1px solid var(--line); }
    .stat:last-child { border-bottom: none; }
  }
  @media (prefers-reduced-motion: reduce) { * { transition: none !important; } }
</style>
</head>
<body>
<div class="page">

  <p class="eyebrow">USGS · Earthquake Catalog</p>
  <h1>แผ่นดินไหวทั่วโลก<br>ตั้งแต่ปี 2015 ถึงปัจจุบัน</h1>
  <p class="lede">
    ดึงจาก USGS FDSN Event API ทีละเดือนด้วย Apache Airflow แบบ incremental
    ทำความสะอาด สรุปลง PostgreSQL ทุกตัวเลขในหน้านี้อ่านจากตารางสรุปโดยตรง
  </p>

  <div id="root"><div class="state">กำลังโหลดข้อมูล…</div></div>

  <footer>
    <span id="provenance">แหล่งข้อมูล: USGS Earthquake Hazards Program</span>
    <span><a href="/covid">ดูแดชบอร์ดโควิด-19</a> · <a href="/">พยากรณ์ราคา Gasohol 95</a></span>
  </footer>

</div>

<script>
const C_MAIN = '#239AAA';
const C_ALT  = '#DC6A33';
const nf = new Intl.NumberFormat('th-TH');

function thaiMonth(iso) {
  const d = new Date(iso + 'T00:00:00');
  return d.toLocaleDateString('th-TH', { month: 'short', year: 'numeric' });
}
function thaiDateTime(iso) {
  return new Date(iso).toLocaleDateString('th-TH', {
    day: 'numeric', month: 'short', year: 'numeric'
  });
}
function el(html) {
  const t = document.createElement('template');
  t.innerHTML = html.trim();
  return t.content.firstElementChild;
}
function esc(s) {
  return String(s).replace(/[&<>"]/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;' }[c]));
}
function barPath(x, y, w, h, r, horizontal) {
  const rr = Math.max(0, Math.min(r, horizontal ? w : h));
  if (horizontal) {
    return `M${x},${y} H${x + w - rr} Q${x + w},${y} ${x + w},${y + rr} ` +
           `V${y + h - rr} Q${x + w},${y + h} ${x + w - rr},${y + h} H${x} Z`;
  }
  return `M${x},${y + h} V${y + rr} Q${x},${y} ${x + rr},${y} ` +
         `H${x + w - rr} Q${x + w},${y} ${x + w},${y + rr} V${y + h} Z`;
}

/* ---------------- เหตุการณ์รายเดือน ---------------- */

function renderMonthly(rows) {
  const W = 1060, H = 320;
  const PAD = { top: 26, right: 18, bottom: 30, left: 62 };
  const plotW = W - PAD.left - PAD.right;
  const plotH = H - PAD.top - PAD.bottom;
  const max = Math.max(...rows.map(r => r.events));
  const x = i => PAD.left + (rows.length <= 1 ? 0 : (i / (rows.length - 1)) * plotW);
  const y = v => PAD.top + plotH - (v / max) * plotH;

  const ticks = [0, 0.25, 0.5, 0.75, 1].map(f => Math.round(max * f));
  const grid = ticks.map(v => `
    <line class="grid-line" x1="${PAD.left}" y1="${y(v)}" x2="${W - PAD.right}" y2="${y(v)}"></line>
    <text class="axis-text" x="${PAD.left - 10}" y="${y(v) + 4}" text-anchor="end">${nf.format(v)}</text>
  `).join('');

  const yearMarks = [];
  let lastYear = null;
  rows.forEach((r, i) => {
    const yr = r.month.slice(0, 4);
    if (yr !== lastYear) { yearMarks.push({ i, year: Number(yr) + 543 }); lastYear = yr; }
  });
  const yearMarkup = yearMarks.filter((_, k) => k % 2 === 0).map(m => `
    <line class="grid-line" x1="${x(m.i)}" y1="${PAD.top}" x2="${x(m.i)}" y2="${PAD.top + plotH}"></line>
    <text class="axis-text" x="${x(m.i) + 6}" y="${H - 10}">พ.ศ. ${m.year}</text>
  `).join('');

  const area = 'M' + x(0) + ',' + y(0) + ' ' +
    rows.map((r, i) => `L${x(i).toFixed(1)},${y(r.events).toFixed(1)}`).join(' ') +
    ` L${x(rows.length - 1)},${y(0)} Z`;
  const line = 'M' + rows.map((r, i) => `${x(i).toFixed(1)},${y(r.events).toFixed(1)}`).join(' L');

  // สามเดือนที่มีเหตุการณ์มากที่สุด ห่างกันอย่างน้อย 10 เดือน
  const peaks = [];
  [...rows.keys()].sort((a, b) => rows[b].events - rows[a].events).forEach(i => {
    if (peaks.length >= 3) return;
    if (peaks.every(p => Math.abs(p - i) > 10)) peaks.push(i);
  });
  peaks.sort((a, b) => a - b);
  const peakMarkup = peaks.map(i => {
    const px = x(i), py = y(rows[i].events);
    const anchor = px > W - 200 ? 'end' : 'start';
    const dx = anchor === 'end' ? -8 : 8;
    return `
      <line class="peak-stem" x1="${px}" y1="${py - 4}" x2="${px}" y2="${py - 20}"></line>
      <circle cx="${px}" cy="${py}" r="3.5" fill="${C_ALT}" stroke="var(--surface)" stroke-width="2"></circle>
      <text class="peak-label" x="${px + dx}" y="${py - 22}" text-anchor="${anchor}">
        ${nf.format(rows[i].events)} ครั้ง · ${esc(thaiMonth(rows[i].month))}
      </text>`;
  }).join('');

  const svg = `
    <svg viewBox="0 0 ${W} ${H}" role="img" aria-label="จำนวนแผ่นดินไหวรายเดือนตั้งแต่ ${esc(thaiMonth(rows[0].month))}">
      ${grid}${yearMarkup}
      <path d="${area}" fill="${C_MAIN}" fill-opacity="0.24"></path>
      <path d="${line}" fill="none" stroke="${C_MAIN}" stroke-width="2" stroke-linejoin="round"></path>
      ${peakMarkup}
      <line id="mCross" x1="0" y1="${PAD.top}" x2="0" y2="${PAD.top + plotH}" stroke="var(--ink-muted)" stroke-width="1" opacity="0"></line>
      <circle id="mDot" r="4" fill="${C_MAIN}" stroke="var(--surface)" stroke-width="2" opacity="0"></circle>
    </svg>`;

  const wrap = el(`<div class="chart-wrap">${svg}<div class="tip" id="mTip"></div></div>`);
  const svgEl = wrap.querySelector('svg');
  const cross = wrap.querySelector('#mCross');
  const dot = wrap.querySelector('#mDot');
  const tip = wrap.querySelector('#mTip');

  svgEl.addEventListener('mousemove', ev => {
    const box = svgEl.getBoundingClientRect();
    const scale = W / box.width;
    let i = Math.round((((ev.clientX - box.left) * scale - PAD.left) / plotW) * (rows.length - 1));
    i = Math.max(0, Math.min(rows.length - 1, i));
    const r = rows[i];
    cross.setAttribute('x1', x(i)); cross.setAttribute('x2', x(i)); cross.setAttribute('opacity', '0.5');
    dot.setAttribute('cx', x(i)); dot.setAttribute('cy', y(r.events)); dot.setAttribute('opacity', '1');
    tip.innerHTML =
      `<div class="tip-day">${esc(thaiMonth(r.month))}</div>` +
      `<div class="tip-row"><span class="swatch dot" style="background:${C_MAIN}"></span>ทั้งหมด ${nf.format(r.events)} ครั้ง</div>` +
      `<div class="tip-row"><span class="swatch dot" style="background:${C_ALT}"></span>ตั้งแต่ M5 ${nf.format(r.events_m5_plus)} ครั้ง</div>` +
      `<div class="tip-row">แรงสุดเดือนนี้ M${r.max_magnitude}</div>`;
    tip.classList.add('on');
    tip.style.left = Math.min((x(i) / scale) + 16, box.width - tip.offsetWidth - 8) + 'px';
    tip.style.top = '8px';
  });
  svgEl.addEventListener('mouseleave', () => {
    cross.setAttribute('opacity', '0'); dot.setAttribute('opacity', '0'); tip.classList.remove('on');
  });
  return wrap;
}

/* ---------------- แผนที่จุดแผ่นดินไหว ---------------- */

function renderMap(points) {
  const W = 1060, H = 540;
  const px = lon => (Number(lon) + 180) / 360 * W;
  const py = lat => (90 - Number(lat)) / 180 * H;

  // เส้นกริดละติจูด/ลองจิจูดทุก 30 องศา
  let graticule = '';
  for (let lon = -180; lon <= 180; lon += 30) {
    graticule += `<line class="grid-line" x1="${px(lon)}" y1="0" x2="${px(lon)}" y2="${H}" opacity="0.5"></line>`;
  }
  for (let lat = -90; lat <= 90; lat += 30) {
    graticule += `<line class="grid-line" x1="0" y1="${py(lat)}" x2="${W}" y2="${py(lat)}" opacity="0.5"></line>`;
  }
  graticule += `<line x1="0" y1="${py(0)}" x2="${W}" y2="${py(0)}" stroke="${C_ALT}" stroke-width="1" opacity="0.35"></line>`;

  const dots = points.map(p => {
    const m = Number(p.magnitude);
    const r = (1.6 + (m - 6) * 1.5).toFixed(1);
    const deep = Number(p.depth_km) >= 70;
    const color = deep ? C_ALT : C_MAIN;
    return `<circle cx="${px(p.longitude).toFixed(1)}" cy="${py(p.latitude).toFixed(1)}" r="${r}" ` +
           `fill="${color}" fill-opacity="0.55" stroke="${color}" stroke-width="0.6">` +
           `<title>M${m} ลึก ${p.depth_km} กม.</title></circle>`;
  }).join('');

  return el(`<svg viewBox="0 0 ${W} ${H}" role="img"
      aria-label="แผนที่ตำแหน่งแผ่นดินไหวขนาด 6 ขึ้นไป ${points.length} จุด เรียงตัวตามแนววงแหวนไฟแปซิฟิก">
      <rect x="0" y="0" width="${W}" height="${H}" fill="#0E2A2B"></rect>
      ${graticule}${dots}
      <text class="axis-text" x="8" y="${py(0) - 8}">เส้นศูนย์สูตร</text>
    </svg>`);
}

/* ---------------- แท่งแนวนอน: ภูมิภาค ---------------- */

function renderRegions(rows) {
  const rowH = 27, labelW = 150, valueW = 76;
  const W = 540, H = rows.length * rowH + 8;
  const barW = W - labelW - valueW;
  const max = Math.max(...rows.map(r => r.events));
  const bars = rows.map((r, i) => {
    const w = Math.max(2, (r.events / max) * barW);
    const y = i * rowH + 6;
    return `
      <text class="bar-label" x="${labelW - 10}" y="${y + 12}" text-anchor="end">${esc(r.region)}</text>
      <path d="${barPath(labelW, y, w, 15, 4, true)}" fill="${C_MAIN}">
        <title>${esc(r.region)} ${nf.format(r.events)} ครั้ง แรงสุด M${r.max_magnitude}</title>
      </path>
      <text class="bar-value" x="${labelW + w + 9}" y="${y + 12}">${nf.format(r.events)}</text>`;
  }).join('');
  return el(`<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="ภูมิภาคที่พบแผ่นดินไหวมากที่สุด">${bars}</svg>`);
}

/* ---------------- แท่งแนวตั้ง: ช่วงขนาด ---------------- */

function renderBands(rows, label) {
  const W = 460, H = 280;
  const PAD = { top: 24, right: 6, bottom: 46, left: 62 };
  const plotW = W - PAD.left - PAD.right;
  const plotH = H - PAD.top - PAD.bottom;
  const max = Math.max(...rows.map(r => r.events));
  const slot = plotW / rows.length;
  const barW = Math.min(40, slot - 10);

  const ticks = [0, 0.5, 1].map(f => Math.round(max * f));
  const grid = ticks.map(v => {
    const y = PAD.top + plotH - (v / max) * plotH;
    return `<line class="grid-line" x1="${PAD.left}" y1="${y}" x2="${W - PAD.right}" y2="${y}"></line>
            <text class="axis-text" x="${PAD.left - 9}" y="${y + 4}" text-anchor="end">${nf.format(v)}</text>`;
  }).join('');

  const bars = rows.map((r, i) => {
    const key = r[label];
    const h = Math.max(2, (r.events / max) * plotH);
    const x = PAD.left + i * slot + (slot - barW) / 2;
    const y = PAD.top + plotH - h;
    const short = String(key).replace('ขึ้นไป', '+').replace('ต่ำกว่า ', '<')
                             .replace(' (น้อยกว่า 70 กม.)', '').replace(' (70 - 300 กม.)', '')
                             .replace(' (เกิน 300 กม.)', '');
    return `
      <path d="${barPath(x, y, barW, h, 4, false)}" fill="${C_MAIN}">
        <title>${esc(key)} — ${nf.format(r.events)} ครั้ง</title>
      </path>
      <text class="axis-text" x="${x + barW / 2}" y="${H - 22}" text-anchor="middle">${esc(short)}</text>`;
  }).join('');

  return el(`<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="การกระจายตาม${esc(label)}">${grid}${bars}</svg>`);
}

/* ---------------- ตาราง ---------------- */

function renderStrongest(rows) {
  const body = rows.map((r, i) => `
    <tr>
      <td class="rank">${i + 1}</td>
      <td class="mag">M${r.magnitude}</td>
      <td>${esc(r.place)}</td>
      <td class="num">${esc(thaiDateTime(r.occurred_at))}</td>
      <td class="num">${r.depth_km}</td>
    </tr>`).join('');
  return el(`
    <table>
      <thead><tr><th></th><th>ขนาด</th><th>ตำแหน่ง</th><th class="num">วันที่</th><th class="num">ลึก (กม.)</th></tr></thead>
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
  let summary, monthly, bands, depths, regions, strongest, points;
  try {
    [summary, monthly, bands, depths, regions, strongest, points] = await Promise.all([
      getJSON('/api/quakes/summary'),
      getJSON('/api/quakes/monthly'),
      getJSON('/api/quakes/magnitude-bands'),
      getJSON('/api/quakes/depth-bands'),
      getJSON('/api/quakes/regions?limit=15'),
      getJSON('/api/quakes/strongest?limit=10'),
      getJSON('/api/quakes/map-points'),
    ]);
  } catch (err) {
    root.innerHTML = '<div class="state"><strong>ยังไม่มีข้อมูลให้แสดง</strong>' + esc(err.message) + '</div>';
    return;
  }
  root.innerHTML = '';

  const monthlyCard = el(`
    <div class="card">
      <div class="card-head"><h2>แผ่นดินไหวรายเดือน</h2></div>
      <p class="card-note">จุดสีส้มคือเดือนที่เกิดถี่ที่สุด มักเป็นช่วงที่มีอาฟเตอร์ช็อกต่อเนื่อง · เลื่อนเมาส์เพื่อดูรายเดือน</p>
    </div>`);
  monthlyCard.appendChild(renderMonthly(monthly));
  root.appendChild(monthlyCard);

  root.appendChild(el(`
    <div class="stats">
      <div class="stat">
        <div class="label">เหตุการณ์ทั้งหมด</div>
        <div class="value">${nf.format(summary.total_events)}</div>
        <div class="sub">${esc(thaiDateTime(summary.first_event))} – ${esc(thaiDateTime(summary.last_event))}</div>
      </div>
      <div class="stat">
        <div class="label">แรงที่สุด</div>
        <div class="value">M${summary.max_magnitude}</div>
        <div class="sub">${esc(summary.strongest_place || '')}</div>
      </div>
      <div class="stat">
        <div class="label">ตั้งแต่ M6 ขึ้นไป</div>
        <div class="value">${nf.format(summary.events_m6_plus)}</div>
        <div class="sub">คิดเป็น ${(summary.events_m6_plus / summary.total_events * 100).toFixed(2)}% ของทั้งหมด</div>
      </div>
      <div class="stat">
        <div class="label">ความลึกเฉลี่ย</div>
        <div class="value">${summary.avg_depth_km} <span style="font-size:15px;color:var(--ink-dim)">กม.</span></div>
        <div class="sub">จาก ${nf.format(summary.region_count)} ภูมิภาค</div>
      </div>
    </div>`));

  const mapCard = el(`
    <div class="card">
      <div class="card-head">
        <h2>ตำแหน่งแผ่นดินไหวตั้งแต่ M6 ขึ้นไป</h2>
        <div class="legend">
          <span><span class="swatch dot" style="background:${C_MAIN}"></span>ตื้นกว่า 70 กม.</span>
          <span><span class="swatch dot" style="background:${C_ALT}"></span>ลึกตั้งแต่ 70 กม.</span>
        </div>
      </div>
      <p class="card-note">${nf.format(points.length)} จุด ขนาดวงกลมแปรตามขนาดแผ่นดินไหว · รูปร่างที่เห็นคือแนววงแหวนไฟแปซิฟิก</p>
    </div>`);
  mapCard.appendChild(renderMap(points));
  root.appendChild(mapCard);

  const row1 = el('<div class="grid-2"></div>');
  const regionCard = el(`
    <div class="card">
      <div class="card-head"><h2>15 ภูมิภาคที่พบมากที่สุด</h2></div>
      <p class="card-note">แกะชื่อภูมิภาคจากคำอธิบายตำแหน่งของ USGS</p>
    </div>`);
  regionCard.appendChild(renderRegions(regions));
  const bandCard = el(`
    <div class="card">
      <div class="card-head"><h2>การกระจายตามขนาด</h2></div>
      <p class="card-note">ยิ่งแรงยิ่งพบน้อยลงอย่างรวดเร็ว</p>
    </div>`);
  bandCard.appendChild(renderBands(bands, 'magnitude_band'));
  row1.append(regionCard, bandCard);
  root.appendChild(row1);

  const row2 = el('<div class="grid-2"></div>');
  const strongCard = el(`
    <div class="card">
      <div class="card-head"><h2>10 ครั้งที่แรงที่สุด</h2></div>
      <p class="card-note">เรียงตามขนาดจากมากไปน้อย</p>
    </div>`);
  strongCard.appendChild(renderStrongest(strongest));
  const depthCard = el(`
    <div class="card">
      <div class="card-head"><h2>การกระจายตามความลึก</h2></div>
      <p class="card-note">ส่วนใหญ่เกิดในระดับตื้น</p>
    </div>`);
  depthCard.appendChild(renderBands(depths, 'depth_band'));
  row2.append(strongCard, depthCard);
  root.appendChild(row2);

  document.getElementById('provenance').textContent =
    'แหล่งข้อมูล: USGS Earthquake Hazards Program · สรุปล่าสุดเมื่อ ' +
    new Date(summary.refreshed_at).toLocaleString('th-TH');
}

build();
</script>
</body>
</html>
"""
