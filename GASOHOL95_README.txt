Gasohol 95 ML Pipeline - v3
===========================

จุดประสงค์
- เก็บราคาขายปลีก Gasohol 95 ลง PostgreSQL
- ใช้ราคา 5 วันย้อนหลัง + Moving Average 5 วันเป็น Features
- เทรน RandomForestRegressor
- Holdout 5 samples
- Champion/Challenger ด้วย RMSE
- Deploy โมเดลใหม่เมื่อ RMSE ดีกว่า Champion

แหล่งข้อมูล
1) Money Buffalo: ราคาน้ำมันย้อนหลังล่าสุด (ใช้เป็นแหล่งหลักสำหรับข้อมูลล่าสุด)
2) EPPO: ใช้เติมข้อมูลย้อนหลังเพิ่มเติมจาก archive ของสำนักงานนโยบายและแผนพลังงาน โดยใช้ราคา PTT ของ Gasohol 95

การเปลี่ยนแปลง v3
- MIN_HISTORY_DAYS = 30
- HOLDOUT_SIZE = 5
- MIN_TRAIN_SAMPLES = 20
- Bootstrap จะพยายามเติมข้อมูลจาก EPPO เมื่อ Money Buffalo มีไม่ถึง 30 วัน
- ถ้าข้อมูลไม่ถึง 30 วัน ระบบจะหยุดและไม่ Train โมเดลที่มีข้อมูลน้อยเกินไป
- Train log ต้องมีอย่างน้อยประมาณ train=20+, holdout=5

หมายเหตุ
- ราคา Gasohol 95 จาก Money Buffalo เป็นราคาขายปลีกที่แสดงในตารางย้อนหลัง
- EPPO ใช้ราคา PTT เพื่อเติมช่วงวันที่เก่ากว่า
- ไม่สร้างข้อมูลปลอมเพื่อให้ครบจำนวนวัน

================================================================
COVID-19 Dashboard Pipeline (เพิ่มเติม)
================================================================

จุดประสงค์
- ทำ ETL ข้อมูลผู้ป่วยยืนยันโควิด-19 ของกรมควบคุมโรค (ไฟล์ Excel)
  ลง PostgreSQL แล้วสรุปเป็นแดชบอร์ดกราฟ

ไฟล์ที่เกี่ยวข้อง
- dags/ml_04_covid_dashboard.py   DAG ชื่อ covid_dashboard_pipeline
- model_service/covid.py          router + หน้าเว็บแดชบอร์ด
- docker-compose.yaml             mount โฟลเดอร์ข้อมูล + เพิ่ม openpyxl

แหล่งข้อมูล
- ไฟล์ confirmed-cases*.xlsx 5 ไฟล์ รวม 4,262,484 แถว
  mount เข้าคอนเทนเนอร์ที่ /opt/airflow/data/covid (อ่านอย่างเดียว)
- เปลี่ยนตำแหน่งโฟลเดอร์ได้ด้วยตัวแปร COVID_DATA_DIR เช่น
  COVID_DATA_DIR=D:/Darta docker compose up -d

ขั้นตอนใน DAG
1. check_source_files  ตรวจว่าไฟล์ต้นทางครบ ขาดไฟล์ไหน fail ทันที
2. create_raw_table    สร้างตาราง covid_cases_raw ใหม่ (รันซ้ำได้ไม่ซ้ำข้อมูล)
3. load_raw__*         5 task ขนานกัน อ่าน Excel แบบ streaming (openpyxl
                       read_only) แล้ว INSERT ทีละ 50,000 แถว ไม่โหลดทั้ง
                       ไฟล์เข้าหน่วยความจำ
4. clean_transform     แปลง Excel serial เป็นวันที่, normalize เพศ,
                       รวมสัญชาติที่สะกดต่างกัน (Thai/Thailand,
                       Burmese/Burma), ตัดอายุนอกช่วง 0-120,
                       ตัดเคสซ้ำด้วย case_no -> ตาราง covid_cases
5. build_aggregates    สร้างตารางสรุป covid_daily, covid_by_province,
                       covid_by_age_group, covid_by_sex,
                       covid_by_nationality, covid_summary
6. data_quality_check  ตรวจแถวว่าง, case_no ซ้ำ, วันที่ null, อายุนอกช่วง
                       ไม่ผ่านข้อใด DAG fail
7. log_result          สรุปผลการรันลง log

หมายเหตุผลการรันจริง
- ทั้ง 5 ไฟล์ไม่มี case_no ซ้ำกันเลย จำนวนแถวหลังทำความสะอาดจึงเท่ากับ
  แถวดิบพอดี (4,262,484) ขั้นตัดซ้ำยังคงไว้เพื่อกันข้อมูลชุดใหม่ที่อาจทับกัน
- ช่วงข้อมูล 12 ม.ค. 2563 ถึง 1 พ.ค. 2565 รวม 785 วัน
- โหลดครบทั้ง 4.26 ล้านแถวใช้เวลาประมาณ 3 นาที

วิธีรัน
1) docker compose up -d
2) ตั้ง Airflow Connection ชื่อ postgres_target (ทำครั้งเดียว):
   docker exec airflow_webserver airflow connections add postgres_target \
     --conn-type postgres --conn-host postgres_target --conn-port 5432 \
     --conn-schema etl_db --conn-login etluser --conn-password etlpass
3) เปิด http://localhost:8080 (airflow/airflow) unpause แล้วสั่งรัน
   covid_dashboard_pipeline
4) เปิดแดชบอร์ดที่ http://localhost:8001/covid

Endpoint ที่หน้าเว็บเรียกใช้
- GET /api/covid/summary         ตัวเลขสรุปรวม
- GET /api/covid/daily           เคสรายวัน + ค่าเฉลี่ย 7 วัน
- GET /api/covid/provinces       จังหวัดเรียงตามจำนวน
- GET /api/covid/age-groups      กลุ่มอายุ
- GET /api/covid/sex             สัดส่วนเพศ
- GET /api/covid/nationalities   สัญชาติ

----------------------------------------------------------------
ไฟล์สำหรับ Power BI
----------------------------------------------------------------

task export_for_powerbi ใน DAG จะเขียนไฟล์ CSV ลงโฟลเดอร์ exports/
ทุกครั้งที่ pipeline รันสำเร็จ ทุกไฟล์เป็น UTF-8 พร้อม BOM จึงเปิดภาษาไทย
ได้ถูกต้องโดยไม่ต้องเลือก encoding เอง

ไฟล์ที่ได้
- fact_covid_cases.csv   450,336 แถว (29 MB) ตารางข้อเท็จจริง
                         ระดับ วัน x จังหวัด x เพศ x ช่วงอายุ x สัญชาติ
                         คอลัมน์วัดคือ "จำนวนผู้ป่วย" รวมได้ 4,262,484
- dim_date.csv           841 แถว มิติวันที่ มีปี พ.ศ./ค.ศ., เดือนไทย,
                         ไตรมาส, วันในสัปดาห์, วันหยุดสุดสัปดาห์
- dim_province.csv       80 แถว มิติจังหวัด พร้อมอันดับ
- dim_age_group.csv      9 แถว มิติช่วงอายุ พร้อมคอลัมน์ "ลำดับ"
- summary_daily.csv      785 แถว เคสรายวัน + ค่าเฉลี่ย 7 วัน
- summary_overall.csv    1 แถว ตัวเลขสรุปรวม

ขั้นตอนใน Power BI Desktop
1) Get Data > Text/CSV เลือกไฟล์ทีละไฟล์ 6 รอบ
   (ห้ามใช้ Get Data > Folder กับโฟลเดอร์นี้ เพราะไฟล์คนละโครงสร้างกัน
   การกด Combine จะรวมเป็นตารางเดียวที่ใช้ไม่ได้)
2) ตรวจที่ Transform Data ว่าคอลัมน์ "วันที่" เป็นชนิด Date และ
   "จำนวนผู้ป่วย" เป็น Whole Number
3) Model view: ลากสร้างความสัมพันธ์แบบ one-to-many
     dim_date["วันที่"]        -> fact_covid_cases["วันที่"]
     dim_province["จังหวัด"]   -> fact_covid_cases["จังหวัด"]
     dim_age_group["ช่วงอายุ"] -> fact_covid_cases["ช่วงอายุ"]
4) เลือกตาราง dim_age_group คอลัมน์ "ช่วงอายุ" แล้วกด
   Column tools > Sort by column > "ลำดับ"
   (ถ้าไม่ทำ แกนกราฟจะเรียง 0-9, 10-19, 70+ ตามตัวอักษร ซึ่งผิดลำดับ)
5) ทำ measure พื้นฐาน
     ผู้ป่วยสะสม = SUM(fact_covid_cases[จำนวนผู้ป่วย])
     เฉลี่ย 7 วัน = AVERAGEX(DATESINPERIOD(dim_date[วันที่],
                     MAX(dim_date[วันที่]), -7, DAY), [ผู้ป่วยสะสม])

ถ้าอยากต่อฐานข้อมูลสดแทนการใช้ไฟล์
Get Data > PostgreSQL database
  Server   : localhost:5433
  Database : etl_db
  Username : etluser
  Password : etlpass
แล้วเลือกตาราง covid_cases หรือตารางสรุป covid_* ได้โดยตรง

----------------------------------------------------------------
ไฟล์โปรเจกต์ Power BI สำเร็จรูป (powerbi/)
----------------------------------------------------------------

เปิดไฟล์ powerbi/COVID19-Thailand.pbip ด้วย Power BI Desktop
ได้โมเดลและหน้ารายงานครบโดยไม่ต้องตั้งค่าเอง

สิ่งที่มีมาให้แล้ว
- ตาราง 4 ตาราง (fact_covid_cases, dim_date, dim_province, dim_age_group)
  ดึงจากไฟล์ CSV ในโฟลเดอร์ exports/
- ความสัมพันธ์ครบ 3 เส้น fact -> dim ทั้งสาม
- dim_age_group["ช่วงอายุ"] ตั้ง Sort by column เป็น "ลำดับ" ไว้แล้ว
  แกนกราฟจึงเรียง 0-9, 10-19, ... 70+ ถูกต้องตั้งแต่แรก
- dim_date["เดือน"] ตั้ง Sort by column เป็น "เลขเดือน" ไว้แล้ว
- measure 4 ตัว: ผู้ป่วยสะสม, เฉลี่ย 7 วัน, จำนวนจังหวัด, วันที่พบสูงสุด
- หน้ารายงาน "ภาพรวมโควิด-19" มี 8 visual
  (ตัวกรองปี พ.ศ., การ์ด 3 ใบ, กราฟเส้นรายวัน, แท่งจังหวัด,
   แท่งช่วงอายุ, โดนัทสัดส่วนเพศ)

สิ่งที่ต้องทำเองหนึ่งครั้งหลังเปิด
- กด Refresh now บนแถบแจ้งเตือนด้านบน หรือ Home > Refresh
  รูปแบบ .pbip เก็บแต่โครงสร้าง ไม่เก็บข้อมูล จึงต้องโหลด CSV
  เข้ามาครั้งแรกเสมอ (ประมาณ 1-2 นาที สำหรับ 450,336 แถว)
- ถ้าอยากได้ไฟล์ที่เปิดแล้วมีข้อมูลเลย ให้ File > Save as
  แล้วเลือกนามสกุล .pbix หลังจาก refresh เสร็จ

ถ้าย้ายโฟลเดอร์โปรเจกต์
พาธของไฟล์ CSV เขียนไว้แบบเต็มใน powerbi/COVID19-Thailand.Dataset/model.bim
(D:\data1\95-\exports\...) ถ้าย้ายที่เก็บข้อมูล ต้องแก้พาธในไฟล์นั้น
หรือแก้ใน Power BI ที่ Transform data > Data source settings

ข้อควรระวังที่เจอมาแล้ว
- ห้ามใช้ Get Data > Folder กับโฟลเดอร์ exports เพราะไฟล์ทั้ง 6
  คนละโครงสร้าง การกด Combine จะได้ตารางพัง ถ้าจะโหลดเองให้ใช้
  Get Data > Text/CSV ทีละไฟล์
- ชื่อตัวแปร VAR ใน DAX ใช้ภาษาไทยไม่ได้ ต้องเป็นอักษรอังกฤษ

================================================================
USGS Earthquake Pipeline (ดึงข้อมูลผ่าน API)
================================================================

ต่างจาก pipeline โควิดตรงที่แหล่งข้อมูลเป็น REST API ไม่ใช่ไฟล์
จึงทำเป็น incremental ETL เต็มรูปแบบ

ไฟล์ที่เกี่ยวข้อง
- dags/ml_05_usgs_quake_ingest.py   DAG ดึงข้อมูล (usgs_quake_ingest)
- dags/ml_06_usgs_quake_marts.py    DAG สร้างตารางสรุป (usgs_quake_marts)
- model_service/quakes.py           หน้าแดชบอร์ด /quakes + API

แหล่งข้อมูล
- USGS FDSN Event API
  https://earthquake.usgs.gov/fdsnws/event/1/query
- ไม่ต้องใช้ API key ไม่มีค่าใช้จ่าย

จุดที่ต้องออกแบบรับข้อจำกัดของ API
- ขอข้อมูลช่วงยาวทีเดียวไม่ได้ เซิร์ฟเวอร์ตอบ 503/504 (ทดสอบแล้ว)
  จึงยิงทีละเดือน เดือนหนึ่งมีราว 6,500-16,500 เหตุการณ์
- หนึ่ง request ได้สูงสุด 20,000 แถว จึงมีการแบ่งหน้าด้วย limit/offset
- มี retry พร้อม backoff เพราะเซิร์ฟเวอร์ล้าเป็นช่วง ๆ

สิ่งที่โชว์ความสามารถของ Airflow
- schedule="@monthly" + catchup=True ให้ Airflow ไล่ backfill ย้อนหลังเอง
  141 DAG run ตั้งแต่ ม.ค. 2015 ถึงปัจจุบัน สำเร็จทั้งหมด
- แต่ละ run รับผิดชอบเฉพาะเดือนของตัวเองผ่าน data_interval
- เขียนแบบ idempotent (ลบช่วงเวลานั้นก่อนแล้วใส่ใหม่) รันซ้ำได้ไม่ซ้ำข้อมูล
- max_active_runs=3 จำกัดไม่ให้ยิง API พร้อมกันเกินไป

ผลการรันจริง
- 1,794,431 เหตุการณ์ ครอบคลุม 141 เดือนต่อเนื่อง
- แรงที่สุด M8.8 คัมชัตคา รัสเซีย เมื่อ 29 ก.ค. 2025
- ตั้งแต่ M6 ขึ้นไป 1,579 ครั้ง (0.09% ของทั้งหมด)
- ภูมิภาคที่พบมากสุด California 557,783 ครั้ง
- ความลึกเฉลี่ย 23.9 กม.

ปัญหาข้อมูลจริงที่ด่านตรวจจับได้และแก้แล้ว
- USGS ใส่ค่า sentinel -9.99 และ -5 แทน "ไม่มีค่าขนาด" ไม่ใช่ขนาดจริง
  แปลงเป็น NULL ส่วนค่าราว -2 เป็นแผ่นดินไหวจิ๋วที่วัดได้จริง เก็บไว้
- ชื่อรัฐเขียนทั้งแบบย่อและเต็มปนกัน (CA กับ California) ทำให้นับแยกกัน
  รวมชื่อด้วยตาราง REGION_ALIASES ก่อนสรุป
- ตอนทดสอบด้วย airflow dags test ไปจอง run slot ของเดือน ก.ค. 2025 ไว้
  ทำให้เดือนนั้นไม่เคยถูกดึง ด่านตรวจ "เดือนที่ขาด" จับได้ แก้โดยลบ run นั้น
  แล้วสร้างใหม่ผ่าน REST API พร้อมระบุ data_interval ให้ชัดเจน

วิธีรัน
1) docker compose up -d
2) เปิด http://localhost:8080 unpause DAG usgs_quake_ingest
   Airflow จะไล่ backfill เองจนครบทุกเดือน (ใช้เวลาราว 10 นาที)
3) สั่งรัน usgs_quake_marts หนึ่งครั้งเพื่อสร้างตารางสรุปและไฟล์ CSV
4) เปิดแดชบอร์ดที่ http://localhost:8001/quakes

ไฟล์ CSV สำหรับ Power BI อยู่ที่ exports/quakes/
- fact_quakes.csv         49,188 แถว ระดับ เดือน x ภูมิภาค x ช่วงขนาด x ช่วงความลึก
- dim_month.csv           141 แถว
- dim_region.csv          625 แถว
- dim_magnitude_band.csv  8 แถว (มีคอลัมน์ลำดับให้ตั้ง Sort by column)
- dim_depth_band.csv      3 แถว
- summary_monthly.csv     141 แถว
- strongest_events.csv    20 แถว

Endpoint ที่หน้าเว็บเรียกใช้
- GET /api/quakes/summary          ตัวเลขสรุปรวม
- GET /api/quakes/monthly          เหตุการณ์รายเดือน
- GET /api/quakes/magnitude-bands  การกระจายตามขนาด
- GET /api/quakes/depth-bands      การกระจายตามความลึก
- GET /api/quakes/regions          ภูมิภาคเรียงตามจำนวน
- GET /api/quakes/strongest        ครั้งที่แรงที่สุด
- GET /api/quakes/map-points       จุดสำหรับวาดแผนที่ (M6 ขึ้นไป)
