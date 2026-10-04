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
