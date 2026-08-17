# 📋 CHECKLIST THỰC HIỆN LAB 17 — DATA PIPELINE ENGINEERING

**Học viên:** Nguyễn Văn Hùng | **Mã học viên:** 2A202601284 | **Lớp:** AICB-P2T2 | **Ngày:** 17/08/2026

---

## 🛠️ PHẦN 0: CHUẨN BỊ MÔI TRƯỜNG & KHỞI TẠO

- [x] **Kiểm tra môi trường local**: Python 3.11+ và `make`.
- [x] **Khởi tạo môi trường & seed data**: Chạy `make setup` *(tạo `.venv`, cài đặt `duckdb`, `dbt-core`, `dbt-duckdb`, sinh 14 ngày dữ liệu seed)*.
- [x] **Kiểm tra đường ống & bảng điểm ban đầu**: Chạy `make pipeline` rồi `make verify` *(xác nhận trạng thái ban đầu 1/4 tiêu chí đạt)*.

---

## 🎯 PHẦN 1: BA NHIỆM VỤ CHÍNH (100 ĐIỂM)

### 1️⃣ Nhiệm vụ 1: Sửa lỗi `gold_training_set` tăng số hàng sau mỗi lần chạy (30 điểm)
> **Bản chất lỗi:** Model incremental thiếu `unique_key` và `incremental_strategy`, khiến dbt mặc định dùng câu lệnh `INSERT` thay vì `MERGE`. Khi Airflow trigger chạy lại hoặc Clear Task, các bản ghi cũ không được ghi đè mà bị nhân bản.

- [x] **Chẩn đoán & khảo sát:**
  - [x] Chạy `make pipeline` hai lần liên tiếp và kiểm tra số hàng bằng query SQL `select count(*) from gold_training_set`.
  - [x] Đọc khối chú thích `KHUNG THỰC HIỆN` và cấu hình `config()` trong file `dbt/models/gold/gold_training_set.sql`.
  - [x] Mở file `dags/ai_training_pipeline.py` để xem hai tham số Airflow DAG (`catchup`, `max_active_runs`).
- [x] **Sửa code:**
  - [x] **File 1:** `dbt/models/gold/gold_training_set.sql`:
    - Cập nhật khối `config()`: Thêm `unique_key='ticket_id'` và `incremental_strategy='merge'`.
  - [x] **File 2:** `dags/ai_training_pipeline.py`:
    - Cấu hình lại hai tham số: `catchup=False` và `max_active_runs=1`.
- [x] **Nghiệm thu Nhiệm vụ 1 (`make verify`):**
  - [x] Bảng `gold_training_set` đạt trạng thái `ỔN ĐỊNH ✓` qua cả 3 lượt chạy (Checksum: `8622572a97`).
  - [x] Số hàng khớp chính xác với `expected/gold_training_set.count` (**12,480** hàng).
  - [x] Dòng kiểm tra `gold_training_set: 1 hàng / 1 ticket` đạt `✓ không lặp`.
  - [x] Dòng kiểm tra DAG `DAG: catchup / max_active_runs` đạt `✓ False / 1`.

---

### 2️⃣ Nhiệm vụ 2: Sửa lỗi `gold_feature_daily` thiếu hàng ở các ngày quá khứ (30 điểm)
> **Bản chất lỗi:** Dữ liệu sự kiện có độ trễ do mạng/client (`_ingested_at > event_time`), nhưng mệnh đề lọc incremental chỉ lấy `event_date > (select max(event_date) from {{ this }})` nên bỏ sót toàn bộ các sự kiện thuộc ngày cũ tới kho muộn.

- [x] **Chẩn đoán & đo đạc:**
  - [x] Chạy query đo phân bố độ trễ giữa `_ingested_at` và `event_time` trong `bronze_events` để xác định percentile **P99** (kết quả đo: **P99 = 2.73 ngày**, max = 2.94 ngày, tỷ lệ late = 5.05%).
  - [x] Ghi lại con số **P99** này vào báo cáo làm căn cứ định lượng để thiết lập Lookback Window (chọn Lookback = 3 ngày).
  - [x] Đọc khối `KHUNG THỰC HIỆN` trong file `dbt/models/gold/gold_feature_daily.sql`.
- [x] **Sửa code:**
  - [x] **File:** `dbt/models/gold/gold_feature_daily.sql`:
    - Cập nhật khối `config()`: Thêm `unique_key=['event_date', 'customer_id']` và `incremental_strategy='merge'`.
    - Mở rộng mệnh đề lọc trong khối `{% if is_incremental() %}` lùi lại Lookback Window 3 ngày: `where event_date >= (select max(event_date) - interval '3 day' from {{ this }})`.
- [x] **Nghiệm thu Nhiệm vụ 2 (`make verify`):**
  - [x] Bảng `gold_feature_daily` đạt trạng thái `ỔN ĐỊNH ✓` qua cả 3 lượt chạy (Checksum: `3db448685c`).
  - [x] Số hàng khớp chính xác với `expected/gold_feature_daily.count` (**9,100** hàng = 14 ngày × 650 customers).
  - [x] `gold_training_set` (Nhiệm vụ 1) vẫn giữ nguyên 12,480 hàng và đạt `ỔN ĐỊNH ✓`.

---

### 3️⃣ Nhiệm vụ 3: Sửa lỗi Data Contract & Xử lý kiểu dữ liệu cột `priority` (20 điểm)
> **Bản chất lỗi:** Backend đổi format cột `priority` từ số sang chuỗi ('urgent', 'high', 'medium', 'low') từ ngày 08-10 và xuất hiện rác (`0`, `5`, `-1`, `P1`, `unknown`, `null`). Cần chuẩn hóa nhãn chuỗi về số nguyên 1..4 và tách các bản ghi lỗi thật vào bảng quarantine thay vì làm crash pipeline.

- [x] **Chẩn đoán & phân loại dữ liệu:**
  - [x] Query `priority_raw` trong `bronze_tickets_cdc` để phân loại 3 nhóm:
    1. *Số hợp lệ (1..4)*: Giữ nguyên (1, 2, 3, 4).
    2. *Nhãn chuỗi hợp lệ ('urgent'->1, 'high'->2, 'medium'->3, 'low'->4)*: Map về 1..4.
    3. *Không hợp lệ/rác*: 312 bản ghi ('0', '', 'unknown', 'P1', 'P2', '5', null, '-1') -> Trả về `NULL` để đưa vào Quarantine.
- [x] **Sửa code tại 4 file:**
  - [x] **File 1:** `dbt/macros/normalize_priority.sql`:
    - Dùng khối `CASE` để chuẩn hóa 3 nhóm trên, trả về `NULL` cho nhóm không hợp lệ; bổ sung `priority_reject_reason`.
  - [x] **File 2:** `dbt/models/silver/silver_tickets.sql`:
    - Lọc bỏ các bản ghi mà macro trả về `NULL` **trước khi** thực hiện xếp hạng `row_number()`, đảm bảo chỉ loại bỏ bản ghi lỗi mà không làm mất ticket hợp lệ từ lần cập nhật trước.
  - [x] **File 3:** `dbt/models/silver/quarantine_tickets.sql`:
    - Thay mệnh đề `WHERE false` bằng điều kiện lọc lấy các bản ghi có macro trả về `NULL`.
  - [x] **File 4:** `dbt/models/silver/schema.yml`:
    - Đổi `enforced: false` thành `enforced: true` ở phần contract của `silver_tickets`.
    - Bỏ comment khối `tests:` của cột `priority` và định nghĩa kiểm tra `not_null` cùng `accepted_values` (miền 1..4).
- [x] **Nghiệm thu Nhiệm vụ 3 (`make verify`):**
  - [x] `dbt test` pass với **11/11 pass** (thêm test mới theo yêu cầu).
  - [x] `quarantine_tickets` đúng **312 / 312** hàng và đạt `ỔN ĐỊNH ✓` (Checksum: `ebb89036fb`).
  - [x] Dòng kiểm tra `silver_tickets.priority ∈ 1..4, không NULL` đạt `✓ sạch`.
  - [x] `silver_tickets` giữ đủ **12,480** tickets hợp lệ.
  - [x] Bảng điểm `make verify` đạt **4/4 tiêu chí đạt**.

---

## 🚀 PHẦN 2: BÀI MỞ RỘNG (TÙY CHỌN — MAX +10 ĐIỂM THƯỞNG)

- [x] **Khởi tạo dữ liệu bài mở rộng:** Chạy `make seed-extra` *(tạo 5.000 file parquet nhỏ trong `data/gold_events/`)*.

### 🅰️ Bài A: Tối ưu Query Dashboard chậm (+5 điểm)
- [x] Chạy `make explain` và `make plan` để đo thông số trước khi tối ưu (`rows scanned` = 5,000,000, `files` = 5,000, thời gian ~44s).
- [x] Sửa file `tools/compact.py`: Viết lệnh `COPY ... TO ...` để gộp 5.000 file Parquet nhỏ thành 14 file partitioned theo Hive layout (`event_date`), `ORDER BY customer_name, event_time`, `ROW_GROUP_SIZE 1000`.
- [x] Sửa file `queries/dashboard.sql`: Trỏ vào thư mục dữ liệu mới `data/gold_events_v2/*/*.parquet`, bật `hive_partitioning=true`, viết lại filter dạng sargable (`event_date = '2026-08-09'`).
- [x] Chạy `make compact` và `make explain`.
- [x] **Nghiệm thu Bài A:** `rows scanned` giảm từ 5,000,000 xuống **9,324 (giảm 536.3×, vượt xa mục tiêu ≥ 10×)**, số file giảm từ 5,000 xuống **14**, `result hash` giữ nguyên `4379e4c5d9f3`, thời gian chạy giảm từ ~44s xuống **~16ms**.

### 🅱️ Bài B: Xử lý Consumer Crash & Delivery Semantics (+5 điểm)
- [x] Chạy `make crash-test` để xem lỗi khi consumer bị kill giữa batch (mất dữ liệu do commit offset trước khi ghi).
- [x] Sửa file `ingest/consumer.py`:
  - Đổi thứ tự xử lý thành At-Least-Once: Ghi batch trước (`write_batch`), commit offset sau (`consumer.commit()`).
  - Dùng câu lệnh ghi idempotent: `INSERT INTO ... ON CONFLICT (event_id) DO UPDATE SET ...` với Primary Key trên `event_id`.
- [x] **Nghiệm thu Bài B:** Chạy `make crash-test` báo `BÀI MỞ RỘNG B: ĐẠT ✓` (không mất bản ghi, không trùng bản ghi, C == A = 20,000).

---

## 📝 PHẦN 3: VIẾT BÁO CÁO & NỘP BÀI

- [x] **Hoàn thiện file `REPORT_TEMPLATE.md` / `REPORT.md`:**
  - [x] **Mục 0:** Chạy `make verify` và dán nguyên văn kết quả 3 lượt chạy (4/4 tiêu chí đạt, 11/11 dbt test pass).
  - [x] **Mục 1:** Ghi Triệu chứng → **Nguyên nhân gốc rễ (cơ chế dbt incremental)** → Cách khắc phục → Bằng chứng.
  - [x] **Mục 2:** Ghi Triệu chứng → **Giá trị P99 độ trễ đo được (2.73 ngày)** → Lý do chọn Lookback Window (3 ngày) → **Nguyên nhân gốc rễ** → Cách khắc phục → Bằng chứng. Trả lời câu hỏi so sánh chi phí giữa P99 và Max.
  - [x] **Mục 3:** Ghi Triệu chứng → **Nguyên nhân gốc rễ** → Bảng 3 nhóm `priority` & cách xử lý → Cách khắc phục → Bằng chứng. Trả lời câu hỏi thiết kế (Bronze vs Silver, vì sao không dừng pipeline).
  - [x] **Mục 4:** Trình bày chi tiết cả 2 bài mở rộng A (Compaction + Sargable Query) và B (Crash-safe Idempotent Consumer).
  - [x] **Mục 5:** Điền bảng tổng kết kinh nghiệm thực tế khi tiếp nhận hệ thống Data Pipeline.
- [x] **Dọn dẹp trước khi nộp:**
  - [x] Kiểm tra kho và đảm bảo các file mã nguồn sạch sẽ, comment rõ ràng.
- [x] **Kiểm tra cuối cùng:** Xác nhận toàn bộ quy trình chạy sạch và pass 100% (110/110 điểm).
