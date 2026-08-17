# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Văn Hưng  **Lớp:** AICB-P2T2  **Mã học viên:** 2A202601284  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details open>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 32.2s
  run 2/3 … 31.9s
  run 3/3 … 31.4s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt** (100% tiêu chí chính + Đạt trọn vẹn cả 2 bài mở rộng A và B)

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| Mục | Chi tiết |
|---|---|
| **Triệu chứng** | Mỗi lần `dbt run` chạy lại (hoặc pipeline được kích hoạt lại), số hàng trong bảng `gold_training_set` tăng thêm đúng 12,480 hàng (12,480 $\rightarrow$ 24,960 $\rightarrow$ 37,440...). Đồng thời DAG Airflow kích hoạt backfill đồng thời nhiều run song song. |
| **Nguyên nhân** | Model `dbt/models/gold/gold_training_set.sql` được cấu hình `materialized = 'incremental'` nhưng **thiếu `unique_key`**. Khi không có `unique_key`, dbt mặc định sử dụng chiến lược `append` (tương đương `INSERT INTO`), dẫn đến mỗi lần chạy sẽ chèn toàn bộ dữ liệu mới tính được vào bảng thay vì upsert/merge. Ngoài ra, DAG Airflow `ai_training_pipeline.py` để `catchup = True` và không giới hạn concurrency khiến các lần chạy quá khứ đồng loạt thực thi và chèn lặp dữ liệu. |
| **Cách khắc phục** | 1. Trong `dbt/models/gold/gold_training_set.sql`: Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`.<br>2. Trong `dags/ai_training_pipeline.py`: Cập nhật `catchup = False` và `max_active_runs = 1`. |
| **Bằng chứng** | trước: 37,440 hàng (tăng sau mỗi lần chạy) · sau: 12,480 hàng cố định · checksum 3 lượt: `8dd7c98653` (khớp 100%, không lặp hàng). |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| Mục | Chi tiết |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` bị thiếu hàng ở các ngày trong quá khứ (chỉ có 8,640 hàng thay vì 9,100 hàng kỳ vọng). |
| **P99 độ trễ đo được** | **2.73 ngày** *(Phân phối độ trễ: P50 = 0.13 ngày, P95 = 1.81 ngày, P99 = 2.73 ngày, Max = 2.94 ngày; 5.05% bản ghi có độ trễ > 0 ngày)* |
| **Lookback đã chọn** | **3 ngày** — vì độ trễ P99 đo được là 2.73 ngày (và Max là 2.94 ngày), làm tròn lên 3 ngày (`interval '3 day'`) sẽ bao phủ toàn bộ 100% dữ liệu về muộn (late-arriving data) mà chỉ cần scan lại một khoảng dữ liệu nhỏ, tối ưu chi phí tính toán. |
| **Nguyên nhân** | Dữ liệu sự kiện client gửi về bị trễ do độ trễ mạng hoặc thiết bị offline (late-arriving data). Khi dbt chạy incremental với bộ lọc mặc định `where event_time > (select max(event_time) from {{ this }})`, dbt chỉ lấy các sự kiện có mốc thời gian lớn hơn thời điểm mới nhất đã xử lý. Những sự kiện xảy ra ở các ngày trước nhưng được ingest muộn sẽ bị bỏ qua vĩnh viễn (data loss). |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_feature_daily.sql`: Thêm `unique_key = ['event_date', 'customer_id']`, `incremental_strategy = 'merge'` và mở rộng điều kiện lọc lookback: `where event_date >= (select max(event_date) - interval '3 day' from {{ this }})`. |
| **Bằng chứng** | trước: 8,640 hàng · sau: 9,100 hàng (đủ 100% kỳ vọng) · checksum 3 lượt: `3db448685c`. |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> **Trả lời:**
> 1. **Bản chất thống kê:** Phân vị P99 phản ánh biên trên thực tế của 99% luồng dữ liệu bình thường, loại bỏ các giá trị ngoại lai (outliers) cá biệt như một thiết bị mất mạng 6 tháng rồi mới đồng bộ.
> 2. **Chi phí tính toán (Compute / I/O Cost):**
>    - Nếu chọn `max`: Giả sử có 1 bản ghi về muộn 180 ngày, mỗi chu kỳ incremental hàng ngày pipeline sẽ bị buộc phải scan và tính toán lại toàn bộ 180 ngày dữ liệu trong quá khứ $\rightarrow$ chi phí compute, I/O và thời gian chạy tăng vọt gấp hàng trăm lần chỉ để xử lý 0.01% dữ liệu.
>    - Nếu chọn `P99` (kết hợp làm tròn hợp lý lên 3 ngày): Pipeline chỉ scan lại đúng 3 ngày gần nhất, giữ chi phí compute ổn định và tối thiểu ở mức O(1). Đối với 1% bản ghi cá biệt trễ vượt quá P99, giải pháp chuẩn trong kỹ nghệ dữ liệu là xử lý thông qua một job đối soát định kỳ (weekly/monthly reconciliation backfill batch job).

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| Mục | Chi tiết |
|---|---|
| **Triệu chứng** | Pipeline bị crash khi dbt contract kiểm tra kiểu dữ liệu của `priority`. Dữ liệu nguồn `bronze_tickets_cdc` bị thay đổi schema và lẫn dữ liệu không hợp lệ. |
| **Nguyên nhân** | Hiện tượng Schema Drift / Schema Evolution từ hệ thống upstream. Backend chuyển đổi kiểu dữ liệu `priority` từ số sang chuỗi text (`'urgent'`, `'high'`, `'medium'`, `'low'`) từ ngày 08-10, đồng thời xuất hiện các giá trị rác (`'0'`, `'5'`, `'-1'`, `'P1'`, `'P2'`, `'unknown'`, `''`, `null`). |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **1. Số hợp lệ (1..4):** Giữ nguyên giá trị số (1, 2, 3, 4) — 6,846 bản ghi.<br>**2. Nhãn chuỗi hợp lệ ('urgent' $\rightarrow$ 1, 'high' $\rightarrow$ 2, 'medium' $\rightarrow$ 3, 'low' $\rightarrow$ 4):** Map về số nguyên 1..4 — 7,142 bản ghi.<br>**3. Không hợp lệ/rác ('0', '', 'unknown', 'P1', 'P2', '5', null, '-1'):** 312 bản ghi $\rightarrow$ Trả về `NULL` và đưa vào bảng `quarantine_tickets` kèm `reject_reason`. |
| **Cách khắc phục** | 1. `dbt/macros/normalize_priority.sql`: Macro chuẩn hóa nhãn chuỗi về 1..4, trả về NULL cho dữ liệu rác.<br>2. `dbt/models/silver/silver_tickets.sql`: Lọc `where priority_clean is not null` trước khi `row_number()` để chỉ loại bỏ bản ghi CDC lỗi mà vẫn giữ nguyên ticket hợp lệ từ lần cập nhật trước (đủ 12,480 tickets).<br>3. `dbt/models/silver/quarantine_tickets.sql`: Thu gom đúng 312 bản ghi lỗi.<br>4. `dbt/models/silver/schema.yml`: Bật `contract: enforced: true`, bật test `not_null` và `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 / 312 hàng (checksum: `ebb89036fb`) · `dbt test` = 11/11 pass · `silver_tickets.priority` sạch 1..4 không NULL · `silver_tickets` đủ 12,480 tickets. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> **Trả lời:**
> 1. **Nên chặn ở tầng Bronze hay Silver?**
>    - **Tầng Bronze (Raw Layer):** Cần giữ nguyên vẹn 100% dữ liệu thô (raw payload) từ nguồn mà không thực hiện validate hay loại bỏ. Tầng Bronze đóng vai trò là "Single Source of Truth" bất biến (immutable data lake) để phục vụ replay, audit, điều tra và tái xử lý khi logic nghiệp vụ thay đổi.
>    - **Tầng Silver (Cleaned Layer):** Là nơi thích hợp nhất để áp dụng Data Contract, kiểm tra schema, chuẩn hóa kiểu dữ liệu và định tuyến bản ghi lỗi sang bảng Quarantine.
> 2. **Vì sao KHÔNG để pipeline dừng khi gặp bản ghi lỗi?**
>    - Tỉ lệ dữ liệu lỗi thường rất nhỏ (trong bài là 312/14,300 $\approx$ 2.18%). Nếu để pipeline dừng (crash), toàn bộ các tiến trình downstream quan trọng (dashboard điều hành, huấn luyện mô hình AI, báo cáo doanh thu) sẽ bị tê liệt hoàn toàn, vi phạm SLA hệ thống.
>    - **Mô hình Dead-Letter Queue / Quarantine Pattern:** Bằng cách cách ly các bản ghi lỗi sang bảng riêng (`quarantine_tickets`) và gửi cảnh báo (alert) cho đội Data Quality/Backend kiểm tra, pipeline chính vẫn tiếp tục vận hành thông suốt và an toàn cho 98% dữ liệu hợp lệ còn lại.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| Mục | Chi tiết |
|---|---|
| **Bài đã làm** | **Cả 2 bài: Bài A (Tối ưu Dashboard Parquet) & Bài B (Consumer Crash-safe Idempotency)** |
| **Nguyên nhân** | **Bài A:** Thư mục `data/gold_events/` có 5.000 file Parquet nhỏ không partition (small-file problem), truy vấn lọc không sargable (`strftime(event_time, '%Y-%m-%d') = '2026-08-09'`) khiến DuckDB phải quét toàn bộ 5,000,000 hàng, mất ~44s.<br>**Bài B:** Consumer ban đầu commit offset trước khi ghi dữ liệu xuống database (At-Most-Once). Khi tiến trình bị `kill -9` giữa chừng, offset đã bị tăng nhưng dữ liệu chưa được ghi, dẫn đến mất mát 500 bản ghi. |
| **Cách khắc phục** | **Bài A:**<br>1. Trong `tools/compact.py`: Dùng `COPY (select * from read_parquet('data/gold_events/*.parquet') order by customer_name, event_time) TO 'data/gold_events_v2' (format parquet, partition_by (event_date), row_group_size 1000)`.<br>2. Trong `queries/dashboard.sql`: Trỏ vào `data/gold_events_v2/*/*.parquet` với `hive_partitioning=true`, viết lại predicate sargable `event_date = '2026-08-09'`.<br>**Bài B:**<br>1. Trong `ingest/consumer.py`: Đổi thứ tự xử lý thành At-Least-Once (ghi database `write_batch` trước, commit offset sau).<br>2. Bổ sung `PRIMARY KEY (event_id)` vào DDL và thực hiện ghi Idempotent bằng câu lệnh `INSERT INTO ... ON CONFLICT (event_id) DO UPDATE SET ...` với multi-row VALUES để đảm bảo hiệu năng cao và không trùng lặp khi batch bị replay sau crash. |
| **Bằng chứng** | **Bài A:**<br>- Số file Parquet giảm từ 5,000 xuống **14 file**.<br>- `rows scanned` giảm từ 5,000,000 xuống **9,324 (giảm 536.3×, vượt xa mục tiêu $\ge$ 10×)**.<br>- `result hash` giữ nguyên: `4379e4c5d9f3`.<br>- Thời gian truy vấn giảm từ ~44,000 ms xuống **~16 ms**.<br>**Bài B (`make crash-test`):**<br>- Không mất bản ghi (`✓`).<br>- Không trùng bản ghi (`✓`).<br>- $C == A = 20,000$ hàng (`✓`).<br>- Báo: `BÀI MỞ RỘNG B: ĐẠT ✓`. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| **1** | Kiểm tra cấu hình `materialized = 'incremental'` trong dbt có khai báo đầy đủ `unique_key` và chiến lược `incremental_strategy = 'merge'` hay không; đồng thời kiểm tra cấu hình `catchup` và `max_active_runs` trong Airflow DAG để đảm bảo tính Idempotency khi chạy lại hoặc retry. |
| **2** | Phân tích phân phối thời gian trễ của dữ liệu nguồn (P50, P95, P99 latency) và kiểm tra xem điều kiện incremental filter có chứa Lookback Window đủ rộng hay không để tránh thất thoát dữ liệu đến muộn (late-arriving data). |
| **3** | Kiểm tra xem hệ thống có Data Contract (schema enforcement) và cơ chế Quarantine / Dead-Letter Queue để cô lập dữ liệu lỗi hay không, tránh để pipeline crash hoặc để lọt dữ liệu bẩn xuống downstream. |
| **Mở rộng A** | Kiểm tra kích thước và số lượng file Parquet trên Data Lake (tránh small-file problem), cấu hình partition layout hợp lý theo tần suất query, và đảm bảo các mệnh đề `WHERE` được viết dưới dạng Sargable để tận dụng tối đa File Pruning & Min/Max Row Group Statistics. |
| **Mở rộng B** | Xác minh thứ tự commit offset và thao tác ghi cơ sở dữ liệu của Consumer, đảm bảo áp dụng At-Least-Once kết hợp Idempotent Write (`ON CONFLICT DO UPDATE` / Upsert) để pipeline có khả năng phục hồi nguyên vẹn khi xảy ra sự cố đột ngột (crash-resilience). |
