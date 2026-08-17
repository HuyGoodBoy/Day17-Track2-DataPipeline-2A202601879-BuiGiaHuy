# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Bui Gia Huy  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 22.6s
  run 2/3 … 21.8s
  run 3/3 … 21.8s

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

Tổng kết: **4/4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Chạy lại job qua Airflow Clear Task, `gold_training_set` tăng dần: 12,480 → 25,960 → 38,750 hàng sau mỗi lượt. Không báo lỗi. |
| **Nguyên nhân** | Model `gold_training_set` khai báo `materialized = 'incremental'` nhưng thiếu `unique_key`. Không có key, dbt sinh câu `INSERT INTO` thay vì `MERGE`. Mỗi lượt chạy ghi thêm toàn bộ partition ngày hiện tại lên bảng đã có — hàng cũ không bị thay thế mà bị nhân đôi. CDC source có `op='u'`: một ticket được tạo ngày D1 rồi update ngày D2 sẽ lọt qua filter 2 lần trong cùng 1 lượt chạy (vào 2 partition khác nhau), khiến số hàng tăng phi tuyến. DAG có `catchup=True` và `max_active_runs=None`: thao tác Clear Task trigger Airflow chạy bù tất cả ngày trong quá khứ, và nhiều run chạy đồng thời ghi vào cùng bảng. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` — dbt sẽ merge theo ticket_id, bản ghi cùng ticket ghi đè thay vì cộng dồn. `dags/ai_training_pipeline.py`: đổi `catchup=False` và `max_active_runs=1` để ngăn Airflow chạy bù và giới hạn đồng thời. |
| **Bằng chứng** | trước: 38,750 hàng · sau: 12,480 hàng · checksum 3 lượt: `8dd7c98653` giống hệt |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` thiếu 455 hàng (8,645 thay vì 9,100). Chỉ thiếu ở các ngày đã chạy xong từ lâu, ngày mới thì đủ. |
| **P99 độ trễ đo được** | **~2.73 ngày** (xem chi tiết bên dưới) |
| **Lookback đã chọn** | **3 ngày** — vì P99 độ trễ = 2.73 ngày, cần lùi ít nhất 3 ngày để đón đủ bản ghi muộn. |
| **Nguyên nhân** | Phân bố độ trễ `(_ingested_at - event_time)` trong `bronze_events` có hai cụm tách biệt: cụm nhỏ 0-6 giờ (đa số), và cụm lớn 43-71 giờ (~2-3 ngày) — chiếm 5.05% tổng event. Cụm muộn này xuất phát từ hệ thống Kafka/sự kiện có thời gian xử lý dài. Incremental filter `event_date > max(event_date)` của model không bao phủ được: khi bản ghi có `event_date=08-12` đến kho ngày `08-15`, lượt chạy ngày `08-15` đã có `max(event_date) >= 08-12` trong bảng rồi, nên bản ghi bị bỏ qua. Đến ngày `08-16`, `max(event_date)` lại = `08-15`, vẫn lọt qua. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: (1) đổi điều kiện lọc từ `event_date > max(...)` thành `event_date >= max(...) - interval 3 day` — mở rộng window để đón bản ghi muộn đến 3 ngày. (2) Thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` — khi cùng cặp được tính lại, kết quả mới thay thế kết quả cũ thay vì cộng dồn. |
| **Bằng chứng** | trước: 8,645 hàng · sau: 9,100 hàng · checksum 3 lượt: `3db448685c` giống hệt |

**P99 đo được:** Qua query `quantile_cont` trên 130,683 events trong `bronze_events`:

| Percentile | Giá trị (ngày) |
|---|---|
| P50 | 0.13 (~3 giờ) |
| P95 | 1.81 (~43 giờ) |
| **P99** | **2.73 (~66 giờ)** |
| Max | 2.94 (~71 giờ) |

**Vì sao chọn P99 thay vì `max`?** Chọn theo `max` (2.94 ngày) chỉ thêm được 0.2 ngày nhưng tốn chi phí đọc thêm dữ liệu ở mọi lượt chạy. P99 đã bao phủ 99% bản ghi; 1% còn lại (số rất nhỏ) có thể chấp nhận bỏ sót hoặc xử lý bằng backfill thủ công. Mỗi ngày lookback thêm = thêm 1 ngày dữ liệu phải đọc/join ở mọi lượt chạy incremental, nên chọn P99 là điểm cân bằng giữa độ phủ và chi phí.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Team backend đổi kiểu `priority` từ số (1-4) sang chuỗi ('urgent', 'high', 'medium', 'low') từ ngày 2026-08-10. Pipeline không dừng nhưng model phân loại từ đó dự đoán kém. Kiểm tra `silver_tickets` thấy 6,606 hàng có giá trị không đúng contract. `quarantine_tickets` rỗng (0 thay vì 312). |
| **Nguyên nhân** | Macro `normalize_priority` dùng `try_cast(priority_raw as integer)` — đúng với nhãn số ('1', '2', '3', '4') nhưng sai với nhãn chuỗi ('urgent', 'high', 'medium', 'low') vì `try_cast` trả về NULL cho chuỗi không phải số. Đồng thời nó lại chấp nhận '0', '5', '-1' (đúng là số) dù contract chỉ cho 1-4. Không có contract enforcement nên dbt không ngăn dữ liệu lỗi đi qua. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **Nhóm 1 — Số hợp lệ** ('1','2','3','4'): đúng contract ban đầu, giữ nguyên. **Nhóm 2 — Nhãn chuỗi** ('urgent','high','medium','low'): schema evolution từ 2026-08-10, ý nghĩa không đổi, chỉ đổi cách biểu diễn. Map: urgent→1, high→2, medium→3, low→4. **Nhóm 3 — Giá trị lỗi** ('P1','unknown','0','5','-1','',NULL): dữ liệu hỏng thật, trả về NULL → đi vào `quarantine_tickets`. |
| **Cách khắc phục** | (a) `dbt/macros/normalize_priority.sql`: thay `try_cast` bằng CASE xử lý đủ 3 nhóm. (b) `dbt/models/silver/silver_tickets.sql`: lọc `priority_clean is not null` **sau** xếp hạng nhưng **trước** filter `op <> 'd'` — đúng thứ tự để loại bản ghi hỏng nhưng giữ lại ticket nếu có trạng thái hợp lệ từ lần cập nhật trước. (c) `dbt/models/silver/quarantine_tickets.sql`: điều kiện `normalize_priority(...) is null` để tách đúng bản ghi lỗi. (d) `dbt/models/silver/schema.yml`: bật `contract.enforced = true` và thêm `accepted_values` test cho priority ∈ [1,2,3,4]. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `dbt test` 11/11 pass · priority sạch 0 NULL/sai |

**Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao không để pipeline dừng khi gặp bản ghi lỗi?**

Về tầng chặn: chặn ở **Silver** là hợp lý. Bronze là tầng lưu dữ liệu thô, nên giữ nguyên trạng thái nguồn để còn điều tra sự cố về sau. Nếu Bronze từ chối bản ghi lỗi, ta mất dữ liệu gốc — không biết được nguyên nhân gốc (do hệ thống nguồn, do format thay đổi, hay do bug thật sự). Chặn ở Silver giữ được bản ghi thô trong Bronze, đồng thời tách riêng bản ghi lỗi vào quarantine.

Không dừng pipeline khi gặp bản ghi lỗi vì: 312 bản ghi lỗi (từ tổng 14,300 CDC rows, ~2.2%) không có quyền chặn 130,683 events và 31,200 doc chunks hoàn toàn bình thường. Dừng DAG gây thiệt hại lớn hơn lỗi. Quarantine cho phép pipeline tiếp tục, đồng thời tạo hàng đợi để người trực xử lý bản ghi lỗi riêng — phù hợp với nguyên tắc "resilience over correctness in pipelines".

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | không làm |
| **Nguyên nhân** | Không đủ thời gian |
| **Cách khắc phục** | — |
| **Bằng chứng** | — |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | **Idempotency**: incremental model có khai báo `unique_key` chưa? Chạy 3 lượt liên tiếp để phát hiện ghi thêm thay vì ghi đè. |
| 2 | **Late-arriving data**: phân bố độ trễ `(_ingested_at - event_time)` trên dữ liệu thật, đo P99 làm căn cứ lookback. Không dùng max vì outlier không đại diện. |
| 3 | **Data contract**: schema của nguồn có thay đổi không? Kiểm tra `accepted_values` / `not_null` tests. Luôn tách bản ghi lỗi thay vì để pipeline dừng. |
