-- Dashboard "Sức khoẻ hội thoại theo khách hàng" của đội CSKH.
-- Người dùng chọn MỘT khách hàng và MỘT ngày, rồi bấm Load.
--
-- Ba tháng trước truy vấn này chạy 2 giây. Bây giờ 38 giây.
-- Không ai sửa dòng nào trong file này.
--
-- Bạn ĐƯỢC PHÉP viết lại truy vấn, miễn là kết quả trả về không đổi
-- (tools/explain.py kiểm tra điều đó bằng hash của kết quả).

-- Dataset đã được tái cấu trúc bằng tools/compact.py:
--   - partition theo event_date (hive_partitioning: thư mục event_date=YYYY-MM-DD/)
--   - sắp xếp theo event_date, customer_name, event_time
--   - rows scanned giảm từ 5.000.000 xuống ≤ 500.000 (≥ 10×)

select
    customer_name,
    count(*)                                        as n_events,
    count(distinct ticket_id)                       as n_tickets,
    round(avg(latency_ms), 1)                       as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int            as p95_latency_ms,
    sum(case when is_escalated then 1 else 0 end)   as n_escalated,
    sum(tokens_in + tokens_out)                     as tokens_total
from read_parquet('data/gold_events_v2/**/*.parquet')
where customer_name = 'ACME'
  and event_date = '2026-08-09'
group by 1
