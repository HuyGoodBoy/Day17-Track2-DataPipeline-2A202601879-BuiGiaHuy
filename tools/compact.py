#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — NHIỆM VỤ 4.  CHƯA CÓ LOGIC.

Hiện trạng: `data/gold_events/` gồm 5.000 file, mỗi file vài chục KB, không
partition, thứ tự hàng ngẫu nhiên.

Yêu cầu: đọc toàn bộ dataset cũ, ghi ra dataset mới có layout hợp lý hơn, sau đó cập
nhật `queries/dashboard.sql` để trỏ vào dataset mới.

    python tools/compact.py       # ghi dataset mới
    python tools/explain.py       # đo lại và so với baseline

KHUNG THỰC HIỆN

    COPY (
        SELECT *
        FROM   read_parquet('data/gold_events/*.parquet')
        ORDER  BY <cột A>, <cột B>
    ) TO 'data/gold_events_v2' (
        FORMAT          parquet,
        PARTITION_BY    (<cột partition>),
        OVERWRITE_OR_IGNORE,
        ROW_GROUP_SIZE  <?>
    )

Ba quyết định, mỗi quyết định cần một lý do viết được ra giấy:

  <cột partition>   Engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi
                    mở file. Thông tin đó đến từ đường dẫn. Vậy cột nào của
                    truy vấn dashboard nên xuất hiện trong tên thư mục? Cột đó
                    có bao nhiêu giá trị phân biệt — tức bao nhiêu thư mục?
                    Partition theo cột có 650 giá trị thì hệ quả là gì?

  <cột A>, <cột B>  Thứ tự hàng trong file quyết định thống kê min/max của mỗi
                    row group có ích hay vô dụng. Sắp thế nào để các hàng cùng
                    một khách hàng nằm liền nhau?

  ROW_GROUP_SIZE    Mặc định 122.880 hàng. Một ngày có khoảng bao nhiêu hàng?
                    Nếu cả ngày gói gọn trong MỘT row group thì min/max của
                    row group đó phủ những gì, và còn tác dụng lọc không?

Sau khi chạy xong, kiểm tra lại bằng `python tools/explain.py`: `rows scanned`
phải giảm, `files` phải giảm, và `result hash` phải GIỮ NGUYÊN.
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"


def main() -> int:
    con = duckdb.connect()

    # Đếm số hàng nguồn trước
    n_src = len(list(SRC.glob("*.parquet")))
    row_count = con.execute(f"""
        select count(*) from read_parquet('{SRC}/*.parquet')
    """).fetchone()[0]
    print(f"  nguồn : {SRC}  ({n_src:,} file, {row_count:,} hàng)")

    # Tái cấu trúc:
    # 1. PARTITION_BY event_date  -> thư mục theo ngày (14 giá trị)
    #    DuckDB hive_partitioning: thư mục 'event_date=2026-08-03/', v.v.
    #    Dashboard filter event_date = '2026-08-09' sẽ bỏ qua 13/14 thư mục.
    # 2. ORDER BY event_date, customer_name, event_time
    #    -> cùng khách nằm liền nhau; min/max row-group phủ customer_name
    #    -> cùng ngày cũng nằm gần nhau (do partition)
    # 3. ROW_GROUP_SIZE giữ mặc định 122.880
    #    Một ngày có ~9.300 hàng << 122.880, nên 1 ngày ≤ 1 row group
    #    -> min/max row group = min/max thực của cả ngày
    #    -> predicate pushdown lọc đúng ngày ở cấp row group luôn
    con.execute(f"""
        copy (
            select *,
                   strftime(event_time, '%Y-%m-%d') as event_date
            from read_parquet('{SRC}/*.parquet')
            order by strftime(event_time, '%Y-%m-%d'), customer_name, event_time
        ) to '{DST}' (
            format parquet,
            partition_by (event_date),
            overwrite_or_ignore
        )
    """)

    n_dst = len(list(DST.rglob("*.parquet")))
    row_count_dst = con.execute(f"""
        select count(*) from read_parquet('{DST}/**/*.parquet')
    """).fetchone()[0]
    print(f"  đích : {DST}  ({n_dst:,} file, {row_count_dst:,} hàng)")

    assert row_count == row_count_dst, (
        f"số hàng không khớp: {row_count} → {row_count_dst}"
    )
    print("  ✓ không mất hàng nào")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
