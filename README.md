
# Airbnb Analytics with DBT

Phân tích dữ liệu Airbnb sử dụng DBT (Data Build Tool).

## Mục đích
Dự án này giúp phân tích, trực quan hóa và tổng hợp dữ liệu Airbnb, bao gồm các bảng hosts, listings, reviews, v.v. Sử dụng DBT để xây dựng các mô hình dữ liệu, kiểm thử và tạo tài liệu tự động.

## Cấu trúc thư mục
- `models/`: Chứa các mô hình dữ liệu (dim, fact, mart, src)
- `macros/`: Các macro SQL dùng chung
- `seeds/`: Dữ liệu nguồn dạng CSV
- `snapshots/`: Lưu trạng thái dữ liệu theo thời gian
- `analyses/`: Phân tích tùy chỉnh
- `dbt_packages/`: Các package mở rộng cho DBT
- `logs/`: Log quá trình chạy DBT
- `target/`: Kết quả biên dịch và tài liệu hóa

## Hướng dẫn sử dụng
1. Cài đặt DBT:
	```bash
	pip install dbt-core dbt-postgres
	```
2. Cấu hình kết nối database trong file `profiles.yml`.
3. Chạy các lệnh DBT:
	- Build models: `dbt run`
	- Test models: `dbt test`
	- Tạo tài liệu: `dbt docs generate`
	- Xem tài liệu: `dbt docs serve`

## Các thành phần chính
- **models/core/**: Các bảng dữ liệu cơ bản (dim_hosts, dim_listings, fact_reviews)
- **models/mart/**: Các bảng tổng hợp phục vụ phân tích (agg_daily_reviews, agg_host_performance, ...)
- **macros/**: Macro SQL hỗ trợ xử lý dữ liệu
- **seeds/**: Dữ liệu nguồn mẫu

## Tài nguyên tham khảo
- [Tài liệu DBT](https://docs.getdbt.com/docs/introduction)
- [Cộng đồng DBT](https://community.getdbt.com/)

## Tác giả
Dự án cá nhân thực hiện bởi Nguyen Nhuong - nhuongblue@gmail.com
