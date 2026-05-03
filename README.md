# Real-Time Fraud Detection Pipeline (Bigdata-Nhom25)

Hệ thống phát hiện gian lận giao dịch tài chính theo thời gian thực (Real-time Fraud Detection) sử dụng **PySpark Streaming**, **Apache Kafka**, **PostgreSQL** và **Machine Learning**. Hệ thống có thể chạy trên nhiều nền tảng (Windows, macOS, Linux).

## Kiến trúc Hệ thống
- **Producer (Kafka)**: Mô phỏng giao dịch liên tục và gửi dữ liệu vào hệ thống Kafka (`producer.py`).
- **Stream Processing (PySpark)**: Đọc dữ liệu từ Kafka, áp dụng **Rule-based Engine** và mô hình **Machine Learning (GBTClassifier)** để chấm điểm giao dịch theo thời gian thực (`main.py`, `stream_processing.py`).
- **Sink (Storage)**: Giao dịch an toàn được lưu trữ vào **PostgreSQL** và các cảnh báo (alerts) nghi ngờ gian lận được lưu dưới định dạng **Parquet**.
- **Containerization**: Các dịch vụ bên thứ 3 như Kafka, Zookeeper và Postgres được triển khai nhanh chóng bằng Docker (`docker-compose.yml`).

## Yêu cầu Môi trường (Prerequisites)
Để chạy dự án, bạn cần cài đặt:
1. **Docker Desktop** (Bắt buộc để khởi chạy Kafka & PostgreSQL).
2. **Python 3.9 - 3.11** (Hỗ trợ tốt nhất cho PySpark 3.5.0).
3. **Java 8 hoặc 11** (Bắt buộc để chạy ứng dụng Apache Spark).
    - Cần thiết lập biến môi trường `JAVA_HOME` trỏ tới thư mục cài đặt Java.
4. *(Riêng cho Windows)*: Thư mục dự án đã có sẵn `BigData/hadoop/bin/` chứa `winutils.exe` và `hadoop.dll` để hỗ trợ Spark hoạt động ổn định.

## Hướng dẫn Cài đặt & Khởi chạy

### Bước 1: Khởi động hệ thống Docker (Kafka & Postgres)
Mở terminal tại thư mục dự án và chạy câu lệnh sau để khởi động hạ tầng:
```bash
cd BigData
docker compose up -d
```
*Bạn có thể truy cập [Kafka UI](http://localhost:8080) sau khi chạy để kiểm tra các cluster đã hoạt động.*

### Bước 2: Cài đặt thư viện Python
Tạo môi trường ảo (Virtual Environment) và cài đặt các package cần thiết:
```bash
pip install -r BigData/requirements.txt
```

### Bước 3: Đặt dữ liệu vào dự án
Tải dataset (ví dụ: `paysim.csv` hoặc `creditcard.csv`) và đặt vào thư mục `BigData/data/`. *(Lưu ý: Do kích thước file lớn nên thư mục data không được đẩy lên Github).*

### Bước 4: Chạy Pipeline
Hệ thống yêu cầu chạy song song 2 tiến trình: 1 tiến trình phát sinh dữ liệu (Producer) và 1 tiến trình xử lý luồng dữ liệu (Spark Streaming).

**Cách 1: Chạy tự động trên Windows**
Mở PowerShell (Run as Administrator) và chạy file script:
```powershell
powershell -ExecutionPolicy Bypass -File BigData/run.ps1
```
*(Nếu gặp lỗi môi trường Python, hãy mở file `run.ps1` để chỉnh sửa lại biến môi trường `PYSPARK_PYTHON` và đường dẫn cho đúng với máy tính của bạn).*

**Cách 2: Chạy thủ công trên mọi hệ điều hành (Windows/macOS/Linux)**
Mở 2 terminal riêng biệt:

*Terminal 1 (Chạy Kafka Producer):*
```bash
cd BigData
python producer.py
```

*Terminal 2 (Chạy Spark Streaming):*
```bash
cd BigData
python main.py
```

## Theo dõi và Đánh giá Kết quả
1. **Giao diện Kafka UI**: Giám sát các message Kafka đang được stream tại địa chỉ `http://localhost:8080` (trong Topic `transactions`).
2. **Console Output**: PySpark sẽ liên tục in ra các batch kết quả sau mỗi chu kỳ xử lý trên Terminal chạy file `main.py`.
3. **PostgreSQL**: Kết nối Database qua công cụ như DBeaver hoặc pgAdmin tại `localhost:5432` (User: `spark_user`, Password: `secret`, DB: `fraud_db`) để truy vấn các giao dịch đã được ghi nhận.
4. **Cảnh báo (Alerts)**: Các giao dịch bị hệ thống phát hiện là có khả năng gian lận cao sẽ được cô lập và lưu tự động vào thư mục `BigData/output/` dưới định dạng Parquet. Chạy lệnh sau để đọc file kết quả:
```bash
python BigData/read_parquet_alerts.py
```

## Cấu trúc thư mục chính
- `BigData/docker-compose.yml`: Kịch bản triển khai Kafka, Zookeeper, Postgres, Kafka-UI.
- `BigData/producer.py`: Kafka producer mô phỏng luồng giao dịch.
- `BigData/main.py`: Entry point chính khởi chạy luồng Spark Streaming.
- `BigData/stream_processing.py`: Logic biến đổi và xử lý stream dữ liệu.
- `BigData/rule_engine.py`: Thiết lập các tập luật để phát hiện gian lận (Rule-based).
- `BigData/ml_training.py`: Code huấn luyện mô hình Machine Learning.
- `BigData/run.ps1`: Script PowerShell hỗ trợ chạy nhanh cho Windows.
