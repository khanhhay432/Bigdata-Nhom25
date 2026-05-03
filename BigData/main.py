# ─────────────────────────────────────────────────────────────────────────────
# main.py  –  Entry point: khởi động pipeline streaming
# Chạy: python main.py
# ─────────────────────────────────────────────────────────────────────────────

from config import create_spark_session
from stream_processing import (
    read_kafka_stream,
    parse_stream,
    load_blacklist,
    enrich_with_blacklist,
    enrich_with_baseline,
    enrich_with_device_features,
    add_velocity_defaults,
    load_ml_model,
    enrich_with_ml_model,
)
from rule_engine import apply_rule_engine
from pyspark.sql.functions import col


def main():
    print("=" * 60)
    print("  Fraud Detection – Real-Time Streaming Pipeline")
    print("=" * 60)

    # 1. Tạo SparkSession
    print("[1/6] Khởi tạo SparkSession ...")
    spark = create_spark_session()
    print(f"      Spark version: {spark.version}")

    # 2. Đọc & parse Kafka stream
    print("[2/6] Kết nối Kafka stream ...")
    raw_stream    = read_kafka_stream(spark)
    parsed_stream = parse_stream(raw_stream)

    # 3. Enrich: blacklist → baseline → device features
    print("[3/6] Nạp dữ liệu tham chiếu (blacklist, baseline, device history) ...")
    blacklisted_bc         = load_blacklist(spark)
    enriched               = enrich_with_blacklist(parsed_stream, blacklisted_bc)
    enriched_with_baseline = enrich_with_baseline(spark, enriched)
    featured_stream        = enrich_with_device_features(spark, enriched_with_baseline)

    # 4. Thêm velocity columns
    #    (txn_count_1m, txn_count_10m – cần cho rule engine)
    print("[4/6] Thêm velocity features ...")
    featured_stream = add_velocity_defaults(featured_stream)

    # 5. Áp dụng Machine Learning Model
    print("[5/7] Dự đoán bằng ML Model (GBTClassifier) ...")
    ml_model = load_ml_model()
    ml_scored = enrich_with_ml_model(featured_stream, ml_model)

    # 6. Áp dụng rule engine và kết hợp điểm
    print("[6/7] Áp dụng Rule Engine & Tính Final Score ...")
    rule_scored = apply_rule_engine(ml_scored)
    
    # Kết hợp điểm: 50% Rule Engine + 50% ML Model
    final_scored = rule_scored.withColumn(
        "final_fraud_score", 
        (col("fraud_rule_score") * 0.5) + (col("ml_fraud_score") * 0.5)
    )

    # 7. Sink → console
    #    Chỉ hiển thị các cột quan trọng
    output_cols = [
        "txn_id", "sender_id", "receiver_id", "amount",
        "txn_type", "channel",
        "bl_receiver_severity", "amount_zscore", "is_new_device", 
        "fraud_rule_score", "ml_fraud_score", "final_fraud_score", 
        "rule_flags",
    ]

    print("[7/7] Bắt đầu streaming query → console output ...")
    print("      (Ctrl+C để dừng)\n")

    query_console = (final_scored
        .select(*output_cols)
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(processingTime="5 seconds")
        .start())

    # 8. Sink → Parquet File (Lưu lại để báo cáo/truy vết)
    query_parquet = (final_scored
        .filter(col("final_fraud_score") >= 0.5) # CHỈ LƯU các giao dịch bị báo động
        .select(*output_cols)
        .writeStream
        .format("parquet")
        .option("path", "d:/BigData/output/fraud_alerts/")
        .option("checkpointLocation", "d:/BigData/checkpoint/fraud_alerts/")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start())

    # Chờ cả 2 luồng cùng chạy
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
