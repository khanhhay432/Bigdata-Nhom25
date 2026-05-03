# ─────────────────────────────────────────────────────────────────────────────
# stream_processing.py  –  Kafka ingestion + feature engineering
# LOCAL MODE: blacklist & baseline dùng dữ liệu mock (không cần PostgreSQL/S3)
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_date,
    broadcast, when, lit, hour, dayofweek,
    count, sum as _sum, avg, stddev,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.functions import vector_to_array

from schemas import TRANSACTION_SCHEMA


# ── 1. Đọc stream từ Kafka ───────────────────────────────────────────────────
def read_kafka_stream(spark: SparkSession):
    raw_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "transactions")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 100_000)   # backpressure control
        .option("failOnDataLoss", "false")
        .load())
    return raw_stream


# ── 2. Parse JSON payload ─────────────────────────────────────────────────────
def parse_stream(raw_stream):
    parsed_stream = (raw_stream
        .select(from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"),
                col("timestamp").alias("kafka_ts"))
        .select("data.*", "kafka_ts")
        .withWatermark("event_time", "30 seconds")   # tolerate 30s late data
        # Watermark = 30s: Spark giữ state cho late data tối đa 30s
        # → window(1min) + watermark(30s) → state cleared after 1.5 min
        # Quá nhỏ → mất late data; quá lớn → state store bùng
        # Monitor: spark.streaming.numRowsDroppedByWatermark
        .repartition(4, col("sender_id")))           # local: 4 partitions là đủ
    return parsed_stream


# ── 3. Transaction velocity per user (1min / 10min windows) ──────────────────
def compute_velocity(parsed_stream):
    from pyspark.sql.functions import window

    velocity_1m = (parsed_stream
        .groupBy(col("sender_id"),
                 window(col("event_time"), "1 minute", "30 seconds"))
        .agg(count("*").alias("txn_count_1m"),
             _sum("amount").alias("total_amount_1m"))
        .select("sender_id",
                col("window.end").alias("window_end"),
                "txn_count_1m", "total_amount_1m"))

    velocity_10m = (parsed_stream
        .groupBy(col("sender_id"),
                 window(col("event_time"), "10 minutes", "1 minute"))
        .agg(count("*").alias("txn_count_10m"),
             _sum("amount").alias("total_amount_10m"),
             avg("amount").alias("avg_amount_10m"),
             stddev("amount").alias("stddev_amount_10m"))
        .select("sender_id",
                col("window.end").alias("window_end_10m"),
                "txn_count_10m", "total_amount_10m",
                "avg_amount_10m", "stddev_amount_10m"))

    return velocity_1m, velocity_10m


# ── 4. Broadcast join blacklist (mock local data) ─────────────────────────────
# Production: đọc từ PostgreSQL
# Local:      dùng mock DataFrame (danh sách đen cứng)
def load_blacklist(spark: SparkSession):
    BLACKLIST_SCHEMA = StructType([
        StructField("entity_id",   StringType(),  False),
        StructField("entity_type", StringType(),  True),
        StructField("severity",    IntegerType(), True),
    ])

    blacklist_data = [
        ("USER_0088", "USER", 5),
        ("USER_0077", "USER", 4),
        ("DEV_1234",  "DEVICE", 3),
        ("192.168.1.100", "IP", 2),
    ]

    # Không dùng .cache() — gây EOFException với Python 3.13 + PySpark 3.5.x
    blacklist_df = spark.createDataFrame(blacklist_data, schema=BLACKLIST_SCHEMA)
    return broadcast(blacklist_df)


def enrich_with_blacklist(parsed_stream, blacklisted_bc):
    enriched = (parsed_stream
        .join(blacklisted_bc.filter(col("entity_type") == "USER"),
              parsed_stream.receiver_id == blacklisted_bc.entity_id, "left")
        .withColumnRenamed("severity", "bl_receiver_severity")
        .drop("entity_id", "entity_type"))
    return enriched


# ── 5. Amount anomaly: z-score vs user baseline (mock local data) ─────────────
# Production: đọc từ Parquet trên S3 với partition pruning
# Local:      mock baseline với avg/stddev cứng cho mỗi user
def enrich_with_baseline(spark: SparkSession, enriched):
    BASELINE_SCHEMA = StructType([
        StructField("user_id",      StringType(), False),
        StructField("avg_amount",   DoubleType(), True),
        StructField("stddev_amount",DoubleType(), True),
    ])

    # Mock: tất cả user có avg=1_000_000 VND, std=500_000 VND
    baseline_data = [
        (f"USER_{i:04d}", 1_000_000.0, 500_000.0)
        for i in range(1, 101)
    ]

    user_baseline = spark.createDataFrame(baseline_data, schema=BASELINE_SCHEMA)

    enriched_with_baseline = (enriched
        .join(broadcast(user_baseline),
              enriched.sender_id == user_baseline.user_id, "left")
        .withColumn("amount_zscore",
                    when(col("stddev_amount") > 0,
                         (col("amount") - col("avg_amount")) / col("stddev_amount"))
                    .otherwise(lit(0.0)))
        .drop("user_id"))
    return enriched_with_baseline


# ── 6. Geo & device features (mock local data) ────────────────────────────────
# Production: đọc từ JDBC user_device_history
# Local:      mock 10 device/user đã biết
def enrich_with_device_features(spark: SparkSession, enriched_with_baseline):
    DEVICE_SCHEMA = StructType([
        StructField("user_id",   StringType(), False),
        StructField("device_id", StringType(), False),
    ])

    known_devices = [(f"USER_{i:04d}", f"DEV_{1000+i}") for i in range(1, 21)]
    device_history = broadcast(
        spark.createDataFrame(known_devices, schema=DEVICE_SCHEMA))

    featured_stream = (enriched_with_baseline
        .join(device_history,
              (enriched_with_baseline.sender_id == device_history.user_id) &
              (enriched_with_baseline.device_id == device_history.device_id),
              "left")
        .withColumn("is_new_device",
                    when(device_history.device_id.isNull(), lit(1)).otherwise(lit(0)))
        .withColumn("hour_of_day", hour(col("event_time")))
        .withColumn("is_weekend",
                    when(dayofweek(col("event_time")).isin([1, 7]), lit(1)).otherwise(lit(0)))
        .drop(device_history.user_id, device_history.device_id))
    return featured_stream


# ── 7. Debug: kiểm tra physical plan TRƯỚC khi submit production ─────────────
def debug_explain(featured_stream):
    featured_stream.explain(mode="formatted")
    # Tìm: Exchange (shuffle), BroadcastHashJoin, SortMergeJoin
    #
    # Các tín hiệu cần tránh:
    # ❌ Exchange hashpartitioning(...) giữa 2 large stream → dùng repartition() trước join
    # ❌ SortMergeJoin khi 1 side nhỏ → force broadcast
    # ✅ BroadcastHashJoin [blacklist, baseline] → đúng
    # ✅ StreamingRelation [Kafka] → đúng


# ── 8. Thêm velocity columns mặc định ────────────────────────────────────────
# Vấn đề: velocity cần groupBy aggregation → tạo ra streaming DataFrame riêng
# → không thể join trực tiếp vào per-row stream mà không dùng stateful join phức tạp.
# Giải pháp cho demo: mỗi row tự coi là 1 txn trong window hiện tại.
# Production: dùng mapGroupsWithState hoặc Redis/Cassandra làm state store.
def add_velocity_defaults(df):
    """
    Thêm cột velocity mặc định vào mỗi row.
    - txn_count_1m  = 1  (mỗi row là 1 txn)
    - txn_count_10m = 1
    - total_amount_1m / 10m = amount của row đó
    - avg/stddev dùng từ baseline đã join trước đó
    """
    return (df
        .withColumn("txn_count_1m",      lit(1))
        .withColumn("total_amount_1m",   col("amount"))
        .withColumn("txn_count_10m",     lit(1))
        .withColumn("total_amount_10m",  col("amount"))
        .withColumn("avg_amount_10m",    col("avg_amount"))
        .withColumn("stddev_amount_10m", col("stddev_amount")))

# ── 9. Dự đoán với mô hình Machine Learning ──────────────────────────────────
def load_ml_model(model_path="d:/BigData/models/fraud_gbt_v1"):
    return PipelineModel.load(model_path)

def enrich_with_ml_model(df, ml_model):
    """
    Áp dụng pipeline ML (Imputer -> Indexer -> Assembler -> Scaler -> GBT) vào luồng streaming
    để lấy điểm xác suất gian lận.
    """
    # Ép kiểu cho bl_receiver_severity (Imputer yêu cầu kiểu số)
    # df = df.withColumn("bl_receiver_severity", col("bl_receiver_severity").cast(DoubleType()))
    # Vì schema đã cho nó là IntegerType, Imputer có thể xử lý, nhưng đề phòng null
    # Trong ML pipeline ta dùng median strategy cho nulls
    
    predictions = ml_model.transform(df)
    
    # 'probability' là cột DenseVector. Vị trí [1] là xác suất của class 1 (gian lận)
    enriched = predictions.withColumn("ml_fraud_score", vector_to_array(col("probability"))[1])
    
    # Dọn dẹp các cột sinh ra bởi ML pipeline (features, rawPrediction, v.v.)
    cols_to_drop = [c for c in predictions.columns if c not in df.columns and c != "ml_fraud_score"]
    return enriched.drop(*cols_to_drop)
