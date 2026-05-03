# ─────────────────────────────────────────────────────────────────────────────
# ml_training.py  –  Offline ML pipeline (GBT on PaySim / Kaggle dataset)
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, Imputer
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


# Feature columns (khớp với stream_processing.py)
NUMERIC_FEATURES = [
    "amount", "txn_count_1m", "txn_count_10m",
    "total_amount_10m", "avg_amount_10m",
    "amount_zscore", "hour_of_day",
    "is_new_device", "is_weekend",
    "bl_receiver_severity"
]

CAT_FEATURES = ["txn_type", "channel"]


import random
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def generate_synthetic_paysim(spark: SparkSession, num_rows=5000):
    """
    Sinh dữ liệu mô phỏng PaySim cục bộ để train model (vì không có sẵn dữ liệu Kaggle 500MB).
    Đảm bảo schema giống với stream_processing.py.
    """
    data = []
    for i in range(num_rows):
        is_fraud = 1 if random.random() < 0.05 else 0  # 5% fraud
        
        # Đặc trưng cơ bản
        amount = random.uniform(10_000, 5_000_000)
        txn_type = random.choice(["TRANSFER", "PAYMENT", "TOPUP"])
        channel = random.choice(["APP", "WEB", "USSD"])
        
        if is_fraud:
            # Giao dịch gian lận thường có giá trị cao, dùng thiết bị mới, gửi cho blacklist
            amount = amount * random.uniform(5, 20)
            is_new_device = 1
            bl_receiver_severity = random.choice([3, 4, 5])
            amount_zscore = random.uniform(3.0, 100.0)
        else:
            # Giao dịch bình thường
            is_new_device = random.choice([0, 1])
            bl_receiver_severity = random.choice([None, 1, 2])
            amount_zscore = random.uniform(-2.0, 2.5)

        txn_count_1m = random.randint(1, 5)
        txn_count_10m = random.randint(txn_count_1m, 15)
        total_amount_10m = amount * txn_count_10m
        avg_amount_10m = total_amount_10m / txn_count_10m
        hour_of_day = random.randint(0, 23)
        is_weekend = random.choice([0, 1])
        step = i # Giả lập time step

        data.append((
            amount, txn_count_1m, txn_count_10m, total_amount_10m, avg_amount_10m,
            amount_zscore, hour_of_day, is_new_device, is_weekend, bl_receiver_severity,
            txn_type, channel, is_fraud, step
        ))
        
    schema = StructType([
        StructField("amount", DoubleType(), True),
        StructField("txn_count_1m", IntegerType(), True),
        StructField("txn_count_10m", IntegerType(), True),
        StructField("total_amount_10m", DoubleType(), True),
        StructField("avg_amount_10m", DoubleType(), True),
        StructField("amount_zscore", DoubleType(), True),
        StructField("hour_of_day", IntegerType(), True),
        StructField("is_new_device", IntegerType(), True),
        StructField("is_weekend", IntegerType(), True),
        StructField("bl_receiver_severity", IntegerType(), True),
        StructField("txn_type", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("isFraud", IntegerType(), True),
        StructField("step", IntegerType(), True),
    ])
    
    return spark.createDataFrame(data, schema)

def train_fraud_model(spark: SparkSession):
    # ── Sinh dữ liệu giả lập (thay cho PaySim từ Kaggle) ──────────────────────
    print("Sinh dữ liệu giả lập PaySim...")
    raw_data = generate_synthetic_paysim(spark, num_rows=10000)

    # ── Class imbalance ──────────────────────────────────────────────────────
    # Thay vì dùng classWeight, thuật toán tree-based xử lý khá tốt.
    # Trong phiên bản GBTClassifier PySpark không hỗ trợ weightCol chuẩn như LogisticRegression,
    # nhưng chúng ta vẫn tiếp tục với các tham số mặc định được tuned.


    # Stage 1: Imputer for nulls
    imputer = Imputer(
        inputCols=NUMERIC_FEATURES,
        outputCols=[f"{c}_imp" for c in NUMERIC_FEATURES],
        strategy="median")

    # Stage 2: StringIndexer for categoricals
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in CAT_FEATURES
    ]

    # Stage 3: VectorAssembler
    assembler = VectorAssembler(
        inputCols=[f"{c}_imp" for c in NUMERIC_FEATURES] +
                  [f"{c}_idx" for c in CAT_FEATURES],
        outputCol="features_raw",
        handleInvalid="keep")

    # Stage 4: StandardScaler
    scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                             withStd=True, withMean=False)

    # Stage 5: GBT Classifier (tốt hơn LR/RF với tabular imbalanced data)
    gbt = GBTClassifier(
        labelCol="isFraud",
        featuresCol="features",
        maxIter=100,
        maxDepth=6,
        stepSize=0.1,
        subsamplingRate=0.8,
        featureSubsetStrategy="sqrt",
        seed=42)

    pipeline = Pipeline(stages=[imputer] + indexers + [assembler, scaler, gbt])

    # ── Train/test split giữ nguyên time order (KHÔNG random split) ──────────
    train_df = raw_data.filter(col("step") <= 600)
    test_df  = raw_data.filter(col("step") >  600)

    model = pipeline.fit(train_df)

    # ── Evaluation ────────────────────────────────────────────────────────────
    evaluator = BinaryClassificationEvaluator(labelCol="isFraud",
                                              metricName="areaUnderPR")
    predictions = model.transform(test_df)
    pr_auc = evaluator.evaluate(predictions)
    print(f"PR-AUC: {pr_auc:.4f}")   # ưu tiên PR-AUC do imbalanced

    # Save model xuống local disk
    model_path = "d:/BigData/models/fraud_gbt_v1"
    model.write().overwrite().save(model_path)
    print(f"Model saved to {model_path}")

    return model

if __name__ == "__main__":
    from config import create_spark_session
    spark = create_spark_session()
    train_fraud_model(spark)
    spark.stop()
