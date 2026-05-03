# ─────────────────────────────────────────────────────────────────────────────
# config.py  –  SparkSession & tuning configs
# ─────────────────────────────────────────────────────────────────────────────

import os
import sys

# ── Fix Windows: HADOOP_HOME + winutils.exe ───────────────────────────────────
_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
_HADOOP_HOME  = os.path.join(_PROJECT_DIR, "hadoop")

os.environ["HADOOP_HOME"]     = _HADOOP_HOME
os.environ["hadoop.home.dir"] = _HADOOP_HOME
os.environ["PATH"] = os.path.join(_HADOOP_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

# ── Fix "Python worker failed to connect back" ────────────────────────────────
# JVM spawn python workers; nếu không set sẽ dùng Microsoft Store alias → fail
# Lấy đúng executable đang chạy script này
_PYTHON_EXEC = sys.executable   # vd: C:\Program Files\Python313\python.exe
os.environ["PYSPARK_PYTHON"]        = _PYTHON_EXEC
os.environ["PYSPARK_DRIVER_PYTHON"] = _PYTHON_EXEC

# ── Fix Windows: Spark temp dir (tránh NoSuchFileException khi shutdown) ──────
_SPARK_TMP = os.path.join(_PROJECT_DIR, "tmp")
os.makedirs(_SPARK_TMP, exist_ok=True)
os.environ["SPARK_LOCAL_DIRS"] = _SPARK_TMP

from pyspark.sql import SparkSession

# Thư mục checkpoint local
CHECKPOINT_DIR = os.path.join(_PROJECT_DIR, "checkpoint")
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

# Kafka-Spark connector (tự tải qua Maven lần đầu ~50MB)
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4"


def create_spark_session() -> SparkSession:
    _hadoop_fwd = _HADOOP_HOME.replace("\\", "/")

    spark = (SparkSession.builder
        .appName("FraudDetection-Streaming")
        # ── Kafka connector ──────────────────────────────────────────────────
        .config("spark.jars.packages", KAFKA_PACKAGE)
        # ── Shuffle partitions (local: 4 đủ; production: 64) ─────────────────
        .config("spark.sql.shuffle.partitions", "4")
        # ── State store + checkpoint ─────────────────────────────────────────
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        # ── Temp dir (fix NoSuchFileException trên Windows) ───────────────────
        .config("spark.local.dir", _SPARK_TMP)
        # ── Broadcast threshold: 50 MB ────────────────────────────────────────
        .config("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))
        # ── Fix Windows: trỏ JVM tới HADOOP_HOME ─────────────────────────────
        .config("spark.hadoop.fs.file.impl",
                "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.driver.extraJavaOptions",
                f"-Dhadoop.home.dir={_hadoop_fwd} "
                f"-Djava.io.tmpdir={_SPARK_TMP.replace(chr(92), '/')}")
        # ── Spark UI ──────────────────────────────────────────────────────────
        .config("spark.ui.port", "4040")
        .master("local[*]")
        .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    return spark
