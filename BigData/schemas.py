# ─────────────────────────────────────────────────────────────────────────────
# schemas.py  –  PySpark StructType schemas
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)

TRANSACTION_SCHEMA = StructType([
    StructField("txn_id",        StringType(),    False),
    StructField("sender_id",     StringType(),    False),
    StructField("receiver_id",   StringType(),    False),
    StructField("amount",        DoubleType(),    False),
    StructField("currency",      StringType(),    True),
    StructField("txn_type",      StringType(),    True),   # TRANSFER | PAYMENT | TOPUP
    StructField("channel",       StringType(),    True),   # APP | WEB | USSD
    StructField("device_id",     StringType(),    True),
    StructField("device_type",   StringType(),    True),
    StructField("ip_address",    StringType(),    True),
    StructField("lat",           DoubleType(),    True),
    StructField("lon",           DoubleType(),    True),
    StructField("merchant_id",   StringType(),    True),
    StructField("merchant_cat",  StringType(),    True),   # MCC code
    StructField("event_time",    TimestampType(), False),  # device time
    StructField("ingest_time",   TimestampType(), True),   # Kafka timestamp
])

BLACKLIST_SCHEMA = StructType([
    StructField("entity_id",     StringType(),    False),
    StructField("entity_type",   StringType(),    True),   # USER | DEVICE | MERCHANT | IP
    StructField("reason",        StringType(),    True),
    StructField("severity",      IntegerType(),   True),   # 1-5
    StructField("listed_at",     TimestampType(), True),
    StructField("expires_at",    TimestampType(), True),
])
