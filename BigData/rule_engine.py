# ─────────────────────────────────────────────────────────────────────────────
# rule_engine.py  –  Weighted rule scoring → fraud_rule_score ∈ [0.0, 1.0]
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql.functions import col, lit, when, least, array


def apply_rule_engine(df):
    """
    Weighted rule scoring → fraud_rule_score ∈ [0.0, 1.0]
    Mỗi rule trả về score component, cộng dồn rồi normalize.
    """
    return (df
        # Rule 1: velocity > 5 txns / 1 phút
        .withColumn("r1_velocity",
            when(col("txn_count_1m") >= 5, lit(0.35)).otherwise(lit(0.0)))

        # Rule 2: giao dịch tới blacklist receiver
        .withColumn("r2_blacklist",
            when(col("bl_receiver_severity").isNotNull(),
                 (col("bl_receiver_severity") / 5.0) * 0.40
            ).otherwise(lit(0.0)))

        # Rule 3: amount > avg_user * 5 (hoặc z-score > 4)
        .withColumn("r3_amount_anomaly",
            when((col("amount") > col("avg_amount") * 5) |
                 (col("amount_zscore") > 4.0),
                 lit(0.30)).otherwise(lit(0.0)))

        # Rule 4: new device login
        .withColumn("r4_new_device",
            when(col("is_new_device") == 1, lit(0.15)).otherwise(lit(0.0)))

        # Rule 5: odd hours (1am-5am local) + large amount
        .withColumn("r5_odd_hour",
            when((col("hour_of_day").between(1, 5)) &
                 (col("amount") > 1_000_000),   # VND
                 lit(0.20)).otherwise(lit(0.0)))

        # Rule 6: high velocity TO blacklist (10 txns/10min)
        .withColumn("r6_bl_velocity",
            when((col("txn_count_10m") >= 10) &
                 col("bl_receiver_severity").isNotNull(),
                 lit(0.50)).otherwise(lit(0.0)))

        # Aggregate & cap at 1.0
        .withColumn("fraud_rule_score",
            least(lit(1.0),
                  col("r1_velocity") + col("r2_blacklist") +
                  col("r3_amount_anomaly") + col("r4_new_device") +
                  col("r5_odd_hour") + col("r6_bl_velocity")))
        .withColumn("rule_flags",
            array(
                when(col("r1_velocity") > 0, lit("HIGH_VELOCITY")),
                when(col("r2_blacklist") > 0, lit("BLACKLIST_RECEIVER")),
                when(col("r3_amount_anomaly") > 0, lit("AMOUNT_ANOMALY")),
                when(col("r4_new_device") > 0, lit("NEW_DEVICE")),
                when(col("r5_odd_hour") > 0, lit("ODD_HOUR_LARGE_TXN")),
                when(col("r6_bl_velocity") > 0, lit("HIGH_BL_VELOCITY"))
            ))
    )
