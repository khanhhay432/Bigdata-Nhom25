# ─────────────────────────────────────────────────────────────────────────────
# producer.py  –  Sinh giao dịch giả và đẩy vào Kafka topic "transactions"
# Chạy: python producer.py
# ─────────────────────────────────────────────────────────────────────────────

import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

# ── Cấu hình ─────────────────────────────────────────────────────────────────
KAFKA_BROKER   = "localhost:9092"
TOPIC          = "transactions"
SEND_INTERVAL  = 0.5   # giây giữa mỗi message

# ── Dữ liệu mẫu ───────────────────────────────────────────────────────────────
CURRENCIES     = ["VND", "USD", "EUR"]
TXN_TYPES      = ["TRANSFER", "PAYMENT", "TOPUP"]
CHANNELS       = ["APP", "WEB", "USSD"]
DEVICE_TYPES   = ["ANDROID", "IOS", "BROWSER"]
MERCHANT_CATS  = ["RETAIL", "FOOD", "TRAVEL", "FINANCE", "GAMING"]

# Một số sender/receiver cố định để tạo pattern velocity
SENDERS        = [f"USER_{i:04d}" for i in range(1, 51)]
RECEIVERS      = [f"USER_{i:04d}" for i in range(51, 101)]
BLACKLISTED    = {"USER_0088", "USER_0077"}   # simulate blacklist


def make_transaction(fraudulent: bool = False) -> dict:
    now = datetime.now(timezone.utc)

    sender_id   = random.choice(SENDERS)
    receiver_id = random.choice(list(BLACKLISTED)) if fraudulent else random.choice(RECEIVERS)
    amount      = round(random.uniform(5_000_000, 50_000_000), 2) if fraudulent \
                  else round(random.uniform(10_000, 2_000_000), 2)

    return {
        "txn_id":       str(uuid.uuid4()),
        "sender_id":    sender_id,
        "receiver_id":  receiver_id,
        "amount":       amount,
        "currency":     random.choice(CURRENCIES),
        "txn_type":     random.choice(TXN_TYPES),
        "channel":      random.choice(CHANNELS),
        "device_id":    f"DEV_{random.randint(1000, 9999)}",
        "device_type":  random.choice(DEVICE_TYPES),
        "ip_address":   f"192.168.{random.randint(0,255)}.{random.randint(1,254)}",
        "lat":          round(random.uniform(10.0, 23.0), 6),   # Vietnam lat range
        "lon":          round(random.uniform(102.0, 109.5), 6), # Vietnam lon range
        "merchant_id":  f"MER_{random.randint(100, 999)}",
        "merchant_cat": random.choice(MERCHANT_CATS),
        "event_time":   now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
        "ingest_time":  now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
    }


def main():
    print(f"[Producer] Kết nối tới Kafka broker: {KAFKA_BROKER}")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )

    count    = 0
    fraud_n  = 0
    print(f"[Producer] Đang gửi transaction vào topic '{TOPIC}' ... (Ctrl+C để dừng)\n")

    try:
        while True:
            # ~10% là giao dịch gian lận
            is_fraud = random.random() < 0.10
            txn      = make_transaction(fraudulent=is_fraud)

            producer.send(TOPIC, value=txn)
            count   += 1
            fraud_n += int(is_fraud)

            label = "🚨 FRAUD" if is_fraud else "   normal"
            print(f"[{count:>5}] {label} | sender={txn['sender_id']} "
                  f"amount={txn['amount']:>14,.0f} VND → {txn['receiver_id']}")

            time.sleep(SEND_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n[Producer] Đã dừng. Tổng: {count} txn, fraud: {fraud_n}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
