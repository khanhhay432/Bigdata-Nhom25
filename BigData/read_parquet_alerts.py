from config import create_spark_session

def main():
    print("Khởi động Spark để đọc file Parquet...")
    spark = create_spark_session()
        
    print("Đang tải dữ liệu từ: d:/BigData/output/fraud_alerts/")
    
    # Đọc thư mục chứa Parquet
    df = spark.read.parquet("d:/BigData/output/fraud_alerts/")
    
    print("\n--- DANH SÁCH GIAO DỊCH GIAN LẬN ĐÃ LƯU ---")
    df.select("txn_id", "sender_id", "receiver_id", "amount", "final_fraud_score", "rule_flags").show(truncate=False)
    
    print(f"Tổng số giao dịch đã block: {df.count()}")
    spark.stop()

if __name__ == "__main__":
    main()
