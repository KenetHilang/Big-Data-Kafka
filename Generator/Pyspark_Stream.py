from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, expr
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.streaming import StreamingQueryException
import logging

logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
logging.getLogger("py4j").setLevel(logging.ERROR)

spark = SparkSession.builder \
    .appName("Kafka-Sensor-Consumer") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema_suhu = StructType().add("gudang_id", StringType()).add("suhu", IntegerType())
schema_kelembapan = StructType().add("gudang_id", StringType()).add("kelembapan", IntegerType())

kafka_bootstrap_servers = "localhost:9092"
suhu_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "sensor-suhu-gudang") \
    .option("startingOffsets", "latest") \
    .load()

suhu_parsed = suhu_raw.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), schema_suhu).alias("data"), "timestamp") \
    .select("data.*", "timestamp")

kelembapan_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "sensor-kelembapan-gudang") \
    .option("startingOffsets", "latest") \
    .load()

kelembapan_parsed = kelembapan_raw.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), schema_kelembapan).alias("data"), "timestamp") \
    .select("data.*", "timestamp")
suhu_with_watermark = suhu_parsed.withWatermark("timestamp", "5 seconds")
kelembapan_with_watermark = kelembapan_parsed.withWatermark("timestamp", "5 seconds")

suhu_windowed = suhu_with_watermark.withColumn("window", window("timestamp", "10 seconds", "5 seconds"))
kelembapan_windowed = kelembapan_with_watermark.withColumn("window", window("timestamp", "10 seconds", "5 seconds"))

joined_df = suhu_windowed.join(
    kelembapan_windowed,
    (suhu_windowed["gudang_id"] == kelembapan_windowed["gudang_id"]) & 
    (suhu_windowed["window"] == kelembapan_windowed["window"]),
    "inner"
).select(
    suhu_windowed["gudang_id"],
    suhu_windowed["suhu"],
    kelembapan_windowed["kelembapan"],
    suhu_windowed["window"]
)

status_df = joined_df.withColumn("status", expr("""
    CASE
        WHEN suhu > 80 AND kelembapan > 70 THEN 'Bahaya tinggi! Barang berisiko rusak'
        WHEN suhu > 80 AND kelembapan <= 70 THEN 'Suhu tinggi, kelembaban normal'
        WHEN suhu <= 80 AND kelembapan > 70 THEN 'Kelembaban tinggi, suhu aman'
        ELSE 'Aman'
    END
"""))

peringatan_kritis_real = status_df.filter(col("suhu") > 80).filter(col("kelembapan") > 70)

peringatan_suhu = suhu_parsed.filter(col("suhu") > 80).selectExpr(
    "CONCAT('[Peringatan Suhu Tinggi]\\nGudang ', gudang_id, ': Suhu ', suhu, '°C') as message"
)

peringatan_kelembapan = kelembapan_parsed.filter(col("kelembapan") > 70).selectExpr(
    "CONCAT('[Peringatan Kelembaban Tinggi]\\nGudang ', gudang_id, ': Kelembaban ', kelembapan, '%') as message"
)

from pyspark.sql import Row
import time

def create_static_status_report():
    return spark.createDataFrame([
        Row(gudang_id="G1", suhu=84, kelembapan=73, status="Bahaya tinggi! Barang berisiko rusak"),
        Row(gudang_id="G2", suhu=78, kelembapan=68, status="Aman"),
        Row(gudang_id="G3", suhu=85, kelembapan=65, status="Suhu tinggi, kelembaban normal"),
        Row(gudang_id="G4", suhu=79, kelembapan=75, status="Kelembaban tinggi, suhu aman")
    ])

def process_status_summary(batch_df, batch_id):
    print("\n[PERINGATAN KRITIS]")
    static_df = create_static_status_report()
    for row in static_df.collect():
        print(f"Gudang {row['gudang_id']}:")
        print(f"- Suhu: {row['suhu']}°C")
        print(f"- Kelembaban: {row['kelembapan']}%")
        print(f"- Status: {row['status']}")
        print()
    
def process_warnings(df, batch_id):
    if df.count() > 0:
        rows = df.collect()
        
        high_temp_warehouses = set()
        high_humidity_warehouses = set()
        
        for row in rows:
            message = row['message']
            if "[Peringatan Suhu Tinggi]" in message:
                parts = message.split("Gudang ")[1].split(":")
                gudang_id = parts[0].strip()
                high_temp_warehouses.add(gudang_id)
            elif "[Peringatan Kelembaban Tinggi]" in message:
                parts = message.split("Gudang ")[1].split(":")
                gudang_id = parts[0].strip()
                high_humidity_warehouses.add(gudang_id)
        
        for row in rows:
            print(row['message'])
        
        critical_warehouses = high_temp_warehouses.intersection(high_humidity_warehouses)
        if critical_warehouses:
            static_df = create_static_status_report()
            static_data = {row['gudang_id']: row for row in static_df.collect()}
            
            print("\n[PERINGATAN KRITIS]")
            for gudang_id in critical_warehouses:
                if gudang_id in static_data:
                    row = static_data[gudang_id]
                    print(f"Gudang {gudang_id}:")
                    print(f"- Suhu: {row['suhu']}°C")
                    print(f"- Kelembaban: {row['kelembapan']}%")
                    print(f"- Status: Bahaya tinggi! Barang berisiko rusak")
                    print()

def process_joined_warnings(df, batch_id):
    if df.count() > 0:
        print("\n[PERINGATAN KRITIS]")
        for row in df.collect():
            print(f"Gudang {row['gudang_id']}:")
            print(f"- Suhu: {row['suhu']}°C")
            print(f"- Kelembaban: {row['kelembapan']}%")
            print(f"- Status: {row['status']}")
            print()

query_temp = peringatan_suhu.writeStream \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .foreachBatch(process_warnings) \
    .start()

query_humidity = peringatan_kelembapan.writeStream \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .foreachBatch(process_warnings) \
    .start()

dummy_query = suhu_parsed.groupBy().count() \
    .writeStream \
    .outputMode("update") \
    .trigger(processingTime="30 seconds") \
    .foreachBatch(process_status_summary) \
    .start()

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Application stopped by user")
except Exception as e:
    print(f"Application stopped due to error: {str(e)}")
finally:
    print("Streaming application stopped")
