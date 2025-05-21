from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.streaming import StreamingQueryException
import logging

logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
logging.getLogger("py4j").setLevel(logging.ERROR)

spark = SparkSession.builder \
    .appName("Kafka-Sensor-Consumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .master("local[*]") \
    .getOrCreate()

schema_suhu = StructType() \
    .add("gudang_id", StringType()) \
    .add("suhu", IntegerType())

schema_kelembapan = StructType() \
    .add("gudang_id", StringType()) \
    .add("kelembapan", IntegerType())  

kafka_bootstrap_servers = "localhost:9092"

suhu_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "sensor-suhu-gudang") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.request.timeout.ms", "120000") \
    .option("kafka.session.timeout.ms", "120000") \
    .option("kafka.max.poll.interval.ms", "300000") \
    .option("kafka.heartbeat.interval.ms", "10000") \
    .option("kafka.reconnect.backoff.ms", "5000") \
    .option("kafka.reconnect.backoff.max.ms", "30000") \
    .load()

suhu_parsed = suhu_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_suhu).alias("data")) \
    .select("data.*")

kelembapan_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "sensor-kelembapan-gudang") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.request.timeout.ms", "60000") \
    .option("kafka.session.timeout.ms", "60000") \
    .load()

kelembapan_parsed = kelembapan_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_kelembapan).alias("data")) \
    .select("data.*")

peringatan_suhu = suhu_parsed.filter(col("suhu") > 80)
peringatan_kelembapan = kelembapan_parsed.filter(col("kelembapan") > 70)

def process_batch(df, epoch_id):
    rows = df.collect()
    for row in rows:
        print(row["message"])

# Format warning messages
temp_warnings = peringatan_suhu.selectExpr(
    "CONCAT('[High Temperature Warning] Warehouse ', gudang_id, ': Temperature ', suhu, '°C') as message"
)

humidity_warnings = peringatan_kelembapan.selectExpr(
    "CONCAT('[High Humidity Warning] Warehouse ', gudang_id, ': Humidity ', kelembapan, '%') as message"
)

query1 = temp_warnings.writeStream.outputMode("append") \
    .foreachBatch(process_batch) \
    .trigger(processingTime="10 seconds") \
    .start()

query2 = humidity_warnings.writeStream.outputMode("append") \
    .foreachBatch(process_batch) \
    .trigger(processingTime="10 seconds") \
    .start()

try:
    spark.streams.awaitAnyTermination()
except StreamingQueryException as e:
    print(f"Query terminated with error: {e.message}")
finally:
    print("Streaming application stopped")

