from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

# Create SparkSession with Kafka package
spark = SparkSession.builder \
    .appName("KafkaSensorConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema_suhu = StructType() \
    .add("gudang_id", StringType()) \
    .add("suhu", IntegerType())

schema_kelembaban = StructType() \
    .add("gudang_id", StringType()) \
    .add("kelembaban", IntegerType())

kafka_bootstrap_servers = "localhost:9092"

suhu_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "sensor-suhu-gudang") \
    .load()

suhu_parsed = suhu_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_suhu).alias("data")) \
    .select("data.*")

kelembaban_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load()

kelembaban_parsed = kelembaban_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_kelembaban).alias("data")) \
    .select("data.*")

peringatan_suhu = suhu_parsed.filter(col("suhu") > 80)
peringatan_kelembaban = kelembaban_parsed.filter(col("kelembaban") > 70)

peringatan_suhu.writeStream.outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

peringatan_kelembaban.writeStream.outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

spark.streams.awaitAnyTermination()

