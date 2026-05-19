from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

LAKE_ROOT = "C:/tmp/datalake"
RAW_PATH = f"{LAKE_ROOT}/raw/source=kafka/topic=sensor-events"
CURATED_PATH = f"{LAKE_ROOT}/curated/domain=iot"
CONSUMPTION_PATH = f"{LAKE_ROOT}/consumption/use_case=sensor_averages"
CKPT_RAW = "C:/tmp/checkpoints/raw"
CKPT_CURATED = "C:/tmp/checkpoints/curated"
CKPT_CONSUMPTION = "C:/tmp/checkpoints/consumption"

KAFKA_BROKERS = "localhost:9092,localhost:9094,localhost:9096"
TOPIC = "sensor-events"

SCHEMA = StructType([
    StructField("sensor", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("source", StringType(), True),
    StructField("anomaly", BooleanType(), True)
])

spark = SparkSession.builder \
    .appName("ExamPipeline") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed = raw_stream.select(
    from_json(col("value").cast("string"), SCHEMA).alias("data"),
    col("timestamp").alias("kafka_ts")
).select("data.*", "kafka_ts")

parsed = parsed.withColumn("event_time", to_timestamp(col("timestamp") / 1000))

parsed = parsed.filter(
    (col("sensor") == "temperature") & (col("value").between(15, 45)) |
    (col("sensor") == "humidity") & (col("value").between(30, 95)) |
    (col("sensor") == "pressure") & (col("value").between(980, 1040))
)

parsed = parsed.withColumn("is_anomaly_detected",
    when(col("sensor") == "temperature", col("value") > 35.0)
    .when(col("sensor") == "humidity", col("value") > 90.0)
    .when(col("sensor") == "pressure", (col("value") < 990.0) | (col("value") > 1030.0))
    .otherwise(lit(False))
)

raw_zone = parsed.select(
    to_json(struct("*")).alias("raw_json"),
    col("kafka_ts").alias("ingestion_ts"),
    year(col("kafka_ts")).alias("year"),
    month(col("kafka_ts")).alias("month"),
    dayofmonth(col("kafka_ts")).alias("day"),
    hour(col("kafka_ts")).alias("hour")
)
raw_query = raw_zone.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", RAW_PATH) \
    .option("checkpointLocation", CKPT_RAW) \
    .partitionBy("year", "month", "day", "hour") \
    .trigger(processingTime="30 seconds") \
    .start()

curated_zone = parsed.select(
    col("sensor").alias("sensor_type"),
    col("value"),
    col("unit"),
    col("event_time"),
    col("is_anomaly_detected").alias("is_anomaly"),
    col("source"),
    year(col("event_time")).alias("year"),
    month(col("event_time")).alias("month"),
    dayofmonth(col("event_time")).alias("day")
)
curated_query = curated_zone.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", CURATED_PATH) \
    .option("checkpointLocation", CKPT_CURATED) \
    .option("compression", "snappy") \
    .partitionBy("sensor_type", "year", "month", "day") \
    .trigger(processingTime="30 seconds") \
    .start()

windowed = parsed \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("sensor").alias("sensor_type")
    ).agg(
        avg("value").alias("avg_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value"),
        count("value").alias("observation_count"),
        sum(col("is_anomaly_detected").cast("int")).alias("anomaly_count")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sensor_type"),
        col("avg_value"),
        col("min_value"),
        col("max_value"),
        col("observation_count"),
        col("anomaly_count")
    ).withColumn("year", year(col("window_start"))) \
     .withColumn("month", month(col("window_start"))) \
     .withColumn("day", dayofmonth(col("window_start")))

consumption_query = windowed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", CONSUMPTION_PATH) \
    .option("checkpointLocation", CKPT_CONSUMPTION) \
    .partitionBy("sensor_type", "year", "month") \
    .trigger(processingTime="30 seconds") \
    .start()

print("All streams started. Press Ctrl+C to stop.")
spark.streams.awaitAnyTermination()