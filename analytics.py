from pyspark.sql import SparkSession
import time
import os

spark = SparkSession.builder \
    .appName("Analytics") \
    .master("local[*]") \
    .getOrCreate()

CURATED_PATH = "C:/tmp/datalake/curated/domain=iot"
OUTPUT_DIR = "outputs/analytics"
os.makedirs(OUTPUT_DIR, exist_ok=True)

df = spark.read.parquet(CURATED_PATH)
df.createOrReplaceTempView("curated")

q1 = spark.sql("""
    SELECT year, month, day, SUM(CAST(is_anomaly AS INT)) as anomalies
    FROM curated
    GROUP BY year, month, day
    ORDER BY anomalies DESC
    LIMIT 5
""")
q1.show()
q1.write.csv(f"{OUTPUT_DIR}/q1_top5_anomaly_days.csv", header=True, mode="overwrite")

q2 = spark.sql("""
    SELECT sensor_type,
           AVG(value) AS mean_value,
           MIN(value) AS min_value,
           MAX(value) AS max_value,
           STDDEV(value) AS stddev_value,
           SUM(CAST(is_anomaly AS INT)) / COUNT(*) * 100 AS anomaly_rate_pct
    FROM curated
    GROUP BY sensor_type
""")
q2.show()
q2.write.csv(f"{OUTPUT_DIR}/q2_sensor_stats.csv", header=True, mode="overwrite")

q3 = spark.sql("""
    SELECT year, month, day,
           AVG(value) AS avg_temp,
           SUM(CAST(is_anomaly AS INT)) AS anomaly_count
    FROM curated
    WHERE sensor_type = 'temperature'
    GROUP BY year, month, day
    ORDER BY year, month, day
""")
q3.show()
q3.write.csv(f"{OUTPUT_DIR}/q3_temperature_daily.csv", header=True, mode="overwrite")

print("\n=== Partition Pruning Demo ===")
start = time.time()
count_all = spark.sql("SELECT COUNT(*) FROM curated").collect()[0][0]
full_time = time.time() - start
print(f"Full scan: {count_all} records, {full_time:.2f}s")

start = time.time()
count_filtered = spark.sql("SELECT COUNT(*) FROM curated WHERE sensor_type='temperature' AND year=2026 AND month=5").collect()[0][0]
pruned_time = time.time() - start
print(f"Filtered scan: {count_filtered} records, {pruned_time:.2f}s")
print(f"Speedup: {full_time / pruned_time:.1f}x")

with open(f"{OUTPUT_DIR}/pruning_speedup.txt", "w") as f:
    f.write(f"Full scan time: {full_time:.2f}s\nPruned scan time: {pruned_time:.2f}s\nSpeedup: {full_time / pruned_time:.1f}x")

spark.stop()