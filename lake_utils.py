from pyspark.sql import SparkSession
from typing import List, Dict, Any

def _get_spark():
    return SparkSession.builder.master("local[*]").appName("LakeUtils").getOrCreate()

def get_sensor_types() -> List[str]:
    spark = _get_spark()
    df = spark.read.parquet("/tmp/datalake/curated/domain=iot")
    types = [row.sensor_type for row in df.select("sensor_type").distinct().collect()]
    spark.stop()
    return types

def get_statistics(sensor_type: str, days: int) -> List[Dict[str, Any]]:
    spark = _get_spark()
    df = spark.read.parquet("/tmp/datalake/curated/domain=iot")
    from pyspark.sql.functions import col, avg, min, max, count
    filtered = df.filter(col("sensor_type") == sensor_type)
    grouped = filtered.groupBy("year", "month", "day") \
        .agg(
            count("value").alias("record_count"),
            avg("value").alias("avg_value"),
            min("value").alias("min_value"),
            max("value").alias("max_value")
        ).orderBy("year", "month", "day")
    stats = grouped.collect()
    spark.stop()
    result = []
    for r in stats[-days:]:
        result.append({
            "date": f"{r.year}-{r.month:02d}-{r.day:02d}",
            "record_count": r.record_count,
            "avg_value": r.avg_value,
            "min_value": r.min_value,
            "max_value": r.max_value,
        })
    return result

def get_anomalies(sensor_type: str, limit: int) -> List[Dict[str, Any]]:
    spark = _get_spark()
    df = spark.read.parquet("/tmp/datalake/curated/domain=iot")
    filtered = df.filter(col("sensor_type") == sensor_type) \
                 .filter(col("is_anomaly") == True) \
                 .orderBy(col("event_time").desc()) \
                 .limit(limit)
    rows = filtered.collect()
    spark.stop()
    return [{"value": r.value, "timestamp": r.event_time.isoformat()} for r in rows]