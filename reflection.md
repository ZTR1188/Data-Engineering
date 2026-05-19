1.Raw zone data is safe; curated zone loses the batch. Checkpoint on curated zone prevents this.

2.Kafka network and Spark driver memory. Fix by increasing partitions, adding more executors, and tuning batch size.

3.Kafka: low latency, replayable, but expensive for long storage. Parquet: cheap, columnar, but no real-time. Prefer Kafka for short-term hot data, Parquet for historical analytics.

4.Anomaly detection flags values outside thresholds. Isolate by adding an is\_corrupt column and filtering queries.

5.Modify producer.py (add CO2 to SENSORS, ranges, anomaly rule), spark\_pipeline.py (add CO2 to validation and anomaly detection), lake\_utils.py and app.py (add CO2 to VALID\_SENSORS).

