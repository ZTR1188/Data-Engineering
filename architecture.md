Architecture and Technical Choices

Overall Architecture

The platform follows a modern data engineering pipeline:



1\. Python Producer generates random sensor readings for temperature, humidity, pressure and publishes them to Kafka topic sensor-events with 10 percent anomalies.

2\. Kafka Cluster with 3 brokers in KRaft mode ingests messages reliably with replication factor 3.

3\. Spark Structured Streaming consumes from Kafka, parses JSON, validates values, detects anomalies, computes 5-minute windowed averages, and writes to three data lake zones.

4\. Data Lake on local filesystem stores raw JSON, curated Parquet, and consumption aggregates with Hive-style partitioning.

5\. Spark SQL runs analytical queries including top anomaly days, per-sensor statistics, daily temperature evolution, and partition pruning demonstration.

6\. Flask REST API exposes six endpoints for health check, sensor list, latest reading, daily statistics, anomalies list, and publishing new readings.



Technical Choices and Justifications



1\. Curated Zone Partitioning Strategy

Choice: Partition by sensor\_type, year, month and day.

Why: Most analytical queries filter by sensor type and date range. Partition pruning reads only matching directories and skips others. The alternative of partitioning by date only would scan all sensor types first and reduce performance.

Alternative considered: Partition only by year, month and day. Without sensor\_type, every query would scan all sensor directories.



2\. Spark Structured Streaming Output Mode

Choice: Use append mode for all sinks.

Why: Raw and curated zones are append-only with no updates. For windowed aggregation, append outputs each window only once when the watermark passes the window end. The alternative update mode would emit every micro-batch, causing many small files and duplicates.

Alternative considered: Update mode would produce more frequent writes but smaller files and potential duplicates.



3\. Replication Factor and min.insync.replicas

Choice: Set replication factor to 3 and min.insync.replicas to 2.

Why: Replication factor 3 tolerates one broker failure without data loss. min.insync.replicas equals 2 ensures writes are acknowledged by at least two brokers, preventing data loss if the leader crashes just after a write. The trade-off is higher write latency.

Alternative considered: Replication factor 2 with min.insync.replicas 1 would not survive two failures and risks data loss.



4\. Event Time vs Ingestion Time

Choice: Raw zone uses ingestion time from Kafka timestamp. Curated and consumption zones use event time from the sensor's own timestamp.

Why: Raw zone records when data arrived, which is useful for debugging pipeline delays. Curated zone needs actual measurement time for business queries such as average temperature on a specific date. A large gap between the two indicates pipeline backlog or network issues.

Alternative considered: Using ingestion time for all zones would misrepresent real-world measurements.



5\. End-to-End Delivery Semantics

Choice: At-least-once.

Why: Spark Structured Streaming with checkpointing guarantees no data is lost on failure. Idempotent writes because Parquet is immutable mean duplicates are acceptable for aggregated analytics. Exactly-once would require transactional sinks like Delta Lake and idempotent producers, which add complexity and are not needed for this use case.

Limitation: On failure, the same micro-batch may be replayed, producing duplicates. Downstream analytics must handle duplicates using DISTINCT or deduplication.



Summary

All technical choices balance performance, fault tolerance, and complexity. The platform meets the functional requirements while remaining reproducible on a single machine.

