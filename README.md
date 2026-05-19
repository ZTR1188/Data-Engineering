AeroSense IoT Sensor Data Platform



1\. Overview

Build an end-to-end data engineering platform for IoT sensor data covering ingestion, stream processing, data lake storage, analytics, and REST API exposure. Technologies: Kafka KRaft with 3 brokers, Spark Structured Streaming 3.5.3, Parquet, Flask, Python 3.9+, Docker.



2\. Architecture

Producer -> Kafka (sensor-events topic, 3 partitions, RF=3) -> Spark Streaming (parse, validate, anomaly detection, 5min window, watermark) -> Data Lake (raw JSON, curated Parquet, consumption aggregates) -> Spark SQL analytics -> Flask REST API (6 endpoints)



3\. Instructions

Prerequisites: Docker Desktop, Python 3.9+, Java 11, virtual environment.

Installation:

cd ZHANG\_TAORAN\_exam

python -m venv venv

venv\\Scripts\\activate

pip install -r requirements.txt

docker compose up -d

docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --create --topic sensor-events --partitions 3 --replication-factor 3

Run pipeline:

python src/producer.py --count 500 --rate 10 --source exam-site

python src/spark\_pipeline.py

python src/analytics.py

python src/api/app.py

Test API:

curl -s http://localhost:5000/api/v1/health | python -m json.tool



4\. Technical Choices

Curated zone partitioning: sensor\_type/year/month/day for partition pruning. Spark output mode: append for all sinks; windowed aggregation outputs only when window finalizes. Replication factor 3, min.insync.replicas 2: tolerates one broker failure. Raw zone uses ingestion time; curated and consumption zones use event time for business logic. Delivery semantics: at-least-once with checkpointing.



5\. Results

Analytical queries: top anomaly days, per-sensor statistics, daily temperature evolution. Partition pruning speedup: 1.4x. API health check returns success JSON. Parquet files written to C:\\tmp\\datalake\\curated\\domain=iot.



6\. Limitations and Improvements

Limitations: single-node, no horizontal scaling, checkpoints stored locally. Improvements: add Delta Lake for exactly-once, deploy on Kubernetes, add API authentication.

