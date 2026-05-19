import json
from kafka import KafkaConsumer, KafkaProducer
from typing import List, Dict, Any

KAFKA_BROKERS = "localhost:9092,localhost:9094,localhost:9096"
TOPIC = "sensor-events"

def get_latest_readings(sensor_type: str, n: int = 1) -> List[Dict[str, Any]]:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='latest',
        enable_auto_commit=False,
        consumer_timeout_ms=2000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
    )
    partitions = consumer.assignment()
    if not partitions:
        consumer.close()
        return []
    end_offsets = consumer.end_offsets(partitions)
    readings = []
    for tp, offset in end_offsets.items():
        if offset == 0:
            continue
        consumer.seek(tp, offset - 1)
        msg = next(consumer, None)
        if msg and msg.key == sensor_type:
            readings.append(msg.value)
    consumer.close()
    readings.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
    return readings[:n]

def publish_reading(reading: Dict[str, Any]) -> Dict[str, Any]:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
    )
    future = producer.send(TOPIC, key=reading['sensor'], value=reading)
    metadata = future.get(timeout=5)
    producer.close()
    return {"partition": metadata.partition, "offset": metadata.offset}