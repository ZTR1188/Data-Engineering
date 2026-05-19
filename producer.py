import argparse
import json
import time
import random
import signal
from kafka import KafkaProducer

BROKERS = ['localhost:9092', 'localhost:9094', 'localhost:9096']
TOPIC = 'sensor-events'
SENSORS = ['temperature', 'humidity', 'pressure']
RANGES = {
    'temperature': (15.0, 45.0),
    'humidity': (30.0, 95.0),
    'pressure': (980.0, 1040.0)
}
ANOMALY_RULES = {
    'temperature': lambda v: v > 35.0,
    'humidity': lambda v: v > 90.0,
    'pressure': lambda v: v < 990.0 or v > 1030.0
}

def gen_value(sensor, force_anomaly):
    low, high = RANGES[sensor]
    if force_anomaly:
        if sensor == 'temperature':
            low = max(low, 35.1)
        elif sensor == 'humidity':
            low = max(low, 90.1)
        else:
            if random.choice([True, False]):
                high = 989.9
            else:
                low = 1030.1
    return round(random.uniform(low, high), 2)

def create_producer():
    return KafkaProducer(
        bootstrap_servers=BROKERS,
        acks='all',
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=10,
        batch_size=32768,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', type=int, default=500)
    parser.add_argument('--rate', type=int, default=10)
    parser.add_argument('--source', type=str, default='exam-site')
    args = parser.parse_args()

    producer = create_producer()
    running = True
    def stop(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    sent = 0
    interval = 1.0 / args.rate if args.rate > 0 else 0
    try:
        while running and sent < args.count:
            sensor = random.choice(SENSORS)
            force_anomaly = (sent % 10 == 0)
            value = gen_value(sensor, force_anomaly)
            is_anomaly = ANOMALY_RULES[sensor](value)
            msg = {
                "sensor": sensor,
                "value": value,
                "unit": {"temperature": "C", "humidity": "%", "pressure": "hPa"}[sensor],
                "timestamp": int(time.time() * 1000),
                "source": args.source,
                "anomaly": is_anomaly
            }
            future = producer.send(TOPIC, key=sensor, value=msg)
            metadata = future.get(timeout=10)
            print(f"[{sent+1}] {sensor}={value} (anomaly={is_anomaly}) -> partition {metadata.partition}")
            sent += 1
            if interval > 0:
                time.sleep(interval)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.flush()
        producer.close()
        print(f"Sent {sent} messages.")

if __name__ == '__main__':
    main()