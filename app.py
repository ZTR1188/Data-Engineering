from flask import Flask, jsonify, request
from datetime import datetime, timezone
from kafka_utils import get_latest_readings, publish_reading
from lake_utils import get_statistics, get_sensor_types, get_anomalies

app = Flask(__name__)
API_PREFIX = "/api/v1"
VALID_SENSORS = {"temperature", "humidity", "pressure"}

def error(message, code):
    return jsonify({"status": "error", "code": code, "message": message}), code

def success(data, code=200):
    return jsonify({"status": "success", "data": data}), code

@app.route(f"{API_PREFIX}/health")
def health():
    return success({"status": "ok", "version": "1.0", "timestamp": datetime.now(timezone.utc).isoformat()})

@app.route(f"{API_PREFIX}/sensors")
def list_sensors():
    try:
        types = get_sensor_types()
        return success({"sensors": types})
    except Exception as e:
        app.logger.error(f"list_sensors: {e}")
        return error("Failed to retrieve sensors", 500)

@app.route(f"{API_PREFIX}/sensors/<sensor_type>/latest")
def latest_reading(sensor_type):
    if sensor_type not in VALID_SENSORS:
        return error(f"Unknown sensor type '{sensor_type}'", 404)
    try:
        readings = get_latest_readings(sensor_type, 1)
        if not readings:
            return error(f"No readings for {sensor_type}", 404)
        return success(readings[0])
    except Exception as e:
        app.logger.error(f"latest_reading: {e}")
        return error("Failed to fetch latest reading", 500)

@app.route(f"{API_PREFIX}/sensors/<sensor_type>/stats")
def sensor_stats(sensor_type):
    if sensor_type not in VALID_SENSORS:
        return error(f"Unknown sensor type '{sensor_type}'", 404)
    try:
        days = int(request.args.get("days", 7))
        if days < 1 or days > 90:
            raise ValueError
    except (ValueError, TypeError):
        return error("Invalid 'days' parameter, must be integer 1-90", 400)
    try:
        stats = get_statistics(sensor_type, days)
        return success({"sensor_type": sensor_type, "days": days, "data": stats})
    except Exception as e:
        app.logger.error(f"sensor_stats: {e}")
        return error("Failed to retrieve statistics", 500)

@app.route(f"{API_PREFIX}/anomalies")
def anomalies():
    sensor = request.args.get("sensor")
    if not sensor:
        return error("Missing 'sensor' parameter", 400)
    if sensor not in VALID_SENSORS:
        return error(f"Invalid sensor type '{sensor}'", 400)
    try:
        limit = int(request.args.get("limit", 10))
        if limit < 1 or limit > 100:
            raise ValueError
    except (ValueError, TypeError):
        return error("Invalid 'limit', must be integer 1-100", 400)
    try:
        anom = get_anomalies(sensor, limit)
        return success({"sensor_type": sensor, "limit": limit, "anomalies": anom})
    except Exception as e:
        app.logger.error(f"anomalies: {e}")
        return error("Failed to fetch anomalies", 500)

@app.route(f"{API_PREFIX}/readings", methods=["POST"])
def create_reading():
    body = request.get_json(silent=True)
    if not body:
        return error("Invalid JSON body", 400)
    required = {"sensor", "value"}
    if not required.issubset(body.keys()):
        missing = required - set(body.keys())
        return error(f"Missing fields: {missing}", 400)
    sensor = body["sensor"]
    if sensor not in VALID_SENSORS:
        return error(f"Unknown sensor type '{sensor}'", 422)
    try:
        value = float(body["value"])
    except (ValueError, TypeError):
        return error("'value' must be a number", 422)
    reading = {
        "sensor": sensor,
        "value": value,
        "unit": body.get("unit", ""),
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "source": "api",
        "anomaly": False
    }
    try:
        meta = publish_reading(reading)
        return success({"reading": reading, "partition": meta["partition"], "offset": meta["offset"]}, 201)
    except Exception as e:
        app.logger.error(f"create_reading: {e}")
        return error("Failed to publish to Kafka", 500)

@app.errorhandler(404)
def handle_404(e):
    return error("Resource not found", 404)

@app.errorhandler(405)
def handle_405(e):
    return error("Method not allowed", 405)

@app.errorhandler(500)
def handle_500(e):
    return error("Internal server error", 500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)