import json
from kafka import KafkaConsumer, KafkaProducer

# ---------------- Kafka Configuration ----------------
consumer = KafkaConsumer(
    "flights_raw",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="data-quality-group-final",
    value_deserializer=lambda x: x.decode("utf-8")
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

VALID_TOPIC = "flights_valid"
INVALID_TOPIC = "flights_not_valid"

# ---------------- Schema Definition ----------------
REQUIRED_FIELDS = {
    "ORIGIN_COUNTRY_NAME": str,
    "DEST_COUNTRY_NAME": str,
    "count": int
}

# ---------------- Validation Function ----------------
def validate_record(record):
    # 1️⃣ Must be a JSON object
    if not isinstance(record, dict):
        return False, "Not a JSON object"

    # 2️⃣ Schema & data types
    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in record:
            return False, f"Missing field: {field}"
        if not isinstance(record[field], expected_type):
            return False, f"Invalid type for {field}"

    # 3️⃣ Business rules
    if not record["ORIGIN_COUNTRY_NAME"].strip():
        return False, "Empty ORIGIN_COUNTRY_NAME"

    if not record["DEST_COUNTRY_NAME"].strip():
        return False, "Empty DEST_COUNTRY_NAME"

    if record["count"] < 0:
        return False, "Negative count"

    return True, "Valid"

# ---------------- Streaming Processing ----------------
for message in consumer:
    raw_value = message.value

    # 1️⃣ Safe JSON parsing
    try:
        record = json.loads(raw_value)
    except json.JSONDecodeError:
        producer.send(INVALID_TOPIC, {
            "error": "Malformed JSON",
            "raw_data": raw_value
        })
        print("❌ Malformed JSON")
        continue

    # 2️⃣ Validation
    is_valid, reason = validate_record(record)

    # 3️⃣ Routing
    if is_valid:
        producer.send(VALID_TOPIC, record)
        print("✅ Valid record routed")
    else:
        producer.send(INVALID_TOPIC, {
            "error": reason,
            "record": record
        })
        print("❌ Invalid record routed:", reason)


