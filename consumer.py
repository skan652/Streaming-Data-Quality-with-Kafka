import json
from kafka import KafkaConsumer, KafkaProducer

# ---------------- Kafka Config ----------------
consumer = KafkaConsumer(
    "flights_raw",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="valid-only-consumer",
    value_deserializer=lambda x: x.decode("utf-8")
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

VALID_TOPIC = "flights_valid"

# ---------------- Schema + Defaults ----------------
SCHEMA = {
    "ORIGIN_COUNTRY_NAME": ("UNKNOWN", str),
    "DEST_COUNTRY_NAME": ("UNKNOWN", str),
    "count": (0, int)
}

# ---------------- Safe JSON Parser ----------------
def safe_json_load(raw):
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        try:
            fixed = raw.replace("'", '"').strip()
            return json.loads(fixed)
        except Exception:
            return None

# ---------------- Cleaning Function ----------------
def clean_record(record):
    cleaned = {}
    for field, (default, expected_type) in SCHEMA.items():
        if field not in record:
            cleaned[field] = default
            continue

        value = record[field]

        if expected_type == int:
            try:
                cleaned[field] = int(value)
            except (ValueError, TypeError):
                return None  # Cannot fix → drop
        elif expected_type == str:
            if value is None or str(value).strip() == "":
                cleaned[field] = default
            else:
                cleaned[field] = str(value).strip()
    return cleaned

# ---------------- Validation ----------------
def is_valid(record):
    if record["count"] < 0:
        return False
    if not record["ORIGIN_COUNTRY_NAME"] or not record["DEST_COUNTRY_NAME"]:
        return False
    return True

# ---------------- Stream Processing ----------------
for msg in consumer:
    raw_value = msg.value

    # 1️⃣ Parse JSON safely
    record = safe_json_load(raw_value)
    if record is None or not isinstance(record, dict):
        continue  # drop unfixable

    # 2️⃣ Clean record
    cleaned_record = clean_record(record)
    if cleaned_record is None:
        continue  # drop unfixable

    # 3️⃣ Validate
    if is_valid(cleaned_record):
        producer.send(VALID_TOPIC, cleaned_record)
        print("✅ Valid record:", cleaned_record)
    else:
        continue  # drop in





