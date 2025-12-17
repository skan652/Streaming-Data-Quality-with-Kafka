from kafka import KafkaProducer
import time

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: v.encode("utf-8")  # 🔥 FIX
)

TOPIC = "flights_raw"

with open("flights_summary.json", "r", encoding="utf-8") as file:
    for line in file:
        line = line.strip()
        if not line:
            continue

        producer.send(TOPIC, line)
        print("📤 Sent:", line)
        time.sleep(0.2)