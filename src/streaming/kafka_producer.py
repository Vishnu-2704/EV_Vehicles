from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


stations = [f"S{i:03d}" for i in range(1, 51)]
customers = [f"C{i:04d}" for i in range(1, 1001)]
vehicles = [f"V{i:04d}" for i in range(1, 1501)]


print("EV Kafka Producer Started...")
print("Sending events to: ev-charging-events")


while True:

    event = {
        "transaction_id": f"STREAM_{int(time.time() * 1000)}",
        "customer_id": random.choice(customers),
        "vehicle_id": random.choice(vehicles),
        "station_id": random.choice(stations),
        "start_time": datetime.now().isoformat(),
        "energy_kwh": round(random.uniform(5, 50), 2),
        "amount_paid": round(random.uniform(50, 500), 2),
        "status": random.choice(["COMPLETED", "COMPLETED", "CHARGING"])
    }

    producer.send(
        "ev-charging-events",
        value=event
    )

    producer.flush()

    print("Sent:", event)

    time.sleep(0.5)
