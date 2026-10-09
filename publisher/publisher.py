import json
import os
import time
import random
from datetime import datetime, timezone
from proton import Message
from proton.utils import BlockingConnection

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASSWORD = os.environ["RABBITMQ_PASSWORD"]
QUEUE = "sensor-data"

connection = BlockingConnection(
    f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:5672"
)
sender = connection.create_sender(f"/queues/{QUEUE}")

try:
    while True:
        data = {
            "temperature": round(random.uniform(20, 35), 2),
            "humidity": round(random.uniform(35, 80), 2),
            "illuminance": round(random.uniform(100, 1000), 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
	    "pressure": round(random.uniform(990, 1030), 2),
	"air_quality": round(random.uniform(400, 1500), 2),
	"noise": round(random.uniform(30, 90), 2),
	"battery": round(random.uniform(20, 100), 2),
        }

        sender.send(Message(body=json.dumps(data)))
        print(f"Enviado: {data}", flush=True)
        time.sleep(10)

except KeyboardInterrupt:
    print("Publicador detenido.")
finally:
    connection.close()
