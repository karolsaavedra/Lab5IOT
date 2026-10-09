import json
import os
import time
from datetime import datetime
from proton import Message
from proton.utils import BlockingConnection
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASSWORD = os.environ["RABBITMQ_PASSWORD"]

INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.environ["INFLUXDB_TOKEN"]
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "lab5")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensores")

QUEUE = "sensor-data"

client = InfluxDBClient(
    url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG
)
write_api = client.write_api(write_options=SYNCHRONOUS)

connection = BlockingConnection(
    f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:5672"
)
receiver = connection.create_receiver(f"/queues/{QUEUE}")

print("Consumidor iniciado. Esperando mediciones...", flush=True)

try:
    while True:
        message = receiver.receive(timeout=30)

        if message is None:
            continue

        try:
            data = json.loads(message.body)

            point = (
                Point("environment")
                .field("temperature", float(data["temperature"]))
                .field("humidity", float(data["humidity"]))
                .field("illuminance", float(data["illuminance"]))

.field("pressure", float(data["pressure"]))
.field("air_quality", float(data["air_quality"]))
.field("noise", float(data["noise"]))
.field("battery", float(data["battery"]))
                .time(
                    datetime.fromisoformat(
                        data["timestamp"].replace("Z", "+00:00")
                    ),
                    WritePrecision.S
                )
            )

            write_api.write(bucket=INFLUXDB_BUCKET, record=point)
            receiver.accept()
            print(f"Guardado en InfluxDB: {data}", flush=True)

        except Exception as error:
            print(f"Error procesando mensaje: {error}", flush=True)
            time.sleep(2)

except KeyboardInterrupt:
    print("Consumidor detenido.")

finally:
    connection.close()
    client.close()
