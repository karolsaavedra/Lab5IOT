import json
import os
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
MQTT_TOPIC = "lab5/mediciones"

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = os.environ["INFLUX_TOKEN"]
INFLUX_ORG = "lab5"
INFLUX_BUCKET = "mediciones"

db = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG,
)
write_api = db.write_api(write_options=SYNCHRONOUS)


def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print("Conectado a Mosquitto")
        client.subscribe(MQTT_TOPIC)
        print(f"Escuchando: {MQTT_TOPIC}")
    else:
        print(f"Error de conexión MQTT: {reason_code}")


def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode("utf-8"))

        campos = [
            "temperature",
            "humidity",
            "illuminance",
            "co2",
            "noise",
        ]

        point = (
            Point("mediciones_ambientales")
            .tag("source", "mqtt")
            .tag("device", "ambiente_hospitalario")
        )

        for campo in campos:
            point = point.field(campo, float(data[campo]))

        point = point.time(datetime.now(timezone.utc), WritePrecision.S)

        write_api.write(bucket=INFLUX_BUCKET, record=point)
        print(f"Guardado MQTT en InfluxDB: {data}")

    except (ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"Mensaje MQTT inválido: {error}")
    except Exception as error:
        print(f"Error escribiendo en InfluxDB: {error}")


client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="lab5-consumidor",
)
client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_forever()
except KeyboardInterrupt:
    print("Recolector detenido")
finally:
    client.disconnect()
    write_api.close()
    db.close()
