import json
import random
import time

import paho.mqtt.client as mqtt

MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
MQTT_TOPIC = "lab5/mediciones"

client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="lab5-publicador",
)

client.connect(MQTT_HOST, MQTT_PORT, 60)
client.loop_start()

try:
    while True:
        medicion = {
            "temperature": round(random.uniform(18, 28), 2),
            "humidity": round(random.uniform(30, 65), 2),
            "illuminance": round(random.uniform(100, 500), 2),
            "co2": round(random.uniform(400, 1200), 2),
            "noise": round(random.uniform(30, 65), 2),
        }

        resultado = client.publish(
            MQTT_TOPIC,
            json.dumps(medicion),
            qos=1,
        )
        resultado.wait_for_publish()

        print(f"Publicado MQTT - Cuarto hospitalario: {medicion}")
        time.sleep(10)

except KeyboardInterrupt:
    print("Publicador detenido")
finally:
    client.loop_stop()
    client.disconnect()
