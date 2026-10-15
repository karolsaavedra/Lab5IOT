import asyncio
import json
import os
from datetime import datetime, timezone

import aiocoap
import aiocoap.resource as resource
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

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


class Mediciones(resource.Resource):
    async def render_post(self, request):
        try:
            data = json.loads(request.payload.decode("utf-8"))

            campos = [
                "pressure",
                "pm25",
                "pm10",
                "voc",
                "air_changes",
            ]

            point = (
                Point("mediciones_ambientales")
                .tag("source", "coap")
                .tag("device", "ventilacion_hospitalaria")
            )

            for campo in campos:
                point = point.field(campo, float(data[campo]))

            point = point.time(
                datetime.now(timezone.utc),
                WritePrecision.S,
            )

            await asyncio.to_thread(
                write_api.write,
                bucket=INFLUX_BUCKET,
                record=point,
            )

            print(f"Medición CoAP guardada: {data}", flush=True)

            return aiocoap.Message(
                code=aiocoap.CHANGED,
                payload=b"Medicion guardada",
            )

        except (ValueError, KeyError, json.JSONDecodeError) as error:
            return aiocoap.Message(
                code=aiocoap.BAD_REQUEST,
                payload=str(error).encode("utf-8"),
            )


async def main():
    root = resource.Site()
    root.add_resource(["mediciones"], Mediciones())

    protocolo = await aiocoap.Context.create_server_context(
        root,
        bind=("0.0.0.0", 5683),
    )

    print("Servidor CoAP escuchando en UDP 5683", flush=True)

    try:
        await asyncio.get_running_loop().create_future()
    finally:
        await protocolo.shutdown()
        write_api.close()
        db.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Servidor CoAP detenido")
