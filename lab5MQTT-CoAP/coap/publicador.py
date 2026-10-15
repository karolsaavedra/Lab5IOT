import asyncio
import json
import random

import aiocoap


async def main():
    protocolo = await aiocoap.Context.create_client_context()
    destino = "coap://127.0.0.1:5683/mediciones"

    try:
        while True:
            medicion = {
                "pressure": round(random.uniform(-15, 15), 2),
                "pm25": round(random.uniform(0, 35), 2),
                "pm10": round(random.uniform(0, 60), 2),
                "voc": round(random.uniform(50, 500), 2),
                "air_changes": round(random.uniform(4, 12), 2),
            }

            solicitud = aiocoap.Message(
                code=aiocoap.POST,
                uri=destino,
                payload=json.dumps(medicion).encode("utf-8"),
                content_format=50,
            )

            respuesta = await protocolo.request(solicitud).response

            print(
                f"Publicado CoAP - Ventilación hospitalaria: {medicion} | "
                f"Respuesta: {respuesta.code}"
            )

            await asyncio.sleep(10)

    except asyncio.CancelledError:
        pass
    finally:
        await protocolo.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Publicador CoAP detenido")
