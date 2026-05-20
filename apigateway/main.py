from importlib import metadata

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer, KafkaProducer
import httpx
import os
import json
import time
import uuid
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")

app = FastAPI()
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"], # Allow your Angular/React app
    allow_methods=["*"],
    allow_headers=["*"],
)

FARES_API_BASE_URL = os.environ.get("FARES_API_BASE_URL", "http://fares-api:8000")

@app.get("/")
async def root():
    return {"message": "APIGateway Service"}



@app.get("/faresapi/get-fares/{flight_id}")
async def get_fares(flight_id: str):
    """Proxy request to the faresapi get fares endpoint."""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{FARES_API_BASE_URL}/get-fares/{flight_id}", timeout=10.0)
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"Failed to connect to faresapi: {exc}")

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()


@app.get("/faresapi/search-fares")
async def search_fares(origin: str | None = None, destination: str | None = None):
    """Proxy request to the faresapi search fares endpoint."""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{FARES_API_BASE_URL}/search-fares",
                params={"origin": origin, "destination": destination},
                timeout=10.0,
            )
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"Failed to connect to faresapi: {exc}")

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()


def wait_for_flight_status_response(consumer: KafkaConsumer, correlation_id: str, timeout: float = 10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        records = consumer.poll(timeout_ms=500)
        for messages in records.values():
            for message in messages:
                value = message.value
                if value.get("correlation_id") == correlation_id:
                    return value
    return None


@app.get("/flights/get-flight-status/{flight_id}")
def read_item(flight_id: str, q: str | None = None):
    correlation_id = str(uuid.uuid4())
    data = {"flight_id": flight_id, "correlation_id": correlation_id}

    consumer = KafkaConsumer(
        "flight-status-response",
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    try:
        consumer.poll(timeout_ms=100)
        future = producer.send("get-flight-status", value=data)
        metadata = future.get(timeout=10)
        logger.info(f"Message sent to topic: {metadata.topic}")
        logger.info(f"Partition assigned: {metadata.partition}")
        logger.info(f"Offset assigned: {metadata.offset}")

        response = wait_for_flight_status_response(consumer, correlation_id, timeout=10.0)
        if response is None:
            raise HTTPException(status_code=504, detail="Timed out waiting for flight status response")

        if response.get("error"):
            raise HTTPException(status_code=404, detail=response["error"])

        return response
    except Exception as e:
        logger.error(f"Error while waiting for Kafka response: {e}")
        raise HTTPException(status_code=502, detail=str(e))
    finally:
        consumer.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)