import os
import json
from typing import List, Dict, Any, Optional
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel, ValidationError
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

flightData: List[Dict[str, Any]] = [
    {
        "flight_id": "BA501",
        "origin": "London",
        "destination": "New York",
        "airport": "JFK",
        "door": "A12",
        "status": "On Time",
        "departure_time": "2024-06-01T08:00:00Z",
        "arrival_time": "2024-06-01T11:00:00Z"
    },
    {
        "flight_id": "AA100",
        "origin": "New York",
        "destination": "Los Angeles",
        "airport": "LAX",
        "door": "B7",
        "status": "Delayed",
        "departure_time": "2024-06-01T09:00:00Z",
        "arrival_time": "2024-06-01T12:30:00Z"
    },
    {
        "flight_id": "DL200",
        "origin": "Los Angeles",
        "destination": "Chicago",
        "airport": "ORD",
        "door": "C3",
        "status": "Cancelled",
        "departure_time": "2024-06-01T10:00:00Z",
        "arrival_time": "2024-06-01T13:00:00Z"
    }
]


class FlightStatusRequest(BaseModel):
    flight_id: str
    correlation_id: Optional[str] = None
    origin: Optional[str] = None
    destination: Optional[str] = None


class UpdateFlightStatusRequest(BaseModel):
    flight_id: str
    airport: Optional[str] = None
    status: Optional[str] = None
    door: Optional[str] = None
    correlation_id: Optional[str] = None


class FlightStatusResponse(BaseModel):
    flight_id: str
    origin: str
    destination: str
    airport: str
    door: str
    status: str
    departure_time: str
    arrival_time: str


class FlightStatusService:
    request_topic = "get-flight-status"
    update_topic = "update-flight-status"
    response_topic = "flight-status-response"

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = KafkaConsumer(
            self.request_topic,
            self.update_topic,
            bootstrap_servers=self.bootstrap_servers.split(","),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="flight-status-service",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )

    def _find_flight(self, flight_id: str) -> Optional[Dict[str, Any]]:
        for flight in flightData:
            if flight["flight_id"] == flight_id:
                return flight
        return None

    def get_flight_status(self, request: FlightStatusRequest) -> FlightStatusResponse:
        flight = self._find_flight(request.flight_id)
        if flight is None:
            raise ValueError(f"Flight not found: {request.flight_id}")
        return FlightStatusResponse(**flight)

    def update_flight_status(self, request: UpdateFlightStatusRequest) -> FlightStatusResponse:
        flight = self._find_flight(request.flight_id)
        if flight is None:
            raise ValueError(f"Flight not found: {request.flight_id}")

        if request.door is not None:
            if request.airport is None:
                raise ValueError("Airport must be provided to update door")
            if request.airport != flight["airport"]:
                raise ValueError(
                    f"Cannot update door because airport does not match current flight airport ({flight['airport']})"
                )
            flight["door"] = request.door

        if request.status is not None:
            flight["status"] = request.status

        return FlightStatusResponse(**flight)

    def _create_response_payload(self, response: FlightStatusResponse, correlation_id: Optional[str] = None) -> Dict[str, Any]:
        payload = response.dict()
        if correlation_id is not None:
            payload["correlation_id"] = correlation_id
        return payload

    def _handle_update_message(self, message: Any) -> None:
        try:
            request = UpdateFlightStatusRequest(**message.value)
        except ValidationError as exc:
            logger.error(f"Invalid update payload: {exc}")
            return

        try:
            response = self.update_flight_status(request)
            response_payload = self._create_response_payload(response, request.correlation_id)
            logger.info(f"Updated flight {request.flight_id}: {response_payload}")
            self.producer.send(self.response_topic, value=response_payload)
            self.producer.flush()
        except ValueError as exc:
            error_payload = {
                "flight_id": request.flight_id,
                "error": str(exc),
            }
            if request.correlation_id is not None:
                error_payload["correlation_id"] = request.correlation_id
            self.producer.send(self.response_topic, value=error_payload)
            self.producer.flush()
            logger.error(str(exc))

    def process_message(self, message: Any) -> None:
        if message.topic == self.update_topic:
            self._handle_update_message(message)
            return

        try:
            request = FlightStatusRequest(**message.value)
        except ValidationError as exc:
            logger.error(f"Invalid request payload: {exc}")
            return

        try:
            response = self.get_flight_status(request)
            response_payload = self._create_response_payload(response, request.correlation_id)
            logger.info(f"Flight Status for {request.flight_id}: {response_payload}")
            self.producer.send(self.response_topic, value=response_payload)
            self.producer.flush()
        except ValueError as exc:
            error_payload = {
                "flight_id": request.flight_id,
                "error": str(exc),
            }
            if request.correlation_id is not None:
                error_payload["correlation_id"] = request.correlation_id
            self.producer.send(self.response_topic, value=error_payload)
            self.producer.flush()
            logger.error(str(exc))

    def run(self) -> None:
        logger.info(f"Starting FlightStatusService consumer on Kafka at {self.bootstrap_servers}")
        for message in self.consumer:
            logger.info(f"Received request message: {message.value}")
            self.process_message(message)


if __name__ == "__main__":
    service = FlightStatusService(bootstrap_servers)
    service.run()
