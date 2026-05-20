from fastapi import FastAPI, HTTPException
import httpx
import asyncio
import os
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import json
import logging                                                                  
import sys 
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
console = logging.StreamHandler(sys.stdout)
logger.addHandler(console)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all"
)

app = FastAPI()

# Pod instances configuration - update these based on your Docker Compose deployment
POD_INSTANCES = [
    {"name": "fares-pod-1", "url": "http://fares-pod-1:8000"},
    {"name": "fares-pod-2", "url": "http://fares-pod-2:8000"},
    {"name": "fares-pod-3", "url": "http://fares-pod-3:8000"},
]

# Timeout for pod requests (seconds)
POD_REQUEST_TIMEOUT = 5


class PodResponse(BaseModel):
    pod_name: str
    url: str
    status: str  # "healthy", "unhealthy", "timeout", "error"
    response_data: Optional[Any] = None
    error_message: Optional[str] = None


class FareValidationResponse(BaseModel):
    flight_id: str
    all_pods_healthy: bool
    healthy_pods_count: int
    unhealthy_pods_count: int
    consensus_fare: Optional[Dict[str, Any]] = None
    pod_responses: List[PodResponse]
    details: str


class SearchValidationResponse(BaseModel):
    origin: Optional[str]
    destination: Optional[str]
    all_pods_healthy: bool
    healthy_pods_count: int
    unhealthy_pods_count: int
    consensus_results: Optional[List[Dict[str, Any]]] = None
    pod_responses: List[PodResponse]
    details: str


def produce_fares_sync_message(pod_name: Optional[str]= "", url: Optional[str]= "", status: Optional[str]= "", error_message: Optional[str]= "", endpoint: Optional[str] = "") -> None:
    try:
        payload = {
            "event": "pod-failure",
            "pod_name": pod_name
        }
        producer.send("fares-sync", value=payload)
        producer.flush()
        logger.info(f"Produced fares-sync event for pod failure: {pod_name} {endpoint}")
    except Exception as exc:
        logger.error(f"Failed to produce fares-sync message: {exc}")


async def fetch_fare_from_pod(pod: Dict[str, str], flight_id: str) -> PodResponse:
    """
    Fetch fare from a single pod instance and return response with validation.
    """
    try:
        async with httpx.AsyncClient(timeout=POD_REQUEST_TIMEOUT) as client:
            response = await client.get(f"{pod['url']}/fare/{flight_id}")
            
            if response.status_code == 200:
                data = response.json()
                return PodResponse(
                    pod_name=pod["name"],
                    url=pod["url"],
                    status="healthy",
                    response_data=data
                )
            elif response.status_code == 404:
                return PodResponse(
                    pod_name=pod["name"],
                    url=pod["url"],
                    status="unhealthy",
                    error_message=f"Flight {flight_id} not found (404)"
                )
            else:
                logger.error(f"HTTP {response.status_code}: {response.text}")
                logger.error(f"Pod failing Detection !!!!")
                produce_fares_sync_message(
                    pod_name=pod["name"],
                    url=pod["url"],
                    status="unhealthy",
                    error_message=f"HTTP {response.status_code}: {response.text}",
                    endpoint=f"/fare/{flight_id}"
                )
                return PodResponse(
                    pod_name=pod["name"],
                    url=pod["url"],
                    status="unhealthy",
                    error_message=f"HTTP {response.status_code}: {response.text}"
                )
    except asyncio.TimeoutError:
        produce_fares_sync_message(
            pod_name=pod["name"]
        )
        return PodResponse(
            pod_name=pod["name"],
            url=pod["url"],
            status="timeout",
            error_message="Request timeout - pod not responding"
        )
    except Exception as e:
        logger.error(f"Error fetching fare from {pod['name']}: {str(e)}")
        logger.error(f"Pod failing Detection !!!! ")
        produce_fares_sync_message(
            pod_name=pod["name"],
            url=pod["url"]
        )
        return PodResponse(
            pod_name=pod["name"],
            url=pod["url"],
            status="error",
            error_message=f"Connection error: {str(e)}"
        )


async def fetch_search_from_pod(pod: Dict[str, str], origin: Optional[str], destination: Optional[str]) -> PodResponse:
    """
    Fetch search results from a single pod instance and return response with validation.
    """
    params = {}
    if origin:
        params["origin"] = origin
    if destination:
        params["destination"] = destination

    endpoint = "/fares"
    try:
        async with httpx.AsyncClient(timeout=POD_REQUEST_TIMEOUT) as client:
            response = await client.get(f"{pod['url']}/fares", params=params)

            if response.status_code == 200:
                data = response.json()
                return PodResponse(
                    pod_name=pod["name"],
                    url=pod["url"],
                    status="healthy",
                    response_data=data
                )
            elif response.status_code == 404:
                return PodResponse(
                    pod_name=pod["name"],
                    url=pod["url"],
                    status="unhealthy",
                    error_message=f"Search not found (404)"
                )
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                logger.error(error_msg)
                logger.error(f"Pod failing Detection !!!! ")
                produce_fares_sync_message(
                    pod_name=pod["name"],
                    url=pod["url"]
                )
                return PodResponse(
                    pod_name=pod["name"],
                    url=pod["url"],
                    status="unhealthy",
                    error_message=error_msg
                )
    except asyncio.TimeoutError:
        error_message = "Request timeout - pod not responding"
        produce_fares_sync_message(
            pod_name=pod["name"],
            url=pod["url"],
            status="timeout",
            error_message=error_message,
            endpoint=endpoint
        )
        return PodResponse(
            pod_name=pod["name"],
            url=pod["url"],
            status="timeout",
            error_message=error_message
        )
    except Exception as e:
        error_message = f"Connection error: {str(e)}"
        produce_fares_sync_message(
            pod_name=pod["name"],
            url=pod["url"],
            status="error",
            error_message=error_message,
            endpoint=endpoint
        )
        return PodResponse(
            pod_name=pod["name"],
            url=pod["url"],
            status="error",
            error_message=error_message
        )


def validate_pod_responses(responses: List[PodResponse]) -> tuple:
    """
    Validate that all healthy pod responses contain the same fare data.
    Returns: (all_valid, consensus_data, healthy_count, unhealthy_count)
    """
    healthy_responses = [r for r in responses if r.status == "healthy"]
    unhealthy_count = len(responses) - len(healthy_responses)
    
    if not healthy_responses:
        return False, None, 0, unhealthy_count
    
    # Get consensus data from first healthy response
    consensus = healthy_responses[0].response_data
    
    # Check if all healthy responses match
    all_match = all(
        r.response_data == consensus for r in healthy_responses
    )
    
    return all_match, consensus, len(healthy_responses), unhealthy_count


@app.get("/")
async def root():
    return {"message": "faresApi Function Service"}


@app.get("/get-fares/{flight_id}")
async def validate_fares_across_pods(flight_id: str, detail: Optional[str] = None) -> Dict[str, Any]:
    """
    Validate fare consistency across all pod instances.

    Returns only the consensus fare unless detail=on is provided.
    """
    # Fetch from all pods concurrently
    tasks = [fetch_fare_from_pod(pod, flight_id) for pod in POD_INSTANCES]
    pod_responses = await asyncio.gather(*tasks)

    # Validate responses
    all_match, consensus, healthy_count, unhealthy_count = validate_pod_responses(pod_responses)

    show_details = (detail or "").lower() == "on"
    if not show_details:
        return {"consensus_fare": consensus}

    # Build details message
    healthy_pods = [r.pod_name for r in pod_responses if r.status == "healthy"]
    unhealthy_pods = [r.pod_name for r in pod_responses if r.status != "healthy"]

    details_parts = []
    if healthy_count == len(POD_INSTANCES):
        details_parts.append(f"✓ All {healthy_count} pods are responding correctly")
        if not all_match:
            details_parts.append("⚠ WARNING: Pod responses do not match!")
            logger.error("⚠ WARNING: Pod responses do not match!")
            logger.error(f"Pod failing Detection !!!!")
            produce_fares_sync_message( )

    else:
        details_parts.append(f"✗ {unhealthy_count} pod(s) not responding correctly: {', '.join(unhealthy_pods)}")
        logger.error(f"✗ {unhealthy_count} pod(s) not responding correctly: {', '.join(unhealthy_pods)}")
        logger.error(f"Pod failing Detection !!!!")
        produce_fares_sync_message()
        details_parts.append(f"✓ {healthy_count} pod(s) responding correctly: {', '.join(healthy_pods)}")

    if consensus:
        details_parts.append(f"Consensus fare: ${consensus.get('fare', 'N/A')} {consensus.get('currency', 'N/A')}")

    return {
        "flight_id": flight_id,
        "all_pods_healthy": (healthy_count == len(POD_INSTANCES)) and all_match,
        "healthy_pods_count": healthy_count,
        "unhealthy_pods_count": unhealthy_count,
        "consensus_fare": consensus,
        "pod_responses": pod_responses,
        "details": " | ".join(details_parts)
    }


@app.get("/search-fares")
async def validate_search_fares(
    origin: Optional[str] = None,
    destination: Optional[str] = None,
    detail: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Validate fare search results across all pod instances.

    Returns only the consensus results unless detail=on is provided.
    """
    if not origin and not destination:
        raise HTTPException(status_code=400, detail="Provide origin and/or destination to search fares.")

    tasks = [fetch_search_from_pod(pod, origin, destination) for pod in POD_INSTANCES]
    pod_responses = await asyncio.gather(*tasks)

    healthy_responses = [r for r in pod_responses if r.status == "healthy"]
    unhealthy_count = len(pod_responses) - len(healthy_responses)

    consensus = healthy_responses[0].response_data if healthy_responses else None
    all_match = all(
        r.response_data == consensus for r in healthy_responses
    ) if healthy_responses else False

    show_details = (detail or "").lower() == "on"
    if not show_details:
        return {"consensus_results": consensus.get("fares") if isinstance(consensus, dict) else None}

    healthy_pods = [r.pod_name for r in pod_responses if r.status == "healthy"]
    unhealthy_pods = [r.pod_name for r in pod_responses if r.status != "healthy"]

    details_parts = []
    if healthy_responses and len(healthy_responses) == len(POD_INSTANCES):
        details_parts.append(f"✓ All {len(healthy_responses)} pods are responding correctly")
        if not all_match:
            details_parts.append("⚠ WARNING: Pod responses do not match!")
    else:
        details_parts.append(f"✗ {unhealthy_count} pod(s) not responding correctly: {', '.join(unhealthy_pods)}")
        logger.error(f"✗ {unhealthy_count} pod(s) not responding correctly: {', '.join(unhealthy_pods)}")
        logger.error(f"Pod failing Detection !!!!")
        produce_fares_sync_message()

        details_parts.append(f"✓ {len(healthy_responses)} pod(s) responding correctly: {', '.join(healthy_pods)}")
 
    if consensus is not None:
        count = consensus.get("count") if isinstance(consensus, dict) else None
        details_parts.append(f"Consensus results count: {count if count is not None else 'N/A'}")

    return {
        "origin": origin,
        "destination": destination,
        "all_pods_healthy": (len(healthy_responses) == len(POD_INSTANCES)) and all_match,
        "healthy_pods_count": len(healthy_responses),
        "unhealthy_pods_count": unhealthy_count,
        "consensus_results": consensus.get("fares") if isinstance(consensus, dict) else None,
        "pod_responses": pod_responses,
        "details": " | ".join(details_parts)
    }


@app.get("/pod-health")
async def check_pod_health() -> Dict[str, Any]:
    """
    Quick health check for all pod instances.
    Tests connectivity and response status without fetching specific data.
    """
    tasks = [fetch_fare_from_pod(pod, "test") for pod in POD_INSTANCES]
    responses = await asyncio.gather(*tasks)
    
    pod_statuses = {
        r.pod_name: {
            "url": r.url,
            "status": r.status,
            "message": r.error_message
        }
        for r in responses
    }
    
    healthy = sum(1 for r in responses if r.status == "healthy")
    
    return {
        "total_pods": len(POD_INSTANCES),
        "healthy_pods": healthy,
        "unhealthy_pods": len(POD_INSTANCES) - healthy,
        "pod_details": pod_statuses,
        "timestamp": asyncio.get_event_loop().time()
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)