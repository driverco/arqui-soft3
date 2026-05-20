from fastapi import FastAPI, HTTPException
import csv
import json
import os
import time
from threading import Thread
from typing import Optional, Dict, Any, List
import multiprocessing
import requests
from kafka import KafkaConsumer
import logging                                                                  
import sys 


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
console = logging.StreamHandler(sys.stdout)
logger.addHandler(console)


app = FastAPI()

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

FARES_MASTER_URL = os.environ.get('FARES_MASTER_URL', 'http://fares-master:8000')

# Path to the local fare data file
FARE_DATA_FILE = os.path.join(os.path.dirname(__file__), 'fares.csv')
POD_NAME = os.environ.get('POD_NAME', os.environ.get('HOSTNAME', 'fares-sync-service'))


def make_kafka_group_id() -> str:
    '''Build a Kafka group_id using the Kubernetes pod name when available.'''
    normalized = POD_NAME.lower().replace('_', '-').replace('.', '-')
    return f'fares-sync-service-{normalized}'


def load_all_fares() -> List[Dict[str, Any]]:
    '''Load all fare records from the local fare data file.'''
    if not os.path.exists(FARE_DATA_FILE):
        raise FileNotFoundError(f'Fare data file not found at {FARE_DATA_FILE}')

    try:
        with open(FARE_DATA_FILE, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            fares: List[Dict[str, Any]] = []
            for row in reader:
                fares.append({
                    'flight_id': row['flight_id'],
                    'origin': row['origin'],
                    'destination': row['destination'],
                    'fare': float(row['fare']),
                    'currency': row.get('currency', 'USD'),
                    'available_seats': int(row.get('available_seats', 0))
                })
            return fares
    except Exception as e:
        raise ValueError(f'Error reading fare data: {str(e)}')


def get_flight_fare(flight_id: str, origin: Optional[str] = None, destination: Optional[str] = None) -> Dict[str, Any]:
    '''
    Retrieve flight fare information from local text file.

    Args:
        flight_id: The flight identifier
        origin: Optional origin city to filter results
        destination: Optional destination city to filter results

    Returns:
        Dictionary containing flight fare details

    Raises:
        FileNotFoundError: If the fare data file is not found
        ValueError: If flight is not found
    '''
    fares = load_all_fares()
    for fare in fares:
        if fare['flight_id'] != flight_id:
            continue
        if origin and fare['origin'].lower() != origin.lower():
            continue
        if destination and fare['destination'].lower() != destination.lower():
            continue
        return fare

    raise ValueError(f'Flight {flight_id} not found in fare database')


def search_fares(origin: Optional[str] = None, destination: Optional[str] = None) -> List[Dict[str, Any]]:
    '''Search fares by origin and/or destination city.'''
    fares = load_all_fares()
    matches: List[Dict[str, Any]] = []
    for fare in fares:
        if origin and fare['origin'].lower() != origin.lower():
            continue
        if destination and fare['destination'].lower() != destination.lower():
            continue
        matches.append(fare)
    return matches


@app.get('/')
async def root():
    return {'message': 'fares API Service'}


@app.get('/fare/{flight_id}')
async def get_fare(flight_id: str, origin: Optional[str] = None, destination: Optional[str] = None):
    '''
    Get flight fare by flight ID.

    Query Parameters:
        - origin: Optional origin city filter
        - destination: Optional destination city filter
    '''
    try:
        fare_info = get_flight_fare(flight_id, origin, destination)
        return fare_info
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/fares')
async def find_fares(origin: Optional[str] = None, destination: Optional[str] = None):
    '''Search fares by origin and/or destination city.'''
    try:
        results = search_fares(origin, destination)
        if not results:
            raise HTTPException(status_code=404, detail='No fares found for the given origin/destination.')
        return {'count': len(results), 'fares': results}
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))


def download_and_replace_fares_csv() -> None:
    '''Download the latest fares CSV from faresMaster and replace the local fares file.'''
    csv_url = FARES_MASTER_URL.rstrip('/') + '/fares.csv'
    logger.info(f'[fares-sync] downloading fares CSV from {csv_url}')
    response = requests.get(csv_url, timeout=15)
    response.raise_for_status()

    temp_path = FARE_DATA_FILE + '.tmp'
    with open(temp_path, 'wb') as tmp_file:
        tmp_file.write(response.content)
    os.replace(temp_path, FARE_DATA_FILE)
    logger.info(f'[fares-sync] replaced local fares CSV at {FARE_DATA_FILE}')


class FaresSyncConsumer():
    request_topic = "fares-sync"

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = KafkaConsumer(
            self.request_topic,
            bootstrap_servers=self.bootstrap_servers.split(","),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=make_kafka_group_id(),
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )


    def process_message(self, message: Dict[str, Any]) -> None:

        logger.info(f'[fares-sync] received message: {message.value}')
        try:
            download_and_replace_fares_csv()
        except Exception as sync_error:
            logger.error(f'[fares-sync] sync error: {sync_error}')
        except Exception as consumer_error:
            logger.error(f'[fares-sync] consumer error: {consumer_error}')
            time.sleep(5)

    def run(self) -> None:
        logger.info(f"Starting FaresSyncConsumer on Kafka at {self.bootstrap_servers}")
        for message in self.consumer:
            logger.info(f"Received request message: {message.value}")
            self.process_message(message)



if __name__ == '__main__':
    import uvicorn
    service = FaresSyncConsumer(bootstrap_servers)
    multiprocessing.Process(target=uvicorn.run, args=(app,), kwargs={'host': '0.0.0.0', 'port': 8000}).start()
    multiprocessing.Process(target=service.run).start()
    #uvicorn.run(app, host='0.0.0.0', port=8000)
    
