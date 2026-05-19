from fastapi import FastAPI, HTTPException
import csv
import os
from typing import Optional, Dict, Any, List

app = FastAPI()

# Path to the local fare data file
FARE_DATA_FILE = os.path.join(os.path.dirname(__file__), "fares.csv")


def load_all_fares() -> List[Dict[str, Any]]:
    """Load all fare records from the local fare data file."""
    if not os.path.exists(FARE_DATA_FILE):
        raise FileNotFoundError(f"Fare data file not found at {FARE_DATA_FILE}")

    try:
        with open(FARE_DATA_FILE, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            fares: List[Dict[str, Any]] = []
            for row in reader:
                fares.append({
                    "flight_id": row['flight_id'],
                    "origin": row['origin'],
                    "destination": row['destination'],
                    "fare": float(row['fare']),
                    "currency": row.get('currency', 'USD'),
                    "available_seats": int(row.get('available_seats', 0))
                })
            return fares
    except Exception as e:
        raise ValueError(f"Error reading fare data: {str(e)}")


def get_flight_fare(flight_id: str, origin: Optional[str] = None, destination: Optional[str] = None) -> Dict[str, Any]:
    """
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
    """
    fares = load_all_fares()
    for fare in fares:
        if fare['flight_id'] != flight_id:
            continue
        if origin and fare['origin'].lower() != origin.lower():
            continue
        if destination and fare['destination'].lower() != destination.lower():
            continue
        return fare

    raise ValueError(f"Flight {flight_id} not found in fare database")


def search_fares(origin: Optional[str] = None, destination: Optional[str] = None) -> List[Dict[str, Any]]:
    """Search fares by origin and/or destination city."""
    fares = load_all_fares()
    matches: List[Dict[str, Any]] = []
    for fare in fares:
        if origin and fare['origin'].lower() != origin.lower():
            continue
        if destination and fare['destination'].lower() != destination.lower():
            continue
        matches.append(fare)
    return matches


@app.get("/")
async def root():
    return {"message": "fares API Service"}


@app.get("/fare/{flight_id}")
async def get_fare(flight_id: str, origin: Optional[str] = None, destination: Optional[str] = None):
    """
    Get flight fare by flight ID.

    Query Parameters:
        - origin: Optional origin city filter
        - destination: Optional destination city filter
    """
    try:
        fare_info = get_flight_fare(flight_id, origin, destination)
        return fare_info
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/fares")
async def find_fares(origin: Optional[str] = None, destination: Optional[str] = None):
    """Search fares by origin and/or destination city."""
    try:
        results = search_fares(origin, destination)
        if not results:
            raise HTTPException(status_code=404, detail="No fares found for the given origin/destination.")
        return {"count": len(results), "fares": results}
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)