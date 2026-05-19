from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os

app = FastAPI()

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


@app.get("/items/{item_id}")
def read_item(item_id: int, q: str | None = None):
    return {"item_id": item_id, "q": q}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)