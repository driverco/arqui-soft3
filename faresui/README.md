# Fares UI

Angular frontend for the API Gateway fares services.

## Run locally

1. From the project root:
   ```bash
   cd faresui
   npm install
   npm start
   ```

2. Open http://localhost:4200

The Angular dev server proxies requests under `/api` to `http://localhost:8000`.

## API endpoints used

- `GET /api/faresapi/get-fares/{flightId}`
- `GET /api/faresapi/search-fares?origin={origin}&destination={destination}`
