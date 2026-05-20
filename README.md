# Arqui-Soft— Sistema de Tarifas Aéreas

Plataforma de microservicios para consulta y validación de tarifas de vuelo, con arquitectura basada en eventos (Apache Kafka), replicación activa de datos y un API Gateway que expone los servicios al frontend.

---

## Arquitectura general

```
┌─────────────────────────────────────────────────────────────────┐
│                          Cliente                                │
│                    Angular UI  :4200                            │
└───────────────────────────┬─────────────────────────────────────┘
                            │ HTTP
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                     API Gateway  :8000                          │
│  - Proxy HTTP → faresApi                                        │
│  - Proxy Kafka → flightStatus                                   │
└────────────┬───────────────────────────────┬────────────────────┘
             │ HTTP                          │ Kafka
             ▼                              ▼
┌──────────────────────┐       ┌────────────────────────┐
│   faresApi  :8010    │       │  flightStatus (worker) │
│  Validación de       │       │  Consume/produce        │
│  consistencia entre  │       │  get-flight-status      │
│  pods                │       │  flight-status-response │
└──┬───────────────────┘       └────────────────────────┘
   │ HTTP (fan-out)
   ├──────────────────┐──────────────────┐
   ▼                  ▼                  ▼
┌────────┐      ┌────────┐        ┌────────┐
│ fares  │      │ fares  │        │ fares  │
│ pod-1  │      │ pod-2  │        │ pod-3  │
│ :8001  │      │ :8002  │        │ :8003  │
└───┬────┘      └───┬────┘        └───┬────┘
    │               │                 │
    └───────────────┼─────────────────┘
                    │ Kafka (fares-sync)
                    ▼
         ┌──────────────────────┐
         │  faresMaster  :8020  │
         │  Fuente de verdad    │
         │  Sirve fares.csv     │
         └──────────────────────┘

Infraestructura transversal:
  Apache Kafka + Zookeeper   :9092
  Kafka UI                   :8080
```

---

## Servicios

| Servicio            | Puerto     | Tecnología        | Descripción                                         |
| ------------------- | ---------- | ------------------ | ---------------------------------------------------- |
| `faresui`         | 4200       | Angular 15         | Frontend — búsqueda y visualización de tarifas    |
| `api-gateway`     | 8000       | FastAPI            | Punto de entrada único; proxy HTTP y Kafka          |
| `fares-api`       | 8010       | FastAPI            | Valida consistencia de tarifas entre los 3 pods      |
| `fares-pod-1/2/3` | 8001–8003 | FastAPI            | Réplicas de datos de tarifas (CSV)                  |
| `fares-master`    | 8020       | FastAPI            | Fuente de verdad; distribuye `fares.csv` vía HTTP |
| `flight-status`   | —         | Python worker      | Microservicio Kafka para estado de vuelos            |
| `kafka`           | 9092       | Confluent Kafka    | Bus de mensajes asíncrono                           |
| `kafka-ui`        | 8080       | Provectus Kafka UI | Monitoreo de tópicos y mensajes                     |

### Tópicos Kafka

| Tópico                    | Productor     | Consumidor      | Descripción                                           |
| -------------------------- | ------------- | --------------- | ------------------------------------------------------ |
| `get-flight-status`      | api-gateway   | flight-status   | Solicitud de estado de vuelo                           |
| `flight-status-response` | flight-status | api-gateway     | Respuesta con datos del vuelo                          |
| `fares-sync`             | fares-api     | fares-pod-1/2/3 | Señal de re-sincronización al detectar pod con fallo |

---

## Stack tecnológico

- **Backend:** Python 3.11, FastAPI, Uvicorn
- **Mensajería:** Apache Kafka (Confluent), kafka-python-ng
- **Frontend:** Angular 15, TypeScript
- **Infraestructura:** Docker, Docker Compose, Kubernetes (Minikube), Kustomize
- **Comunicación:** HTTP/REST (síncrono), Kafka (asíncrono)

---

## Prerrequisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop) ≥ 24
- [Docker Compose](https://docs.docker.com/compose/) v2
- [kubectl](https://kubernetes.io/docs/tasks/tools/) (solo para Kubernetes)
- [Minikube](https://minikube.sigs.k8s.io/docs/start/) ≥ 1.30 (solo para Kubernetes)
- Node.js 18+ (solo para desarrollo local del frontend)

---

## Ejecución con Docker Compose

### Levantar todos los servicios

```bash
cd arqui-soft3

# Construir imágenes
docker compose build

# Levantar en segundo plano
docker compose up -d

# Verificar estado
docker compose ps

# Ver logs en tiempo real
docker compose logs -f

# Detener
docker compose down
```

### URLs disponibles

| Servicio         | URL                   |
| ---------------- | --------------------- |
| Frontend Angular | http://localhost:4200 |
| API Gateway      | http://localhost:8000 |
| Fares API        | http://localhost:8010 |
| Fares Pod 1      | http://localhost:8001 |
| Fares Pod 2      | http://localhost:8002 |
| Fares Pod 3      | http://localhost:8003 |
| Fares Master     | http://localhost:8020 |
| Kafka UI         | http://localhost:8080 |

---

## API Reference

Todos los endpoints del frontend deben consumirse a través del **API Gateway** (`http://localhost:8000`).

### API Gateway

#### `GET /`

Health check del gateway.

#### `GET /faresapi/get-fares/{flight_id}`

Consulta la tarifa de un vuelo con validación de consistencia entre pods.

**Parámetros de query opcionales:**

- `detail=on` — incluye detalle de respuesta por pod

**Ejemplo:**

```bash
curl "http://localhost:8000/faresapi/get-fares/BA501"
curl "http://localhost:8000/faresapi/get-fares/BA501?detail=on"
```

#### `GET /faresapi/search-fares`

Busca tarifas por origen y/o destino.

**Parámetros de query:**

- `origin` — ciudad de origen (ej. `London`)
- `destination` — ciudad de destino (ej. `New York`)

**Ejemplo:**

```bash
curl "http://localhost:8000/faresapi/search-fares?origin=London"
curl "http://localhost:8000/faresapi/search-fares?origin=London&destination=New%20York"
```

#### `GET /flights/get-flight-status/{flight_id}`

Consulta el estado de un vuelo vía Kafka (request/reply).

**Vuelos de ejemplo:** `BA501`, `AA100`, `DL200`

```bash
curl "http://localhost:8000/flights/get-flight-status/BA501"
```

---

### Fares API (validación interna)

#### `GET /pod-health`

Estado de conectividad de los 3 pods de tarifas.

```bash
curl "http://localhost:8010/pod-health"
```

**Respuesta:**

```json
{
  "total_pods": 3,
  "healthy_pods": 3,
  "unhealthy_pods": 0,
  "pod_details": { ... },
  "timestamp": 1716134445123
}
```

#### `GET /get-fares/{flight_id}`

Valida consistencia entre pods y retorna consenso.

#### `GET /search-fares`

Valida búsquedas por origen/destino contra todos los pods.

---

## Estructura del proyecto

```
arqui-soft3/
├── apigateway/          # API Gateway (FastAPI)
│   └── main.py
├── fares/               # Réplica de tarifas (FastAPI + CSV)
│   ├── main.py
│   └── fares.csv
├── faresApi/            # Validador de consistencia (FastAPI)
│   └── main.py
├── faresMaster/         # Fuente de verdad de tarifas (FastAPI)
│   ├── main.py
│   └── fares.csv
├── flightStatus/        # Worker de estado de vuelos (Kafka consumer)
│   └── main.py
├── faresui/             # Frontend Angular
│   └── src/
│       └── app/
│           ├── app.component.ts
│           └── services/fares.service.ts
├── k8s/                 # Manifiestos Kubernetes
│   ├── kustomization.yaml
│   ├── fares-deployment.yaml
│   ├── faresapi-deployment.yaml
│   └── apigateway-deployment.yaml
├── docker-compose.yml
├── Dockerfile.apigateway
├── Dockerfile.fares
├── Dockerfile.faresApi
├── Dockerfile.faresMaster
└── Dockerfile.flightStatus
```

---

## Flujo de sincronización de tarifas

Cuando `faresApi` detecta un pod con respuesta inconsistente o caída:

1. Produce un evento `pod-failure` en el tópico `fares-sync`
2. Cada pod `fares` consume el evento y descarga el `fares.csv` actualizado desde `faresMaster`
3. El pod reemplaza su copia local atómicamente (escritura en `.tmp` + `os.replace`)

Este mecanismo garantiza **eventual consistency** entre las réplicas sin intervención manual.

---

## Variables de entorno relevantes

| Variable                    | Servicio     | Valor por defecto            | Descripción                                 |
| --------------------------- | ------------ | ---------------------------- | -------------------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS` | todos        | `kafka:9092`               | Brokers Kafka                                |
| `FARES_API_BASE_URL`      | api-gateway  | `http://fares-api:8000`    | URL interna del validador                    |
| `FARES_MASTER_URL`        | fares        | `http://fares-master:8000` | URL del servicio maestro                     |
| `FARES_OUTPUT_FILE`       | fares-master | `fares.csv`                | Ruta del archivo de tarifas                  |
| `POD_NAME` / `HOSTNAME` | fares        | —                           | Identificador del pod para group_id de Kafka |
