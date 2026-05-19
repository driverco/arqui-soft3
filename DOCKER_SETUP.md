# Docker & Kubernetes Setup for Arqui-Soft3

## Quick Start with Docker Compose (Local Testing)

### Build and Run with Docker Compose
```bash
# Navigate to project root
cd arqui-soft3

# Build all services
docker-compose build

# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

### Access Services Locally
- **API Gateway**: http://localhost:8000
- **Fares API**: http://localhost:8010
- **Fares Pod 1**: http://localhost:8001
- **Fares Pod 2**: http://localhost:8002
- **Fares Pod 3**: http://localhost:8003

### Test the Services
```bash
# Test Fares Pod
curl http://localhost:8001/fare/AA100

# Test Fares API validation endpoint
curl http://localhost:8010/validate-fares/AA100

# Check pod health
curl http://localhost:8010/pod-health

# Test API Gateway
curl http://localhost:8000/
```

---

## Kubernetes Deployment on Minikube

### Prerequisites
```bash
# Install Minikube
brew install minikube  # macOS
# or
choco install minikube  # Windows

# Install kubectl
brew install kubectl  # macOS
# or
choco install kubernetes-cli  # Windows

# Install Docker
# Download from https://www.docker.com/products/docker-desktop
```

### Setup Minikube

```bash
# Start minikube cluster
minikube start --driver=docker --memory=4096 --cpus=2

# Enable required addons
minikube addons enable metrics-server
minikube addons enable dashboard

# Configure Docker to use minikube's Docker daemon
eval $(minikube docker-env)
```

### Build Images for Minikube

```bash
# Set Docker environment to minikube
eval $(minikube docker-env)

# Build images (they'll be available in minikube)
docker build -f Dockerfile.fares -t fares:latest .
docker build -f Dockerfile.faresApi -t fares-api:latest .
docker build -f Dockerfile.apigateway -t api-gateway:latest .

# Verify images are in minikube
docker images
```

### Deploy to Minikube

```bash
# Deploy using kubectl
kubectl apply -k k8s/

# Or deploy individual manifests
kubectl apply -f k8s/fares-deployment.yaml
kubectl apply -f k8s/fares-service.yaml
kubectl apply -f k8s/faresapi-deployment.yaml
kubectl apply -f k8s/faresapi-service.yaml
kubectl apply -f k8s/apigateway-deployment.yaml
kubectl apply -f k8s/apigateway-service.yaml

# Check deployment status
kubectl get deployments
kubectl get pods
kubectl get services
```

### Monitor Deployments

```bash
# Watch pod status
kubectl get pods -w

# View detailed pod information
kubectl describe pod <pod-name>

# Check logs from a pod
kubectl logs <pod-name>
kubectl logs -f <pod-name>  # Follow logs

# Get resource usage
kubectl top pods
kubectl top nodes
```

### Access Services in Minikube

```bash
# Get minikube IP
minikube ip

# Port forward to access services locally
kubectl port-forward svc/api-gateway-service 8000:8000
kubectl port-forward svc/fares-api-service 8010:8010
kubectl port-forward svc/fares-service 8001:8000

# Or expose as NodePort
minikube service api-gateway-service
```

### Test Kubernetes Deployment

```bash
# Port forward to test
kubectl port-forward svc/api-gateway-service 8000:8000 &
kubectl port-forward svc/fares-api-service 8010:8010 &

# Test endpoint
curl http://localhost:8010/validate-fares/AA100
curl http://localhost:8010/pod-health
```

### Cleanup

```bash
# Delete all deployments and services
kubectl delete -k k8s/

# Or delete individual resources
kubectl delete deployment fares fares-api api-gateway
kubectl delete service fares-service fares-api-service api-gateway-service

# Stop minikube
minikube stop

# Delete minikube cluster
minikube delete
```

### Useful Kubectl Commands

```bash
# Get all resources
kubectl get all

# Describe resources
kubectl describe deployment fares
kubectl describe service fares-service

# Scale deployment
kubectl scale deployment fares --replicas=5

# Update deployment image
kubectl set image deployment/fares fares=fares:v2

# Delete resources
kubectl delete pod <pod-name>
kubectl delete deployment fares

# Edit resources
kubectl edit deployment fares

# View resource YAML
kubectl get deployment fares -o yaml
```

### Dashboard Access

```bash
# Open Kubernetes dashboard
minikube dashboard

# Or access via port forwarding
kubectl proxy
# Then open: http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/
```

---

## Troubleshooting

### Images not found in minikube
```bash
# Make sure to use minikube's Docker daemon
eval $(minikube docker-env)
docker images  # Should show your images
```

### Pods not starting
```bash
# Check pod logs
kubectl logs <pod-name>
kubectl describe pod <pod-name>

# Check resource constraints
kubectl top pods
```

### Services not accessible
```bash
# Check service DNS
kubectl get svc
kubectl exec -it <pod-name> -- nslookup fares-service

# Port forward to test
kubectl port-forward svc/fares-service 8001:8000
```

### Reset everything
```bash
minikube stop
minikube delete
minikube start --driver=docker --memory=4096 --cpus=2
```

---

## Architecture Overview

```
┌─────────────────────────────────────┐
│     Kubernetes Cluster (Minikube)   │
├─────────────────────────────────────┤
│                                     │
│  ┌─────────────────────────────┐   │
│  │   API Gateway (1 replica)   │   │
│  │  Service: api-gateway-svc   │   │
│  │  NodePort: 30000/8000       │   │
│  └─────────────────────────────┘   │
│           │                         │
│           ↓                         │
│  ┌─────────────────────────────┐   │
│  │   Fares API (1 replica)     │   │
│  │  Service: fares-api-svc     │   │
│  │  Port: 8010/8000            │   │
│  └─────────────────────────────┘   │
│           │                         │
│           ↓                         │
│  ┌─────────────────────────────┐   │
│  │  Fares Service (3 replicas) │   │
│  │  Service: fares-svc         │   │
│  │  Port: 8000/8000            │   │
│  │  ┌─────┐ ┌─────┐ ┌─────┐   │   │
│  │  │Pod 1│ │Pod 2│ │Pod 3│   │   │
│  │  └─────┘ └─────┘ └─────┘   │   │
│  └─────────────────────────────┘   │
│                                     │
└─────────────────────────────────────┘
```

