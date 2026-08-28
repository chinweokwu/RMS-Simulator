# RMS Simulators — EOS Platform

Live production-grade Remote Management System (RMS) simulator suite for the EOS Platform.
Includes pre-configured profiles for **Production** (2,000 sites) and **Local Testbed** (100 sites).

---

## 📁 Directory Structure

```
RMS simulators/
├── production/                ← FOR REMOTE SERVER (2,000 Sites, 16 Workers)
│   ├── simulate_soap_server.py
│   ├── simulate_rms_sites.py
│   ├── simulate_local_dashboard.py
│   ├── Dockerfile.soap_server
│   ├── Dockerfile.rms_sites
│   └── docker-compose.yml
│
└── testbed/                   ← FOR LOCAL DEV MACHINE (100 Sites, 4 Workers)
    ├── simulate_soap_server.py
    ├── simulate_rms_sites.py
    ├── simulate_local_dashboard.py
    ├── Dockerfile.soap_server
    ├── Dockerfile.rms_sites
    └── docker-compose.yml
```

---

## 🚀 Quick Start

### 1. Local Testbed (100 Sites) — Laptop / Dev Machine
```bash
cd "RMS simulators/testbed"

# Option A: Run via Docker Compose
docker-compose up --build

# Option B: Run locally without Docker
python3 simulate_soap_server.py --port 8090 --sites 100 --push
python3 simulate_rms_sites.py --sites 100 --mode normal --workers 4

# Option C: View live Terminal Dashboard
python3 simulate_local_dashboard.py --sites 100
```

### 2. Production (2,000 Sites) — Cloud / Remote Server
```bash
cd "RMS simulators/production"

# Start background services on cloud server
GATEWAY_URL=http://your-eos-server:8081 docker-compose up --build -d

# Trigger on-demand regional alarm storm (32 workers across 300 sites)
docker-compose --profile storm run rms-storm
```

---

## ⚡ Key Features

- **Wall-Clock Alarm Lifecycle**:
  - `RAISED` → `ACKNOWLEDGED` (30 mins real time)
  - `CLEARED` → **Hard Deleted from Memory** (2 hours real time) to save server memory.
- **Heartbeat & Keepalive**: Quiet sites send periodic telemetry pings.
- **SOAP Fault XML Envelopes**: Compliant error envelopes for invalid requests / 401 Unauthorized.
- **Bearer Token Auth**: Validates `Authorization: Bearer` headers on ingestion.
- **Rest API**: `GET /soap/api/v1/sites/{id}` and `GET /soap/api/v1/alarms`.
