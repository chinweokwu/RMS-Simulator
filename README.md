# RMS Simulators — EOS Platform

Live production-grade Remote Management System (RMS) simulator suite for the EOS Platform.
Simulates 2,000 telecom cell sites across multiple vendors (Galooli, Huawei NetEco, ZTE iFMS, Vertiv).

## Files

| File | Purpose |
|---|---|
| `simulate_soap_server.py` | Enterprise SOAP Server — receives SOAP XML alarms, serves WSDL, exposes REST query API |
| `simulate_rms_sites.py` | Galooli / Multi-Vendor site pusher — pushes live JSON telemetry to EOS Gateway |
| `simulate_local_dashboard.py` | Local terminal dashboard — displays in-memory site state |
| `Dockerfile.soap_server` | Docker image for SOAP Server |
| `Dockerfile.rms_sites` | Docker image for Galooli/RMS Pusher |
| `docker-compose.yml` | Orchestrates both services together |

---

## Run Locally (No Docker)

```bash
# Start SOAP Server (Port 8090)
python3 simulate_soap_server.py --port 8090 --sites 2000

# Start Galooli Pusher (normal stream)
python3 simulate_rms_sites.py --sites 2000 --mode normal --workers 16 --gateway http://localhost:8081/ingress/rms/alarms

# Run Regional Alarm Storm
python3 simulate_rms_sites.py --sites 2000 --mode storm --workers 32
```

---

## Run with Docker Compose

```bash
# Build and start everything (SOAP Server + Galooli Pusher)
docker-compose up --build

# With a custom EOS Gateway URL
GATEWAY_URL=http://your-eos-server:8081 docker-compose up --build

# Trigger Regional Alarm Storm (on-demand)
docker-compose --profile storm run rms-storm
```

---

## Endpoints (SOAP Server)

| Endpoint | Method | Description |
|---|---|---|
| `/soap/AlarmService?wsdl` | GET | WSDL Contract |
| `/soap/AlarmService` | POST | Receive SOAP XML alarm |
| `/soap/api/v1/sites/{id}` | GET | Query persistent state of specific site |
| `/soap/api/v1/alarms` | GET | All currently active alarms across 2,000 sites |

---

## Features

- ✅ Persistent 2,000-site in-memory state (survives across requests)
- ✅ Full alarm lifecycle: `RAISED` → `ACKNOWLEDGED` → `CLEARED`
- ✅ Heartbeat / Keepalive pings for quiet sites
- ✅ Proactive outbound SOAP webhook push to EOS Gateway
- ✅ Multi-threaded server (`ThreadingTCPServer`) and concurrent pusher (`ThreadPoolExecutor`)
- ✅ SOAP Fault XML envelopes for auth errors, bad XML, unknown endpoints
- ✅ Bearer Token authentication validation
- ✅ REST site query API
