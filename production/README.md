# Enterprise Remote Management System (RMS) & Galooli Telemetry Simulator Suite

> **Production-Grade Infrastructure Simulation Suite for the EOS Platform**  
> Simulates thousands of multi-vendor telecom cell sites with high-concurrency pushing, enterprise SOAP/XML services, Galooli REST JSON payloads, persistent site state, wall-clock alarm lifecycles, RTU communication loss ("Ball Drop"), and power-recovery reboot events.

---

## 📋 Executive Overview

This simulator suite mimics a live network of **2,000+ telecom cell towers** across multiple regions and equipment vendors (Huawei NetEco, Galooli RMM, ZTE iFMS, Vertiv Enersure, Cummins PowerCommand). It is designed to stress-test, validate, and demonstrate your **EOS Gateway Ingestion Engine**, Kafka buffer pipelines, deduplication rules, and heartbeat watchdog workers under realistic production conditions.

---

## 📁 Repository & Architecture Layout

```
RMS simulators/
├── production/                        ← REMOTE SERVER / ORACLE CLOUD ENVIRONMENT
│   ├── docker-compose.yml             (Configured for 2,000 sites, 16 worker threads)
│   ├── Dockerfile.soap_server         (Container manifest for Enterprise SOAP Server)
│   ├── Dockerfile.rms_sites           (Container manifest for Galooli REST Pusher)
│   ├── simulate_soap_server.py        (SOAP Server + WSDL + REST Query API)
│   ├── simulate_rms_sites.py          (Galooli JSON Concurrent Pusher + Storm Engine)
│   └── simulate_local_dashboard.py    (Color-Coded Live Terminal UI)
│
├── testbed/                           ← LOCAL DEVELOPMENT / LAPTOP ENVIRONMENT
│   ├── docker-compose.yml             (Configured for 100 sites, 4 worker threads)
│   ├── Dockerfile.soap_server
│   ├── Dockerfile.rms_sites
│   ├── simulate_soap_server.py
│   ├── simulate_rms_sites.py
│   └── simulate_local_dashboard.py
│
└── README.md                          ← Comprehensive Technical Documentation
```

---

## ⚡ Key Behavioral Features

### 1. Persistent Thread-Safe Site Memory
Sites maintain real-time physical telemetry state across time in thread-safe memory (`threading.Lock()`). Voltages, fuel levels, battery State of Charge (SOC), State of Health (SOH), shelter ambient temperatures, and door sensor contacts fluctuate realistically instead of generating disconnected random numbers.

### 2. Wall-Clock Alarm Lifecycle State Machine
Alarms transition through standard telecom lifecycle phases based on real wall-clock time:
- **`RAISED`**: Triggered when a fault occurs (e.g. AC Blackout, Fuel Theft, High Temp).
- **`ACKNOWLEDGED`**: Auto-transitions after **30 real-world minutes**.
- **`CLEARED` & Auto-Delete**: Auto-clears after **2 real-world hours** and deletes the alarm object from memory, returning the site telemetry to healthy baseline and preserving server RAM.

### 3. RTU "Ball Drop" (Communication Loss / Network Outage)
- **1.5% of sites** periodically experience total communication loss (4G signal RSSI drops to `-120 dBm` / 0V DC).
- During Comm Loss, the site goes **100% silent** and sends **ZERO HTTP/SOAP traffic** for 15 minutes.
- **Purpose**: Tests if your EOS Gateway Watchdog correctly detects missing heartbeats and flags silent sites as `COMMUNICATION_LOSS / UNREACHABLE`.

### 4. Power Recovery Reboot Alarm (`RTU_REBOOT_POWER_RECOVERY`)
- When a site recovers from a 15-minute Comm Loss / total blackout, **BEFORE resuming normal heartbeats**, it fires an explicit recovery reboot alarm:
  - **Alarm Code**: `RTU_REBOOT_POWER_RECOVERY`
  - **Description**: *"RTU controller booted up after total site blackout & communication loss"*
- Once fired, the site returns to sending standard keepalive heartbeats.

### 5. Multi-Threaded Concurrency Engine
The Galooli site pusher uses Python’s `concurrent.futures.ThreadPoolExecutor` to push telemetry concurrently, achieving ingest rates of **200+ events/second** in Regional Alarm Storm mode.

---

## 🚀 Quick Start Guide

### 1. Local Development (Testbed - 100 Sites)

#### Method A: Live Terminal Dashboard (Visual Check)
To watch the live 100-site simulation stream in real-time in your terminal:
```bash
cd "RMS simulators/testbed"
python3 simulate_local_dashboard.py --sites 100
```

#### Method B: Python Direct
```bash
cd "RMS simulators/testbed"

# Start Enterprise SOAP Server (Port 8090)
python3 simulate_soap_server.py --sites 100 --port 8090 --push

# Start Galooli REST Pusher (4 Workers)
python3 simulate_rms_sites.py --sites 100 --mode normal --workers 4 --gateway http://localhost:8081/ingress/rms/alarms
```

#### Method C: Docker Compose
```bash
cd "RMS simulators/testbed"
docker-compose up --build
```

---

### 2. Production Deployment (2,000 Sites - Oracle Cloud / VPS)

Deploy to your remote server or Oracle Cloud Free Tier instance:

```bash
cd "RMS simulators/production"

# Start continuous 2,000-site background simulation stack
GATEWAY_URL=http://your-eos-server:8081 docker-compose up --build -d

# Check live logs
docker-compose logs -f
```

#### Triggering an On-Demand Regional Alarm Storm
Simulate a major regional power outage across 300 sites pushing simultaneous `AC_MAINS_BLACKOUT_FAULT` and `BATTERY_DC_LOW_DISCHARGE` alarms:
```bash
docker-compose --profile storm run rms-storm
```

---

## 📡 API & Payload Reference

### SOAP Server Endpoints (`simulate_soap_server.py`)

| Endpoint | Method | Content-Type | Description |
|---|---|---|---|
| `/soap/AlarmService?wsdl` | `GET` | `text/xml` | Returns full WSDL XML Service Contract |
| `/soap/AlarmService` | `POST` | `text/xml` | Ingests SOAP XML Alarm Envelopes |
| `/soap/api/v1/sites/{id}` | `GET` | `application/json` | REST API — Returns persistent live state of site #{id} |
| `/soap/api/v1/alarms` | `GET` | `application/json` | REST API — Returns active alarms across managed sites |

---

### Sample Payload Formats

#### 1. Galooli REST JSON Payload (`simulate_rms_sites.py`)
```json
{
  "vendor": "Galooli_RMM",
  "environment": "PRODUCTION_LIVE",
  "schema_version": "v3.2-production",
  "event_type": "ALARM_RAISED",
  "event_id": "galooli-prod-a1b2c3d4e5f6",
  "correlation_id": "corr-cluster-742",
  "timestamp": "2026-08-28T21:30:00.000Z",
  "site": {
    "site_id": "GAL_SITE_0042",
    "site_name": "Telecom Tower Site #0042",
    "site_type": "HUB_SITE",
    "sla_tier": "TIER_1_CRITICAL_SLA_99_99",
    "region": "Lagos_West",
    "geolocation": { "latitude": 6.5664, "longitude": 3.4212, "altitude_m": 45.2 },
    "maintenance_vendor": "Huawei_NetEco",
    "assigned_field_engineer": "Eng. Chidi Okafor"
  },
  "rtu_gateway": {
    "unit_id": "RTU_GAL_0042",
    "firmware_version": "v5.24.1-galooli-prod",
    "cellular_signal_rssi_dbm": -72,
    "connection_mode": "4G_LTE_PRIMARY"
  },
  "alarm": {
    "alarm_code": "AC_MAINS_BLACKOUT_FAULT",
    "category": "POWER_GRID",
    "severity": "CRITICAL",
    "status": "ACTIVE_RAISED",
    "first_occurrence_time": "2026-08-28T21:30:00.000Z",
    "description": "Complete 3-Phase AC Grid Utility Power Outage detected at Site Transfer Switch"
  },
  "telemetry_snapshot": {
    "ac_power_grid": { "grid_status": "OUTAGE", "phase_a_volts": 0.0, "phase_b_volts": 0.0, "phase_c_volts": 0.0, "frequency_hz": 0.0 },
    "generator_subsystem": { "status": "RUNNING", "fuel_level_percent": 84.5, "fuel_volume_liters": 422.5, "coolant_temp_c": 89.5 },
    "battery_storage_bank": { "dc_bus_voltage_v": 52.8, "state_of_charge_percent": 94.2, "state_of_health_percent": 98.1 },
    "shelter_environment": { "ambient_temp_c": 24.5, "hvac_status": "RUNNING", "door_contact": "CLOSED" }
  }
}
```

#### 2. Enterprise SOAP XML Payload (`simulate_soap_server.py`)
```xml
<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:rms="http://rms.telecom.enterprise/services/AlarmService">
   <soapenv:Header>
      <rms:SecurityHeader>
         <rms:AccountID>ACC_TELECOM_OPERATOR_01</rms:AccountID>
         <rms:AuthToken>BEARER-PROD-TOKEN-SITE-0042-ACTIVE</rms:AuthToken>
         <rms:MessageID>evt-prod-9f8e7d6c5b4a</rms:MessageID>
      </rms:SecurityHeader>
   </soapenv:Header>
   <soapenv:Body>
      <rms:RaiseProductionSiteAlarmRequest>
         <rms:SiteMetadata>
            <rms:SiteID>SOAP_SITE_0042</rms:SiteID>
            <rms:SiteName>Tower #0042</rms:SiteName>
            <rms:SiteType>HUB_SITE_CRITICAL</rms:SiteType>
            <rms:SLATier>TIER_1_SLA_99_99</rms:SLATier>
            <rms:Region>Lagos_West</rms:Region>
         </rms:SiteMetadata>
         <rms:AlarmEvent>
            <rms:AlarmCode>AC_MAINS_BLACKOUT_FAULT</rms:AlarmCode>
            <rms:Severity>CRITICAL</rms:Severity>
            <rms:Status>RAISED</rms:Status>
            <rms:Description>Complete 3-Phase AC Grid Utility Power Outage</rms:Description>
         </rms:AlarmEvent>
      </rms:RaiseProductionSiteAlarmRequest>
   </soapenv:Body>
</soapenv:Envelope>
```

---

## 🧪 Verification & Testing Checklist

When testing your **EOS Gateway Backend** against this simulator suite, verify the following:

1. ✅ **Ingestion Throughput**: EOS Gateway accepts 200+ Galooli JSON payloads/second without dropping connections.
2. ✅ **SOAP XML Parsing**: EOS correctly parses `<rms:SiteID>` and `<rms:AlarmCode>` from WSDL-compliant SOAP envelopes.
3. ✅ **Heartbeat Watchdog**: EOS flags sites as `COMMUNICATION_LOSS` when a simulator site undergoes a 15-minute Comm Loss ("Ball Drop").
4. ✅ **Power Recovery Handling**: EOS receives `RTU_REBOOT_POWER_RECOVERY` alarms when a site recovers from a blackout.
5. ✅ **Alarm Deduplication**: EOS deduplicates identical raised alarms for the same site code.
