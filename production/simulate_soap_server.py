#!/usr/bin/env python3
"""
Production-Grade Live Enterprise RMS SOAP Server Simulator (2,000 Sites)
==========================================================================
Rebuilt from Scratch with Production-Grade Features:
 1. Persistent Thread-Safe Site State (2,000 Sites tracked across time)
 2. Full Alarm Lifecycle (RAISED -> ACKNOWLEDGED -> CLEARED state machine)
 3. Proactive Outbound Webhook Pusher (Sends live SOAP XML alarms to EOS Gateway)
 4. Multi-Threaded High-Concurrency HTTP Server (ThreadingTCPServer)
 5. Proper SOAP Fault Envelopes (<soapenv:Fault> for errors, bad XML, 401s)
 6. Bearer Token Auth & Security Header Validation
 7. Periodic Heartbeat / Keepalive Ping Dispatcher
 8. REST & SOAP Query API (GET /soap/api/v1/sites/{site_id}, GET /soap/api/v1/alarms)
"""

import argparse
import http.server
import json
import random
import socketserver
import sys
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import urllib.request
import urllib.error

DEFAULT_PORT = 8090
DEFAULT_SITES = 2000
DEFAULT_GATEWAY_URL = "http://localhost:8081/ingress/rms/soap"

SITE_TYPES = ["HUB_SITE_CRITICAL", "MACRO_TOWER_3G_4G_5G", "SOLAR_HYBRID_MICROGRID", "REMOTE_REPEATER"]
SITE_TIERS = ["TIER_1_SLA_99_99", "TIER_2_SLA_99_95", "TIER_3_SLA_99_90"]
REGIONS = ["Lagos_West", "Lagos_East", "Kano_Zone", "PortHarcourt_Cluster", "Abuja_FCT", "Ibadan_West"]
VENDORS = ["Huawei_NetEco", "ZTE_iFMS", "Vertiv_Enersure", "Cummins_PowerCommand"]
ENGINEERS = ["Eng. Chidi Okafor", "Eng. Aminu Bello", "Eng. Tunde Bakare", "Eng. Grace Danjuma"]

LIVE_ALARM_CATALOG = [
    {
        "code": "AC_MAINS_BLACKOUT_FAULT",
        "severity": "CRITICAL",
        "category": "POWER_GRID",
        "desc": "Complete 3-Phase AC Grid Utility Power Outage detected at Site Transfer Switch",
        "root_cause_type": "PRIMARY_POWER_DISRUPTION",
        "sla_target_minutes": 15
    },
    {
        "code": "BATTERY_DC_LOW_DISCHARGE",
        "severity": "CRITICAL",
        "category": "ENERGY_STORAGE",
        "desc": "LiFePO4 DC Bus Voltage dropped below critical threshold.",
        "root_cause_type": "POWER_DEPLETION",
        "sla_target_minutes": 20
    },
    {
        "code": "FUEL_THEFT_SUDDEN_DROP",
        "severity": "CRITICAL",
        "category": "SECURITY_FUEL",
        "desc": "Ultrasonic fuel sensor detected sudden rapid drop in generator main tank",
        "root_cause_type": "PHYSICAL_SECURITY_INCIDENT",
        "sla_target_minutes": 10
    },
    {
        "code": "GEN_FAIL_TO_START_AUTO",
        "severity": "CRITICAL",
        "category": "GENERATOR",
        "desc": "ATS commanded Generator #1 to start after grid fail, but engine crank failed after 3 attempts",
        "root_cause_type": "EQUIPMENT_HARDWARE_FAILURE",
        "sla_target_minutes": 30
    },
    {
        "code": "SHELTER_HIGH_TEMP_ALARM",
        "severity": "MAJOR",
        "category": "ENVIRONMENT",
        "desc": "Shelter internal ambient temperature reached high threshold due to HVAC Compressor Fault",
        "root_cause_type": "COOLING_FAILURE",
        "sla_target_minutes": 45
    },
    {
        "code": "SECURITY_INTRUSION_DOOR_OPEN",
        "severity": "CRITICAL",
        "category": "PHYSICAL_SECURITY",
        "desc": "Shelter perimeter magnetic door contact broken without valid RFID technician check-in",
        "root_cause_type": "UNAUTHORIZED_ACCESS",
        "sla_target_minutes": 15
    }
]

WSDL_DOCUMENT = """<?xml version="1.0" encoding="UTF-8"?>
<definitions name="ProductionEnterpriseRMSAlarmService"
             targetNamespace="http://rms.telecom.enterprise/services/AlarmService"
             xmlns:tns="http://rms.telecom.enterprise/services/AlarmService"
             xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
             xmlns:xsd="http://www.w3.org/2001/XMLSchema"
             xmlns="http://schemas.xmlsoap.org/wsdl/">

  <types>
    <xsd:schema targetNamespace="http://rms.telecom.enterprise/services/AlarmService">
      <xsd:element name="RaiseSiteAlarmRequest">
        <xsd:complexType>
          <xsd:sequence>
            <xsd:element name="SiteID" type="xsd:string"/>
            <xsd:element name="AlarmCode" type="xsd:string"/>
            <xsd:element name="Severity" type="xsd:string"/>
            <xsd:element name="Timestamp" type="xsd:string"/>
          </xsd:sequence>
        </xsd:complexType>
      </xsd:element>
      <xsd:element name="RaiseSiteAlarmResponse">
        <xsd:complexType>
          <xsd:sequence>
            <xsd:element name="Status" type="xsd:string"/>
            <xsd:element name="TransactionID" type="xsd:string"/>
          </xsd:sequence>
        </xsd:complexType>
      </xsd:element>
    </xsd:schema>
  </types>

  <message name="RaiseAlarmInput">
    <part name="parameters" element="tns:RaiseSiteAlarmRequest"/>
  </message>
  <message name="RaiseAlarmOutput">
    <part name="parameters" element="tns:RaiseSiteAlarmResponse"/>
  </message>

  <portType name="RMSAlarmPortType">
    <operation name="RaiseSiteAlarm">
      <input message="tns:RaiseAlarmInput"/>
      <output message="tns:RaiseAlarmOutput"/>
    </operation>
  </portType>

  <binding name="RMSAlarmBinding" type="tns:RMSAlarmPortType">
    <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
    <operation name="RaiseSiteAlarm">
      <soap:operation soapAction="http://rms.telecom.enterprise/services/RaiseSiteAlarm"/>
      <input><soap:body use="literal"/></input>
      <output><soap:body use="literal"/></output>
    </operation>
  </binding>

  <service name="RMSAlarmService">
    <port name="RMSAlarmPort" binding="tns:RMSAlarmBinding">
      <soap:address location="http://localhost:8090/soap/AlarmService"/>
    </port>
  </service>
</definitions>
"""

# Thread-safe persistent state memory for 2,000 sites
STATE_LOCK = threading.Lock()
PERSISTENT_SITES = {}
PROACTIVE_PUSH_ENABLED = False
GATEWAY_URL = DEFAULT_GATEWAY_URL
TOTAL_SITES_COUNT = DEFAULT_SITES

class SiteState:
    """Represents persistent state of a single cell site over time."""
    def __init__(self, site_id: int):
        self.site_id = site_id
        self.site_code = f"SOAP_SITE_{site_id:04d}"
        self.site_name = f"Tower #{site_id:04d}"
        self.site_type = SITE_TYPES[site_id % len(SITE_TYPES)]
        self.tier = SITE_TIERS[site_id % len(SITE_TIERS)]
        self.region = REGIONS[site_id % len(REGIONS)]
        self.vendor = VENDORS[site_id % len(VENDORS)]
        self.engineer = ENGINEERS[site_id % len(ENGINEERS)]
        self.lat = round(6.5244 + (site_id * 0.001), 4)
        self.lng = round(3.3792 + (site_id * 0.001), 4)

        # Dynamic State Variables
        self.grid_status = "HEALTHY"
        self.phase_a_v = round(random.uniform(220.0, 238.0), 1)
        self.phase_b_v = round(random.uniform(218.0, 236.0), 1)
        self.phase_c_v = round(random.uniform(221.0, 240.0), 1)
        self.grid_freq = round(random.uniform(49.8, 50.2), 2)

        self.tank_capacity_l = random.choice([300.0, 500.0, 750.0, 1000.0])
        self.fuel_pct = round(random.uniform(60.0, 95.0), 1)
        self.fuel_vol_l = round((self.fuel_pct / 100.0) * self.tank_capacity_l, 1)
        self.gen_status = "OFF"
        self.gen_coolant_temp = 28.0
        self.gen_oil_pressure = 4.5
        self.gen_starter_v = 25.2
        self.gen_run_hours = round(random.uniform(1000.0, 6000.0), 1)

        self.dc_bus_v = round(random.uniform(52.5, 54.2), 1)
        self.soc_pct = round(random.uniform(85.0, 99.0), 1)
        self.soh_pct = round(random.uniform(90.0, 99.0), 1)
        self.dischg_amps = 12.0
        self.autonomy_mins = 360

        self.shelter_temp = round(random.uniform(22.0, 27.0), 1)
        self.humidity_pct = round(random.uniform(45.0, 65.0), 1)
        self.hvac_status = "RUNNING"
        self.door_contact = "CLOSED"

        # Active Alarm Lifecycle Management
        self.active_alarm = None  # None or dict: {code, severity, status, event_id, raised_at}

    def simulate_tick(self) -> dict:
        """
        Wall-clock alarm lifecycle:
          RAISED  -> ACKNOWLEDGED after 30 real minutes
          CLEARED -> alarm deleted from memory after 2 real hours (saves server space)
        """
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        event_generated = None

        if self.active_alarm is None:
            if random.random() < 0.02:
                alarm_def = random.choice(LIVE_ALARM_CATALOG)
                self.active_alarm = {
                    "event_id": f"evt-prod-{uuid.uuid4().hex[:12]}",
                    "correlation_id": f"corr-cluster-{random.randint(100, 999)}",
                    "code": alarm_def["code"],
                    "category": alarm_def["category"],
                    "severity": alarm_def["severity"],
                    "status": "RAISED",
                    "desc": alarm_def["desc"],
                    "root_cause": alarm_def["root_cause_type"],
                    "sla_target_minutes": alarm_def["sla_target_minutes"],
                    "raised_at": now_iso,
                    "raised_epoch": now.timestamp()  # wall-clock reference
                }
                # Apply fault impact on telemetry
                if "BLACKOUT" in alarm_def["code"]:
                    self.grid_status = "OUTAGE"
                    self.phase_a_v = 0.0
                    self.phase_b_v = 0.0
                    self.phase_c_v = 0.0
                    self.grid_freq = 0.0
                    self.gen_status = "RUNNING"
                    self.gen_coolant_temp = 89.5
                elif "BATTERY" in alarm_def["code"]:
                    self.dc_bus_v = round(random.uniform(40.2, 42.5), 1)
                    self.soc_pct = round(random.uniform(9.0, 14.0), 1)
                    self.autonomy_mins = 20
                elif "FUEL" in alarm_def["code"]:
                    self.fuel_pct = round(random.uniform(6.0, 14.0), 1)
                    self.fuel_vol_l = round((self.fuel_pct / 100.0) * self.tank_capacity_l, 1)
                elif "TEMP" in alarm_def["code"]:
                    self.shelter_temp = round(random.uniform(45.0, 52.0), 1)
                    self.hvac_status = "COMPRESSOR_FAULT"
                elif "DOOR" in alarm_def["code"]:
                    self.door_contact = "UNAUTHORIZED_OPEN"

                event_generated = {"type": "ALARM_RAISED", "alarm": self.active_alarm.copy()}
            else:
                event_generated = {"type": "HEARTBEAT", "alarm": None}
        else:
            elapsed_minutes = (now.timestamp() - self.active_alarm["raised_epoch"]) / 60.0

            # ACK after 30 real minutes
            if elapsed_minutes >= 30.0 and self.active_alarm["status"] == "RAISED":
                self.active_alarm["status"] = "ACKNOWLEDGED"
                event_generated = {"type": "ALARM_ACKNOWLEDGED", "alarm": self.active_alarm.copy()}

            # DELETE after 2 real hours — no history kept, frees server memory
            elif elapsed_minutes >= 120.0:
                cleared_alarm = self.active_alarm.copy()
                cleared_alarm["status"] = "CLEARED"
                cleared_alarm["cleared_at"] = now_iso

                # Restore telemetry to healthy baseline
                self.grid_status = "HEALTHY"
                self.phase_a_v = round(random.uniform(222.0, 240.0), 1)
                self.phase_b_v = round(random.uniform(220.0, 238.0), 1)
                self.phase_c_v = round(random.uniform(223.0, 241.0), 1)
                self.grid_freq = round(random.uniform(49.8, 50.2), 2)
                self.gen_status = "OFF"
                self.gen_coolant_temp = 30.0
                self.dc_bus_v = round(random.uniform(52.5, 54.0), 1)
                self.soc_pct = round(random.uniform(88.0, 99.0), 1)
                self.autonomy_mins = 360
                self.shelter_temp = round(random.uniform(22.0, 26.5), 1)
                self.hvac_status = "RUNNING"
                self.door_contact = "CLOSED"

                self.active_alarm = None  # hard delete, no history stored
                event_generated = {"type": "ALARM_CLEARED", "alarm": cleared_alarm}
            else:
                event_generated = {"type": "HEARTBEAT", "alarm": None}

        return event_generated

    def to_soap_xml(self, event_data: dict) -> str:
        """Serializes current persistent site state and alarm event into SOAP XML Envelope."""
        now_iso = datetime.now(timezone.utc).isoformat()
        alarm = event_data.get("alarm")
        event_id = alarm["event_id"] if alarm else f"ping-{uuid.uuid4().hex[:12]}"
        correlation_id = alarm.get("correlation_id", f"corr-cluster-{random.randint(100, 999)}") if alarm else "N/A"
        alarm_code = alarm["code"] if alarm else "HEARTBEAT_KEEPALIVE_PING"
        category = alarm["category"] if alarm else "TELEMETRY_HEARTBEAT"
        severity = alarm["severity"] if alarm else "INFO"
        status = alarm["status"] if alarm else "NORMAL"
        desc = alarm["desc"] if alarm else "Periodic telemetry keepalive ping"
        root_cause = alarm.get("root_cause", "SYSTEM_HEALTHY") if alarm else "NONE"
        sla_target = alarm.get("sla_target_minutes", 0) if alarm else 0

        return f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:rms="http://rms.telecom.enterprise/services/AlarmService">
   <soapenv:Header>
      <rms:SecurityHeader>
         <rms:AccountID>ACC_TELECOM_OPERATOR_01</rms:AccountID>
         <rms:AuthToken>BEARER-PROD-TOKEN-SITE-{self.site_id:04d}-ACTIVE</rms:AuthToken>
         <rms:MessageID>{event_id}</rms:MessageID>
         <rms:CorrelationID>{correlation_id}</rms:CorrelationID>
      </rms:SecurityHeader>
   </soapenv:Header>
   <soapenv:Body>
      <rms:RaiseProductionSiteAlarmRequest>
         <rms:SiteMetadata>
            <rms:SiteID>{self.site_code}</rms:SiteID>
            <rms:SiteName>{self.site_name}</rms:SiteName>
            <rms:SiteType>{self.site_type}</rms:SiteType>
            <rms:SLATier>{self.tier}</rms:SLATier>
            <rms:Region>{self.region}</rms:Region>
            <rms:Coordinates lat="{self.lat}" lng="{self.lng}"/>
            <rms:MaintenanceVendor>{self.vendor}</rms:MaintenanceVendor>
            <rms:AssignedEngineer>{self.engineer}</rms:AssignedEngineer>
         </rms:SiteMetadata>

         <rms:AlarmEvent>
            <rms:EventID>{event_id}</rms:EventID>
            <rms:AlarmCode>{alarm_code}</rms:AlarmCode>
            <rms:Category>{category}</rms:Category>
            <rms:Severity>{severity}</rms:Severity>
            <rms:Status>{status}</rms:Status>
            <rms:FirstOccurrenceTime>{alarm.get("raised_at", now_iso) if alarm else now_iso}</rms:FirstOccurrenceTime>
            <rms:LastUpdatedTime>{now_iso}</rms:LastUpdatedTime>
            <rms:SLAResponseTargetMinutes>{sla_target}</rms:SLAResponseTargetMinutes>
            <rms:Description>{desc}</rms:Description>
            <rms:RootCauseType>{root_cause}</rms:RootCauseType>
         </rms:AlarmEvent>

         <rms:LiveTelemetrySnapshot>
            <rms:ACPowerGrid>
               <rms:GridStatus>{self.grid_status}</rms:GridStatus>
               <rms:PhaseAVoltage unit="V">{self.phase_a_v}</rms:PhaseAVoltage>
               <rms:PhaseBVoltage unit="V">{self.phase_b_v}</rms:PhaseBVoltage>
               <rms:PhaseCVoltage unit="V">{self.phase_c_v}</rms:PhaseCVoltage>
               <rms:Frequency unit="Hz">{self.grid_freq}</rms:Frequency>
            </rms:ACPowerGrid>

            <rms:GeneratorSubsystem>
               <rms:EngineStatus>{self.gen_status}</rms:EngineStatus>
               <rms:FuelLevelPercent unit="%">{self.fuel_pct}</rms:FuelLevelPercent>
               <rms:FuelVolumeLiters unit="L">{self.fuel_vol_l}</rms:FuelVolumeLiters>
               <rms:TankCapacityLiters unit="L">{self.tank_capacity_l}</rms:TankCapacityLiters>
               <rms:CoolantTemp unit="C">{self.gen_coolant_temp}</rms:CoolantTemp>
               <rms:OilPressure unit="Bar">{self.gen_oil_pressure}</rms:OilPressure>
               <rms:BatteryStarterVoltage unit="V">{self.gen_starter_v}</rms:BatteryStarterVoltage>
               <rms:TotalRunHours>{self.gen_run_hours}</rms:TotalRunHours>
            </rms:GeneratorSubsystem>

            <rms:BatteryStorageBank>
               <rms:Chemistry>Lithium_LiFePO4_48V</rms:Chemistry>
               <rms:DCBusVoltage unit="V">{self.dc_bus_v}</rms:DCBusVoltage>
               <rms:StateOfChargePercent unit="%">{self.soc_pct}</rms:StateOfChargePercent>
               <rms:StateOfHealthPercent unit="%">{self.soh_pct}</rms:StateOfHealthPercent>
               <rms:CurrentDischargeAmps unit="A">{self.dischg_amps}</rms:CurrentDischargeAmps>
               <rms:EstimatedAutonomyRemainingMinutes>{self.autonomy_mins}</rms:EstimatedAutonomyRemainingMinutes>
            </rms:BatteryStorageBank>

            <rms:ShelterEnvironment>
               <rms:AmbientTemperature unit="C">{self.shelter_temp}</rms:AmbientTemperature>
               <rms:HumidityPercent unit="%">{self.humidity_pct}</rms:HumidityPercent>
               <rms:HVACUnit1Status>{self.hvac_status}</rms:HVACUnit1Status>
               <rms:DoorSensorStatus>{self.door_contact}</rms:DoorSensorStatus>
               <rms:SmokeDetectorStatus>NORMAL</rms:SmokeDetectorStatus>
               <rms:FloodSensorStatus>NORMAL</rms:FloodSensorStatus>
            </rms:ShelterEnvironment>
         </rms:LiveTelemetrySnapshot>
      </rms:RaiseProductionSiteAlarmRequest>
   </soapenv:Body>
</soapenv:Envelope>"""

def initialize_persistent_sites(count: int):
    with STATE_LOCK:
        PERSISTENT_SITES.clear()
        for i in range(1, count + 1):
            PERSISTENT_SITES[i] = SiteState(i)

def generate_soap_fault_xml(fault_code: str, fault_string: str, detail: str = "") -> str:
    """Generates standard WSDL SOAP Fault Envelope for errors/401s/bad XML."""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
   <soapenv:Body>
      <soapenv:Fault>
         <faultcode>{fault_code}</faultcode>
         <faultstring>{fault_string}</faultstring>
         <detail>{detail}</detail>
      </soapenv:Fault>
   </soapenv:Body>
</soapenv:Envelope>"""

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Multi-threaded TCP server ensuring high-concurrency non-blocking handling."""
    allow_reuse_address = True

class ProductionSOAPHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        sys_time = datetime.now().strftime("%H:%M:%S")
        print(f"  [{sys_time}] Live Enterprise RMS SOAP Server: {args[0]} - {args[1]}")

    def do_GET(self):
        if self.path.startswith("/soap/AlarmService") and "wsdl" in self.path.lower():
            self.send_response(200)
            self.send_header("Content-Type", "text/xml; charset=utf-8")
            self.end_headers()
            self.wfile.write(WSDL_DOCUMENT.encode("utf-8"))
        elif self.path.startswith("/soap/api/v1/sites/"):
            # Query specific site status (e.g. GET /soap/api/v1/sites/42)
            try:
                site_id = int(self.path.split("/")[-1])
                with STATE_LOCK:
                    site_obj = PERSISTENT_SITES.get(site_id)
                if site_obj:
                    res_body = json.dumps({
                        "site_id": site_obj.site_code,
                        "tier": site_obj.tier,
                        "region": site_obj.region,
                        "grid_status": site_obj.grid_status,
                        "fuel_percent": site_obj.fuel_pct,
                        "battery_soc": site_obj.soc_pct,
                        "shelter_temp": site_obj.shelter_temp,
                        "active_alarm": site_obj.active_alarm
                    }, indent=2).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(res_body)
                    return
            except ValueError:
                pass
            self.send_response(404)
            self.end_headers()
        elif self.path.startswith("/soap/api/v1/alarms"):
            # Return live alarm snapshot across persistent site memory
            with STATE_LOCK:
                active_list = []
                for s_id, s_obj in PERSISTENT_SITES.items():
                    if s_obj.active_alarm:
                        active_list.append({
                            "site_id": s_obj.site_code,
                            "alarm": s_obj.active_alarm,
                            "grid_status": s_obj.grid_status
                        })
            res_body = json.dumps({
                "server": "Live_Enterprise_SOAP_RMS_Server",
                "total_sites": TOTAL_SITES_COUNT,
                "active_alarm_count": len(active_list),
                "active_alarms": active_list[:50]  # Return top 50 active
            }, indent=2).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(res_body)
        else:
            fault_xml = generate_soap_fault_xml("SOAP-ENV:Client", "Resource Not Found", f"Unknown path {self.path}")
            self.send_response(404)
            self.send_header("Content-Type", "text/xml; charset=utf-8")
            self.end_headers()
            self.wfile.write(fault_xml.encode("utf-8"))

    def do_POST(self):
        if self.path.startswith("/soap/AlarmService"):
            content_len = int(self.headers.get("Content-Length", 0))
            post_body = self.rfile.read(content_len).decode("utf-8", errors="ignore")

            # Auth Token Check
            auth_header = self.headers.get("Authorization", "")
            if not auth_header and "AuthToken" not in post_body:
                fault_xml = generate_soap_fault_xml("SOAP-ENV:Client.Authentication", "401 Unauthorized: Missing Bearer AuthToken")
                self.send_response(401)
                self.send_header("Content-Type", "text/xml; charset=utf-8")
                self.end_headers()
                self.wfile.write(fault_xml.encode("utf-8"))
                return

            site_id = "UNKNOWN"
            alarm_code = "UNKNOWN"
            try:
                root = ET.fromstring(post_body)
                for elem in root.iter():
                    if elem.tag.endswith("SiteID"):
                        site_id = elem.text
                    elif elem.tag.endswith("AlarmCode"):
                        alarm_code = elem.text
            except Exception as e:
                fault_xml = generate_soap_fault_xml("SOAP-ENV:Client.XMLParseException", "Invalid SOAP XML Payload", str(e))
                self.send_response(400)
                self.send_header("Content-Type", "text/xml; charset=utf-8")
                self.end_headers()
                self.wfile.write(fault_xml.encode("utf-8"))
                return

            tx_id = f"tx_prod_soap_{uuid.uuid4().hex[:10]}"
            response_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:rms="http://rms.telecom.enterprise/services/AlarmService">
   <soapenv:Body>
      <rms:RaiseProductionSiteAlarmResponse>
         <rms:Status>SUCCESS_PROCESSED</rms:Status>
         <rms:TransactionID>{tx_id}</rms:TransactionID>
         <rms:ReceivedSite>{site_id}</rms:ReceivedSite>
         <rms:ReceivedAlarm>{alarm_code}</rms:ReceivedAlarm>
         <rms:ActiveManagedSites>{TOTAL_SITES_COUNT}</rms:ActiveManagedSites>
         <rms:KafkaIngestionStatus>READY</rms:KafkaIngestionStatus>
         <rms:ProcessedAt>{datetime.now(timezone.utc).isoformat()}</rms:ProcessedAt>
      </rms:RaiseProductionSiteAlarmResponse>
   </soapenv:Body>
</soapenv:Envelope>"""

            self.send_response(200)
            self.send_header("Content-Type", "text/xml; charset=utf-8")
            self.end_headers()
            self.wfile.write(response_xml.encode("utf-8"))
        else:
            fault_xml = generate_soap_fault_xml("SOAP-ENV:Client", "Endpoint Not Found", self.path)
            self.send_response(404)
            self.send_header("Content-Type", "text/xml; charset=utf-8")
            self.end_headers()
            self.wfile.write(fault_xml.encode("utf-8"))

def proactive_background_pusher(gateway_url: str):
    """Background worker that continuously updates persistent site states and proactively pushes webhooks."""
    print(f"[+] Proactive Outbound Webhook Worker active. Target: {gateway_url}")
    while True:
        try:
            site_id = random.randint(1, TOTAL_SITES_COUNT)
            with STATE_LOCK:
                site_obj = PERSISTENT_SITES.get(site_id)
                event_data = site_obj.simulate_tick() if site_obj else None

            if event_data and PROACTIVE_PUSH_ENABLED:
                soap_xml = site_obj.to_soap_xml(event_data)
                req = urllib.request.Request(
                    gateway_url,
                    data=soap_xml.encode("utf-8"),
                    headers={"Content-Type": "application/soap+xml", "Authorization": "Bearer PROD-RMS-SECRET"},
                    method="POST"
                )
                try:
                    with urllib.request.urlopen(req, timeout=3) as resp:
                        sys_time = datetime.now().strftime("%H:%M:%S")
                        alarm_code = event_data['alarm']['code'] if event_data.get('alarm') else 'HEARTBEAT'
                        print(f"  [{sys_time}] Proactive SOAP Push -> Site #{site_id:04d} | Alarm: {alarm_code:<25} ({event_data['type']}) | Gateway HTTP {resp.status}")
                except Exception:
                    pass
        except Exception:
            pass
        time.sleep(0.1)

def run_soap_server(port: int, sites: int, gateway: str, enable_push: bool):
    global TOTAL_SITES_COUNT, PROACTIVE_PUSH_ENABLED, GATEWAY_URL
    TOTAL_SITES_COUNT = sites
    PROACTIVE_PUSH_ENABLED = enable_push
    GATEWAY_URL = gateway

    initialize_persistent_sites(sites)

    # Start proactive background worker thread
    pusher_thread = threading.Thread(target=proactive_background_pusher, args=(gateway,), daemon=True)
    pusher_thread.start()

    print("=========================================================================")
    print("  REBUILT Production Enterprise RMS SOAP Server (2,000+ Sites)          ")
    print("  PERSISTENT SITE MEMORY | ALARM LIFECYCLE | PROACTIVE WEBHOOK PUSHER      ")
    print("=========================================================================")
    print(f"  Active Sites Managed  : {sites} Sites (SOAP_SITE_0001 -> SOAP_SITE_{sites:04d})")
    print(f"  SOAP Receiver URL     : http://localhost:{port}/soap/AlarmService")
    print(f"  WSDL Contract         : http://localhost:{port}/soap/AlarmService?wsdl")
    print(f"  Site Query REST API   : http://localhost:{port}/soap/api/v1/sites/42")
    print(f"  Proactive Gateway Push: {'ENABLED -> ' + gateway if enable_push else 'DISABLED (Use --push to enable)'}")
    print("=========================================================================\n")
    print(f"[+] Multi-threaded server running on port {port}... Press Ctrl+C to stop.")

    server = ThreadedTCPServer(("", port), ProductionSOAPHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[!] Shutting down Enterprise SOAP Server.")
        server.server_close()

def main():
    parser = argparse.ArgumentParser(description="Live Production Enterprise RMS SOAP Server (2,000 Sites)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port to run SOAP Server on (default: 8090)")
    parser.add_argument("--sites", type=int, default=DEFAULT_SITES, help="Number of managed sites (default: 2000)")
    parser.add_argument("--gateway", type=str, default=DEFAULT_GATEWAY_URL, help="EOS Gateway URL for proactive webhooks")
    parser.add_argument("--push", action="store_true", help="Enable proactive background SOAP webhook pushes to EOS Gateway")
    args = parser.parse_args()

    run_soap_server(args.port, args.sites, args.gateway, args.push)

if __name__ == "__main__":
    main()
