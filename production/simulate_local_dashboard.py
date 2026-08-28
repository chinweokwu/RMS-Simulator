#!/usr/bin/env python3
"""
Standalone Live RMS & Galooli In-Memory Telemetry Dashboard
============================================================
Runs locally with ZERO network or database dependencies.
Fully upgraded to match simulate_rms_sites.py production logic:
 - Persistent 2,000-site state (PersistentGalooliSite reused)
 - Full Alarm Lifecycle (RAISED -> ACKNOWLEDGED -> CLEARED)
 - Heartbeat / Keepalive Telemetry Events
 - Full 4-subsystem telemetry (Grid, Generator, Battery, Environment)
 - Root Cause, SLA Countdown, Door Contact, HVAC Status
 - Live terminal UI refreshed every second
"""

import os
import random
import threading
import time
import uuid
from datetime import datetime, timezone

# ─── Site Catalog ─────────────────────────────────────────────────────────────
SITE_TIERS   = ["TIER_1_CRITICAL_SLA_99_99", "TIER_2_SLA_99_95", "TIER_3_SLA_99_90"]
SITE_TYPES   = ["HUB_SITE", "MACRO_TOWER_3G_4G_5G", "SOLAR_HYBRID_MICROGRID", "REMOTE_REPEATER"]
REGIONS      = ["Lagos_West", "Lagos_East", "Kano_Zone", "PortHarcourt_Cluster", "Abuja_FCT", "Ibadan_West"]
VENDORS      = ["Huawei_NetEco", "Galooli_RMM", "ZTE_iFMS", "Vertiv_Enersure", "Cummins_PowerCommand"]
ENGINEERS    = ["Eng. Chidi Okafor", "Eng. Aminu Bello", "Eng. Tunde Bakare", "Eng. Grace Danjuma"]

LIVE_ALARM_CATALOG = [
    {"code": "AC_MAINS_BLACKOUT_FAULT",      "severity": "CRITICAL", "category": "POWER_GRID",       "desc": "3-Phase AC Grid Outage at Site Transfer Switch",                          "root_cause": "PRIMARY_POWER_DISRUPTION",    "sla_target_minutes": 15},
    {"code": "BATTERY_DC_LOW_DISCHARGE",     "severity": "CRITICAL", "category": "ENERGY_STORAGE",   "desc": "LiFePO4 DC Bus below critical threshold. Impending site outage.",          "root_cause": "POWER_DEPLETION",             "sla_target_minutes": 20},
    {"code": "FUEL_THEFT_SUDDEN_DROP",       "severity": "CRITICAL", "category": "SECURITY_FUEL",    "desc": "Ultrasonic sensor detected sudden fuel drop in main tank",                 "root_cause": "PHYSICAL_SECURITY_INCIDENT",  "sla_target_minutes": 10},
    {"code": "GEN_FAIL_TO_START_AUTO",       "severity": "CRITICAL", "category": "GENERATOR",        "desc": "ATS commanded Generator #1 to start after grid fail. Crank failed x3.",   "root_cause": "EQUIPMENT_HARDWARE_FAILURE",  "sla_target_minutes": 30},
    {"code": "SHELTER_HIGH_TEMP_ALARM",      "severity": "MAJOR",    "category": "ENVIRONMENT",      "desc": "Shelter temp exceeded limit. HVAC Compressor Fault detected.",             "root_cause": "COOLING_FAILURE",             "sla_target_minutes": 45},
    {"code": "SECURITY_INTRUSION_DOOR_OPEN", "severity": "CRITICAL", "category": "PHYSICAL_SECURITY","desc": "Magnetic door contact broken without valid RFID technician check-in.",      "root_cause": "UNAUTHORIZED_ACCESS",         "sla_target_minutes": 15},
]

# ─── Thread-Safe State ────────────────────────────────────────────────────────
STATE_LOCK     = threading.Lock()
PERSISTENT_SITES = {}
ALARM_LOG      = []   # Rolling last 10 lifecycle events
METRICS        = {"total_events": 0, "critical": 0, "major": 0, "heartbeats": 0,
                  "raised": 0, "acknowledged": 0, "cleared": 0, "grid_blackouts": 0}

# ─── Persistent Site Class ────────────────────────────────────────────────────
class PersistentGalooliSite:
    """Persistent live site state — matches simulate_rms_sites.py exactly."""
    def __init__(self, site_id: int):
        self.site_id     = site_id
        self.site_code   = f"GAL_SITE_{site_id:04d}"
        self.site_name   = f"Telecom Tower Site #{site_id:04d}"
        self.site_type   = SITE_TYPES[site_id % len(SITE_TYPES)]
        self.sla_tier    = SITE_TIERS[site_id % len(SITE_TIERS)]
        self.region      = REGIONS[site_id % len(REGIONS)]
        self.vendor      = VENDORS[site_id % len(VENDORS)]
        self.engineer    = ENGINEERS[site_id % len(ENGINEERS)]

        self.grid_status = "HEALTHY"
        self.phase_a_v   = round(random.uniform(220.0, 240.0), 1)
        self.phase_b_v   = round(random.uniform(218.0, 238.0), 1)
        self.phase_c_v   = round(random.uniform(221.0, 241.0), 1)
        self.grid_freq   = round(random.uniform(49.8, 50.2), 2)

        self.tank_capacity_l = random.choice([300.0, 500.0, 750.0, 1000.0])
        self.fuel_pct        = round(random.uniform(65.0, 98.0), 1)
        self.fuel_vol_l      = round((self.fuel_pct / 100.0) * self.tank_capacity_l, 1)
        self.gen_status      = "OFF"
        self.gen_coolant_temp = 28.5
        self.gen_oil_pressure = 4.4
        self.gen_starter_v    = 25.1
        self.gen_run_hours    = round(random.uniform(900.0, 8500.0), 1)

        self.dc_bus_v      = round(random.uniform(52.8, 54.4), 1)
        self.soc_pct       = round(random.uniform(88.0, 99.5), 1)
        self.soh_pct       = round(random.uniform(90.0, 99.5), 1)
        self.dischg_amps   = 14.2
        self.autonomy_mins = 420

        self.shelter_temp  = round(random.uniform(22.0, 26.8), 1)
        self.humidity_pct  = round(random.uniform(42.0, 68.0), 1)
        self.hvac_status   = "RUNNING"
        self.door_contact  = "CLOSED"
        self.active_alarm  = None

    def tick(self) -> dict:
        """
        Wall-clock alarm lifecycle:
          RAISED  -> ACKNOWLEDGED after 30 real minutes
          CLEARED -> alarm deleted from memory after 2 real hours (saves server space)
        """
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()

        if self.active_alarm is None:
            if random.random() < 0.03:
                alarm_def = random.choice(LIVE_ALARM_CATALOG)
                self.active_alarm = {
                    "event_id":   f"galooli-prod-{uuid.uuid4().hex[:12]}",
                    "code":       alarm_def["code"],
                    "category":   alarm_def["category"],
                    "severity":   alarm_def["severity"],
                    "status":     "ACTIVE_RAISED",
                    "desc":       alarm_def["desc"],
                    "root_cause": alarm_def["root_cause"],
                    "sla_target_minutes": alarm_def["sla_target_minutes"],
                    "raised_at":  now_iso,
                    "raised_epoch": now.timestamp()  # wall-clock reference
                }
                if "BLACKOUT" in alarm_def["code"]:
                    self.grid_status = "OUTAGE";  self.phase_a_v = 0.0; self.phase_b_v = 0.0; self.phase_c_v = 0.0; self.gen_status = "RUNNING"
                elif "BATTERY" in alarm_def["code"]:
                    self.dc_bus_v = round(random.uniform(40.5, 42.6), 1); self.soc_pct = round(random.uniform(9.0, 14.0), 1)
                elif "FUEL" in alarm_def["code"]:
                    self.fuel_pct = round(random.uniform(7.0, 15.0), 1); self.fuel_vol_l = round((self.fuel_pct / 100.0) * self.tank_capacity_l, 1)
                elif "TEMP" in alarm_def["code"]:
                    self.shelter_temp = round(random.uniform(45.5, 51.5), 1); self.hvac_status = "COMPRESSOR_FAULT"
                elif "DOOR" in alarm_def["code"]:
                    self.door_contact = "UNAUTHORIZED_OPEN"
                return {"event_type": "ALARM_RAISED", "alarm": self.active_alarm.copy()}
            else:
                return {"event_type": "HEARTBEAT", "alarm": None}
        else:
            elapsed_minutes = (now.timestamp() - self.active_alarm["raised_epoch"]) / 60.0

            # ACK after 30 real minutes
            if elapsed_minutes >= 30.0 and self.active_alarm["status"] == "ACTIVE_RAISED":
                self.active_alarm["status"] = "ACKNOWLEDGED"
                return {"event_type": "ALARM_ACKNOWLEDGED", "alarm": self.active_alarm.copy()}

            # DELETE after 2 real hours — no history kept, frees server memory
            elif elapsed_minutes >= 120.0:
                cleared = self.active_alarm.copy(); cleared["status"] = "CLEARED"; cleared["cleared_at"] = now_iso
                self.grid_status = "HEALTHY"; self.phase_a_v = round(random.uniform(222.0, 240.0), 1)
                self.phase_b_v = round(random.uniform(220.0, 238.0), 1); self.phase_c_v = round(random.uniform(223.0, 241.0), 1)
                self.gen_status = "OFF"; self.dc_bus_v = round(random.uniform(52.8, 54.4), 1)
                self.soc_pct = round(random.uniform(88.0, 99.0), 1); self.shelter_temp = round(random.uniform(22.0, 26.5), 1)
                self.hvac_status = "RUNNING"; self.door_contact = "CLOSED"
                self.active_alarm = None  # hard delete, no history stored
                return {"event_type": "ALARM_CLEARED", "alarm": cleared}
            else:
                return {"event_type": "HEARTBEAT", "alarm": None}

# ─── Engine: Background Site State Ticker ────────────────────────────────────
def simulation_engine(sites_count: int):
    """Background thread: ticks all 2,000 sites continuously."""
    while True:
        site_id = random.randint(1, sites_count)
        with STATE_LOCK:
            site_obj = PERSISTENT_SITES[site_id]
            event_data = site_obj.tick()

        et = event_data["event_type"]
        alarm = event_data["alarm"]

        with STATE_LOCK:
            METRICS["total_events"] += 1
            if et == "HEARTBEAT":
                METRICS["heartbeats"] += 1
            elif et == "ALARM_RAISED":
                METRICS["raised"] += 1
                if alarm["severity"] == "CRITICAL": METRICS["critical"] += 1
                elif alarm["severity"] == "MAJOR":  METRICS["major"] += 1
                if "BLACKOUT" in alarm["code"]:     METRICS["grid_blackouts"] += 1
                ALARM_LOG.append({"time": datetime.now().strftime("%H:%M:%S"), "site": site_obj.site_code,
                                  "code": alarm["code"], "severity": alarm["severity"], "status": "RAISED",
                                  "root_cause": alarm.get("root_cause", "—")})
            elif et == "ALARM_ACKNOWLEDGED":
                METRICS["acknowledged"] += 1
                ALARM_LOG.append({"time": datetime.now().strftime("%H:%M:%S"), "site": site_obj.site_code,
                                  "code": alarm["code"], "severity": alarm["severity"], "status": "ACKNOWLEDGED",
                                  "root_cause": alarm.get("root_cause", "—")})
            elif et == "ALARM_CLEARED":
                METRICS["cleared"] += 1
                ALARM_LOG.append({"time": datetime.now().strftime("%H:%M:%S"), "site": site_obj.site_code,
                                  "code": alarm["code"], "severity": alarm["severity"], "status": "CLEARED",
                                  "root_cause": alarm.get("root_cause", "—")})
            if len(ALARM_LOG) > 12:
                ALARM_LOG.pop(0)

        time.sleep(0.02)

# ─── Terminal Dashboard Renderer ──────────────────────────────────────────────
def render_dashboard(sites_count: int):
    """Renders a live, refreshing terminal dashboard."""
    os.system('cls' if os.name == 'nt' else 'clear')

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with STATE_LOCK:
        m = METRICS.copy()
        log = list(ALARM_LOG)
        # Pick a random site with an active alarm for spotlight
        active_alarm_sites = [s for s in PERSISTENT_SITES.values() if s.active_alarm]
        spotlight = random.choice(active_alarm_sites) if active_alarm_sites else random.choice(list(PERSISTENT_SITES.values()))

    total_active = m["raised"] - m["cleared"]

    # Header
    print("\033[96m" + "═" * 95 + "\033[0m")
    print("\033[96m   ██████╗ ███╗   ███╗███████╗    LIVE RMS IN-MEMORY DASHBOARD\033[0m")
    print(f"\033[96m   EOS Platform — RMS Simulator  |  {now}  |  Sites: {sites_count:,}\033[0m")
    print("\033[96m" + "═" * 95 + "\033[0m")

    # Metrics row
    print(f"\n  {'Total Events':<22}: \033[97m{m['total_events']:>8,}\033[0m   "
          f"{'Heartbeats':<18}: \033[92m{m['heartbeats']:>7,}\033[0m")
    print(f"  {'RAISED':<22}: \033[91m{m['raised']:>8,}\033[0m   "
          f"{'ACKNOWLEDGED':<18}: \033[93m{m['acknowledged']:>7,}\033[0m")
    print(f"  {'CLEARED':<22}: \033[92m{m['cleared']:>8,}\033[0m   "
          f"{'Active Alarms Now':<18}: \033[91m{max(0, total_active):>7,}\033[0m")
    print(f"  {'Critical Alarms':<22}: \033[91m{m['critical']:>8,}\033[0m   "
          f"{'Grid Blackouts':<18}: \033[95m{m['grid_blackouts']:>7,}\033[0m")
    print(f"  {'Major Alarms':<22}: \033[93m{m['major']:>8,}\033[0m")

    # Alarm lifecycle log
    print("\n\033[96m  ─── LIVE ALARM LIFECYCLE STREAM (Last 12 Events) ──────────────────────────────────────\033[0m")
    for entry in reversed(log):
        sev_col  = "\033[91m" if entry["severity"] == "CRITICAL" else "\033[93m"
        if entry["status"] == "RAISED":       st_col = "\033[91m[RAISED]      \033[0m"
        elif entry["status"] == "ACKNOWLEDGED": st_col = "\033[93m[ACKNOWLEDGED]\033[0m"
        else:                                  st_col = "\033[92m[CLEARED]     \033[0m"
        print(f"   [{entry['time']}] {entry['site']} | {st_col} | {sev_col}{entry['code']:<30}\033[0m | {entry['root_cause']}")

    # Site spotlight panel
    print(f"\n\033[96m  ─── SITE SPOTLIGHT: {spotlight.site_code} ({spotlight.region}) ──────────────────────────────────────\033[0m")
    print(f"   Type     : {spotlight.site_type:<35}  SLA Tier : {spotlight.sla_tier}")
    print(f"   Vendor   : {spotlight.vendor:<35}  Engineer : {spotlight.engineer}")

    gstatus_col = "\033[91mOUTAGE \033[0m" if spotlight.grid_status == "OUTAGE" else "\033[92mHEALTHY\033[0m"
    print(f"\n   \033[94m[AC GRID]\033[0m  Status: {gstatus_col}  "
          f"PhA: {spotlight.phase_a_v}V  PhB: {spotlight.phase_b_v}V  PhC: {spotlight.phase_c_v}V  Freq: {spotlight.grid_freq}Hz")

    gen_col = "\033[91m" if spotlight.gen_status in ["RUNNING", "FAULT_STOPPED"] else "\033[92m"
    print(f"   \033[94m[GENSET]\033[0m  Status: {gen_col}{spotlight.gen_status:<16}\033[0m  "
          f"Fuel: {spotlight.fuel_pct}% ({spotlight.fuel_vol_l}L/{spotlight.tank_capacity_l}L)  "
          f"Coolant: {spotlight.gen_coolant_temp}°C  Oil: {spotlight.gen_oil_pressure}Bar  "
          f"RunHrs: {spotlight.gen_run_hours}h")

    soc_col = "\033[91m" if spotlight.soc_pct < 20 else "\033[92m"
    print(f"   \033[94m[BATT  ]\033[0m  DC Bus: {spotlight.dc_bus_v}V  "
          f"SOC: {soc_col}{spotlight.soc_pct}%\033[0m  SOH: {spotlight.soh_pct}%  "
          f"Discharge: {spotlight.dischg_amps}A  Autonomy: {spotlight.autonomy_mins}min")

    hvac_col = "\033[91m" if spotlight.hvac_status != "RUNNING" else "\033[92m"
    door_col = "\033[91m" if spotlight.door_contact != "CLOSED" else "\033[92m"
    print(f"   \033[94m[ENVIRO]\033[0m  Temp: {spotlight.shelter_temp}°C  "
          f"Humidity: {spotlight.humidity_pct}%  "
          f"HVAC: {hvac_col}{spotlight.hvac_status}\033[0m  "
          f"Door: {door_col}{spotlight.door_contact}\033[0m")

    # Active alarm on spotlight site
    if spotlight.active_alarm:
        a = spotlight.active_alarm
        st_colors = {"ACTIVE_RAISED": "\033[91m", "ACKNOWLEDGED": "\033[93m", "CLEARED": "\033[92m"}
        sc = st_colors.get(a["status"], "\033[97m")
        print(f"\n   \033[91m⚠ ACTIVE ALARM\033[0m  {sc}{a['status']}\033[0m  |  {a['code']}  |  Root Cause: {a['root_cause']}")
        print(f"            SLA Target: {a['sla_target_minutes']} min  |  Raised: {a.get('raised_at','—')[:19].replace('T',' ')}  |  Ticks: {a['ticks_active']}")
    else:
        print(f"\n   \033[92m✓ No active alarm — Site operating normally (HEARTBEAT)\033[0m")

    print("\n\033[96m" + "═" * 95 + "\033[0m")
    print("  Press Ctrl+C to stop simulation.")

# ─── Entry Point ──────────────────────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Standalone Live RMS In-Memory Telemetry Dashboard")
    parser.add_argument("--sites", type=int, default=2000, help="Number of simulated sites (default: 2000)")
    parser.add_argument("--refresh", type=float, default=1.0, help="Dashboard refresh interval in seconds (default: 1.0)")
    args = parser.parse_args()

    print(f"[+] Initializing {args.sites:,} persistent site states...")
    with STATE_LOCK:
        for i in range(1, args.sites + 1):
            PERSISTENT_SITES[i] = PersistentGalooliSite(i)
    print(f"[+] {args.sites:,} sites ready. Starting live simulation engine...")

    engine_thread = threading.Thread(target=simulation_engine, args=(args.sites,), daemon=True)
    engine_thread.start()

    time.sleep(0.5)  # Let engine warm up

    try:
        while True:
            render_dashboard(args.sites)
            time.sleep(args.refresh)
    except KeyboardInterrupt:
        print("\n[!] Dashboard stopped. Memory cleared.")

if __name__ == "__main__":
    main()
