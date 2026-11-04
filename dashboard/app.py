"""
ChildSafe dashboard + geofence engine.
Run:  streamlit run app.py
Env:  TG_TOKEN, TG_CHAT_ID (Telegram), optional DEVICE_ID, MQTT_HOST, MQTT_PORT
"""
import collections, json, math, os, threading, time

import pandas as pd
import paho.mqtt.client as mqtt
import pydeck as pdk
import requests
import streamlit as st

DEVICE_ID = os.getenv("DEVICE_ID", "kid01-change-me-7f3a")   # must match firmware
MQTT_HOST = os.getenv("MQTT_HOST", "broker.hivemq.com")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

EARTH_R_M = 6371008.8   # mean Earth radius (metres)
CONFIRM_N = 3           # consecutive outside-fixes needed before a breach alert (kills GPS jitter)
HYST_M    = 10.0        # must come back this far INSIDE the fence to re-arm (prevents flapping)
OFFLINE_S = 90          # armed + silent this long => "tracker offline" alert


# ------------------------------------------------------------------ geometry
def haversine_m(lat1, lon1, lat2, lon2):
    """Great-circle distance in metres between two WGS-84 points."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * EARTH_R_M * math.asin(min(1.0, math.sqrt(a)))   # min() guards float rounding


# ------------------------------------------------------------------ alerts
def send_telegram(text, lat=None, lng=None):
    token, chat = os.getenv("TG_TOKEN"), os.getenv("TG_CHAT_ID")
    if not token or not chat:
        print("[ALERT - Telegram not configured]", text)
        return
    base = f"https://api.telegram.org/bot{token}"
    try:
        requests.post(f"{base}/sendMessage", json={"chat_id": chat, "text": text}, timeout=8)
        if lat is not None and lng is not None:      # drops a tappable map pin in the chat
            requests.post(f"{base}/sendLocation",
                          json={"chat_id": chat, "latitude": lat, "longitude": lng}, timeout=8)
    except requests.RequestException as e:
        print("[Telegram error]", e)


# ------------------------------------------------------------------ engine
class Engine:
    """Thread-safe shared state: written by MQTT thread, read by Streamlit sessions."""

    def __init__(self):
        self.lock = threading.RLock()
        self.cfg = dict(armed=False, lat=0.0, lng=0.0, radius=200.0)
        self.latest, self.last_seen = {}, 0.0
        self.last_pos = None                         # (lat, lng) of last good fix
        self.track = collections.deque(maxlen=500)
        self.dist = None
        self.outside_streak, self.breach = 0, False
        self.prev_sos, self.last_sos_n = False, None
        self.offline_alerted = False
        self.device_status = "unknown"
        self.events = collections.deque(maxlen=200)

    # -- helpers (call with lock held)
    def _log(self, kind, msg):
        self.events.append({"time": time.strftime("%H:%M:%S"), "type": kind, "message": msg})

    def _alert(self, kind, msg, lat=None, lng=None):
        self._log(kind, msg)
        threading.Thread(target=send_telegram, args=(msg, lat, lng), daemon=True).start()

    # -- public API
    def set_geofence(self, armed, lat, lng, radius):
        with self.lock:
            self.cfg.update(armed=armed, lat=lat, lng=lng, radius=radius)
            if not armed:
                self.breach, self.outside_streak = False, 0

    def set_status(self, s):
        with self.lock:
            self.device_status = s
            self._log("STATUS", f"Device reported: {s}")

    def handle_telemetry(self, p):
        with self.lock:
            self.latest, self.last_seen = p, time.time()
            if self.offline_alerted:
                self.offline_alerted = False
                self._log("INFO", "Tracker is back online")

            fix = bool(p.get("fix")) and p.get("lat") is not None
            lat, lng = (float(p["lat"]), float(p["lng"])) if fix else (None, None)
            if fix:
                self.last_pos = (lat, lng)
                self.track.append((lat, lng))

            # ---- SOS: rising edge OR new press number (survives device reboot)
            sos, sos_n = bool(p.get("sos")), p.get("sos_n")
            if sos and (not self.prev_sos or sos_n != self.last_sos_n):
                where = f"({lat:.6f}, {lng:.6f})" if fix else "(location unknown - no GPS fix)"
                self._alert("SOS", f"🚨 SOS! Child pressed the emergency button {where}",
                            *(self.last_pos if self.last_pos else (None, None)))
            self.prev_sos, self.last_sos_n = sos, sos_n

            # ---- Geofence
            if fix:
                c = self.cfg
                self.dist = haversine_m(c["lat"], c["lng"], lat, lng)
                if c["armed"]:
                    if self.dist > c["radius"]:
                        self.outside_streak += 1
                        if not self.breach and self.outside_streak >= CONFIRM_N:
                            self.breach = True
                            self._alert("GEOFENCE",
                                        f"⚠️ Child left the safe zone: {self.dist:.0f} m from center "
                                        f"(limit {c['radius']:.0f} m)", lat, lng)
                    else:
                        self.outside_streak = 0
                        if self.breach and self.dist < c["radius"] - HYST_M:
                            self.breach = False
                            self._alert("INFO", "✅ Child is back inside the safe zone", lat, lng)

    def watchdog(self):
        """Background thread: armed fence + silent tracker is itself a red flag."""
        while True:
            time.sleep(5)
            with self.lock:
                if (self.cfg["armed"] and self.last_seen and not self.offline_alerted
                        and time.time() - self.last_seen > OFFLINE_S):
                    self.offline_alerted = True
                    self._alert("OFFLINE",
                                f"📡 No data from tracker for {OFFLINE_S}s (Wi-Fi lost / battery dead?). "
                                "Last known position attached.", *(self.last_pos or (None, None)))

    def snapshot(self):
        with self.lock:
            lat, lng = self.last_pos if self.last_pos else (None, None)
            return dict(cfg=dict(self.cfg), latest=dict(self.latest), last_seen=self.last_seen,
                        lat=lat, lng=lng, dist=self.dist, breach=self.breach,
                        track=list(self.track), events=list(self.events),
                        status=self.device_status)


# ------------------------------------------------------------------ MQTT
def start_mqtt(engine):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"dash-{DEVICE_ID}-{os.getpid()}")

    def on_connect(c, userdata, flags, reason_code, properties):
        c.subscribe(f"childsafe/{DEVICE_ID}/#")        # re-subscribes after every reconnect

    def on_message(c, userdata, msg):
        try:
            if msg.topic.endswith("/telemetry"):
                engine.handle_telemetry(json.loads(msg.payload))
            elif msg.topic.endswith("/status"):
                engine.set_status(msg.payload.decode())
        except Exception as e:                           # never let a bad packet kill the thread
            print("[MQTT parse error]", e)

    client.on_connect, client.on_message = on_connect, on_message
    client.reconnect_delay_set(1, 30)
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_start()


@st.cache_resource           # one engine + one MQTT thread per server process
def get_engine():
    e = Engine()
    start_mqtt(e)
    threading.Thread(target=e.watchdog, daemon=True).start()
    return e


# ------------------------------------------------------------------ UI
st.set_page_config(page_title="ChildSafe Tracker", layout="wide")
engine = get_engine()
st.title("🛡️ ChildSafe Tracker")

if "c_lat" not in st.session_state:                       # init widget state from engine
    st.session_state.update(c_lat=engine.cfg["lat"], c_lng=engine.cfg["lng"],
                            radius=int(engine.cfg["radius"]), armed=False)

def use_device_pos():                                     # sidebar button callback
    s = engine.snapshot()
    if s["lat"] is not None:
        st.session_state.c_lat, st.session_state.c_lng = s["lat"], s["lng"]

with st.sidebar:
    st.header("Safe zone")
    st.button("📍 Use device position as center", on_click=use_device_pos)
    st.number_input("Center latitude", -90.0, 90.0, key="c_lat", format="%.6f", step=0.0001)
    st.number_input("Center longitude", -180.0, 180.0, key="c_lng", format="%.6f", step=0.0001)
    st.slider("Radius (m)", 50, 5000, step=10, key="radius")
    st.toggle("Arm geofence", key="armed")
    engine.set_geofence(st.session_state.armed, st.session_state.c_lat,
                        st.session_state.c_lng, float(st.session_state.radius))
    st.caption(f"Device: `{DEVICE_ID}`")


def build_map(s):
    cfg = s["cfg"]
    view_lat, view_lng = (s["lat"], s["lng"]) if s["lat"] is not None else (cfg["lat"], cfg["lng"])
    zoom = max(10, min(18, 16 - math.log2(max(cfg["radius"], 50) / 250)))
    layers = []
    if cfg["lat"] or cfg["lng"]:
        layers.append(pdk.Layer("ScatterplotLayer",
            data=[{"lat": cfg["lat"], "lng": cfg["lng"]}], get_position="[lng, lat]",
            get_radius=cfg["radius"], stroked=True, filled=True, line_width_min_pixels=2,
            get_fill_color=[0, 120, 255, 40], get_line_color=[0, 120, 255, 255]))
    if len(s["track"]) > 1:
        layers.append(pdk.Layer("PathLayer",
            data=[{"path": [[lng, lat] for lat, lng in s["track"]]}], get_path="path",
            get_color=[90, 90, 90, 200], width_min_pixels=2))
    if s["lat"] is not None:
        alarm = s["breach"] or s["latest"].get("sos")
        layers.append(pdk.Layer("ScatterplotLayer",
            data=[{"lat": s["lat"], "lng": s["lng"]}], get_position="[lng, lat]",
            get_radius=6, radius_min_pixels=7, filled=True,
            get_fill_color=[220, 30, 30, 255] if alarm else [20, 170, 80, 255]))
    return pdk.Deck(layers=layers,
                    initial_view_state=pdk.ViewState(latitude=view_lat, longitude=view_lng, zoom=zoom))


@st.fragment(run_every=2)       # re-renders only this block every 2 s
def live_view():
    s = engine.snapshot()
    p = s["latest"]
    age = time.time() - s["last_seen"] if s["last_seen"] else None
    online = age is not None and age < 15

    if p.get("sos"):
        st.error("🚨 SOS ACTIVE: child pressed the emergency button!")
    if s["breach"]:
        st.warning(f"⚠️ Outside safe zone ({s['dist']:.0f} m from center)")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Link", "ONLINE" if online else "OFFLINE")
    c2.metric("GPS", "FIX" if p.get("fix") else "Acquiring…")
    c3.metric("Distance from center", f"{s['dist']:.0f} m" if s["dist"] is not None else "–")
    c4.metric("Satellites / HDOP", f"{p.get('sat', 0)} / {p.get('hdop', '–')}")
    c5.metric("Last seen", f"{age:.0f}s ago" if age is not None else "never")

    st.pydeck_chart(build_map(s), use_container_width=True)
    st.subheader("Event log")
    if s["events"]:
        st.dataframe(pd.DataFrame(s["events"][::-1]), use_container_width=True, hide_index=True)
    else:
        st.caption("No events yet.")

live_view()