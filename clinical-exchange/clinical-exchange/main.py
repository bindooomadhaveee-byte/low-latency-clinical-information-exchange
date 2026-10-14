"""Low-Latency Clinical Information Exchange - base project.

Key low-latency ideas:
  1. Async I/O (FastAPI + uvicorn)
  2. In-memory TTL cache in front of SQLite (read-through, write-invalidate)
  3. WebSocket pub/sub: clinicians get new observations pushed instantly
  4. ETag / 304 responses + GZip to cut payload size
  5. Per-request timing header so latency is measurable
"""
import asyncio, hashlib, json, sqlite3, threading, time, uuid
from collections import defaultdict
from typing import Optional

from fastapi import (Depends, FastAPI, Header, HTTPException, Request, Response,
                     WebSocket, WebSocketDisconnect)
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

DB_PATH = "clinical.db"
CACHE_TTL = 30  # seconds

# Demo API keys -> roles. Replace with OAuth2/SMART-on-FHIR in production.
API_KEYS = {"doctor-key": "clinician", "nurse-key": "clinician", "lab-key": "lab"}

# ---------------------------------------------------------------- storage
_db = sqlite3.connect(DB_PATH, check_same_thread=False)
_db.row_factory = sqlite3.Row
_db.execute("PRAGMA journal_mode=WAL")
_lock = threading.Lock()
_db.executescript("""
CREATE TABLE IF NOT EXISTS patients(
  id TEXT PRIMARY KEY, name TEXT, dob TEXT, gender TEXT, allergies TEXT);
CREATE TABLE IF NOT EXISTS observations(
  id TEXT PRIMARY KEY, patient_id TEXT, code TEXT, value REAL, unit TEXT,
  ts REAL, source TEXT);
CREATE INDEX IF NOT EXISTS idx_obs_patient ON observations(patient_id, ts DESC);
CREATE TABLE IF NOT EXISTS audit(
  ts REAL, actor TEXT, action TEXT, patient_id TEXT);
""")


class TTLCache:
    def __init__(self, ttl): self.ttl, self.data = ttl, {}
    def get(self, k):
        v = self.data.get(k)
        if v and v[0] > time.time(): return v[1]
        self.data.pop(k, None)
    def set(self, k, val): self.data[k] = (time.time() + self.ttl, val)
    def drop_prefix(self, p):
        for k in [k for k in self.data if k.startswith(p)]: self.data.pop(k, None)

cache = TTLCache(CACHE_TTL)

# ---------------------------------------------------------------- pub/sub
class Hub:
    def __init__(self): self.subs = defaultdict(set)
    async def connect(self, pid, ws): await ws.accept(); self.subs[pid].add(ws)
    def disconnect(self, pid, ws): self.subs[pid].discard(ws)
    async def publish(self, pid, msg: dict):
        dead = []
        async def send(ws):
            try: await ws.send_json(msg)
            except Exception: dead.append(ws)
        await asyncio.gather(*(send(ws) for ws in list(self.subs[pid])))
        for ws in dead: self.disconnect(pid, ws)

hub = Hub()

# ---------------------------------------------------------------- models
class PatientIn(BaseModel):
    name: str
    dob: str = Field(examples=["1980-04-12"])
    gender: str
    allergies: list[str] = []

class ObservationIn(BaseModel):
    code: str = Field(examples=["heart-rate"])
    value: float
    unit: str = Field(examples=["bpm"])

# Simple critical-value rules to demonstrate alerting
CRITICAL = {"heart-rate": (40, 140), "spo2": (90, 100), "temperature": (35, 39.5)}

# ---------------------------------------------------------------- app
app = FastAPI(title="Low-Latency Clinical Information Exchange")
app.add_middleware(GZipMiddleware, minimum_size=500)


@app.middleware("http")
async def timing(request: Request, call_next):
    t = time.perf_counter()
    resp = await call_next(request)
    resp.headers["X-Process-Time-ms"] = f"{(time.perf_counter() - t) * 1000:.2f}"
    return resp


def auth(x_api_key: str = Header(...)) -> str:
    if x_api_key not in API_KEYS:
        raise HTTPException(401, "Invalid API key")
    return x_api_key


def audit(actor, action, pid):
    with _lock:
        _db.execute("INSERT INTO audit VALUES(?,?,?,?)", (time.time(), actor, action, pid))
        _db.commit()


def etag_response(request: Request, payload) -> Response:
    body = json.dumps(payload, separators=(",", ":"))
    tag = '"' + hashlib.md5(body.encode()).hexdigest() + '"'
    if request.headers.get("if-none-match") == tag:
        return Response(status_code=304, headers={"ETag": tag})
    return Response(body, media_type="application/json", headers={"ETag": tag})


@app.post("/patients", status_code=201)
def create_patient(p: PatientIn, key: str = Depends(auth)):
    pid = str(uuid.uuid4())
    with _lock:
        _db.execute("INSERT INTO patients VALUES(?,?,?,?,?)",
                    (pid, p.name, p.dob, p.gender, ",".join(p.allergies)))
        _db.commit()
    audit(key, "create_patient", pid)
    return {"id": pid, **p.model_dump()}


@app.get("/patients/{pid}")
def get_patient(pid: str, request: Request, key: str = Depends(auth)):
    ck = f"patient:{pid}"
    data = cache.get(ck)
    if data is None:
        row = _db.execute("SELECT * FROM patients WHERE id=?", (pid,)).fetchone()
        if not row: raise HTTPException(404, "Patient not found")
        data = dict(row)
        data["allergies"] = [a for a in data["allergies"].split(",") if a]
        cache.set(ck, data)
    audit(key, "read_patient", pid)
    return etag_response(request, data)


@app.post("/patients/{pid}/observations", status_code=201)
async def add_observation(pid: str, o: ObservationIn, key: str = Depends(auth)):
    if not _db.execute("SELECT 1 FROM patients WHERE id=?", (pid,)).fetchone():
        raise HTTPException(404, "Patient not found")
    obs = {"id": str(uuid.uuid4()), "patient_id": pid, "code": o.code,
           "value": o.value, "unit": o.unit, "ts": time.time(), "source": key}
    with _lock:
        _db.execute("INSERT INTO observations VALUES(:id,:patient_id,:code,:value,:unit,:ts,:source)", obs)
        _db.commit()
    cache.drop_prefix(f"obs:{pid}")
    lo_hi = CRITICAL.get(o.code)
    obs["critical"] = bool(lo_hi and not (lo_hi[0] <= o.value <= lo_hi[1]))
    audit(key, "write_observation", pid)
    await hub.publish(pid, {"type": "observation", "data": obs})  # push instantly
    return obs


@app.get("/patients/{pid}/observations")
def list_observations(pid: str, request: Request, limit: int = 50, key: str = Depends(auth)):
    ck = f"obs:{pid}:{limit}"
    data = cache.get(ck)
    if data is None:
        rows = _db.execute("SELECT * FROM observations WHERE patient_id=? ORDER BY ts DESC LIMIT ?",
                           (pid, limit)).fetchall()
        data = [dict(r) for r in rows]
        cache.set(ck, data)
    audit(key, "read_observations", pid)
    return etag_response(request, data)


@app.websocket("/ws/{pid}")
async def ws_patient(ws: WebSocket, pid: str, api_key: Optional[str] = None):
    if api_key not in API_KEYS:
        await ws.close(code=4401); return
    await hub.connect(pid, ws)
    try:
        while True:
            await ws.receive_text()  # keep-alive; clients may send pings
    except WebSocketDisconnect:
        hub.disconnect(pid, ws)


@app.get("/")
def client():
    return FileResponse("client.html")
