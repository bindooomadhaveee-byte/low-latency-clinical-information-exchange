import os, pytest
os.environ.setdefault("X", "1")
from fastapi.testclient import TestClient
from main import app

H = {"X-API-Key": "doctor-key"}

def test_flow():
    c = TestClient(app)
    p = c.post("/patients", json={"name": "A. Rao", "dob": "1980-01-01", "gender": "F",
                                  "allergies": ["penicillin"]}, headers=H).json()
    pid = p["id"]
    with c.websocket_connect(f"/ws/{pid}?api_key=doctor-key") as ws:
        r = c.post(f"/patients/{pid}/observations",
                   json={"code": "heart-rate", "value": 180, "unit": "bpm"}, headers=H)
        assert r.status_code == 201
        msg = ws.receive_json()
        assert msg["data"]["critical"] is True
    g = c.get(f"/patients/{pid}", headers=H)
    assert g.status_code == 200 and "X-Process-Time-ms" in g.headers
    assert c.get(f"/patients/{pid}", headers={**H, "If-None-Match": g.headers["etag"]}).status_code == 304

def test_auth():
    assert TestClient(app).get("/patients/x", headers={"X-API-Key": "bad"}).status_code == 401
