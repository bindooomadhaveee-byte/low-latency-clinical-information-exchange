# Low-Latency Clinical Information Exchange (base project)

## Run
    pip install -r requirements.txt
    uvicorn main:app --reload
    # API docs: http://localhost:8000/docs   Live monitor: http://localhost:8000/
    pytest

## Try it
    curl -X POST localhost:8000/patients -H "X-API-Key: doctor-key" -H "Content-Type: application/json" \
      -d '{"name":"A. Rao","dob":"1980-01-01","gender":"F","allergies":["penicillin"]}'
    curl -X POST localhost:8000/patients/<ID>/observations -H "X-API-Key: lab-key" -H "Content-Type: application/json" \
      -d '{"code":"heart-rate","value":180,"unit":"bpm"}'
Open the monitor page, paste the ID, subscribe, then post observations - they appear instantly.

## Endpoints
POST /patients | GET /patients/{id} | POST/GET /patients/{id}/observations | WS /ws/{id}?api_key=

## Low-latency techniques
Async server, TTL cache, WebSocket push, ETag/304, GZip, SQLite WAL + index, X-Process-Time-ms header.

## Next steps for production
HL7 FHIR resource shapes, OAuth2/SMART on FHIR, TLS, encryption at rest, Redis pub/sub (multi-node),
PostgreSQL, HIPAA/DPDP-compliant audit & consent, load tests (locust/k6).
