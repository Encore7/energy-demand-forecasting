from fastapi import FastAPI

from libs.common.otel import setup_tracing

app = FastAPI(title="id-svc", version="0.1.0")
setup_tracing(service_name="id-svc")


@app.get("/health")
def health():
    return {"status": "ok"}
