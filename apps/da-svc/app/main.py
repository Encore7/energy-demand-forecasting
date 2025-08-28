from datetime import date

from fastapi import FastAPI, Query

from libs.common.otel import setup_tracing

app = FastAPI(title="da-svc", version="0.1.0")
setup_tracing(service_name="da-svc")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/forecast/day-ahead")
def day_ahead(delivery_date: date = Query(...)):
    # Placeholder; later will read from Parquet/Kafka
    return {"horizon": "DA", "delivery_date": str(delivery_date), "rows": []}
