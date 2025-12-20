FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy only what we need for this service + shared libs
COPY pyproject.toml /app/pyproject.toml
COPY libs /app/libs
COPY services/api_gateway /app/services/api_gateway

# Install deps
RUN pip install --no-cache-dir -U pip \
    && pip install --no-cache-dir -e /app/services/api_gateway

# Ensure repo root is importable (libs/)
ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
