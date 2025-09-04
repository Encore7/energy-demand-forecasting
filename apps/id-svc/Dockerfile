FROM python:3.11-slim

ARG PROJECT_ROOT=/workspace
ENV PROJECT_ROOT=${PROJECT_ROOT}

WORKDIR ${PROJECT_ROOT}

COPY libs ./libs
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir ./libs

COPY apps/id-svc ./apps/id-svc
WORKDIR ${PROJECT_ROOT}/apps/id-svc
RUN pip install --no-cache-dir .

EXPOSE 8001
CMD ["uvicorn", "app.main:app", "--host","0.0.0.0","--port","8001"]
