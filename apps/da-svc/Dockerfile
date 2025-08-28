FROM python:3.11-slim
ARG PROJECT_ROOT=/workspace
WORKDIR ${PROJECT_ROOT}
COPY libs/common ./libs/common
RUN pip install --no-cache-dir ./libs/common
COPY apps/da-svc ./apps/da-svc
WORKDIR ${PROJECT_ROOT}/apps/da-svc
RUN pip install --no-cache-dir .
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host","0.0.0.0","--port","8000"]
