FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 AIP_HTTP_PORT=8080
WORKDIR /app
COPY vertex/predictor/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt \
    && useradd --create-home --uid 10001 predictor
COPY credit_risk /app/credit_risk
COPY vertex /app/vertex
USER 10001
EXPOSE 8080
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "vertex.predictor.app:create_runtime_app()"]
