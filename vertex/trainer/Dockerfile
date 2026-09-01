FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY vertex/trainer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt \
    && useradd --create-home --uid 10001 trainer
COPY credit_risk /app/credit_risk
COPY vertex /app/vertex
USER 10001
ENTRYPOINT ["python", "-m", "vertex.trainer.train"]
