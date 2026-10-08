FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 DBT_PROFILES_DIR=/app/dbt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
