FROM python:3.11-slim

WORKDIR /app

COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

COPY serving/ ./serving/

CMD ["uvicorn", "serving.api:app", "--host", "0.0.0.0", "--port", "8000"]
