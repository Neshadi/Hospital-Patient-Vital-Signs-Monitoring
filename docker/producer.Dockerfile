FROM python:3.11-slim

WORKDIR /app

COPY requirements-producer.txt .
RUN pip install --no-cache-dir -r requirements-producer.txt

COPY data-sources/ ./data-sources/
COPY observability/ ./observability/

CMD ["python", "data-sources/vitals_stream_producer.py"]
