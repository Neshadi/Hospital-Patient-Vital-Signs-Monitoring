FROM apache/spark:3.5.1-python3

# apache/spark images run as a non-root "spark" user by default; switch to
# root only to install our extra Python deps and copy application code.
USER root
WORKDIR /app

COPY requirements-spark.txt .
RUN pip install --no-cache-dir -r requirements-spark.txt

COPY processing/ ./processing/
COPY observability/ ./observability/

# Ivy needs a writable home dir when spark-submit resolves --packages at
# container runtime (the default spark user's home isn't writable).
ENV HOME=/tmp

CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
     "/app/processing/jobs/streaming_job.py"]
