"""
Simulated streaming source: bedside monitors emitting vital-sign readings.

Emits one event every EVENT_INTERVAL_SECONDS per active patient, to the
Kafka topic `vitals-stream`. Occasionally injects an abnormal spike so the
downstream alerting logic has something real to catch.

Run standalone:
    python vitals_stream_producer.py
"""
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | vitals-producer | %(levelname)s | %(message)s",
)
log = logging.getLogger("vitals-producer")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "vitals-stream"
EVENT_INTERVAL_SECONDS = float(os.environ.get("EVENT_INTERVAL_SECONDS", "3"))
NUM_PATIENTS = int(os.environ.get("NUM_PATIENTS", "10"))
SPIKE_PROBABILITY = float(os.environ.get("SPIKE_PROBABILITY", "0.03"))

# Fixed patient roster so lab results (batch side) can reference the same IDs.
PATIENT_IDS = [f"P{str(i).zfill(4)}" for i in range(1, NUM_PATIENTS + 1)]


def normal_reading(patient_id: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "patient_id": patient_id,
        "heart_rate": round(random.gauss(75, 8), 1),
        "spo2": round(random.gauss(97, 1.2), 1),
        "systolic_bp": round(random.gauss(120, 10), 1),
        "diastolic_bp": round(random.gauss(80, 7), 1),
        "temperature": round(random.gauss(36.8, 0.3), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "is_simulated_spike": False,
    }


def spike_reading(patient_id: str) -> dict:
    """Injects a physiologically abnormal event to exercise alerting logic."""
    reading = normal_reading(patient_id)
    spike_type = random.choice(["tachycardia", "hypoxia", "hypertension", "fever"])
    if spike_type == "tachycardia":
        reading["heart_rate"] = round(random.uniform(130, 170), 1)
    elif spike_type == "hypoxia":
        reading["spo2"] = round(random.uniform(80, 88), 1)
    elif spike_type == "hypertension":
        reading["systolic_bp"] = round(random.uniform(170, 200), 1)
    elif spike_type == "fever":
        reading["temperature"] = round(random.uniform(38.5, 40.0), 1)
    reading["is_simulated_spike"] = True
    reading["spike_type"] = spike_type
    return reading


def build_producer(max_retries: int = 10, retry_delay_seconds: float = 5.0) -> KafkaProducer:
    """Connects to Kafka with retries, since the broker may still be
    finishing startup even after depends_on/health-check conditions pass."""
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                retries=5,
                linger_ms=50,
            )
        except Exception as exc:
            last_exc = exc
            log.warning(
                "Kafka not ready yet (attempt %d/%d): %s. Retrying in %ss...",
                attempt, max_retries, exc, retry_delay_seconds,
            )
            time.sleep(retry_delay_seconds)
    raise RuntimeError(f"Could not connect to Kafka after {max_retries} attempts") from last_exc


def main():
    producer = build_producer()
    log.info(
        "Starting vitals stream for %d patients every %ss -> topic '%s'",
        NUM_PATIENTS, EVENT_INTERVAL_SECONDS, TOPIC,
    )
    sent = 0
    try:
        while True:
            patient_id = random.choice(PATIENT_IDS)
            event = (
                spike_reading(patient_id)
                if random.random() < SPIKE_PROBABILITY
                else normal_reading(patient_id)
            )
            future = producer.send(TOPIC, key=patient_id, value=event)
            try:
                future.get(timeout=10)
                sent += 1
                if event.get("is_simulated_spike"):
                    log.warning("SPIKE injected: %s -> %s", patient_id, event.get("spike_type"))
                if sent % 20 == 0:
                    log.info("Sent %d vitals events so far", sent)
            except Exception as exc:
                log.error("Failed to publish event for %s: %s", patient_id, exc)
            time.sleep(EVENT_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        log.info("Shutting down vitals producer (sent=%d)", sent)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
