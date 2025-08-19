import os
import logging

import json
from fastapi import FastAPI, status, Request
from kafka import KafkaProducer, KafkaConsumer

app = FastAPI(title="CinemaAbyss Event service")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class A:
    partition = 1
    offset = 1

def publish_event(topic:str, event: dict):
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BROKERS"),
        # bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    future = producer.send(topic, event)
    producer.send(topic, event)

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=os.getenv("KAFKA_BROKERS"),
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id="movie-consumers",
            consumer_timeout_ms = 1000  # stop waiting after 1s if no message
        )

        # message = next(consumer)
    except Exception as e:
        logger.error(f"Fail read messages from kafka: {e}")
    finally:
        consumer.close()

    # record_metadata = future.get(timeout=10)
    return future.get(timeout=10)


@app.get("/api/events/health")
async def root():
    return json.dumps({"status": True})


@app.post("/api/events/movie", status_code=status.HTTP_201_CREATED)
async def publish_movie_event(request: Request):
    record_metadata = publish_event("movie-events", r := await request.json())
    return {"status": "success", "partition": record_metadata.partition, "offset": record_metadata.offset, "event": r}

@app.post("/api/events/user", status_code=status.HTTP_201_CREATED)
async def publish_user_event(request: Request):
    # publish_event("user-events", await request.json())
    record_metadata = publish_event("user-events", r := await request.json())
    return {"status": "success", "partition": record_metadata.partition, "offset": record_metadata.offset, "event": r}


@app.post("/api/events/payment", status_code=status.HTTP_201_CREATED)
async def publish_payment_event(request: Request):
    # publish_event("payment-events", await request.json())
    record_metadata = publish_event("payment-events", r := await request.json())
    return {"status": "success", "partition": record_metadata.partition, "offset": record_metadata.offset,
            "event": r}