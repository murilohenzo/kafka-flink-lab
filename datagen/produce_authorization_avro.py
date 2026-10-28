"""Publish one authorization using Confluent's Avro wire format."""

from __future__ import annotations

import os
import time
import uuid
from decimal import Decimal
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

TOPIC = "pix.autorizacao.eventos.avro"


def main() -> None:
    schema_path = Path(__file__).parent / "schemas" / "authorization-event.avsc"
    registry = SchemaRegistryClient(
        {"url": os.environ.get("PIX_SCHEMA_REGISTRY_URL", "http://localhost:8081")}
    )
    serializer = AvroSerializer(registry, schema_path.read_text(encoding="utf-8"))
    producer = Producer(
        {"bootstrap.servers": os.environ.get("PIX_BOOTSTRAP_SERVERS", "localhost:9092"), "acks": "all"}
    )

    now_ms = time.time_ns() // 1_000_000
    event = {
        "id_transacao_pix": str(uuid.uuid4()),
        "event_id": str(uuid.uuid4()),
        "event_type": "AUTHORIZATION",
        "event_version": 1,
        "valor": Decimal("123.45"),
        "status_autorizacao": "APROVADA",
        "event_time": now_ms - 4_000,
        "produced_at": now_ms,
    }
    delivery_errors: list[str] = []

    def on_delivery(error, _message) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    producer.produce(
        topic=TOPIC,
        key=event["id_transacao_pix"].encode("utf-8"),
        value=serializer(event, SerializationContext(TOPIC, MessageField.VALUE)),
        on_delivery=on_delivery,
    )
    pending = producer.flush(15)
    if pending or delivery_errors:
        raise RuntimeError({"pending": pending, "delivery_errors": delivery_errors})
    print(f"Publicado {event['event_id']} em {TOPIC}")


if __name__ == "__main__":
    main()
