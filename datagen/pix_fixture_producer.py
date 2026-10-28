"""Publish one bounded, repeatable PIX scenario to the lab Kafka topics."""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPICS = {
    "AUTH": "pix.autorizacao.eventos",
    "LIQ": "pix.liquidacao.eventos",
    "FRAUD": "pix.antifraude.eventos",
}
PARTITIONS = 3


def epoch_ms() -> int:
    return time.time_ns() // 1_000_000


def make_events(run_id: str, base_ms: int) -> list[tuple[str, dict]]:
    tx = f"fixture-{run_id}"
    auth = {
        "id_transacao_pix": tx,
        "event_id": f"{tx}-auth-v1",
        "event_type": "AUTHORIZATION",
        "event_version": 1,
        "valor": 125.50,
        "status_autorizacao": "APROVADA",
        "event_time": base_ms + 2_000,
    }
    settlement = {
        "id_transacao_pix": tx,
        "event_id": f"{tx}-liq-v1",
        "event_type": "SETTLEMENT",
        "event_version": 1,
        "id_liquidacao_spi": f"{tx}-spi",
        "status_liquidacao": "LIQUIDADA",
        "event_time": base_ms + 6_000,
    }
    fraud = {
        "id_transacao_pix": tx,
        "event_id": f"{tx}-fraud-v1",
        "event_type": "FRAUD",
        "event_version": 1,
        "score_risco": 12.5,
        "status_antifraude": "APROVADA",
        "event_time": base_ms + 10_000,
    }
    events = [("AUTH", auth), ("LIQ", settlement), ("FRAUD", fraud)]
    return events


def control_event(origin: str, run_id: str, partition: int, event_time: int) -> dict:
    tx = f"CTRL-{run_id}-{partition}"
    event = {
        "id_transacao_pix": tx,
        "event_id": f"{tx}-{origin.lower()}",
        "event_version": 1,
        "event_time": event_time,
    }
    if origin == "AUTH":
        event.update(
            event_type="AUTHORIZATION",
            valor=0.01,
            status_autorizacao="APROVADA",
        )
    elif origin == "LIQ":
        event.update(
            event_type="SETTLEMENT",
            id_liquidacao_spi=f"{tx}-spi",
            status_liquidacao="LIQUIDADA",
        )
    else:
        event.update(
            event_type="FRAUD",
            score_risco=0.0,
            status_antifraude="APROVADA",
        )
    return event


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        choices=("normal", "missing-fraud", "duplicate-auth", "out-of-order", "late-after-close"),
        default="normal",
    )
    parser.add_argument(
        "--run-id",
        help="Stable identifier for the case; use a new value for each clean run.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Use kafka:29092 when running inside the Compose network.",
    )
    args = parser.parse_args()

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,48}", run_id):
        parser.error("--run-id accepts only letters, digits, underscore and hyphen (max 48)")

    # Place the test in a future aligned bucket so the three source watermarks
    # can advance through it after the event records have been published.
    now = epoch_ms()
    base_ms = ((now // 15_000) + 2) * 15_000
    window_end_ms = base_ms + 15_000
    control_time_ms = base_ms + 45_000

    producer = Producer({"bootstrap.servers": args.bootstrap_servers, "acks": "all"})
    delivery_errors: list[str] = []
    events = make_events(run_id, base_ms)

    def delivery_report(error, _message) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    if args.scenario == "missing-fraud":
        events = [row for row in events if row[0] != "FRAUD"]
    elif args.scenario == "duplicate-auth":
        auth = next(row for row in events if row[0] == "AUTH")
        events.append((auth[0], dict(auth[1])))
    elif args.scenario == "out-of-order":
        events = [events[2], events[0], events[1]]

    def send(topic: str, event: dict, partition: int | None = None) -> None:
        payload = dict(event)
        payload["produced_at"] = epoch_ms()
        message_value = json.dumps(payload, separators=(",", ":"))
        if partition is not None:
            producer.produce(
                topic=topic,
                key=payload["id_transacao_pix"],
                value=message_value,
                partition=partition,
                on_delivery=delivery_report,
            )
        else:
            producer.produce(
                topic=topic,
                key=payload["id_transacao_pix"],
                value=message_value,
                on_delivery=delivery_report,
            )

    for origin, event in events:
        send(TOPICS[origin], event)

    # Advance all topic partitions. A single control record is insufficient:
    # Flink computes the source watermark from the active input partitions.
    for origin, topic in TOPICS.items():
        for partition in range(PARTITIONS):
            send(
                topic,
                control_event(origin, run_id, partition, control_time_ms),
                partition=partition,
            )

    if args.scenario == "late-after-close":
        late = dict(events[0][1])
        late["event_id"] = f"fixture-{run_id}-late-after-close"
        late["event_time"] = base_ms + 3_000
        send(TOPICS[events[0][0]], late)

    pending = producer.flush(30)
    if pending or delivery_errors:
        raise RuntimeError({"pending": pending, "delivery_errors": delivery_errors})

    print(
        json.dumps(
            {
                "scenario": args.scenario,
                "run_id": run_id,
                "transaction_id": f"fixture-{run_id}",
                "window_start_ms": base_ms,
                "window_end_ms": window_end_ms,
                "control_event_time_ms": control_time_ms,
                "control_ids_prefix": f"CTRL-{run_id}-",
                "expected": {
                    "normal": "CONSOLIDATED",
                    "missing-fraud": "INCOMPLETE; FRAUD ausente",
                    "duplicate-auth": "CONSOLIDATED; AUTH repetida com o mesmo event_id",
                    "out-of-order": "CONSOLIDATED se as evidências forem admitidas antes do fechamento",
                    "late-after-close": "CONSOLIDATED seguido por uma evidência AUTH posterior ao fechamento",
                }[args.scenario],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
