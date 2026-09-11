import heapq
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


BOOTSTRAP_SERVERS = os.getenv(
    "BOOTSTRAP_SERVERS",
    "kafka:29092"
)

TRANSACTION_INTERVAL_MS = int(
    os.getenv(
        "TRANSACTION_INTERVAL_MS",
        "1000"
    )
)

DUPLICATE_RATE = float(
    os.getenv(
        "DUPLICATE_RATE",
        "0.10"
    )
)

MISSING_EVENT_RATE = float(
    os.getenv(
        "MISSING_EVENT_RATE",
        "0.10"
    )
)

LATE_EVENT_RATE = float(
    os.getenv(
        "LATE_EVENT_RATE",
        "0.10"
    )
)

MAX_ARRIVAL_DELAY_SECONDS = int(
    os.getenv(
        "MAX_ARRIVAL_DELAY_SECONDS",
        "12"
    )
)


TOPIC_AUTH = "pix.autorizacao.eventos"

TOPIC_SETTLEMENT = "pix.liquidacao.eventos"

TOPIC_FRAUD = "pix.antifraude.eventos"


producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "client.id": "pix-datagen",
    "acks": "all"
})


# --------------------------------------------------------------
# Scheduler
#
# Guarda:
#
#   (momento_envio, contador, topic, key, event)
#
# Dessa maneira event_time e arrival_time podem ser diferentes.
# --------------------------------------------------------------

queue = []

counter = 0


def now_ms():
    return int(time.time() * 1000)


def iso(ms):
    return datetime.fromtimestamp(
        ms / 1000,
        tz=timezone.utc
    ).isoformat()


def schedule(topic, key, event, delay_seconds):
    global counter

    counter += 1

    due = time.time() + delay_seconds

    heapq.heappush(
        queue,
        (
            due,
            counter,
            topic,
            key,
            event
        )
    )


def delivery_report(err, msg):

    if err is not None:
        print(
            f"[ERROR] Kafka delivery: {err}"
        )


def send(topic, key, event):

    event["produced_at"] = now_ms()

    producer.produce(
        topic=topic,
        key=key,
        value=json.dumps(event),
        callback=delivery_report
    )

    producer.poll(0)

    print(
        f"[SEND] "
        f"topic={topic} "
        f"transaction={key} "
        f"eventId={event['event_id']} "
        f"eventTime={iso(event['event_time'])} "
        f"producedAt={iso(event['produced_at'])}"
    )


def create_transaction():

    transaction_id = str(uuid.uuid4())

    base_time = now_ms()

    value = round(
        random.uniform(10, 5000),
        2
    )

    # ----------------------------------------------------------
    # Event-time
    #
    # A sequência lógica será:
    #
    # AUTH  = T
    # LIQ   = T + 4s
    # FRAUD = T + 8s
    #
    # MAS a sequência de chegada no Kafka será aleatória.
    # ----------------------------------------------------------

    authorization = {
        "id_transacao_pix": transaction_id,
        "event_id": str(uuid.uuid4()),
        "event_type": "AUTHORIZATION",
        "event_version": 1,
        "valor": value,
        "status_autorizacao": "APROVADA",
        "event_time": base_time
    }

    settlement = {
        "id_transacao_pix": transaction_id,
        "event_id": str(uuid.uuid4()),
        "event_type": "SETTLEMENT",
        "event_version": 1,
        "id_liquidacao_spi": str(uuid.uuid4()),
        "status_liquidacao": "LIQUIDADA",
        "event_time": base_time + 4_000
    }

    fraud = {
        "id_transacao_pix": transaction_id,
        "event_id": str(uuid.uuid4()),
        "event_type": "FRAUD",
        "event_version": 1,
        "score_risco": round(
            random.uniform(0, 100),
            2
        ),
        "status_antifraude": "APROVADA",
        "event_time": base_time + 8_000
    }

    events = [
        (TOPIC_AUTH, authorization),
        (TOPIC_SETTLEMENT, settlement),
        (TOPIC_FRAUD, fraud)
    ]

    # ----------------------------------------------------------
    # Transação incompleta
    # ----------------------------------------------------------

    if random.random() < MISSING_EVENT_RATE:

        removed = random.randrange(
            len(events)
        )

        removed_event = events.pop(
            removed
        )

        print(
            f"[MISSING] "
            f"transaction={transaction_id} "
            f"source={removed_event[0]}"
        )

    # ----------------------------------------------------------
    # Arrival delay independente do event-time.
    #
    # Isso cria naturalmente:
    #
    # AUTH -> FRAUD -> LIQ
    # FRAUD -> AUTH -> LIQ
    # LIQ -> AUTH -> FRAUD
    # etc.
    # ----------------------------------------------------------

    late_index = None

    if (
        events
        and random.random() < LATE_EVENT_RATE
    ):
        late_index = random.randrange(
            len(events)
        )

    for index, (topic, event) in enumerate(events):

        if index == late_index:

            # Evento muito atrasado.
            #
            # Com janelas de 15 segundos e watermark,
            # este evento pode chegar depois do fechamento.
            delay = random.randint(
                25,
                35
            )

            print(
                f"[LATE-SCHEDULED] "
                f"transaction={transaction_id} "
                f"topic={topic} "
                f"delay={delay}s"
            )

        else:

            delay = random.uniform(
                0,
                MAX_ARRIVAL_DELAY_SECONDS
            )

        schedule(
            topic,
            transaction_id,
            event,
            delay
        )

        # ------------------------------------------------------
        # Duplicate
        #
        # Mesmo event_id propositalmente.
        # ------------------------------------------------------

        if random.random() < DUPLICATE_RATE:

            duplicate_delay = (
                delay
                + random.uniform(
                    0.5,
                    4
                )
            )

            schedule(
                topic,
                transaction_id,
                event.copy(),
                duplicate_delay
            )

            print(
                f"[DUPLICATE-SCHEDULED] "
                f"transaction={transaction_id} "
                f"eventId={event['event_id']}"
            )


def process_queue():

    while queue:

        due, _, topic, key, event = queue[0]

        if due > time.time():
            break

        heapq.heappop(
            queue
        )

        send(
            topic,
            key,
            event
        )


print("")
print("========================================")
print(" PIX Kafka Event Generator")
print("========================================")
print(f"Kafka: {BOOTSTRAP_SERVERS}")
print("")
print(f"duplicate rate: {DUPLICATE_RATE}")
print(f"missing rate:   {MISSING_EVENT_RATE}")
print(f"late rate:      {LATE_EVENT_RATE}")
print(
    f"max normal arrival delay: "
    f"{MAX_ARRIVAL_DELAY_SECONDS}s"
)
print("========================================")
print("")


next_transaction = time.time()


try:

    while True:

        process_queue()

        now = time.time()

        if now >= next_transaction:

            create_transaction()

            next_transaction = (
                now
                + TRANSACTION_INTERVAL_MS / 1000
            )

        producer.poll(0)

        time.sleep(0.05)

except KeyboardInterrupt:

    pass

finally:

    producer.flush(10)