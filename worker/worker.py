"""Учебный CDC-consumer: Kafka -> транзакционная SCD Type 2 в PostgreSQL.

Это НЕ Spark и НЕ Iceberg; их место честно показано в схеме и отчёте.
"""
import json
import logging
import os
import time

from confluent_kafka import Consumer, KafkaException
import psycopg
from psycopg.rows import dict_row

from logic import unwrap, event_timestamp, after_or_before, monotonic_start, same_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("scd2")
INFINITY = "9999-01-01 00:00:00+00"


def apply_message(conn, msg):
    if msg.value() is None:  # возможный Kafka tombstone, бизнес-операция уже пришла отдельно
        return "tombstone"
    event = unwrap(json.loads(msg.value().decode("utf-8")))
    op, row = after_or_before(event)
    ts = event_timestamp(event)
    account_id = int(row["account_id"])

    # Запись об обработанном Kafka-offset и изменение SCD идут ОДНОЙ транзакцией.
    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """INSERT INTO processed_events(topic, partition_id, offset_id, event_op, source_event_at)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT DO NOTHING RETURNING offset_id""",
                (msg.topic(), msg.partition(), msg.offset(), op, ts),
            )
            if cur.fetchone() is None:
                return "duplicate"
            cur.execute(
                """SELECT version_id, owner_name, status, valid_from
                   FROM account_history
                   WHERE account_id = %s AND is_current = TRUE FOR UPDATE""",
                (account_id,),
            )
            current = cur.fetchone()

            if op == "d":
                if current is not None:
                    effective_ts = monotonic_start(ts, current["valid_from"])
                    cur.execute(
                        """UPDATE account_history
                           SET valid_to=%s, is_current=FALSE, closed_by_op='d'
                           WHERE version_id=%s""",
                        (effective_ts, current["version_id"]),
                    )
                return "delete"

            if current is not None and same_values(row, current):
                # Если обновилось только updated_at, не создаём лишнюю историческую версию.
                return "no_business_change"

            effective_ts = monotonic_start(ts, current["valid_from"] if current else None)
            if current is not None:
                cur.execute(
                    """UPDATE account_history
                       SET valid_to=%s, is_current=FALSE, closed_by_op=%s
                       WHERE version_id=%s""",
                    (effective_ts, op, current["version_id"]),
                )
            cur.execute(
                """INSERT INTO account_history
                       (account_id,owner_name,status,valid_from,valid_to,is_current,opened_by_op)
                       VALUES (%s,%s,%s,%s,%s,TRUE,%s)""",
                (account_id, row["owner_name"], row["status"], effective_ts, INFINITY, op),
            )
    return "new_version"


def main():
    consumer = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "group.id": "oti-p2-scd2-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([os.environ["KAFKA_TOPIC"]])
    LOG.info("Ожидание CDC: topic=%s", os.environ["KAFKA_TOPIC"])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                LOG.warning("Kafka: %s", msg.error())
                continue
            try:
                with psycopg.connect(os.environ["HISTORY_DSN"], autocommit=True) as conn:
                    result = apply_message(conn, msg)
                consumer.commit(message=msg, asynchronous=False)
                LOG.info("event %s:%s:%s -> %s", msg.topic(), msg.partition(), msg.offset(), result)
            except (Exception, KafkaException):
                LOG.exception("Не обработано сообщение Kafka; смещение НЕ подтверждено, попробуем ещё")
                time.sleep(3)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
