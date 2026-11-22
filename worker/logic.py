"""Чистые функции для разбора сообщений Debezium: без Docker и БД."""
from datetime import datetime, timezone, timedelta


def unwrap(event):
    """JSON из Kafka при schemas.enable=false; поддерживает и оболочку payload."""
    if not isinstance(event, dict):
        raise ValueError("Ожидался JSON-объект Debezium")
    return event.get("payload", event)


def event_timestamp(event):
    """Время источника; для snapshot оно относится к моменту чтения снимка."""
    source = event.get("source") or {}
    for field, scale in (("ts_ns", 1_000_000_000), ("ts_us", 1_000_000), ("ts_ms", 1000)):
        stamp = source.get(field)
        if isinstance(stamp, (int, float)) and stamp > 0:
            return datetime.fromtimestamp(stamp / scale, tz=timezone.utc)
    stamp = event.get("ts_ms")
    if isinstance(stamp, (int, float)) and stamp > 0:
        return datetime.fromtimestamp(stamp / 1000, tz=timezone.utc)
    return datetime.now(timezone.utc)


def after_or_before(event):
    op = event.get("op")
    if op not in ("c", "u", "d", "r"):
        raise ValueError(f"Неизвестная операция: {op}")
    row = event.get("before") if op == "d" else event.get("after")
    if not isinstance(row, dict) or "account_id" not in row:
        raise ValueError("В событии отсутствует запись счёта")
    return op, row


def monotonic_start(candidate, old_start):
    """Точность времён ограничена: одинаковое время разных событий сдвигаем на 1 мкс."""
    return max(candidate, old_start + timedelta(microseconds=1)) if old_start else candidate


def same_values(row, old):
    return (row["owner_name"], row["status"]) == (old["owner_name"], old["status"])
