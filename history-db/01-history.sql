-- Учебная реализация SCD Type 2 в PostgreSQL.
-- В полной архитектуре её логика переносится в Iceberg (silver).
CREATE TABLE public.account_history (
    version_id bigserial PRIMARY KEY,      -- уникальный ключ версии
    account_id integer NOT NULL,           -- бизнес-ключ счёта
    owner_name text NOT NULL,
    status text NOT NULL,
    valid_from timestamptz NOT NULL,       -- начало действия версии, включительно
    valid_to timestamptz NOT NULL DEFAULT '9999-01-01 00:00:00+00', -- конец, не включительно
    is_current boolean NOT NULL DEFAULT TRUE,
    opened_by_op char(1) NOT NULL,
    closed_by_op char(1),
    CHECK (valid_to > valid_from)
);
CREATE UNIQUE INDEX one_current_version_per_account
ON public.account_history (account_id) WHERE is_current = TRUE;
CREATE INDEX history_by_account_dates
ON public.account_history (account_id, valid_from, valid_to);

-- Идемпотентная обработка Kafka: журнал учтённых сообщений.
CREATE TABLE public.processed_events (
    topic text NOT NULL,
    partition_id integer NOT NULL,
    offset_id bigint NOT NULL,
    event_op char(1) NOT NULL,
    source_event_at timestamptz NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (topic, partition_id, offset_id)
);
