# ОТИ. Практическая № 2. Вариант 1

**Задача:** сохранить историю владельцев и статусов банковских счетов при изменениях в операционной PostgreSQL.

В репозитории две связанные части:

1. **Целевая архитектура** CDC → Kafka → Spark Structured Streaming → Iceberg (MinIO + Lakekeeper) → Trino; представлена в `architecture.drawio` и `architecture.png`. Это архитектурное проектирование, стек целиком не развёрнут.
2. **Учебная проверка** с Docker Compose: PostgreSQL (источник), Kafka, Debezium, Python consumer и PostgreSQL с историей SCD Type 2. Это облегчённая демонстрация принципа; она **не** записывает данные в Iceberg.

Основной отчёт: [`отчет.md`](отчет.md).

**Для быстрого старта** (из папки проекта):

```powershell
docker compose up -d --build
docker compose ps
Invoke-RestMethod -Method Post -Uri http://localhost:8083/connectors -ContentType 'application/json' -InFile './connectors/postgres-accounts.json'
docker compose logs --tail=30 history-worker
```

Не размещайте реальные банковские данные и настоящие пароли. Все данные и пароль `demo_password` – только для локальной учебной среды.

**Доказательства запуска:** четыре пользовательских скриншота находятся в папке [`screenshots`](screenshots/) и встроены в [`отчет.md`](отчет.md).
