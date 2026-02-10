[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/README.md) | [ES](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/es/README.md) | RU

# Avtomatika Worker SDK

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![Code Style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

Официальный SDK для создания воркеров, совместимых с **[Avtomatika Orchestrator](https://github.com/avtomatika-ai/avtomatika)**. SDK берет на себя опрос задач, heartbeat-сообщения, передачу больших файлов через S3 и корректное завершение работы, позволяя вам сосредоточиться на бизнес-логике.

## Установка

```bash
pip install avtomatika-worker
```

Дополнительно:
- `pip install "avtomatika-worker[pydantic]"` — для валидации параметров через Pydantic.
- `pip install "avtomatika-worker[dev]"` — для функций разработки, таких как `--reload`.

## Быстрый старт

### Вариант 1: Использование CLI (Рекомендуется)

Определите ваш воркер в модуле Python (например, `app/main.py`):

```python
from avtomatika_worker import Worker

worker = Worker(worker_type="image-processor")

@worker.task("resize")
async def resize_image(params: dict, **kwargs):
    return {"status": "success", "data": {"result": "ok"}}
```

Запустите его с помощью встроенной команды `worker`:

```bash
export ORCHESTRATOR_URL="http://localhost:8080"
export WORKER_TOKEN="your-secret-token"

# Обычный запуск
worker run --app app.main:worker

# Режим разработки с автоперезагрузкой при изменении кода
worker run --app app.main:worker --reload
```

### Вариант 2: Программный запуск

```python
if __name__ == "__main__":
    worker.run_with_health_check()
```

## Ключевые возможности

### 1. Структурное логирование
SDK поддерживает текстовый и JSON форматы логов.
- `LOG_FORMAT=json` — для продакшена (ELK, Grafana Loki).
- `LOG_FORMAT=text` — для локальной разработки (по умолчанию).
- Все логи автоматически содержат контекст: `worker_id`, `task_id` и `job_id`.

### 2. Корректное завершение (Graceful Shutdown)
Встроенная обработка сигналов `SIGTERM` и `SIGINT`. При получении сигнала воркер:
1. Входит в "Drain Mode" (перестает брать новые задачи).
2. Дожидается завершения текущих задач (таймаут настраивается через `WORKER_SHUTDOWN_TIMEOUT`).
3. Отправляет финальные heartbeat-сообщения и закрывает соединения.

### 3. Работа с файлами и S3
- **TaskFiles**: Асинхронный помощник для работы в изолированной директории задачи.
- **S3 Payload Offloading**: Автоматическое скачивание и загрузка тяжелых файлов, если в параметрах указаны `s3://` ссылки.

## Справочник по конфигурации

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `WORKER_ID` | Уникальный ID экземпляра воркера. | UUID |
| `ORCHESTRATOR_URL` | Адрес оркестратора. | `http://localhost:8080` |
| `LOG_FORMAT` | Формат логов: `text` или `json`. | `text` |
| `LOG_LEVEL` | Уровень логирования (DEBUG, INFO и др). | `INFO` |
| `WORKER_SHUTDOWN_TIMEOUT`| Макс. время ожидания задач при выключении (сек). | `30.0` |
| `WORKER_ENABLE_WEBSOCKETS`| Включить WebSocket для команд (напр. отмена). | `false` |
| `TASK_FILES_DIR` | Локальная директория для временных данных S3. | `/tmp/payloads` |

## Документация

- [Руководство по разработке](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/DEVELOPMENT.md) — Подробные инструкции по созданию кастомных воркеров, использованию middleware и работе с S3.

## Использование в Docker

Используйте готовый `Dockerfile` для развертывания:

```bash
docker build -t my-worker .
docker run -e ORCHESTRATOR_URL=... my-worker worker run --app app:worker
```

## Разработка

Установка зависимостей для тестов:
```bash
pip install -e .[test,dev]
pytest
```