[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/README.md) | [ES](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/es/README.md) | RU

# Avtomatika Worker SDK

[![License: MPL 2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)
[![PyPI version](https://img.shields.io/pypi/v/avtomatika-worker.svg)](https://pypi.org/project/avtomatika-worker/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/release/python-3110/)

Официальный SDK для создания воркеров, совместимых с **[Avtomatika Orchestrator](https://github.com/avtomatika-ai/avtomatika)**. SDK берет на себя опрос задач, heartbeat-сообщения, передачу больших файлов через S3 и корректное завершение работы, позволяя вам сосредоточиться на бизнес-логике.

## Установка

```bash
pip install avtomatika-worker
```

Рекомендуется для полной функциональности:
```bash
pip install "avtomatika-worker[s3,pydantic,metrics]"
```

Дополнительно:
- `[s3]` — для S3-оффлоадинга (требует `obstore`).
- `[pydantic]` — для валидации параметров через Pydantic.
- `[metrics]` — для OpenTelemetry (трассировка и метрики).
- `[dev]` — для функций разработки, таких как `--reload`.

## Быстрый старт

### Вариант 1: Использование CLI (Рекомендуется)

Определите ваш воркер в модуле Python (например, `app/main.py`). SDK автоматически выведет имена и схемы навыков из вашего кода!

```python
from avtomatika_worker import Worker
from pydantic import BaseModel

worker = Worker(worker_type="image-processor")

class ResizeParams(BaseModel):
    width: int
    height: int
    url: str

# Автоматически: name="resize", схема из ResizeParams
@worker.skill()
async def resize(params: ResizeParams):
    print(f"Изменение размера до {params.width}px")
    return {"status": "success", "data": {"result": "ok"}}
```

### Вариант 2: Динамическая загрузка скиллов

Просто поместите ваши обработчики в папку `skills/` (например, `skills/my_skills.py`):

```python
from avtomatika_worker import SkillBlueprint

bp = SkillBlueprint()

# Добавьте метаданные для Биржи (опционально)
@bp.skill(price=0.5, category="AI")
async def generate_preview(params: dict):
    return {"status": "success"}
```

Запустите воркер, и он автоматически подхватит все навыки из этой папки:

```bash
# По умолчанию поиск идет в папке ./skills
worker run --app app.main:worker
```

## Ключевые возможности

### 1. Zero-Trust Безопасность (HLN Identity Chain)
- **Криптографические подписи:** При настройке `WORKER_TOKEN`, SDK автоматически подписывает все регистрации, хартбиты, результаты задач и события с помощью HMAC SHA256. Это гарантирует целостность данных и предотвращает подмену воркеров.

### 2. Умная регистрация навыков
- **Zero Configuration:** Имена и схемы выводятся автоматически из названий функций и аннотаций типов.
- **Авто-контракты:** Генерация `input_schema` и `output_schema` из Pydantic-моделей или стандартных Dataclasses.
- **Универсальные события:** Декларация кастомных сигналов через `@worker.skill(events={"alert": Schema})` и их отправка через хелпер `send_event`. Прогресс также является системным событием.

### 3. Оптимизация сети и производительности
- **Skills Hashing:** Воркер отправляет полный список навыков только при его изменении. Периодические Heartbeat-сообщения используют легковесный `skills_hash`.
- **Предотвращение Thundering Herd:** SDK автоматически применяет задержку `next_heartbeat_jitter_ms`, полученную от оркестратора, чтобы избежать пиковых нагрузок на сеть при массовом перезапуске воркеров.
- **Самовосстановление (Self-Healing):** Если оркестратор теряет метаданные воркера, он может запросить `Full Sync` через ответ на Heartbeat, обеспечивая бесшовное восстановление.

### 4. Поддержка нескольких оркестраторов (Waterfall Priority)
- **Стратегия "Водопад":** По умолчанию воркер опрашивает оркестраторы строго в порядке приоритета. После завершения любой задачи он всегда возвращается к самому приоритетному серверу, гарантируя обработку VIP-задач в первую очередь.
- **Failover и Round Robin:** Альтернативные стратегии для распределения нагрузки.

### 4. Наблюдаемость (OpenTelemetry)
- **Распределенная трассировка:** Каждое выполнение задачи создает Span в OpenTelemetry. Операции с S3 отслеживаются как дочерние спаны, обеспечивая полную видимость в Jaeger/Tempo.
- **Метрики:** Встроенные метрики для Prometheus: количество задач, длительность выполнения и производительность S3. Доступны по адресу `http://localhost:8083/metrics` (при установке `[metrics]`).

### 5. Fail-Fast Валидация
- **Локальный контроль:** SDK проверяет результаты задач и события на соответствие схемам **до** их отправки в оркестратор. Ошибки логируются мгновенно, предотвращая передачу некорректных данных.

### 6. Структурное логирование
SDK поддерживает текстовый и JSON форматы логов.
- `LOG_FORMAT=json` — для продакшена (ELK, Grafana Loki).
- `LOG_FORMAT=text` — для локальной разработки (по умолчанию).
- Все логи автоматически содержат контекст: `worker_id`, `task_id` и `job_id`.

### 7. Надежность работы с файлами и S3
- **TaskFiles**: Асинхронный помощник для работы в изолированной директории задачи.
- **S3 SDK**: Высокопроизводительная асинхронная загрузка/выгрузка с автоматическими повторами и **Graceful Shutdown** (воркер дождется завершения всех загрузок перед выходом).

## Справочник по конфигурации

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `WORKER_ID` | Уникальный ID экземпляра воркера. | UUID |
| `ORCHESTRATOR_URL` | Адрес оркестратора. | `http://localhost:8080` |
| `LOG_FORMAT` | Формат логов: `text` или `json`. | `text` |
| `LOG_LEVEL` | Уровень логирования (DEBUG, INFO и др). | `INFO` |
| `WORKER_PORT` | Порт для сервера проверки здоровья (health-check). | `8083` |
| `WORKER_SHUTDOWN_TIMEOUT`| Макс. время ожидания задач при выключении (сек). | `30.0` |
| `WORKER_ENABLE_WEBSOCKETS`| Включить WebSocket для команд (напр. отмена). | `false` |
| `MULTI_ORCHESTRATOR_MODE` | Стратегия опроса: `WATERFALL`, `ROUND_ROBIN`, `FAILOVER`. | `WATERFALL` |
| `WORKER_ENABLE_METRICS` | Включить OpenTelemetry (метрики и трейсинг). | `false` |
| `REGISTRATION_RETRY_INITIAL_DELAY`| Начальная пауза между попытками регистрации (сек). | `1.0` |
| `REGISTRATION_RETRY_MAX_DELAY`| Максимальная пауза между попытками регистрации (сек). | `60.0` |
| `TASK_FILES_DIR` | Локальная директория для временных данных S3. | `/tmp/payloads` |
| `WORKER_SKILLS_DIR` | Папка для динамической загрузки скиллов. | `skills` |

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
pip install -e .[dev]
pytest
```