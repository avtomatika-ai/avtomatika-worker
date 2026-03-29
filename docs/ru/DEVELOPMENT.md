[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/DEVELOPMENT.md) | [ES](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/es/DEVELOPMENT.md) | RU

# Руководство по разработке воркеров

Этот документ описывает, как создать кастомный воркер, совместимый с оркестратором, используя `avtomatika-worker`.

**Требования:** Python 3.11 или выше.

## Основная концепция

Воркеры, созданные с помощью SDK, реализуют гибридную модель взаимодействия с оркестратором:
- **PULL-модель для получения задач:** Воркер инициирует соединение с оркестратором и «вытягивает» задачи из своей очереди. Это позволяет воркерам работать из любой сети (даже за NAT или корпоративными файрволами) без необходимости внешнего IP-адреса.
- **WebSocket для связи в реальном времени:** Опциональный двунаправленный канал для получения команд (например, отмена задачи) и отправки промежуточного прогресса выполнения.
- **Оптимизация HLN:** SDK использует протокол **Reverse Axon (RXON)**, который снижает нагрузку на сеть за счет хеширования списка навыков и передачи только изменений.
- **Отказоустойчивость:** 
    - **Независимые оркестраторы:** Каждое соединение управляется отдельной задачей. Сбой одного сервера не мешает работе с другими.
    - **Повторы регистрации:** Бесконечные попытки регистрации с экспоненциальной задержкой, если оркестратор недоступен.
    - **Неблокирующий запуск:** Воркер начинает опрос задач сразу после успешной регистрации хотя бы на одном оркестраторе.

## Как создать воркер с помощью SDK

### Шаг 1: Установка `avtomatika-worker`

Убедитесь, что SDK установлен в вашей среде. Рекомендуется установить со всеми основными расширениями (S3 и Pydantic):
```bash
pip install "avtomatika-worker[s3,pydantic,metrics]"
```

Если вы работаете в основном репозитории, вы можете установить его в режиме редактирования:
```bash
pip install -e .[dev]
```

### Шаг 2: Создание файла воркера

Создайте Python-файл (например, `my_worker.py`) и импортируйте класс `Worker`. SDK использует **автоматический вывод (Inference)** для сокращения шаблонного кода.

```python
import asyncio
from avtomatika_worker import Worker
from pydantic import BaseModel

# 1. Инициализация класса Worker
worker = Worker(worker_type="my-custom-worker")

# 2. Определение моделей данных для ваших навыков
class ReportParams(BaseModel):
    data_source: str
    format: str = "pdf"

# 3. Определение обработчиков навыков с помощью декоратора @worker.skill
# SDK автоматически выведет:
# - name: "generate_report" (из имени функции)
# - input_schema: сгенерирована из ReportParams
@worker.skill(description="Генерация сложных отчетов")
async def generate_report(params: ReportParams, send_progress, send_event, **kwargs) -> dict:
    """
    - `params` (ReportParams): Проверенные и типизированные параметры. 
      ВАЖНО: Аргумент ДОЛЖЕН называться 'params' для автоматического вывода схемы.
    - `send_progress`: Асинхронная функция для отправки обновлений прогресса.
    - `send_event`: Асинхронная функция для отправки кастомных событий.
    - `**kwargs`: Метаданные: task_id, job_id и т.д.
    """
    task_id = kwargs.get("task_id")

    print(f"Генерация {params.format} отчета из {params.data_source}")

    # Отправка прогресса (стандартное событие)
    await send_progress(progress=0.5, message="Обработка данных...")
    
    # Отправка кастомного события
    await send_event("milestone", {"name": "data_parsed"})

    return {
        "status": "success",
        "data": {"report_url": f"s3://bucket/reports/{task_id}.pdf"}
    }

# Динамическое расширение полей: добавим 'price' для Биржи (Marketplace)
@worker.skill(name="send_email", price=0.01)
async def send_email(params: dict, **kwargs) -> dict:
    print(f"Отправка email: {params}")
    return {"status": "success"}

# 4. Запуск воркера
if __name__ == "__main__":
    worker.run()
```

### Шаг 3: Запуск воркера

Вы можете запустить воркер напрямую через Python или использовать встроенный CLI для лучшего управления:

```bash
# Рекомендуется: запускает воркер и включает сервер проверки здоровья (health-check) на порту 8083 по умолчанию
worker run --app my_worker:worker

# Для разработки (автоматический перезапуск при изменении кода)
worker run --app my_worker:worker --reload
```

### Шаг 4: Конфигурация соединения и аутентификации

#### Вариант 1: Простое соединение (один оркестратор)

```dotenv
ORCHESTRATOR_URL=http://localhost:8080
WORKER_ID=report-worker-01
WORKER_TOKEN=a-super-secret-token-for-this-worker
```

#### Вариант 2: Расширенное соединение (несколько оркестраторов)

```dotenv
ORCHESTRATORS_CONFIG='[
    {"url": "http://main-orchestrator:8080", "priority": 1, "weight": 5},
    {"url": "http://backup-orchestrator:8080", "priority": 2, "weight": 1}
]'
MULTI_ORCHESTRATOR_MODE=WATERFALL  # Или ROUND_ROBIN, FAILOVER
```

- **WATERFALL (По умолчанию):** Опрашивает оркестраторы в порядке приоритета. Всегда возвращается к самому приоритетному после выполнения любой задачи.
- **ROUND_ROBIN:** Распределяет запросы на основе весов.
- **FAILOVER:** Опрашивает следующий сервер, только если предыдущий пуст.

### Шаг 5: Связь в реальном времени (WebSocket)

Чтобы включить эту функциональность, установите `WORKER_ENABLE_WEBSOCKETS=true`. Это позволит вам:
1.  **Отправлять прогресс и события:** Используйте функции `send_progress` и `send_event`.
2.  **Отменять задачи:** Оркестратор может отправить команду, которая мгновенно вызовет `asyncio.CancelledError` в вашем обработчике.


### Шаг 6: Модульные навыки (SkillBlueprint)

Организуйте задачи в модули в директории `skills/`.

`skills/image_skills.py`:
```python
from avtomatika_worker import SkillBlueprint
from pydantic import BaseModel

class ResizeParams(BaseModel):
    w: int
    h: int

bp = SkillBlueprint()

@bp.skill() # name="resize", схема из ResizeParams
async def resize(params: ResizeParams):
    return {"status": "success"}
```

Воркер автоматически загрузит все навыки из директории, указанной в `WORKER_SKILLS_DIR`.

### Шаг 7: Работа с большими файлами (S3 Offloading)

SDK поддерживает **"Payload Offloading"** через S3-совместимые хранилища с использованием высокопроизводительной библиотеки **`obstore`**.

1.  **Авто-загрузка (Download):** Если `params` содержит URI `s3://`, SDK скачает файл в локальную временную папку перед вызовом вашего обработчика.
2.  **Авто-выгрузка (Upload):** Если ваш обработчик возвращает локальный путь, SDK загрузит его в S3 и вернет URI оркестратору.
3.  **TaskFiles:** Используйте класс `TaskFiles` для простых асинхронных операций с файлами в изолированной директории задачи.

```python
from avtomatika_worker import Worker, TaskFiles

@worker.skill()
async def process_video(params: dict, files: TaskFiles):
    # 'video_url' в params может быть S3 URI, теперь заменен на локальный путь
    local_path = params["video_url"]
    
    # Создание результирующего файла
    result_path = await files.path_to("output.mp4")
    # ... обработка ...
    
    return {"status": "success", "data": {"result": result_path}}
```

#### Настройка S3
- `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_DEFAULT_BUCKET`.
- `TASK_FILES_DIR`: Локальный корень для временных данных (по умолчанию: `/tmp/payloads`).

> **Примечание:** SDK автоматически очищает всю директорию задачи после её завершения.

### Шаг 8: Наблюдаемость (OpenTelemetry)

SDK предоставляет встроенную поддержку **распределенной трассировки** и **метрик** с использованием OpenTelemetry.

1.  **Трассировка:** Каждое выполнение задачи оборачивается в Span (`task.{type}`). Операции с S3 являются дочерними спанами.
2.  **Метрики:** Метрики в формате Prometheus доступны по адресу `http://localhost:8083/metrics` (требуется расширение `metrics`).
3.  **Внедрение зависимостей:** Вы можете запросить `ObservabilityManager` в вашем обработчике навыков для создания кастомных спанов.

```python
from avtomatika_worker import Worker, ObservabilityManager

@worker.skill()
async def monitored_task(params: dict, obs: ObservabilityManager):
    with obs.tracer.start_as_current_span("my-custom-step"):
        # ... логика ...
        pass
    return {"status": "success"}
```

Включите поддержку через переменную окружения: `WORKER_ENABLE_METRICS=true`.

### Шаг 9: Проверки здоровья (Health Checks)

По умолчанию SDK запускает небольшой aiohttp-сервер на `0.0.0.0:8083`. Вы можете проверить статус воркера по адресу `/health`.
Это полезно для Kubernetes (Liveness/Readiness пробы) или систем мониторинга.
- Переменная: `WORKER_PORT` (по умолчанию: 8083)
- Флаг CLI: `--health-check` (включен по умолчанию)
