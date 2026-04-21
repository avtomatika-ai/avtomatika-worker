[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/DEVELOPMENT.md) | [ES](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/es/DEVELOPMENT.md) | RU

# Руководство по разработке воркеров

Этот документ описывает процесс создания пользовательских воркеров (Workers), совместимых с оркестратором Avtomatika, с использованием `avtomatika-worker` SDK.

**Требования:** Python 3.11 или выше.

## Основная концепция

Воркеры, созданные с помощью этого SDK, реализуют гибридную модель взаимодействия с оркестратором:
- **PULL-модель для получения задач:** Воркер сам инициирует соединение и «забирает» задачи из своей очереди. Это позволяет воркерам работать в любой сети (в том числе за NAT или корпоративными файрволами) без необходимости в публичном IP-адресе.
- **WebSocket для связи в реальном времени:** Опциональный двусторонний канал для получения команд (например, отмена задачи) и отправки промежуточного прогресса.
- **Оптимизация HLN:** SDK использует протокол **Reverse Axon (RXON)**, который снижает объем передаваемого трафика за счет хеширования списков навыков и отправки обновлений только при изменениях.
- **Отказоустойчивость соединений:** 
    - **Независимые оркестраторы:** Каждое соединение управляется отдельной фоновой задачей. Сбой одного сервера не блокирует связь с другими.
    - **Регистрация с ретраями:** Бесконечные попытки регистрации с экспоненциальной задержкой, если оркестратор недоступен.
    - **Неблокирующий старт:** Воркер начинает опрашивать задачи сразу после успешной регистрации хотя бы на одном оркестраторе.

## Как создать воркер с помощью SDK

### Шаг 1: Установка `avtomatika-worker`

Убедитесь, что SDK установлен в вашей среде. Рекомендуется устанавливать со всеми дополнениями (S3 и Pydantic):
```bash
pip install "avtomatika-worker[s3,pydantic,metrics]"
```

Если вы работаете в основном репозитории, можно установить в режиме редактирования:
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

# 2. Определение моделей данных для навыков
class ReportParams(BaseModel):
    data_source: str
    format: str = "pdf"

# 3. Определение обработчиков навыков с помощью декоратора @worker.skill
# SDK автоматически выведет:
# - name: "generate_report" (из имени функции)
# - input_schema: генерируется на основе ReportParams
@worker.skill(description="Генерирует сложные отчеты")
async def generate_report(params: ReportParams, send_progress, send_event, **kwargs) -> dict:
    """
    - `params` (ReportParams): Проверенные и типизированные параметры. 
      ВАЖНО: Аргумент ДОЛЖЕН называться 'params' для работы авто-вывода схем.
    - `send_progress`: Асинхронная функция для отправки прогресса.
    - `send_event`: Асинхронная функция для отправки кастомных событий.
    - `**kwargs`: Метаданные: task_id, job_id и т.д.
    """
    task_id = kwargs.get("task_id")

    print(f"Генерация отчета {params.format} из источника {params.data_source}")

    # Отправка прогресса (стандартное событие)
    await send_progress(progress=0.5, message="Обработка данных...")
    
    # Отправка кастомного события
    await send_event("milestone", {"name": "data_parsed"})

    return {
        "status": "success",
        "data": {"report_url": f"s3://bucket/reports/{task_id}.pdf"}
    }

# Пример с динамическими полями: добавление 'price' для маркетплейса
@worker.skill(name="send_email", price=0.01)
async def send_email(params: dict, **kwargs) -> dict:
    print(f"Отправка почты: {params}")
    return {"status": "success"}

# 4. Запуск воркера
if __name__ == "__main__":
    worker.run()
```

### Шаг 3: Запуск воркера

Воркер можно запустить напрямую через Python или использовать встроенный CLI для более удобного управления:

```bash
# Рекомендуется: запускает воркер и сервер проверки здоровья (порт 8083 по умолчанию)
worker run --app my_worker:worker

# Для разработки (автоматическая перезагрузка при изменении кода)
worker run --app my_worker:worker --reload
```

### Шаг 4: Настройка соединений и авторизации

#### Вариант 1: Простое соединение (Один оркестратор)

```dotenv
ORCHESTRATOR_URL=http://localhost:8080
WORKER_ID=report-worker-01
WORKER_TOKEN=a-super-secret-token-for-this-worker
```

#### Вариант 2: Расширенное соединение (Несколько оркестраторов)

```dotenv
ORCHESTRATORS_CONFIG='[
    {"url": "http://main-orchestrator:8080", "priority": 1, "weight": 5},
    {"url": "http://backup-orchestrator:8080", "priority": 2, "weight": 1}
]'
MULTI_ORCHESTRATOR_MODE=WATERFALL  # Или ROUND_ROBIN, FAILOVER
```

- **WATERFALL (По умолчанию):** Опрашивает оркестраторы строго в порядке приоритета. Всегда возвращается к самому высокоприоритетному после завершения любой задачи.
- **ROUND_ROBIN:** Распределяет запросы согласно весам.
- **FAILOVER:** Переходит к следующему только если предыдущий вернул пустоту.

### Шаг 5: Связь в реальном времени (WebSocket)

Для включения этого функционала установите `WORKER_ENABLE_WEBSOCKETS=true`. Это позволяет:
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

SDK поддерживает **автоматическую работу с S3** через высокопроизводительную библиотеку **`obstore`**.

1.  **Автоматическая загрузка (Input):** Если `params` содержит URI `s3://`, SDK скачает его в локальную временную папку перед вызовом вашего обработчика.
2.  **Автоматическая выгрузка (Output):** Если ваш обработчик возвращает локальный путь, SDK загрузит его в S3 и вернет оркестратору готовый URI.
3.  **TaskFiles:** Используйте класс `TaskFiles` для простых асинхронных операций в изолированной директории задачи.

```python
from avtomatika_worker import Worker, TaskFiles

@worker.skill()
async def process_video(params: dict, files: TaskFiles):
    # 'video_url' в params мог быть S3 URI, теперь он заменен на локальный путь
    local_path = params["video_url"]
    
    # Создание пути для результата
    result_path = await files.path_to("output.mp4")
    # ... обработка ...
    
    return {"status": "success", "data": {"result": result_path}}
```

#### Настройка S3
- `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_DEFAULT_BUCKET`.
- `TASK_FILES_DIR`: Локальный корень для временных данных (по умолчанию: `/tmp/payloads`).

> **Примечание:** SDK автоматически очищает всю директорию задачи после её завершения.

### Шаг 8: Наблюдаемость (OpenTelemetry)

SDK имеет встроенную поддержку **распределенной трассировки** и **метрик** через OpenTelemetry.

1.  **Distributed Tracing:** Каждое выполнение задачи обернуто в Span (`task.{type}`). Операции с S3 являются дочерними спанами.
2.  **Метрики:** Метрики в формате Prometheus доступны по адресу `http://localhost:8083/metrics` (требуется экстра `metrics`).
3.  **Injections:** Вы можете запросить `ObservabilityManager` в свой обработчик для создания кастомных спанов.

```python
from avtomatika_worker import Worker, ObservabilityManager

@worker.skill()
async def monitored_task(params: dict, obs: ObservabilityManager):
    with obs.tracer.start_as_current_span("my-custom-step"):
        # ... логика ...
        pass
    return {"status": "success"}
```

Включите через переменную окружения: `WORKER_ENABLE_METRICS=true`.

### Шаг 9: Проверки здоровья (Health Checks)

По умолчанию SDK запускает небольшой сервер aiohttp на `0.0.0.0:8083`. Вы можете проверить статус воркера по адресу `/health`.
Это полезно для Kubernetes (Liveness/Readiness probes) или систем мониторинга.
- Переменная: `WORKER_PORT` (по умолчанию: 8083)
- Флаг CLI: `--health-check` (включен по умолчанию)
