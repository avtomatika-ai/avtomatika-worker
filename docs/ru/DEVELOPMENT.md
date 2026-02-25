[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/DEVELOPMENT.md) | [ES](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/es/DEVELOPMENT.md) | RU

# Руководство по разработке Воркера

Этот документ описывает, как создать кастомный Воркер, совместимый с Оркестратором, используя `avtomatika-worker`.

**Требования:** Python 3.11 или выше.

## Основная концепция

Воркеры, созданные с помощью SDK, реализуют гибридную модель взаимодействия с Оркестратором:
- **PULL-модель для получения задач:** Воркер сам инициирует соединение с Оркестратором и "вытягивает" (pull) задачи из своей персональной очереди. Это позволяет Воркерам работать из любой сети, в том числе за NAT или корпоративным файрволом.
- **WebSocket для Real-time коммуникации:** Опциональный двунаправленный канал для получения команд (например, отмена задачи) и отправки промежуточного прогресса выполнения.

## Как создать Воркер с помощью SDK

### Шаг 1: Установка `avtomatika-worker`

Убедитесь, что SDK установлен в вашем окружении.
```bash
pip install avtomatika-worker
```

Для поддержки S3:
```bash
pip install "avtomatika-worker[s3]"
```

### Шаг 2: Создание файла Воркера

SDK использует **автоматическое выведение данных (Inference)**, чтобы минимизировать количество шаблонного кода.

```python
import asyncio
from avtomatika_worker import Worker
from pydantic import BaseModel

# 1. Инициализируйте класс Worker
worker = Worker(worker_type="my-custom-worker")

# 2. Определите модели данных для ваших навыков
class ReportParams(BaseModel):
    data_source: str
    format: str = "pdf"

# 3. Определите обработчики с помощью декоратора @worker.skill
# SDK автоматически выведет:
# - имя: "generate_report" (из названия функции)
# - схему: сгенерирует из ReportParams для Биржи
@worker.skill(description="Генерация сложных отчетов")
async def generate_report(params: ReportParams, send_progress, send_event, **kwargs) -> dict:
    """
    - `params` (ReportParams): Валидированные и типизированные параметры.
    - `send_progress`: Асинхронная функция для отправки прогресса.
    - `send_event`: Асинхронная функция для отправки кастомных событий.
    - `**kwargs`: Метаданные: task_id, job_id и др.
    """
    task_id = kwargs.get("task_id")

    print(f"Генерация отчета {params.format} из {params.data_source}")

    # Отправка промежуточного прогресса (стандартное событие)
    await send_progress(progress=0.5, message="Обработка данных...")
    
    # Отправка кастомного события
    await send_event("milestone", {"name": "data_parsed"})

    return {
        "status": "success",
        "data": {"report_url": f"s3://bucket/reports/{task_id}.pdf"}
    }

# Динамические поля: добавление цены для Маркетплейса
@worker.skill(name="send_email", price=0.01)
async def send_email(params: dict, **kwargs) -> dict:
    print(f"Отправка email: {params}")
    return {"status": "success"}

# 4. Запустите воркер
if __name__ == "__main__":
    worker.run()
```

### Шаг 3: Настройка подключения

```dotenv
ORCHESTRATOR_URL=http://localhost:8080
WORKER_ID=report-worker-01
WORKER_INDIVIDUAL_TOKEN=a-super-secret-token-for-this-worker
```

### Шаг 4: Real-time коммуникация (WebSocket)

Для включения установите `WORKER_ENABLE_WEBSOCKETS=true`. Это позволит:
1.  **Отправлять прогресс и события:** Используйте внедренные функции `send_progress` и `send_event`.
2.  **Отмена задач:** Оркестратор может отправить команду, которая мгновенно вызовет `asyncio.CancelledError` в вашем обработчике.

### Шаг 5: Модульные навыки (SkillBlueprint)

Вы можете организовать навыки в модули в папке `skills/`.

`skills/image_skills.py`:
```python
from avtomatika_worker import SkillBlueprint
from pydantic import BaseModel

class ResizeParams(BaseModel):
    w: int
    h: int

bp = SkillBlueprint()

@bp.skill() # имя="resize", схема из ResizeParams
async def resize(params: ResizeParams):
    return {"status": "success"}
```

Воркер автоматически загрузит все навыки из директории `WORKER_SKILLS_DIR`.

### Шаг 6: Продвинутая регистрация навыков

Декоратор `.skill()` поддерживает три режима:

1.  **Zero-config:** `@worker.skill()` (всё выводится из кода).
2.  **Метаданные:** `@worker.skill(price=0.5, category="AI")` (динамические расширения для Биржи).
3.  **Строгий контракт:** 
    ```python
    @dataclass(frozen=True)
    class MyContract(SkillInfo):
        price: float
        
    @worker.skill(MyContract(name="pro_render", price=1.0))
    async def render(params: RenderModel): ...
    ```

### Шаг 7: Работа с большими файлами (S3 Offloading)

SDK поддерживает автоматическую передачу тяжелых данных через S3 с использованием библиотеки **`obstore`**.

1.  **Авто-скачивание:** Если в `params` есть `s3://` ссылка, SDK скачает её во временную папку до вызова вашего кода.
2.  **Авто-загрузка:** Если вы вернете локальный путь к файлу, SDK сам загрузит его в S3.
3.  **TaskFiles:** Используйте класс `TaskFiles` для асинхронной работы с файлами в изолированной папке задачи.

```python
from avtomatika_worker import Worker, TaskFiles

@worker.skill()
async def process_video(params: dict, files: TaskFiles):
    # 'video_url' подменен локальным путем после скачивания из S3
    local_path = params["video_url"]
    
    # Создание файла результата
    result_path = await files.path_to("output.mp4")
    # ... обработка ...
    
    return {"status": "success", "data": {"result": result_path}}
```

#### Настройка S3
Используйте переменные: `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_DEFAULT_BUCKET`.

> **Примечание:** SDK автоматически удаляет всю директорию задачи после её завершения.