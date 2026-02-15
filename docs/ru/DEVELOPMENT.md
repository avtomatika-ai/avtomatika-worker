[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/DEVELOPMENT.md) | [ES](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/es/DEVELOPMENT.md) | RU

# Руководство по разработке Воркера

Этот документ описывает, как создать кастомный Воркер, совместимый с Оркестратором, используя `avtomatika-worker`.

**Требования:** Python 3.11 или выше.

## Основная концепция

Воркеры, созданные с помощью SDK, реализуют гибридную модель взаимодействия с Оркестратором:
- **PULL-модель для получения задач:** Воркер сам инициирует соединение с Оркестратором и "вытягивает" (pull) задачи из своей персональной очереди. Это позволяет Воркерам работать из любой сети, в том числе за NAT или корпоративным файрволом, без необходимости иметь публичный IP-адрес.
- **WebSocket для Real-time коммуникации:** Опциональный двунаправленный канал для получения команд (например, отмена задачи) и отправки промежуточного прогресса выполнения.

## Как создать Воркер с помощью SDK

### Шаг 1: Установка `avtomatika-worker`

Убедитесь, что SDK установлен в вашем окружении. Если вы работаете в основном репозитории, вы можете установить его в режиме редактирования:
```bash
pip install -e .
```

Чтобы включить поддержку S3, установите экстра-пакет `s3`:
```bash
pip install "avtomatika-worker[s3]"
```

### Шаг 2: Создание файла Воркера

Создайте Python-файл (например, `my_worker.py`) и импортируйте класс `Worker`.

```python
import asyncio
from avtomatika_worker import Worker

# 1. Инициализируйте класс Worker
# Вы можете указать уникальный тип вашего воркера.
worker = Worker(worker_type="my-custom-worker")

# 2. Определите обработчики задач с помощью декоратора @worker.task
@worker.task("generate_report")
async def generate_report_handler(params: dict, **kwargs) -> dict:
    """
    Эта функция будет вызвана, когда Оркестратор отправит
    задачу типа "generate_report".

    - `params` (dict): Позиционный аргумент, содержащий параметры для выполнения задачи.
    - `**kwargs`: Именованные аргументы с метаданными задачи:
        - `task_id` (str): Уникальный ID задачи.
        - `job_id` (str): ID родительского Job.
        - `priority` (float): Приоритет задачи.
    """
    task_id = kwargs.get("task_id")
    job_id = kwargs.get("job_id")
    priority = kwargs.get("priority", 0.0)

    print(f"Получены параметры: {params}")

    # Имитация долгой работы с отправкой прогресса
    print("Начало генерации отчета...")
    await asyncio.sleep(2)
    # Используем worker.send_progress для отправки обновления в Оркестратор
    await worker.send_progress(task_id, job_id, progress=0.5, message="Проанализировано 50% данных")
    await asyncio.sleep(2)
    print("Генерация отчета завершена.")


    # 3. Верните результат
    #    - 'status' (обязательно): "success", "failure" или кастомный статус.
    #    - 'data' (опционально): Словарь с данными, которые будут добавлены в контекст Job.
    #    - 'error' (опционально при status="failure"): Словарь с деталями ошибки.
    #      - 'code': "TRANSIENT_ERROR", "PERMANENT_ERROR" или "INVALID_INPUT_ERROR".
    #      - 'message': Человекочитаемое описание ошибки.
    return {
        "status": "success",
        "data": {"report_url": "/path/to/report.pdf"}
    }

    # Пример возврата ошибки
    # return {
    #     "status": "failure",
    #     "error": {
    #         "code": "TRANSIENT_ERROR",
    #         "message": "Could not connect to external service."
    #     }
    # }

@worker.task("send_email")
async def send_email_handler(params: dict, **kwargs) -> dict:
    print(f"Отправка email с параметрами: {params}")
    await asyncio.sleep(1)
    return {"status": "success"}

# 4. Запустите воркер
if __name__ == "__main__":
    worker.run()
```

### Шаг 3: Настройка подключения и аутентификации

#### Вариант 1: Простое подключение (один Оркестратор)

Это самый простой способ, подходящий для большинства случаев.

```dotenv
# Адрес вашего Оркестратора
ORCHESTRATOR_URL=http://localhost:8080

# Рекомендуемый способ аутентификации
WORKER_ID=report-worker-01
WORKER_INDIVIDUAL_TOKEN=a-super-secret-token-for-this-worker

# (Опционально) Устаревший способ аутентификации с общим токеном
# WORKER_TOKEN=your-secret-worker-token
```

#### Вариант 2: Продвинутое подключение (несколько Оркестраторов)

Этот способ используется для обеспечения высокой доступности (failover) или для балансировки нагрузки (round robin).

-   `ORCHESTRATORS_CONFIG`: Вместо `ORCHESTRATOR_URL` используется эта переменная. Она содержит JSON-строку со списком всех Оркестраторов.
-   `MULTI_ORCHESTRATOR_MODE`: Определяет, как Воркер будет взаимодействовать с этим списком.

**Пример для отказоустойчивости (Failover):**
В этом режиме Воркер будет работать с `main-orchestrator`. Если он станет недоступен, Воркер автоматически переключится на `backup-orchestrator`.

```dotenv
# Воркер будет опрашивать 'main-orchestrator'. Если он упадет,
# SDK автоматически переключится на 'backup-orchestrator'.
ORCHESTRATORS_CONFIG='[
    {"url": "http://main-orchestrator:8080"},
    {"url": "http://backup-orchestrator:8080"}
]'

# Режим FAILOVER используется по умолчанию, но его можно указать явно.
MULTI_ORCHESTRATOR_MODE=FAILOVER

# Настройки аутентификации остаются такими же
WORKER_ID=report-worker-01
WORKER_INDIVIDUAL_TOKEN=a-super-secret-token-for-this-worker
```

**Пример для балансировки нагрузки (Round Robin):**
В этом режиме Воркер будет поочередно отправлять запросы на получение задач к `orchestrator-1` и `orchestrator-2`, распределяя нагрузку.

```dotenv
# Воркер будет поочередно опрашивать оба Оркестратора.
ORCHESTRATORS_CONFIG='[
    {"url": "http://orchestrator-1:8080"},
    {"url": "http://orchestrator-2:8080"}
]'

MULTI_ORCHESTRATOR_MODE=ROUND_ROBIN

WORKER_ID=report-worker-01
WORKER_INDIVIDUAL_TOKEN=a-super-secret-token-for-this-worker
```
*Примечание: При использовании `ORCHESTRATORS_CONFIG` переменная `ORCHESTRATOR_URL` игнорируется.*

### Шаг 4: Real-time коммуникация (WebSocket)

Для включения этой функциональности установите переменную окружения `WORKER_ENABLE_WEBSOCKETS=true`. После этого вам станут доступны две новые возможности:

#### Отправка прогресса

Внутри вашего обработчика задач вы можете вызывать метод `worker.send_progress()`, чтобы информировать Оркестратор о ходе выполнения длительной операции.

```python
await worker.send_progress(
    task_id="...",      # ID текущей задачи
    job_id="...",       # ID родительского Job
    progress=0.75,      # Число от 0.0 до 1.0
    message="Обработано 75% видео"  # Опциональное сообщение
)
```
> **Важно:** `task_id` и `job_id` теперь всегда передаются в ваш обработчик как именованные аргументы, наряду с `params`. См. пример в Шаге 2.

#### Отмена задачи

SDK предоставляет два механизма отмены задач:

1.  **WebSocket (Push-модель):** Если WebSocket включен, Оркестратор может отправить команду на немедленную отмену. Это вызывает исключение `asyncio.CancelledError` в вашем обработчике. Этот способ обеспечивает самую быструю реакцию.

2.  **Redis (Pull-модель):** Даже без WebSocket, вы можете реализовать "кооперативную" отмену для очень долгих задач. Для этого SDK предоставляет асинхронную функцию `worker.check_for_cancellation(task_id)`. Вы должны периодически вызывать ее внутри вашего цикла обработки. Если функция вернет `True`, это значит, что Оркестратор запросил отмену. Ваш код должен корректно прервать выполнение, выполнить очистку и вернуть статус `cancelled`.

**Пример использования `check_for_cancellation`:**
```python
@worker.task("train_model")
async def train_model_handler(params: dict, task_id: str, job_id: str) -> dict:
    for epoch in range(params.get("epochs", 100)):
        # ... здесь логика обучения модели ...

        # Проверяем флаг отмены в конце каждой эпохи
        if await worker.check_for_cancellation(task_id):
            print("Отмена задачи обнаружена. Завершаем обучение...")
            # ... здесь код для очистки (например, удаление временных файлов) ...
            return {"status": "cancelled", "message": "Training was cancelled by user."}

    return {"status": "success"}
```

Эта гибридная модель обеспечивает как быструю отмену через WebSocket, так и надежный фолбэк-механизм через Redis, который не требует постоянного соединения.

### Шаг 5: Запуск

Вы можете запускать воркер с помощью встроенной консольной команды `worker`. Это рекомендуемый способ как для разработки, так и для продакшена.

```bash
# Обычный запуск
worker run --app my_worker:worker

# Режим разработки (авто-перезапуск при изменении кода)
worker run --app my_worker:worker --reload
```

Функция `--reload` требует установленного пакета `watchdog` (устанавливается через `pip install avtomatika-worker[dev]`). Она отслеживает изменения в `.py` файлах в текущей директории и автоматически перезапускает процесс воркера.

### Шаг 6: Динамическая загрузка скиллов (Модульная архитектура)

Вместо того чтобы определять все задачи в одном файле, вы можете организовать их в модули и поместить в специальную директорию (по умолчанию: `skills/`).

#### Использование SkillBlueprint

`SkillBlueprint` позволяет определять задачи без необходимости иметь экземпляр `Worker` сразу. Это удобно для создания переносимых "пакетов навыков".

Создайте файл `skills/image_skills.py`:
```python
from avtomatika_worker import SkillBlueprint

# 1. Создаем blueprint
bp = SkillBlueprint()

# 2. Регистрируем задачи на blueprint
@bp.task("resize_image")
async def resize_handler(params: dict, **kwargs):
    return {"status": "success"}

@bp.task("convert_format")
async def convert_handler(params: dict, **kwargs):
    return {"status": "success"}
```

#### Загрузка скиллов в Воркер

При инициализации `Worker` автоматически сканирует директорию, указанную в `WORKER_SKILLS_DIR` (по умолчанию `skills/` в текущем рабочем каталоге).

Если вы хотите вручную подключить blueprint:
```python
from avtomatika_worker import Worker
from skills.image_skills import bp

worker = Worker()
worker.include_blueprint(bp)
```

### Шаг 7 (Опционально): Работа с большими файлами через "Payload Offloading"

Если ваши задачи требуют обработки больших объемов данных (видео, HD-изображения, большие текстовые файлы), передавать их напрямую через Оркестратор неэффективно. SDK поддерживает механизм **"Payload Offloading"**, который позволяет передавать "тяжелые" данные через S3-совместимое хранилище. Для этих операций используется высокопроизводительная библиотека **`obstore`** (на базе Rust).

#### Как это работает:

1.  **Клиент** перед созданием Job загружает входные файлы в S3 и передает в параметрах задачи только URI вида `s3://my-bucket/path/to/file.mp4`.
2.  **Worker SDK** автоматически обнаруживает такие URI в параметрах задачи.
3.  Перед вызовом вашего обработчика, SDK **скачивает файл** из S3 во временную директорию и подменяет `s3://` URI на локальный путь к файлу.
4.  Ваш код в обработчике работает с файлом как с обычным локальным файлом.
5.  Если ваш обработчик **возвращает локальный путь к файлу** в качестве результата, SDK автоматически **загружает этот файл в S3** и подменяет локальный путь на `s3://` URI.
6.  SDK также **автоматически очищает** все скачанные временные файлы после завершения задачи.

#### Пример использования S3

Если Оркестратор присылает задачу с параметром `{"video_path": "s3://bucket/input.mp4"}`, ваш код будет выглядеть так:

```python
import os
from avtomatika_worker import Worker

worker = Worker(worker_type="video-processor")

@worker.task("resize_video")
async def resize_video(params: dict, **kwargs):
    # SDK уже скачал файл. В params['video_path'] теперь локальный путь
    input_file = params["video_path"]
    output_file = os.path.join(os.path.dirname(input_file), "resized.mp4")

    # Вы работаете с файлами как с обычными локальными данными
    print(f"Обработка файла {input_file}...")
    # ... логика обработки (например, вызов ffmpeg) ...

    # Возвращаем путь к созданному файлу. 
    # Важно: файл должен находиться внутри TASK_FILES_DIR (по умолчанию /tmp/payloads)
    return {
        "status": "success",
        "data": {
            "result_url": output_file
        }
    }
```

#### Работа с файловой системой (TaskFiles)

Для удобства работы с путями и временными файлами SDK предоставляет класс `TaskFiles`. Он позволяет не заботиться о создании директорий вручную и предоставляет асинхронный интерфейс для работы с файлами. Просто добавьте аргумент с типом `TaskFiles` в вашу функцию:

```python
from avtomatika_worker import Worker, TaskFiles

@worker.task("generate_file")
async def generate_file(params: dict, files: TaskFiles, **kwargs):
    # 1. Быстрая запись и чтение
    await files.write("report.txt", "Some data")
    content = await files.read("report.txt")
    
    # 2. Получение пути (папка создастся автоматически)
    output_path = await files.path_to("result.mp4")
    
    # 3. Проверка и список файлов
    if await files.exists("input.jpg"):
        all_files = await files.list()
        
    return {"data": {"file": output_path}}
```

**Доступные методы (все асинхронные):**
- `await path_to(name)` — возвращает полный путь к файлу (создает папку задачи).
- `await read(name, mode='r')` — читает файл целиком.
- `await write(name, data, mode='w')` — записывает данные в файл.
- `await list()` — список имен файлов в папке задачи.
- `await exists(name)` — проверка существования.
- `async with open(name, mode)` — контекстный менеджер для продвинутой работы.

#### Работа с папками

SDK также поддерживает рекурсивную передачу директорий:

1.  **Скачивание:** Если S3-ссылка заканчивается на `/` (например, `s3://bucket/dataset/`), SDK скачает всё содержимое этого префикса в локальную папку. В параметрах задачи будет путь к этой папке.
2.  **Загрузка:** Если вы вернете путь к локальной директории, SDK рекурсивно загрузит всё её содержимое в S3, сохранив структуру файлов. Ссылка в результате будет иметь вид `s3://bucket/directory_name/`.

#### Настройка S3

Чтобы включить эту функциональность, вам необходимо настроить следующие переменные окружения:

-   `S3_ENDPOINT_URL`: URL вашего S3-совместимого хранилища (например, `https://s3.amazonaws.com` или `http://localhost:9000` for MinIO).
-   `S3_ACCESS_KEY`: Ключ доступа к S3.
-   `S3_SECRET_KEY`: Секретный ключ к S3.
-   `S3_DEFAULT_BUCKET`: Название бакета, в который будут загружаться результаты.
-   `S3_REGION`: Регион хранилища S3 (требуется некоторыми провайдерами, напр. `us-east-1`).
-   `TASK_FILES_DIR`: **(Важно для безопасности)** Локальная директория, в которой создаются изолированные рабочие области для задач. SDK загружает в S3 только те файлы, которые находятся внутри этой директории. По умолчанию: `/tmp/payloads`.

При наличии этих настроек механизм "Payload Offloading" будет работать полностью автоматически, не требуя изменений в коде ваших обработчиков.

> **Важно: Автоматическая очистка**
>
> SDK автоматически удаляет всю папку задачи (включая все скачанные и созданные через `TaskFiles` файлы) сразу после завершения обработки и отправки результата. Вам не нужно заботиться об удалении временных файлов.

> **Важно: Согласованность S3**
>
> SDK **не проверяет автоматически**, что Воркер и Оркестратор используют одно и то же хранилище. Вы должны самостоятельно убедиться, что:
> 1. Воркер имеет доступ к тому же `S3_ENDPOINT_URL`, что и Оркестратор (или имеет сетевой доступ к нему).
> 2. Учетные данные Воркера (`S3_ACCESS_KEY`/`S3_SECRET_KEY`) имеют права на чтение из бакетов, ссылки на которые присылает Оркестратор.
> 3. Учетные данные имеют права на запись в `S3_DEFAULT_BUCKET` для загрузки результатов.