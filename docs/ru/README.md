# Avtomatika Worker SDK

Официальный SDK для создания воркеров, совместимых с оркестратором **Avtomatika**. Он автоматизирует низкоуровневые задачи: опрос (polling), хартбиты (heartbeats), управление S3-полезной нагрузкой и корректное завершение работы (graceful shutdown).

## 🚀 Основные особенности

- **Язык:** Python 3.11+
- **Протокол:** На базе **RXON** (Reverse Axon Protocol) для иерархических сетей (Holarchy).
- **Модель взаимодействия:**
  - **PULL:** Воркер опрашивает задачи у оркестраторов (работает за NAT/Firewall).
  - **WebSocket:** Канал для команд в реальном времени (отмена задач, кастомные команды).
- **Zero Trust Security (Безопасность):**
  - Обязательная подпись HMAC SHA256 для всех сообщений при наличии `WORKER_TOKEN`.
  - Поддержка Identity Chain и Origin Worker ID для отслеживания происхождения данных.
  - Защита от повторных атак (Replay Protection) через временные метки.
- **Оптимизация трафика:**
  - **Трёхуровневые навыки:** *Supported* (каталог), *Available* (лимиты), *Hot* (кэшированные).
  - **Stable Hashing:** Передача полного каталога навыков только при изменениях.
- **S3 Streaming:** Высокопроизводительная потоковая передача данных через `obstore`. Никаких OOM на больших файлах.
- **Мониторинг железа:** Встроенный сбор метрик CPU, RAM и NVIDIA GPU (через `psutil` и `GPUtil`).

## 🛠 Установка

```bash
pip install avtomatika-worker[s3,pydantic]
```

Для разработки:
```bash
pip install -e .[test,dev]
```

## 💻 Быстрый старт

```python
from avtomatika_worker import Worker, TaskFiles

worker = Worker()

@worker.skill("hello_world")
async def my_skill(params: dict, files: TaskFiles):
    """Простой навык, который говорит привет."""
    return {"message": f"Привет, {params.get('name', 'Мир')}!"}

@worker.on_command("reboot")
async def handle_reboot(command):
    print("Перезагрузка воркера...")

if __name__ == "__main__":
    worker.run()
```

## ⚙️ Конфигурация

Управляется через переменные окружения:
- `ORCHESTRATORS_CONFIG`: JSON-список конфигураций оркестраторов (URL, приоритеты, веса).
- `ORCHESTRATOR_URL`: Простой URL оркестратора, если используется только один (по умолчанию: `http://localhost:8080`).
- `WORKER_TOKEN`: Секретный ключ для HMAC-подписи (Zero Trust).
- `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_DEFAULT_BUCKET`: Настройки S3 для передачи больших объёмов данных.
- `STRICT_EVENT_VALIDATION`: (По умолчанию: `True`) Проверка событий на соответствие схемам перед отправкой.
- `LOG_LEVEL`: Уровень детализации логов (DEBUG, INFO, WARNING, ERROR).
- `POLL_BACKOFF_INITIAL`: Начальная задержка после ошибки 429 или сетевого сбоя (по умолчанию: `1.0`). Учитывает заголовок `Retry-After`.
- `POLL_BACKOFF_MAX`: Максимальная задержка при повторных попытках (по умолчанию: `60.0`).
- `POLL_BACKOFF_FACTOR`: Множитель экспоненциальной задержки (по умолчанию: `2.0`).
- `MAX_CONCURRENT_TASKS`: Глобальный лимит на количество одновременно выполняемых задач.

## 📜 Лицензия

Mozilla Public License v. 2.0.
