[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/CONTRIBUTING.md) | [ES](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/es/CONTRIBUTING.md) | RU

# Участие в разработке Avtomatika Worker SDK

Спасибо за помощь в улучшении Оболочки (Shell) нашей экосистемы!

## Подготовка

1.  Клонируйте репозиторий и перейдите в эту директорию.
2.  Установите зависимости:
    ```bash
    pip install -e .[test,dev,s3,pydantic]
    ```

## Тестирование

Запуск тестов воркера:
```bash
pytest tests/
```

## Добавление новых функций

-   При добавлении нового параметра конфигурации обновите `src/avtomatika_worker/config.py`.
-   При изменении взаимодействия по протоколу обеспечьте совместимость с пакетом `rxon`.
-   **Всегда** обновляйте `README.md`, если меняется публичный API (Skill API, Events).
-   Используйте `ruff check .` и `ruff format .` для проверки стиля кода.
-   Убедитесь, что `mypy src/avtomatika_worker` не находит ошибок в типах.
