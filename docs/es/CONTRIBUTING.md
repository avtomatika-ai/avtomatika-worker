[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/CONTRIBUTING.md) | ES | [RU](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/CONTRIBUTING.md)

# Contribuir a Avtomatika Worker SDK

¡Gracias por ayudar a mejorar el Shell de nuestro ecosistema!

## Preparación

1.  Clone el repositorio y navegue a este directorio.
2.  Instale las dependencias:
    ```bash
    pip install -e .[test,dev,s3,pydantic]
    ```

## Pruebas

Ejecute las pruebas específicas del worker:
```bash
pytest tests/
```

## Agregar Nuevas Funciones

-   Si agrega un nuevo parámetro de configuración, actualice `src/avtomatika_worker/config.py`.
-   Si cambia la interacción del protocolo, asegure la compatibilidad con el paquete `rxon`.
-   **Siempre** actualice el `README.md` si cambia la API orientada al usuario (Skill API, Events).
-   Use `ruff check .` y `ruff format .` para verificar el estilo del código.
-   Asegúrese de que `mypy src/avtomatika_worker` no encuentre errores de tipo.
