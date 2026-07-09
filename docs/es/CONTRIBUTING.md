[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/CONTRIBUTING.md) | ES | [RU](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/CONTRIBUTING.md)

# Contribuyendo al SDK del Worker de Avtomatika

¡Gracias por ayudarnos a mejorar el caparazón (Shell) de nuestro ecosistema!

## Configuración

1. Clone el repositorio y navegue hasta este directorio.
2. Instale las dependencias de desarrollo:
   ```bash
   pip install -e .[dev]
   ```

## Control de Calidad

Utilizamos `ruff` para el linting y el formateo, y `mypy` para la comprobación de tipos. Por favor, asegúrese de que sus cambios pasen estas comprobaciones:

```bash
# Linting y formateo
ruff check .
ruff format .

# Comprobación de tipos
mypy src/avtomatika_worker
```

## Pruebas

Ejecute las pruebas específicas del worker:

```bash
pytest
```

## Adición de Nuevas Características

- Si agrega un nuevo parámetro de configuración, actualice `src/avtomatika_worker/config.py`.
- Si cambia la interacción del protocolo, asegúrese de la compatibilidad con el paquete `rxon`.
- Actualice siempre el `README.md` si cambia la API de cara al usuario.
- Todo el código nuevo debe tener sugerencias de tipos.
