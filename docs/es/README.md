[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/README.md) | ES | [RU](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/README.md)

# Avtomatika Worker SDK

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![Code Style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

El SDK oficial para crear workers compatibles con el **[Orquestador Avtomatika](https://github.com/avtomatika-ai/avtomatika)**. Maneja el sondeo (polling), los latidos (heartbeats), la descarga de carga útil de S3 y el cierre ordenado (graceful shutdown) para que puedas concentrarte en tu lógica de negocio.

## Instalación

```bash
pip install avtomatika-worker
```

Extras:
- `pip install "avtomatika-worker[pydantic]"` — para validación de parámetros basada en Pydantic.
- `pip install "avtomatika-worker[dev]"` — para funciones de desarrollo como CLI `--reload`.

## Inicio Rápido

### Opción 1: Uso de CLI (Recomendado)

Define tu worker en un módulo de Python (ej., `app/main.py`):

```python
from avtomatika_worker import Worker

worker = Worker(worker_type="image-processor")

@worker.task("resize")
async def resize_image(params: dict, **kwargs):
    return {"status": "success", "data": {"result": "ok"}}
```

Ejecútalo usando el comando `worker` integrado:

```bash
export ORCHESTRATOR_URL="http://localhost:8080"
export WORKER_TOKEN="tu-token-secreto"

# Ejecución estándar
worker run --app app.main:worker

# Modo de desarrollo con recarga automática
worker run --app app.main:worker --reload
```

### Opción 2: Ejecución Programática

```python
if __name__ == "__main__":
    worker.run_with_health_check()
```

## Características Clave

### 1. Registro Estructurado (Logging)
El SDK admite el registro tanto en formato legible por humanos como en JSON.
- `LOG_FORMAT=json` — para producción (ELK, Grafana Loki).
- `LOG_FORMAT=text` — para desarrollo (por defecto).
- Todos los registros incluyen automáticamente el contexto de `worker_id`, `task_id` y `job_id`.

### 2. Cierre Ordenado (Graceful Shutdown)
Manejo integrado de `SIGTERM` y `SIGINT`. Cuando se recibe una señal, el worker:
1. Entra en "Modo Drenaje" (deja de aceptar nuevas tareas).
2. Espera a que se completen las tareas activas (configurable mediante `WORKER_SHUTDOWN_TIMEOUT`).
3. Envía los latidos finales y cierra las conexiones.

### 3. Sistema de Archivos y Descarga de S3
- **TaskFiles**: Asistente asíncrono para espacios de trabajo de tareas aislados.
- **S3 Payload Offloading**: Descarga/carga automática de archivos grandes mediante URIs de S3 en los parámetros de la tarea.

## Referencia de Configuración

| Variable | Descripción | Por Defecto |
|----------|-------------|-------------|
| `WORKER_ID` | Identificador único para la instancia del worker. | UUID |
| `ORCHESTRATOR_URL` | Dirección del orquestador. | `http://localhost:8080` |
| `LOG_FORMAT` | Formato de registro: `text` o `json`. | `text` |
| `LOG_LEVEL` | Nivel de registro mínimo (DEBUG, INFO, etc). | `INFO` |
| `WORKER_SHUTDOWN_TIMEOUT`| Segundos máx. para esperar tareas durante el cierre. | `30.0` |
| `WORKER_ENABLE_WEBSOCKETS`| Habilitar comandos en tiempo real (ej. cancelación). | `false` |
| `TASK_FILES_DIR` | Directorio local para cargas útiles temporales de S3. | `/tmp/payloads` |

## Documentación

- [Guía de Desarrollo](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/es/DEVELOPMENT.md) — Instrucciones detalladas sobre cómo crear workers personalizados, usar middlewares y manejar la descarga de S3.

## Uso con Docker

Utiliza el `Dockerfile` proporcionado para un despliegue sencillo:

```bash
docker build -t my-worker .
docker run -e ORCHESTRATOR_URL=... my-worker worker run --app app:worker
```

## Desarrollo

Instala las dependencias de desarrollo:
```bash
pip install -e .[test,dev]
pytest
```
