[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/README.md) | ES | [RU](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/README.md)

# Avtomatika Worker SDK

[![License: MPL 2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)
[![PyPI version](https://img.shields.io/pypi/v/avtomatika-worker.svg)](https://pypi.org/project/avtomatika-worker/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/release/python-3110/)

El SDK oficial para crear workers compatibles con el **[Orquestador Avtomatika](https://github.com/avtomatika-ai/avtomatika)**. Maneja el sondeo (polling), los latidos (heartbeats), la descarga de carga útil de S3 y el cierre ordenado (graceful shutdown) para que puedas concentrarte en tu lógica de negocio.

## Instalación

```bash
pip install avtomatika-worker
```

Extras:
- `pip install "avtomatika-worker[s3]"` — para descarga de S3 (requiere `obstore`).
- `pip install "avtomatika-worker[pydantic]"` — para validación de parámetros basada en Pydantic.
- `pip install "avtomatika-worker[dev]"` — para funciones de desarrollo como CLI `--reload`.

## Inicio Rápido

### Opción 1: Uso de CLI (Recomendado)

¡El SDK infiere automáticamente los nombres y esquemas de los skills a partir de tu código!

```python
from avtomatika_worker import Worker
from pydantic import BaseModel

worker = Worker(worker_type="image-processor")

class ResizeParams(BaseModel):
    width: int
    height: int
    url: str

# Automático: name="resize", esquema de ResizeParams
@worker.skill()
async def resize(params: ResizeParams):
    print(f"Redimensionando a {params.width}px")
    return {"status": "success", "data": {"result": "ok"}}
```

### Opción 2: Carga Dinámica de Skills

Coloque sus manejadores de habilidades en el directorio `skills/` (ej., `skills/my_skills.py`):

```python
from avtomatika_worker import SkillBlueprint

bp = SkillBlueprint()

# Agregar metadatos para el Marketplace (opcional)
@bp.skill(price=0.5, category="AI")
async def generate_preview(params: dict):
    return {"status": "success"}
```

Ejecute el worker y cargará automáticamente todos los skills del directorio:

```bash
# Buscará en ./skills por defecto
worker run --app app.main:worker
```

## Características Clave

### 1. Registro Inteligente de Skills
- **Configuración Cero:** Los nombres y esquemas se infieren automáticamente de los nombres de las funciones и las pistas de tipo.
- **Auto-Contratos:** Generación de `input_schema` y `output_schema` a partir de modelos Pydantic o Dataclasses estándar.
- **Eventos Genéricos:** Declare señales personalizadas mediante `@worker.skill(events={"alert": Schema})` y emítalas usando el ayudante `send_event`. El progreso también es un evento del sistema.
- **Extensiones Dinámicas:** Pase cualquier campo personalizado (como `price` o `category`) directamente al decorador.

### 2. Tráfico de Red Optimizado
- **Hashing de Skills:** Los workers solo envían la lista completa de habilidades cuando realmente cambia. Los latidos periódicos utilizan un `skills_hash` ligero.
- **Sincronización Autorrecuperable (Self-Healing):** Si el orquestador pierde los metadatos del worker, puede solicitar una sincronización completa a través de la respuesta del latido, asegurando una recuperación perfecta.
- **Transportes Inteligentes:** Los eventos se envían a través de WebSocket si está disponible, recurriendo a HTTP automáticamente.

### 3. Validación Fail-Fast
- **Cumplimiento Local:** El SDK valida los resultados de las tareas и los eventos contra sus esquemas declarados localmente. Los errores se registran de inmediato, evitando la transmisión de datos "rotos".

### 4. Registro Estructurado (Logging)
El SDK admite el registro tanto en formato legible por humanos como en JSON.
- `LOG_FORMAT=json` — para producción (ELK, Grafana Loki).
- `LOG_FORMAT=text` — para desarrollo (por defecto).
- Todos los registros incluyen automáticamente el contexto de `worker_id`, `task_id` и `job_id`.

### 5. Cierre Ordenado (Graceful Shutdown)
Manejo integrado de `SIGTERM` и `SIGINT`.

### 4. Sistema de Archivos y Descarga de S3
- **TaskFiles**: Asistente asíncrono para espacios de trabajo de tareas aislados.
- **S3 Payload Offloading**: Descarga/carga automática de archivos grandes mediante URIs de S3 en los parámetros de la tarea (requiere extra `[s3]`).

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
| `WORKER_SKILLS_DIR` | Directorio para cargar skills dinámicamente. | `skills` |

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