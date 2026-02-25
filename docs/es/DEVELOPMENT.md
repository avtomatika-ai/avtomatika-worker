[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/DEVELOPMENT.md) | ES | [RU](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/DEVELOPMENT.md)

# Guía de Desarrollo del Worker

Este documento describe cómo crear un Worker personalizado compatible con el Orquestador utilizando `avtomatika-worker`.

**Requisitos:** Python 3.11 o superior.

## Concepto Principal

Los Workers creados con el SDK implementan un modelo de interacción híbrido con el Orquestador:
- **Modelo PULL:** El worker inicia la conexión y "tira" (pull) de las tareas de su cola personal.
- **WebSocket:** Canal bidireccional opcional para recibir comandos (ej., cancelación) y enviar progreso.

## Cómo crear un Worker con el SDK

### Paso 1: Instalar `avtomatika-worker`

```bash
pip install avtomatika-worker
```
Para soporte de S3: `pip install "avtomatika-worker[s3]"`

### Paso 2: Crear un archivo de Worker

El SDK utiliza **Inferencia Automática** para reducir el código repetitivo.

```python
import asyncio
from avtomatika_worker import Worker
from pydantic import BaseModel

# 1. Inicializar la clase Worker
worker = Worker(worker_type="mi-worker-personalizado")

# 2. Definir modelos de datos para sus habilidades
class ReportParams(BaseModel):
    data_source: str
    format: str = "pdf"

# 3. Definir manejadores usando el decorador @worker.skill
# El SDK infiere automáticamente:
# - nombre: "generar_informe" (del nombre de la función)
# - esquema: generado desde ReportParams para el Marketplace
@worker.skill(description="Genera informes complejos")
async def generar_informe(params: ReportParams, send_progress, send_event, **kwargs) -> dict:
    """
    - `params` (ReportParams): Parámetros validados y tipados.
    - `send_progress`: Función asíncrona para enviar el progreso.
    - `send_event`: Función asíncrona para emitir eventos personalizados.
    - `**kwargs`: Metadatos: task_id, job_id, etc.
    """
    task_id = kwargs.get("task_id")

    print(f"Generando informe {params.format} desde {params.data_source}")

    # Enviar progreso (evento estándar)
    await send_progress(progress=0.5, message="Procesando...")
    
    # Enviar evento personalizado
    await send_event("milestone", {"name": "data_parsed"})

    return {
        "status": "success",
        "data": {"report_url": f"s3://bucket/reports/{task_id}.pdf"}
    }

# Extensión Dinámica: agregar 'precio' para el Marketplace
@worker.skill(name="enviar_email", price=0.01)
async def enviar_email(params: dict, **kwargs) -> dict:
    print(f"Enviando email: {params}")
    return {"status": "success"}

# 4. Ejecutar el worker
if __name__ == "__main__":
    worker.run()
```

### Paso 3: Configuración

```dotenv
ORCHESTRATOR_URL=http://localhost:8080
WORKER_ID=report-worker-01
WORKER_INDIVIDUAL_TOKEN=mi-token-secreto
```

### Paso 4: Comunicación en Tiempo Real (WebSocket)

Para habilitar esta funcionalidad, establezca `WORKER_ENABLE_WEBSOCKETS=true`. Esto le permite:
1.  **Enviar progreso y eventos:** Use las funciones inyectadas `send_progress` y `send_event`.
2.  **Cancelación de tareas:** El Orquestador puede enviar un comando que lanzará instantáneamente un `asyncio.CancelledError` en su manejador.

### Paso 5: Skills Modulares (SkillBlueprint)

Puede organizar sus habilidades en archivos dentro del directorio `skills/`.

`skills/image_skills.py`:
```python
from avtomatika_worker import SkillBlueprint
from pydantic import BaseModel

class ResizeParams(BaseModel):
    w: int
    h: int

bp = SkillBlueprint()

@bp.skill() # nombre="resize", esquema desde ResizeParams
async def resize(params: ResizeParams):
    return {"status": "success"}
```

El Worker cargará automáticamente todos los skills del directorio `WORKER_SKILLS_DIR`.

### Paso 6: Registro Avanzado de Skills

El decorador `.skill()` soporta tres modos:

1.  **Cero-configuración:** `@worker.skill()` (todo se infiere del código).
2.  **Metadatos:** `@worker.skill(price=0.5, category="AI")` (crea extensiones dinámicas).
3.  **Contrato estricto:** Pase un objeto `SkillInfo` (o descendiente) directamente.

### Paso 7: Trabajo con Archivos Grandes (S3 Offloading)

El SDK admite la transferencia automática de datos pesados a través de S3 utilizando la librería **`obstore`**.

1.  **Descarga Automática:** Si `params` contiene una URI `s3://`, el SDK la descarga antes de llamar a su código.
2.  **Carga Automática:** Si devuelve una ruta local, el SDK la sube a S3 automáticamente.
3.  **TaskFiles:** Use la clase `TaskFiles` para operaciones de archivos asíncronas en el directorio aislado de la tarea.

```python
from avtomatika_worker import Worker, TaskFiles

@worker.skill()
async def procesar_video(params: dict, files: TaskFiles):
    # 'video_url' reemplazado por la ruta local tras la descarga de S3
    ruta_local = params["video_url"]
    
    # Crear archivo de resultado
    ruta_resultado = await files.path_to("output.mp4")
    # ... procesar ...
    
    return {"status": "success", "data": {"result": ruta_resultado}}
```

#### Configuración de S3
Use las variables: `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_DEFAULT_BUCKET`.

> **Nota:** El SDK elimina automáticamente todo el directorio de la tarea después de su finalización.