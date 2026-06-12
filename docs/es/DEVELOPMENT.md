[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/DEVELOPMENT.md) | ES | [RU](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/DEVELOPMENT.md)

# Guía de Desarrollo de Workers

Este documento describe cómo crear un Worker personalizado compatible con el Orquestador utilizando `avtomatika-worker`.

**Requisitos:** Python 3.11 o superior.

## Concepto Central

Los Workers creados con el SDK implementan un modelo de interacción híbrido con el Orquestador:
- **Modelo PULL para la Obtención de Tareas:** El worker inicia la conexión con el Orquestador y "tira" de las tareas de su cola personal. Esto permite que los Workers operen desde cualquier red (incluyendo detrás de NAT o firewalls corporativos) sin necesidad de una dirección IP pública.
- **WebSocket para Comunicación en Tiempo Real:** Un canal bidireccional opcional para recibir comandos (ej., cancelación de tareas) y enviar el progreso de ejecución intermedio.
- **Optimización HLN:** El SDK utiliza el protocolo **Reverse Axon (RXON)**, que reduce el tráfico mediante el hashing de listas de habilidades y el envío de actualizaciones solo cuando ocurren cambios.
- **Robustez de Conexión:** 
    - **Orquestadores Independientes:** Cada conexión con un orquestador es gestionada por una tarea separada. El fallo de un servidor no bloquea las comunicaciones con otros.
    - **Reintentos de Registro:** Reintentos infinitos con retroceso exponencial si un orquestador está fuera de línea.
    - **Inicio No Bloqueante:** El worker comienza a solicitar tareas tan pronto como se registra con éxito en al menos un orquestador.

## Cómo Crear un Worker con el SDK

### Paso 1: Instalar `avtomatika-worker`

Asegúrese de que el SDK esté instalado en su entorno. Recomendado para todas las funciones (S3 y Pydantic):
```bash
pip install "avtomatika-worker[s3,pydantic,metrics]"
```

Si está trabajando en el repositorio principal, puede instalarlo en modo editable:
```bash
pip install -e .[dev]
```

### Paso 2: Crear un Archivo de Worker

Cree un archivo Python (ej., `my_worker.py`) e importe la clase `Worker`. El SDK utiliza la **Inferencia Automática** para reducir el código repetitivo.

```python
import asyncio
from avtomatika_worker import Worker
from pydantic import BaseModel

# 1. Inicializar la clase Worker
worker = Worker(worker_type="my-custom-worker")

# 2. Definir modelos de datos para sus habilidades
class ReportParams(BaseModel):
    data_source: str
    format: str = "pdf"

# 3. Definir manejadores de habilidades usando el decorador @worker.skill
# El SDK infiere automáticamente:
# - name: "generate_report" (del nombre de la función)
# - input_schema: generado a partir de ReportParams
@worker.skill(description="Genera informes complejos")
async def generate_report(params: ReportParams, send_progress, send_event, **kwargs) -> dict:
    """
    - `params` (ReportParams): Parámetros validados y tipados. 
      IMPORTANTE: El argumento DEBE llamarse 'params' para que funcione la inferencia automática de esquemas.
    - `send_progress`: Función asíncrona para enviar actualizaciones de progreso.
    - `send_event`: Función asíncrona para emitir eventos personalizados.
    - `**kwargs`: Metadatos: task_id, job_id, etc.
    """
    task_id = kwargs.get("task_id")

    print(f"Generando informe {params.format} desde {params.data_source}")

    # Enviar progreso (evento estándar)
    await send_progress(progress=0.5, message="Procesando datos...")
    
    # Enviar evento personalizado
    await send_event("milestone", {"name": "data_parsed"})

    return {
        "status": "success",
        "data": {"report_url": f"s3://bucket/reports/{task_id}.pdf"}
    }

> **Nota de Seguridad (Zero Trust):** Aunque el SDK genera automáticamente esquemas basados en sus modelos, el **Orquestador tiene la autoridad final**. Si se define un `output_schema` estricto en el blueprint, el orquestador filtrará cualquier campo en su resultado que no cumpla con la "ley del blueprint". Esto protege al sistema contra ataques de inyección de estado.

# Extensión de campo dinámico: agregar 'price' para el Marketplace
@worker.skill(name="send_email", price=0.01)
async def send_email(params: dict, **kwargs) -> dict:
    print(f"Enviando correo: {params}")
    return {"status": "success"}

# 4. Ejecutar el worker
if __name__ == "__main__":
    worker.run()
```

### Paso 3: Ejecutar el Worker

Puede ejecutar el worker directamente a través de Python o usar la CLI integrada para un mejor control:

```bash
# Recomendado: ejecuta el worker y habilita el servidor de comprobación de salud (puerto 8083 por defecto)
worker run --app my_worker:worker

# Para el desarrollo (reinicio automático en cambios de código)
worker run --app my_worker:worker --reload
```

### Paso 4: Configuración de Conexión y Autenticación

#### Opción 1: Conexión Simple (Un Solo Orquestador)

```dotenv
ORCHESTRATOR_URL=http://localhost:8080
WORKER_ID=report-worker-01
WORKER_TOKEN=a-super-secret-token-for-this-worker
```

#### Opción 2: Conexión Avanzada (Múltiples Orquestadores)

```dotenv
ORCHESTRATORS_CONFIG='[
    {"url": "http://main-orchestrator:8080", "priority": 1, "weight": 5},
    {"url": "http://backup-orchestrator:8080", "priority": 2, "weight": 1}
]'
MULTI_ORCHESTRATOR_MODE=WATERFALL  # O ROUND_ROBIN, FAILOVER
```

#### Configuración de Sondeo y Backoff (Stable Beta 15+)

Para evitar "tormentas de reintentos" durante una carga alta (errores 429) o fallos de red, el SDK utiliza una estrategia de backoff exponencial. **Nota:** El SDK respeta el encabezado `Retry-After` del orquestador, que tiene prioridad sobre los cálculos locales de backoff.

```dotenv
TASK_POLL_TIMEOUT=30        # Segundos máx. para esperar una respuesta de tarea
POLL_BACKOFF_INITIAL=1.0    # Retraso inicial (seg) después de un error
POLL_BACKOFF_MAX=60.0       # Límite máximo de retraso
POLL_BACKOFF_FACTOR=2.0     # Multiplicador para cada reintento
```

- **WATERFALL (Por defecto):** Sondea los orquestadores en orden de prioridad. Siempre regresa al de mayor prioridad después de cualquier tarea.
- **ROUND_ROBIN:** Distribuye las solicitudes según los pesos.
- **FAILOVER:** Sondea al siguiente solo si el anterior está vacío.

### Paso 5: Comunicación en Tiempo Real (WebSocket)

Para habilitar esta funcionalidad, configure `WORKER_ENABLE_WEBSOCKETS=true`. Esto le permite:
1.  **Enviar Progreso y Eventos:** Use las funciones inyectadas `send_progress` y `send_event`.
2.  **Cancelación de Tareas:** El Orquestador puede enviar un comando que lanzará instantáneamente un `asyncio.CancelledError` en su manejador.


### Paso 6: Habilidades Modulares (SkillBlueprint)

Organice las tareas en módulos en el directorio `skills/`.

`skills/image_skills.py`:
```python
from avtomatika_worker import SkillBlueprint
from pydantic import BaseModel

class ResizeParams(BaseModel):
    w: int
    h: int

bp = SkillBlueprint()

@bp.skill() # name="resize", esquema de ResizeParams
async def resize(params: ResizeParams):
    return {"status": "success"}
```

El Worker cargará automáticamente todas las habilidades del directorio especificado en `WORKER_SKILLS_DIR`.

### Paso 7: Trabajo con Archivos Grandes (Descarga de S3)

El SDK admite la **"Descarga de Carga Útil"** (Payload Offloading) a través de almacenamiento compatible con S3 utilizando la biblioteca de alto rendimiento **`obstore`**.

1.  **Descarga Automática:** Si `params` contiene un URI `s3://`, el SDK lo descarga a una carpeta temporal local antes de llamar a su manejador.
2.  **Carga Automática:** Si su manejador devuelve una ruta local, el SDK la sube a S3 y devuelve el URI al Orquestador.
3.  **TaskFiles:** Use la clase `TaskFiles` para operaciones de archivo asíncronas fáciles en el directorio aislado de la tarea.

```python
from avtomatika_worker import Worker, TaskFiles

@worker.skill()
async def process_video(params: dict, files: TaskFiles):
    # 'video_url' en params podría ser un URI de S3, ahora reemplazado con la ruta local
    local_path = params["video_url"]
    
    # Crear archivo de resultado
    result_path = await files.path_to("output.mp4")
    # ... procesar ...
    
    return {"status": "success", "data": {"result": result_path}}
```

#### Configuración de S3
- `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_DEFAULT_BUCKET`.
- `TASK_FILES_DIR`: Raíz local para datos temporales (por defecto: `/tmp/payloads`).

> **Nota:** El SDK limpia automáticamente todo el directorio de la tarea una vez que se completa.

### Paso 8: Observabilidad (OpenTelemetry)

El SDK proporciona soporte integrado para **trazado distribuido** y **métricas** utilizando OpenTelemetry. Esto transforma al worker de una "caja negra" a un nodo transparente en el sistema.

1.  **Rastreo de Extremo a Extremo:** El worker extrae automáticamente el `trace_id` de las tareas entrantes y convierte la ejecución de la habilidad en un span hijo (`task.{type}`). Todos los eventos y resultados se vinculan al mismo rastreo.
2.  **Sub-spans Automáticos:** Las operaciones de S3 (carga/descarga) se aíslan automáticamente en spans hijos separados.
3.  **Métricas:** Las métricas se exportan automáticamente al colector OTLP (requiere el extra `metrics` y la variable `OTEL_EXPORTER_OTLP_ENDPOINT`).
4.  **Spans Personalizados (Trabajo Interno):** Puede solicitar el `ObservabilityManager` en su manejador para añadir detalle a su lógica interna.

```python
from avtomatika_worker import Worker, ObservabilityManager

@worker.skill()
async def monitored_task(params: dict, obs: ObservabilityManager):
    """
    Uso del gestor inyectado para un monitoreo profundo.
    """
    # 1. Crear un span personalizado para una etapa pesada
    with obs.tracer.start_as_current_span("heavy_model_inference") as span:
        span.set_attribute("model.name", "whisper-v3")
        # ... trabajo del modelo ...
        result = "datos procesados"

    # 2. Las métricas y logs dentro del span se vinculan al Trace ID de la tarea
    return {"status": "success", "data": result}
```

Habilítelo a través de la variable de entorno: `WORKER_ENABLE_METRICS=true`.


### Paso 9: Gestión Dinámica de Habilidades (Hot Skills)

En escenarios de alto rendimiento (por ejemplo, inferencia de modelos de IA), puede ser útil que el Orquestador sepa qué habilidades están "hot" (ya cargadas en la memoria de la GPU o en el caché). Esto permite una ejecución instantánea de tareas sin retrasos de carga.

El SDK proporciona las funciones `add_to_hot_skills` y `remove_from_hot_skills` que se inyectan en sus manejadores de habilidades.

```python
@worker.skill()
async def heavy_ai_task(params: dict, add_to_hot_skills, **kwargs):
    # 1. Cargue su modelo si es necesario
    model = await load_model("my_large_model")
    
    # 2. Marque este recurso o nombre de habilidad como 'hot'
    # Esto se enviará en el próximo heartbeat al Orquestador
    add_to_hot_skills("my_large_model")
    
    # 3. Procesar
    return {"status": "success"}
```

También puede usar estos métodos directamente en la instancia del worker: `worker.add_to_hot_skills("model_name")`.

### Paso 10: Comprobaciones de Salud (Health Checks)

Por defecto, el SDK inicia un pequeño servidor aiohttp en `0.0.0.0:8083`. Puede comprobar el estado del worker en `/health`.
Esto es útil para Kubernetes (probas de Liveness/Readiness) o sistemas de monitoreo.
- Variable: `WORKER_PORT` (por defecto: 8083)
- Bandera CLI: `--health-check` (habilitada por defecto)
