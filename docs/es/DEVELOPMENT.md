[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/DEVELOPMENT.md) | ES | [RU](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/DEVELOPMENT.md)

# Guía de Desarrollo del Worker

Este documento describe cómo crear un Worker personalizado compatible con el Orquestador utilizando `avtomatika-worker`.

## Concepto Principal

Los Workers creados con el SDK implementan un modelo de interacción híbrido con el Orquestador:
- **Modelo PULL para la obtención de tareas:** El worker inicia la conexión con el Orquestador y "tira" (pull) de las tareas de su cola personal. Esto permite que los Workers operen desde cualquier red, incluso detrás de NAT o firewalls corporativos, sin necesidad de una dirección IP pública.
- **WebSocket para comunicación en tiempo real:** Un canal bidireccional opcional para recibir comandos (ej., cancelación de tareas) y enviar el progreso de ejecución intermedio.

## Cómo crear un Worker con el SDK

### Paso 1: Instalar `avtomatika-worker`

Asegúrate de que el SDK esté instalado en tu entorno. Si estás trabajando en el repositorio principal, puedes instalarlo en modo editable:
```bash
pip install -e .
```

### Paso 2: Crear un archivo de Worker

Crea un archivo de Python (ej., `mi_worker.py`) e importa la clase `Worker`.

```python
import asyncio
from avtomatika_worker import Worker

# 1. Inicializar la clase Worker
# Puedes especificar un tipo único para tu worker.
worker = Worker(worker_type="mi-worker-personalizado")

# 2. Definir manejadores de tareas usando el decorador @worker.task
@worker.task("generar_informe")
async def generar_informe_handler(params: dict, **kwargs) -> dict:
    """
    Esta función será llamada cuando el Orquestador envíe
    una tarea de tipo "generar_informe".

    - `params` (dict): Argumento posicional que contiene los parámetros de ejecución de la tarea.
    - `**kwargs`: Argumentos de palabras clave con metadatos de la tarea:
        - `task_id` (str): ID único de la tarea.
        - `job_id` (str): ID del Trabajo (Job) padre.
        - `priority` (float): Prioridad de la tarea.
    """
    task_id = kwargs.get("task_id")
    job_id = kwargs.get("job_id")
    priority = kwargs.get("priority", 0.0)

    print(f"Parámetros recibidos: {params}")

    # Simular trabajo largo con reporte de progreso
    print("Iniciando generación de informe...")
    await asyncio.sleep(2)
    # Usar worker.send_progress para enviar una actualización al Orquestador
    await worker.send_progress(task_id, job_id, progress=0.5, message="Analizado el 50% de los datos")
    await asyncio.sleep(2)
    print("Generación de informe completada.")


    # 3. Devolver el resultado
    #    - 'status' (requerido): "success", "failure", o un estado personalizado.
    #    - 'data' (opcional): Diccionario con datos que se añadirán al contexto del Trabajo.
    #    - 'error' (opcional cuando status="failure"): Diccionario con detalles del error.
    #      - 'code': "TRANSIENT_ERROR", "PERMANENT_ERROR", o "INVALID_INPUT_ERROR".
    #      - 'message': Descripción del error legible por humanos.
    return {
        "status": "success",
        "data": {"report_url": "/ruta/al/informe.pdf"}
    }

@worker.task("enviar_email")
async def enviar_email_handler(params: dict, **kwargs) -> dict:
    print(f"Enviando email con parámetros: {params}")
    await asyncio.sleep(1)
    return {"status": "success"}

# 4. Ejecutar el worker
if __name__ == "__main__":
    worker.run()
```

### Paso 3: Configuración de Conexión y Autenticación

#### Opción 1: Conexión Simple (Un solo Orquestador)

Este es el método más sencillo, adecuado para la mayoría de los casos.

```dotenv
# Dirección de tu Orquestador
ORCHESTRATOR_URL=http://localhost:8080

# Método de autenticación recomendado
WORKER_ID=report-worker-01
WORKER_INDIVIDUAL_TOKEN=un-token-super-secreto-para-este-worker
```

#### Opción 2: Conexión Avanzada (Múltiples Orquestadores)

Este método se utiliza para Alta Disponibilidad (failover) o Balanceo de Carga (round robin).

-   `ORCHESTRATORS_CONFIG`: En lugar de `ORCHESTRATOR_URL`, se utiliza esta variable. Contiene una cadena JSON con una lista de todos los Orquestadores.
-   `MULTI_ORCHESTRATOR_MODE`: Define cómo interactuará el Worker con esta lista.

**Ejemplo para Alta Disponibilidad (Failover):**
En este modo, el Worker trabajará con `main-orchestrator`. Si no está disponible, el Worker cambia automáticamente a `backup-orchestrator`.

```dotenv
ORCHESTRATORS_CONFIG='[
    {"url": "http://main-orchestrator:8080"},
    {"url": "http://backup-orchestrator:8080"}
]'

# El modo FAILOVER se usa por defecto, pero se puede especificar explícitamente.
MULTI_ORCHESTRATOR_MODE=FAILOVER

WORKER_ID=report-worker-01
WORKER_INDIVIDUAL_TOKEN=un-token-super-secreto-para-este-worker
```

### Paso 4: Comunicación en Tiempo Real (WebSocket)

Para habilitar esta funcionalidad, establece la variable de entorno `WORKER_ENABLE_WEBSOCKETS=true`. Estarán disponibles dos nuevas capacidades:

#### Envío de Progreso

Dentro de tu manejador de tareas, puedes llamar al método `worker.send_progress()` para informar al Orquestador sobre el progreso de una operación de larga duración.

```python
await worker.send_progress(
    task_id="...",      # ID de la tarea actual
    job_id="...",       # ID del Trabajo padre
    progress=0.75,      # Flotante entre 0.0 y 1.0
    message="Procesado el 75% del video"  # Mensaje opcional
)
```

#### Cancelación de Tareas

El SDK proporciona dos mecanismos de cancelación de tareas:

1.  **WebSocket (Modelo Push):** Si WebSocket está habilitado, el Orquestador puede enviar un comando de cancelación inmediata. Esto lanza un `asyncio.CancelledError` en tu manejador. Este método proporciona la reacción más rápida.

2.  **Redis (Modelo Pull):** Incluso sin WebSocket, puedes implementar la cancelación "cooperativa" para tareas muy largas. El SDK proporciona una función asíncrona `worker.check_for_cancellation(task_id)`. Debes llamarla periódicamente dentro de tu bucle de procesamiento. Si la función devuelve `True`, significa que el Orquestador solicitó la cancelación. Tu código debe interrumpir la ejecución de manera ordenada, realizar la limpieza y devolver un estado `cancelled`.

### Paso 5: Ejecución

Puedes ejecutar el worker utilizando el comando CLI `worker` integrado. Esta es la forma recomendada tanto para desarrollo como para producción.

```bash
# Ejecución estándar
worker run --app mi_worker:worker

# Modo de desarrollo (se reinicia automáticamente al cambiar el código)
worker run --app mi_worker:worker --reload
```

La función `--reload` requiere el paquete `watchdog` (instalar mediante `pip install avtomatika-worker[dev]`). Monitoriza el directorio actual en busca de cambios en los archivos `.py` y reinicia el proceso del worker automáticamente.

### Paso 6 (Opcional): Trabajo con archivos grandes mediante "Payload Offloading"

Si tus tareas requieren procesar grandes volúmenes de datos, pasarlos directamente a través del Orquestador es ineficiente. El SDK admite un mecanismo de **"Payload Offloading"**, que permite transferir datos "pesados" a través de almacenamiento compatible con S3. Utiliza la biblioteca de alto rendimiento **`obstore`** (basada en Rust) para estas operaciones.

#### Cómo funciona:

1.  El **Cliente** sube los archivos de entrada a S3 antes de crear un Trabajo y pasa solo URIs como `s3://mi-bucket/ruta/al/archivo.mp4` en los parámetros de la tarea.
2.  El **SDK del Worker** detecta automáticamente dichas URIs en los parámetros de la tarea.
3.  Antes de llamar a tu manejador, el SDK **descarga el archivo** de S3 a un directorio temporal y reemplaza la URI `s3://` con la ruta del archivo local.
4.  El código de tu manejador trabaja con el archivo como un archivo local normal.
5.  Si tu manejador **devuelve una ruta de archivo local** en el resultado, el SDK **sube automáticamente este archivo a S3** y reemplaza la ruta local con una URI `s3://`.
6.  El SDK también **limpia automáticamente** todos los archivos temporales descargados después de que se completa la tarea.

#### Ejemplo de uso de S3

Si el Orquestador envía una tarea con el parámetro `{"video_path": "s3://bucket/entrada.mp4"}`, tu código se vería así:

```python
import os
from avtomatika_worker import Worker

worker = Worker(worker_type="video-processor")

@worker.task("resize_video")
async def resize_video(params: dict, **kwargs):
    # El SDK ya ha descargado el archivo. En params['video_path'] hay ahora una ruta local
    input_file = params["video_path"]
    output_file = os.path.join(os.path.dirname(input_file), "resized.mp4")

    # Trabajas con los archivos como datos locales normales
    print(f"Procesando archivo {input_file}...")
    # ... lógica de procesamiento ...

    # Devuelve la ruta al archivo creado.
    return {
        "status": "success",
        "data": {
            "result_url": output_file
        }
    }
```

#### Trabajo con el Sistema de Archivos (TaskFiles)

Para una gestión cómoda de rutas y archivos temporales, el SDK proporciona la clase `TaskFiles`. Permite ignorar la creación manual de directorios y proporciona una interfaz asíncrona para operaciones de archivos. Solo añade un argumento con el tipo `TaskFiles` a tu función:

```python
from avtomatika_worker import Worker, TaskFiles

@worker.task("generar_archivo")
async def generar_archivo(params: dict, files: TaskFiles, **kwargs):
    # 1. Lectura/escritura rápida
    await files.write("informe.txt", "Algunos datos")
    content = await files.read("informe.txt")
    
    # 2. Obtener ruta (directorio creado automáticamente)
    output_path = await files.path_to("resultado.mp4")
    
    # 3. Comprobar y listar archivos
    if await files.exists("entrada.jpg"):
        all_files = await files.list()
        
    return {"data": {"file": output_path}}
```

> **Importante: Limpieza Automática**
>
> El SDK elimina automáticamente todo el directorio de la tarea (incluidos todos los archivos descargados y creados mediante `TaskFiles`) inmediatamente después de que se completa el procesamiento y se envía el resultado. No tienes que preocuparte por eliminar los archivos temporales.
