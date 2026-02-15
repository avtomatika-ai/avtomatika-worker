[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/DEVELOPMENT.md) | ES | [RU](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/DEVELOPMENT.md)

# Guía de Desarrollo del Worker

Este documento describe cómo crear un Worker personalizado compatible con el Orquestador utilizando `avtomatika-worker`.

**Requisitos:** Python 3.11 o superior.

## Concepto Principal

Los Workers creados con el SDK implementan un modelo de interacción híbrido con el Orquestador:
- **Modelo PULL para la obtención de tareas:** El worker inicia la conexión con el Orquestador y "tira" (pull) de las tareas de su cola personal. Esto permite que los Workers operen desde cualquier red, incluso detrás de NAT o firewalls corporativos, sin necesidad de una dirección IP pública.
- **WebSocket para comunicación en tiempo real:** Un canal bidireccional opcional para recibir comandos (ej., cancelación de tareas) y enviar el progreso de ejecución intermedio.

## Cómo crear un Worker con el SDK

### Paso 1: Instalar `avtomatika-worker`

Asegúrate de que el SDK esté instalado en tu entorno.
```bash
pip install avtomatika-worker
```
Para soporte de S3: `pip install "avtomatika-worker[s3]"`

### Paso 2: Crear un archivo de Worker

Crea un archivo de Python (ej., `mi_worker.py`) e importa la clase `Worker`.

```python
import asyncio
from avtomatika_worker import Worker

# 1. Inicializar la clase Worker
worker = Worker(worker_type="mi-worker-personalizado")

# 2. Definir manejadores de tareas usando el decorador @worker.task
@worker.task("generar_informe")
async def generar_informe_handler(params: dict, **kwargs) -> dict:
    task_id = kwargs.get("task_id")
    job_id = kwargs.get("job_id")

    print(f"Parámetros recibidos: {params}")

    # Simular trabajo largo con reporte de progreso
    await asyncio.sleep(2)
    await worker.send_progress(task_id, job_id, progress=0.5, message="Analizado el 50%")
    
    return {
        "status": "success",
        "data": {"report_url": "/ruta/al/informe.pdf"}
    }

# 3. Ejecutar el worker
if __name__ == "__main__":
    worker.run()
```

### Paso 3: Configuración

Usa variables de entorno para configurar el worker:
- `ORCHESTRATOR_URL`: Dirección del Orquestador.
- `WORKER_ID`: ID único del worker.
- `WORKER_INDIVIDUAL_TOKEN`: Token de autenticación.

### Paso 4: Comunicación en Tiempo Real (WebSocket)

Establece `WORKER_ENABLE_WEBSOCKETS=true` para habilitar la cancelación de tareas y el envío de progreso en tiempo real.

### Paso 5: Ejecución y Recarga

```bash
# Ejecución estándar
worker run --app mi_worker:worker

# Modo de desarrollo (se reinicia al cambiar el código)
worker run --app mi_worker:worker --reload
```

### Paso 6: Carga Dinámica de Skills (Arquitectura Modular)

Puedes organizar tus tareas en módulos y colocarlos en un directorio específico (por defecto: `skills/`).

#### Uso de SkillBlueprint

`SkillBlueprint` permite definir tareas sin necesidad de una instancia de `Worker` inmediata.

Crea un archivo `skills/image_skills.py`:
```python
from avtomatika_worker import SkillBlueprint

bp = SkillBlueprint()

@bp.task("resize_image")
async def resize_handler(params: dict, **kwargs):
    return {"status": "success"}
```

#### Carga en el Worker

Al inicializar `Worker`, este escanea automáticamente el directorio especificado en `WORKER_SKILLS_DIR`.

### Paso 7 (Opcional): "Payload Offloading" con S3

Si tus tareas requieren procesar grandes volúmenes de datos, el SDK admite la transferencia automática a través de S3.

1.  El **Cliente** sube los archivos a S3 y pasa las URIs `s3://...`.
2.  El **SDK del Worker** descarga automáticamente los archivos antes de llamar a tu manejador.
3.  Tu código trabaja con rutas de archivos locales.
4.  El SDK sube los resultados a S3 y limpia los archivos temporales.

Para configurar S3, usa: `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_DEFAULT_BUCKET`.