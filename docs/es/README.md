# Avtomatika Worker SDK

SDK oficial para construir trabajadores compatibles con el orquestador **Avtomatika**. Automatiza tareas de bajo nivel: sondeo (polling), latidos (heartbeats), gestión de carga útil S3 y cierre ordenado (graceful shutdown).

## 🚀 Características Principales

- **Lenguaje:** Python 3.11+
- **Protocolo:** Basado en **RXON** (Reverse Axon Protocol) para Redes Lógicas Jerárquicas (Holarchy).
- **Modelo de Comunicación:**
  - **PULL:** Los trabajadores sondean tareas desde los orquestadores (funciona detrás de NAT/Firewall).
  - **WebSocket:** Canal de comandos en tiempo real (cancelación, comandos personalizados).
- **Seguridad Zero Trust:**
  - Firma HMAC SHA256 obligatoria para todos los mensajes con `WORKER_TOKEN`.
  - Soporte para Identity Chain y Origin Worker ID para rastreo de procedencia.
  - Protección contra ataques de repetición mediante validación de marcas de tiempo.
- **Optimización de Tráfico y Rendimiento:**
  - **Telemetry Throttling (Heartbeat Deadband):** La telemetría de recursos (CPU/RAM/GPU) solo se envía cuando el valor cambia en $>5\%$ o tras un intervalo forzado de 60s, reduciendo drásticamente el uso de ancho de banda.
  - **ETag-Based Blob Caching:** Los archivos grandes (por ejemplo, pesos de modelos de IA) se descargan de S3 una sola vez, se guardan en caché local y se enlazan al espacio de trabajo de la tarea mediante enlaces simbólicos.
  - **Subida de Resultados Asíncrona:** Los resultados se envían mediante una cola no bloqueante `asyncio.Queue` con soporte para reintentos y Rate Limit, liberando inmediatamente al trabajador.
  - **Habilidades en 3 Niveles:** _Supported_ (catálogo), _Available_ (límites dinámicos) y _Hot_ (en caché).
  - **Stable Hashing:** Envía el catálogo completo solo cuando cambia, usando `skills_hash` para latidos ligeros.
- **S3 Streaming:** Transferencia de datos de alto rendimiento usando `obstore`. Sin errores de OOM en archivos grandes.
- **Soporte para Agentes de IA:** Soporte para Chain of Thought y Tool Use mediante la inyección de dependencias `OrchestratorClient` para delegar subtareas.
- **Hardware Awareness:** Monitoreo integrado de CPU, RAM y GPUs NVIDIA (vía `psutil` y `GPUtil`).
- **Observabilidad:**
  - Soporte nativo para **OpenTelemetry** (trazas y métricas).
  - Propagación automática del **Contexto de Rastreo**: Los trabajadores extraen el `trace_id` de las tareas e inyectan el contexto actualizado en los eventos (incluido el progreso), asegurando visibilidad de extremo a extremo en Jaeger/Honeycomb.
  - Exportación automática de métricas vía OTLP (modelo Push) cuando se configura `OTEL_EXPORTER_OTLP_ENDPOINT`.

## 🛡 Resiliencia и Conectividad

- **Gestores Independientes:** La conexión a cada orquestador es gestionada por una tarea de fondo separada. El fallo de un servidor o un límite de velocidad (Rate Limit) no afecta a los demás.
- **Backoff Inteligente:** Sistema unificado de retraso exponencial para registro, sondeo de tareas y latidos (heartbeats).
- **Protección contra Rate Limit:** Soporte completo para el encabezado `Retry-After` (segundos o fecha HTTP). Implementa un "piso de seguridad" obligatorio de 30s para errores 429 sin `Retry-After` para prevenir tormentas de reintentos (Retry Storms).
- **Heartbeat Debouncing:** Limita la frecuencia de los latidos a uno cada 2 segundos. Los eventos no se pierden, sino que se consolidan y envían después del periodo de enfriamiento.
- **Reintentos Infinitos:** Los trabajadores nunca dejan de intentar registrarse con un retraso exponencial.
- **Cierre Ordenado (Graceful Shutdown):** Maneja `SIGTERM` y `SIGINT` correctamente, esperando a que las tareas activas terminen.

## 🛠 Instalación

```bash
pip install avtomatika-worker[s3,pydantic]
```

Para desarrollo:

```bash
pip install -e .[test,dev]
```

## 💻 Inicio Rápido

```python
from avtomatika_worker import Worker, TaskFiles, OrchestratorClient

worker = Worker()

@worker.skill("hola_mundo")
async def mi_habilidad(params: dict, files: TaskFiles):
    """Habilidad simple que dice hola."""
    return {"message": f"¡Hola, {params.get('nombre', 'Mundo')}!"}

@worker.skill("ai_agent_reasoning")
async def agent_skill(params: dict, orchestrator_client: OrchestratorClient):
    """Habilidad de agente de IA que delega una subtarea (Tool Use) a través de OrchestratorClient."""
    search_result = await orchestrator_client.call_skill("web_search", {"query": params["search_query"]})
    return {"result": f"Basado en búsqueda web: {search_result['data']}"}

@worker.on_command("reiniciar")
async def manejar_reinicio(comando):
    print("Reiniciando trabajador...")

if __name__ == "__main__":
    worker.run()
```

## ⚙️ Configuración

Controlada mediante variables de entorno:

- `ORCHESTRATORS_CONFIG`: Lista JSON de configuraciones de orquestadores (URL, prioridades, pesos).
- `ORCHESTRATOR_URL`: URL simple del orquestador si solo se usa uno (por defecto: `http://localhost:8080`).
- `WORKER_TOKEN`: Secreto para firma HMAC (Zero Trust).
- `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_DEFAULT_BUCKET`: Ajustes de almacenamiento S3 para transferencias de datos grandes.
- `WORKER_BLOB_CACHE_DIR`: Directorio para caché de blobs S3 (por defecto: `/tmp/avtomatika_cache`).
- `WORKER_TELEMETRY_DEADBAND`: Umbral (porcentaje) para envío de telemetría (por defecto: `5.0`).
- `WORKER_TELEMETRY_FORCE_INTERVAL`: Intervalo de tiempo máximo (segundos) para enviar telemetría aunque no haya cambios (por defecto: `60.0`).
- `STRICT_EVENT_VALIDATION`: (Por defecto: `True`) Valida eventos contra esquemas antes de emitirlos.
- `LOG_LEVEL`: Nivel de detalle de los logs (DEBUG, INFO, WARNING, ERROR).
- `POLL_BACKOFF_INITIAL`: Retraso inicial después de un error 429 o fallo de red (por defecto: `1.0`). Respeta el encabezado `Retry-After`.
- `POLL_BACKOFF_MAX`: Retraso máximo de backoff (por defecto: `60.0`).
- `POLL_BACKOFF_FACTOR`: Multiplicador para el backoff exponencial (por defecto: `2.0`).
- `MAX_CONCURRENT_TASKS`: Límite global para la ejecución simultánea de tareas.

## 📜 Licencia

Mozilla Public License v. 2.0.
