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
- **Optimización de Tráfico:**
  - **Habilidades en 3 Niveles:** *Supported* (catálogo), *Available* (límites dinámicos) y *Hot* (en caché).
  - **Stable Hashing:** Envía el catálogo completo solo cuando cambia, usando `skills_hash` para latidos ligeros.
- **S3 Streaming:** Transferencia de datos de alto rendimiento usando `obstore`. Sin errores de OOM en archivos grandes.
- **Hardware Awareness:** Monitoreo integrado de CPU, RAM y GPUs NVIDIA (vía `psutil` y `GPUtil`).

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
from avtomatika_worker import Worker, TaskFiles

worker = Worker()

@worker.skill("hola_mundo")
async def mi_habilidad(params: dict, files: TaskFiles):
    """Habilidad simple que dice hola."""
    return {"message": f"¡Hola, {params.get('nombre', 'Mundo')}!"}

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
- `STRICT_EVENT_VALIDATION`: (Por defecto: `True`) Valida eventos contra esquemas antes de emitirlos.
- `LOG_LEVEL`: Nivel de detalle de los logs (DEBUG, INFO, WARNING, ERROR).
- `POLL_BACKOFF_INITIAL`: Retraso inicial después de un error 429 o fallo de red (por defecto: `1.0`). Respeta el encabezado `Retry-After`.
- `POLL_BACKOFF_MAX`: Retraso máximo de backoff (por defecto: `60.0`).
- `POLL_BACKOFF_FACTOR`: Multiplicador para el backoff exponencial (por defecto: `2.0`).
- `MAX_CONCURRENT_TASKS`: Límite global para la ejecución simultánea de tareas.

## 📜 Licencia

Mozilla Public License v. 2.0.
