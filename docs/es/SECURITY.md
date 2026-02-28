[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/SECURITY.md) | ES | [RU](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/SECURITY.md)

# Política de Seguridad

## Informar una Vulnerabilidad

Si descubre una vulnerabilidad de seguridad potencial en el Worker SDK, por favor no abra un Issue público. En su lugar, envíe un correo electrónico a [madgagarin@gmail.com].

## Modelo de Seguridad

El SDK del Worker es un componente central del ecosistema de Avtomatika, diseñado con principios de seguridad primero:

1.  **TLS Mutuo (mTLS)**: Admite certificados de cliente para una comunicación segura y autenticada con el Orquestador.
2.  **Rotación Dinámica de Tokens**: Maneja automáticamente la actualización de los tokens de acceso STS sin necesidad de reiniciar el worker.
3.  **Aislamiento Estricto**: Cada tarea opera en su propio directorio temporal (`TASK_FILES_DIR`). Los datos de una tarea no pueden ser accedidos por otra.
4.  **Limpieza Automática**: Los datos temporales de la tarea se eliminan de forma segura inmediatamente después de la finalización o fallo de la tarea para evitar la permanencia de datos.
5.  **Protección de Datos Sensibles**: Las credenciales de S3 y los tokens del orquestador se manejan exclusivamente a través de variables de entorno u objetos de configuración seguros, nunca se registran en los logs.

Consulte el [Modelo de Seguridad HLN](https://github.com/avtomatika-ai/avtomatika/blob/main/packages/hln/SECURITY.md) completo para obtener detalles sobre todo el ecosistema.
