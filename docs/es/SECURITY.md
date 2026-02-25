[EN](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/SECURITY.md) | ES | [RU](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/SECURITY.md)

# Política de Seguridad

## Informar una Vulnerabilidad

Si descubre una vulnerabilidad de seguridad potencial en el Worker SDK, por favor no abra un Issue público. En su lugar, envíe un correo electrónico a [madgagarin@gmail.com].

## Modelo de Seguridad

Como parte fundamental del ecosistema HLN, el Worker SDK implementa:
- **Soporte de Cliente mTLS**: Manejo automático de certificados de cliente.
- **Rotación de Tokens**: Lógica integrada para refrescar tokens STS.
- **Espacios de Trabajo Aislados**: Aislamiento del sistema de archivos por tarea para prevenir fugas de datos.

Consulte el [Modelo de Seguridad HLN](../../packages/hln/SECURITY.md) completo para obtener más detalles.
