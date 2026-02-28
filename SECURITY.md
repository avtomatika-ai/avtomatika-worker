EN | [ES](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/es/SECURITY.md) | [RU](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/SECURITY.md)

# Security Policy

## Reporting a Vulnerability

If you discover a potential security vulnerability in the Worker SDK, please do not open a public issue. Instead, send an email to [madgagarin@gmail.com].

## Security Model

The Worker SDK is a core component of the Avtomatika ecosystem, designed with security-first principles:

1.  **Mutual TLS (mTLS)**: Supports client certificates for secure, authenticated communication with the Orchestrator.
2.  **Dynamic Token Rotation**: Automatically handles refreshing STS (Security Token Service) access tokens without worker restarts.
3.  **Strict Isolation**: Each task operates in its own temporary directory (`TASK_FILES_DIR`). Data from one task cannot be accessed by another.
4.  **Automatic Cleanup**: Temporary task data is securely wiped immediately after task completion or failure to prevent data lingering.
5.  **Sensitive Data Protection**: S3 credentials and orchestrator tokens are handled exclusively via environment variables or secure configuration objects, never logged.

See the full [HLN Security Model](https://github.com/avtomatika-ai/avtomatika/blob/main/packages/hln/SECURITY.md) for ecosystem-wide details.
