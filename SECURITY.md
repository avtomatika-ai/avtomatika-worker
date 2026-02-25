EN | [ES](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/es/SECURITY.md) | [RU](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/SECURITY.md)

# Security Policy

## Reporting a Vulnerability

If you discover a potential security vulnerability in the Worker SDK, please do not open a public issue. Instead, send an email to [madgagarin@gmail.com].

## Security Model

As a fundamental part of the HLN ecosystem, the Worker SDK implements:
- **mTLS Client Support**: Automatic handling of client certificates.
- **Token Rotation**: Built-in logic for refreshing STS tokens.
- **Isolated Workspaces**: Per-task file system isolation to prevent data leakage.

See the full [HLN Security Model](../../packages/hln/SECURITY.md) for more details.
