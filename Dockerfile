# Stage 1: Builder
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create a virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY . .
# Install dependencies first to leverage Docker cache
RUN pip install --no-cache-dir .

# Stage 2: Final Image
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies (curl for healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the virtual environment from the builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy the source code (though it's already in site-packages, 
# keeping it for local development if mounted)
COPY . .

# Create a non-privileged user
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser

# Health check (using the port defined in WorkerConfig)
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8083/health || exit 1

# Default command (can be overridden)

# Since this is an SDK, we assume the user will provide their own script.

# For example: CMD ["worker", "run", "--app", "my_worker:app"]
