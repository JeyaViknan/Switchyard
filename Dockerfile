# One image, three entrypoints. The gateway, the synthetic fleet and the
# console share a dependency set and differ only by which ASGI app they serve,
# so separate images would be duplicated build cost for no isolation benefit --
# the fleet and console only ever run beside the gateway.
#
# Pinned by digest, not by tag: `python:3.14-slim` moves, and a base image that
# changes underneath you turns a reproducible build into a coincidence. This is
# the multi-arch index digest, so linux/amd64 (the EC2 host) and linux/arm64
# (an Apple Silicon laptop) both still resolve from it.
FROM python:3.14-slim@sha256:35caba83af2fe6da9a5d922f9b0156f21d1246a48278c58a08bd253303ba42e8

# Set by CI from the commit being built. Outside the pipeline these stay at
# their defaults, which is how /readyz can say "dev" honestly rather than
# reporting a sha nobody can look up.
ARG GIT_SHA=dev
ARG VERSION=0.0.0

LABEL org.opencontainers.image.title="Switchyard" \
      org.opencontainers.image.description="Multi-tenant LLM gateway: fair scheduling, token budgets, failover" \
      org.opencontainers.image.source="https://github.com/JeyaViknan/Switchyard" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.revision="$GIT_SHA" \
      org.opencontainers.image.version="$VERSION"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SWITCHYARD_GIT_SHA=$GIT_SHA

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
# A regular install, not editable: the source is baked in and nothing mounts
# over it, so an editable install would only add a .pth indirection.
RUN pip install --no-cache-dir .

# Default home for the budget state file. A volume mounts here, so spend
# survives `docker compose restart` and image rebuilds.
RUN mkdir -p /var/lib/switchyard

# The gateway needs no privileges: it binds an unprivileged port and writes
# only its state file.
RUN useradd --system --uid 10001 switchyard \
    && chown -R switchyard:switchyard /var/lib/switchyard
USER switchyard

EXPOSE 8000 8100 8080

# Liveness, not readiness. Docker restarts an unhealthy container, and while
# the gateway drains it is deliberately refusing new work -- restarting it then
# would kill the in-flight requests the drain exists to finish. Compose uses
# /readyz for `depends_on`, which is the opposite question and wants the
# opposite answer.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://localhost:8000/livez')"

CMD ["uvicorn", "switchyard.gateway.app:create_app", "--factory", \
     "--host", "0.0.0.0", "--port", "8000"]
