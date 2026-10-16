# One image, two entrypoints. The gateway and the synthetic fleet share a
# dependency set and differ only by which ASGI app they serve, so a second image
# would be duplicated build cost for no isolation benefit -- the fleet is a test
# instrument that only ever runs beside the gateway.
FROM python:3.14-slim

LABEL org.opencontainers.image.title="Switchyard" \
      org.opencontainers.image.description="Multi-tenant LLM gateway: fair scheduling, token budgets, failover" \
      org.opencontainers.image.source="https://github.com/JeyaViknan/Switchyard" \
      org.opencontainers.image.licenses="MIT"

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
# A regular install, not editable: the source is baked in and nothing mounts
# over it, so an editable install would only add a .pth indirection.
RUN pip install --no-cache-dir .

# Default home for the budget state file. A named volume mounts here, so spend
# survives `docker compose restart` and image rebuilds.
RUN mkdir -p /var/lib/switchyard

# The gateway needs no privileges: it binds an unprivileged port and writes
# only its state file.
RUN useradd --system --uid 10001 switchyard \
    && chown -R switchyard:switchyard /var/lib/switchyard
USER switchyard

EXPOSE 8000 8100

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://localhost:8000/health')"

CMD ["uvicorn", "switchyard.gateway.app:create_app", "--factory", \
     "--host", "0.0.0.0", "--port", "8000"]
