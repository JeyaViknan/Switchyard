# One image, two entrypoints. The gateway and the synthetic fleet share a
# dependency set and differ only by which ASGI app they serve, so a second image
# would be duplicated build cost for no isolation benefit -- the fleet is a test
# instrument that only ever runs beside the gateway.
FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e .

EXPOSE 8000 8100
CMD ["uvicorn", "switchyard.gateway.app:create_app", "--factory", \
     "--host", "0.0.0.0", "--port", "8000"]
