FROM python:3.14-slim

# Sin buffer: los logs aparecen en `docker compose logs` al instante.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Primero solo requirements: si no cambian, Docker reusa esta capa
# y no reinstala las dependencias en cada build.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/

EXPOSE 8000

# Forma "exec" (lista JSON): python es el proceso principal del contenedor
# y recibe directamente el SIGTERM de `docker stop`. Con la forma
# "shell" (CMD python -m app.main) la señal le llegaría a /bin/sh, no a python.
CMD ["python", "-m", "app.main"]
