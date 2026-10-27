"""Configuración leída de variables de entorno (se setean en docker-compose.yml)."""

import os

# Cantidad de corrutinas worker que consumen la cola.
NUM_WORKERS = int(os.getenv("NUM_WORKERS", "4"))

# Tamaño máximo de la cola. Si se llena, POST /tareas responde 503
# en vez de acumular memoria sin límite (control de flujo).
TAMANIO_COLA = int(os.getenv("TAMANIO_COLA", "1000"))

# Procesos del ProcessPoolExecutor para tareas CPU-bound.
# Por defecto, uno por núcleo: más procesos que núcleos no calculan más rápido.
NUM_PROCESOS = int(os.getenv("NUM_PROCESOS", str(os.cpu_count() or 2)))

# Timeout por defecto de cada tarea, en segundos.
TIMEOUT_DEFECTO = float(os.getenv("TIMEOUT_DEFECTO", "60"))
