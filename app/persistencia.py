"""Volcado del estado a JSON al apagar, y reporte del volcado previo al arrancar."""

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

log = logging.getLogger("persistencia")


def reportar_volcado_previo(directorio: Path) -> None:
    volcados = sorted(directorio.glob("estado_*.json"))
    if not volcados:
        log.info("No hay volcados previos en %s", directorio)
        return
    ultimo = volcados[-1]
    try:
        datos = json.loads(ultimo.read_text())
        log.info("Volcado previo encontrado: %s (%d tareas, por estado: %s)",
                 ultimo.name, len(datos["tareas"]), datos["estadisticas"]["por_estado"])
    except (OSError, ValueError, KeyError) as e:
        log.warning("Volcado previo %s ilegible: %s", ultimo.name, e)


def _escribir(ruta: Path, datos: dict[str, Any]) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_text(json.dumps(datos, indent=2, ensure_ascii=False))


async def volcar(directorio: Path, estadisticas: dict[str, Any], tareas: list[dict[str, Any]]) -> Path:
    ruta = directorio / f"estado_{time.strftime('%Y%m%d_%H%M%S')}.json"
    datos = {"volcado": time.time(), "estadisticas": estadisticas, "tareas": tareas}
    # Escribir un archivo es I/O bloqueante: lo mandamos a un hilo para no
    # frenar el event loop (asyncio no tiene archivos asíncronos nativos).
    await asyncio.to_thread(_escribir, ruta, datos)
    return ruta
