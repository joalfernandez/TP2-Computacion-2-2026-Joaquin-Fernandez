"""Implementación de cada tipo de tarea.

Cada función es una corrutina que recibe los parámetros de la tarea y
devuelve el resultado (algo serializable a JSON).
"""

import asyncio
from typing import Any

from app.modelos import TipoTarea


async def esperar(parametros: dict[str, Any]) -> dict[str, Any]:
    segundos = float(parametros.get("segundos", 1))
    # asyncio.sleep cede el control al event loop: mientras esta tarea
    # "duerme", el loop atiende otras corrutinas. time.sleep congelaría todo.
    await asyncio.sleep(segundos)
    return {"durmio": segundos}


async def descargar(parametros: dict[str, Any]) -> dict[str, Any]:
    raise NotImplementedError("descargar todavía no está implementada")


async def hashear(parametros: dict[str, Any]) -> dict[str, Any]:
    raise NotImplementedError("hashear todavía no está implementada")


EJECUTORES = {
    TipoTarea.ESPERAR: esperar,
    TipoTarea.DESCARGAR: descargar,
    TipoTarea.HASHEAR: hashear,
}
