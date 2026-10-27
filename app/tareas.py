"""Implementación de cada tipo de tarea y validación de sus parámetros.

Cada ejecutor es una corrutina que recibe los parámetros ya validados y el
ProcessPoolExecutor compartido, y devuelve algo serializable a JSON.
"""

import asyncio
import hashlib
import time
from concurrent.futures import Executor
from typing import Any, Literal

import httpx
from pydantic import BaseModel, Field

from app.modelos import TipoTarea


# ----------------------------------------------------------------------
# Parámetros de cada tipo de tarea
# ----------------------------------------------------------------------

class ParamsEsperar(BaseModel):
    segundos: float = Field(default=1, ge=0, le=3600)


class ParamsDescargar(BaseModel):
    urls: list[str] = Field(min_length=1, max_length=200)
    # "secuencial" existe solo para poder medir la diferencia (medición 1).
    modo: Literal["concurrente", "secuencial"] = "concurrente"
    timeout_url: float = Field(default=10, gt=0)


class ParamsHashear(BaseModel):
    texto: str
    iteraciones: int = Field(default=1_000_000, ge=1, le=200_000_000)
    # "proceso" es el modo correcto. "hilo" y "bloqueante" existen solo
    # para poder medir por qué (medición 2).
    modo: Literal["proceso", "hilo", "bloqueante"] = "proceso"


PARAMETROS: dict[TipoTarea, type[BaseModel]] = {
    TipoTarea.ESPERAR: ParamsEsperar,
    TipoTarea.DESCARGAR: ParamsDescargar,
    TipoTarea.HASHEAR: ParamsHashear,
}


def validar_parametros(tipo: TipoTarea, parametros: dict[str, Any]) -> dict[str, Any]:
    """Valida y completa con valores por defecto. Lanza pydantic.ValidationError."""
    return PARAMETROS[tipo].model_validate(parametros).model_dump()


# ----------------------------------------------------------------------
# esperar: I/O simulado
# ----------------------------------------------------------------------

async def esperar(parametros: dict[str, Any], pool: Executor) -> dict[str, Any]:
    p = ParamsEsperar(**parametros)
    # asyncio.sleep cede el control al event loop: mientras esta tarea
    # "duerme", el loop atiende otras corrutinas. time.sleep congelaría todo.
    await asyncio.sleep(p.segundos)
    return {"durmio": p.segundos}


# ----------------------------------------------------------------------
# descargar: I/O-bound
# ----------------------------------------------------------------------

async def _descargar_una(cliente: httpx.AsyncClient, url: str) -> dict[str, Any]:
    inicio = time.perf_counter()
    try:
        # await: mientras la respuesta viaja por la red, el loop atiende otras cosas.
        respuesta = await cliente.get(url)
        return {
            "url": url,
            "estado": respuesta.status_code,
            "bytes": len(respuesta.content),
            "segundos": round(time.perf_counter() - inicio, 4),
        }
    except httpx.HTTPError as e:
        # Una URL que falla no hace fallar a las demás.
        return {
            "url": url,
            "error": f"{type(e).__name__}: {e}",
            "segundos": round(time.perf_counter() - inicio, 4),
        }


async def descargar(parametros: dict[str, Any], pool: Executor) -> dict[str, Any]:
    p = ParamsDescargar(**parametros)
    inicio = time.perf_counter()
    # async with: el cliente (y sus conexiones) se cierra al salir del bloque,
    # aunque salgamos por una excepción o por una cancelación.
    async with httpx.AsyncClient(timeout=p.timeout_url, follow_redirects=True) as cliente:
        if p.modo == "secuencial":
            # Un for con await: cada descarga empieza cuando terminó la anterior.
            resultados = [await _descargar_una(cliente, url) for url in p.urls]
        else:
            # gather: lanza todas a la vez y espera a que terminen todas.
            resultados = await asyncio.gather(*(_descargar_una(cliente, url) for url in p.urls))
    return {
        "modo": p.modo,
        "total_segundos": round(time.perf_counter() - inicio, 4),
        "urls": resultados,
    }


# ----------------------------------------------------------------------
# hashear: CPU-bound
# ----------------------------------------------------------------------

def _hash_iterado(texto: str, iteraciones: int) -> str:
    """Función normal (no corrutina): es la que corre en el proceso hijo.

    Tiene que estar definida a nivel de módulo para que pickle pueda
    mandarla al otro proceso (una lambda o función anidada no se puede).
    """
    dato = texto.encode()
    for _ in range(iteraciones):
        dato = hashlib.sha256(dato).digest()
    return dato.hex()


async def hashear(parametros: dict[str, Any], pool: Executor) -> dict[str, Any]:
    p = ParamsHashear(**parametros)
    inicio = time.perf_counter()
    if p.modo == "bloqueante":
        # MAL A PROPÓSITO: llamada directa, sin await. Mientras calcula,
        # el event loop no puede atender a nadie. Solo para la medición 2.
        digest = _hash_iterado(p.texto, p.iteraciones)
    else:
        loop = asyncio.get_running_loop()
        # None = el ThreadPoolExecutor por defecto del loop.
        executor = pool if p.modo == "proceso" else None
        # run_in_executor manda la función a otro proceso/hilo y devuelve
        # un future al que le hacemos await: el loop queda libre mientras tanto.
        digest = await loop.run_in_executor(executor, _hash_iterado, p.texto, p.iteraciones)
    return {
        "modo": p.modo,
        "iteraciones": p.iteraciones,
        "hash": digest,
        "segundos": round(time.perf_counter() - inicio, 4),
    }


EJECUTORES = {
    TipoTarea.ESPERAR: esperar,
    TipoTarea.DESCARGAR: descargar,
    TipoTarea.HASHEAR: hashear,
}
