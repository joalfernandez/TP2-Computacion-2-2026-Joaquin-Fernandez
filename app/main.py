"""API HTTP. Uvicorn es el dueño del event loop; FastAPI solo registra rutas."""

import logging
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, Request, status
from pydantic import ValidationError

from app import config
from app.gestor import ColaLlena, Gestor
from app.modelos import Estado, TareaEntrada
from app.tareas import validar_parametros

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Antes del yield: arranque. Ya estamos dentro del loop de uvicorn,
    # así que se pueden crear tasks.
    pool = ProcessPoolExecutor(max_workers=config.NUM_PROCESOS)
    gestor = Gestor(config.NUM_WORKERS, config.TAMANIO_COLA, pool)
    await gestor.iniciar()
    app.state.gestor = gestor
    yield
    # Después del yield: apagado. Uvicorn llega acá cuando recibe SIGINT/SIGTERM.
    await gestor.detener()
    # cancel_futures: descarta los cálculos que todavía no arrancaron.
    pool.shutdown(wait=True, cancel_futures=True)


app = FastAPI(title="Plataforma de Tareas Distribuidas", lifespan=lifespan)


def _gestor(request: Request) -> Gestor:
    return request.app.state.gestor


@app.post("/tareas", status_code=status.HTTP_202_ACCEPTED)
async def crear_tarea(entrada: TareaEntrada, request: Request):
    try:
        parametros = validar_parametros(entrada.tipo, entrada.parametros)
    except ValidationError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT,
                            e.errors(include_url=False, include_context=False))
    timeout = entrada.timeout or config.TIMEOUT_DEFECTO
    try:
        tarea = _gestor(request).encolar(entrada.tipo, parametros, timeout)
    except ColaLlena:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Cola llena, reintentar más tarde")
    # 202 Accepted: "lo recibí, todavía no lo hice". Respondemos sin esperar la tarea.
    return {"id": tarea.id, "estado": tarea.estado.value}


@app.get("/tareas")
async def listar_tareas(request: Request, estado: Estado | None = None):
    return [t.a_dict() for t in _gestor(request).listar(estado)]


@app.get("/tareas/{tarea_id}")
async def obtener_tarea(tarea_id: str, request: Request):
    tarea = _gestor(request).obtener(tarea_id)
    if tarea is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Tarea inexistente")
    return tarea.a_dict()


@app.delete("/tareas/{tarea_id}")
async def cancelar_tarea(tarea_id: str, request: Request):
    gestor = _gestor(request)
    tarea = gestor.obtener(tarea_id)
    if tarea is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Tarea inexistente")
    if not gestor.cancelar(tarea):
        raise HTTPException(status.HTTP_409_CONFLICT, f"No se puede cancelar una tarea {tarea.estado.value}")
    return {"id": tarea.id, "estado": tarea.estado.value}


@app.get("/estadisticas")
async def estadisticas(request: Request):
    return _gestor(request).estadisticas()


# Guarda obligatoria en macOS/Windows: ProcessPoolExecutor usa "spawn" y
# re-importa este módulo en cada proceso hijo. Sin el if, cada hijo
# levantaría otro servidor.
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
