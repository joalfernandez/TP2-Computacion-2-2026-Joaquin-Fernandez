"""El corazón del sistema: la cola, el pool de workers y el registro de tareas.

Patrón productor-consumidor: los endpoints producen (encolan ids) y los
workers consumen. Todo corre en el mismo hilo, dentro del event loop de
uvicorn, así que no hace falta Lock: entre dos `await` nadie puede
interrumpir a una corrutina.
"""

import asyncio
import logging
import time
from concurrent.futures import Executor
from typing import Any

from app.modelos import ESTADOS_FINALES, Estado, Tarea, TipoTarea
from app.tareas import EJECUTORES

log = logging.getLogger("gestor")


class ColaLlena(Exception):
    """La cola alcanzó su maxsize: el cliente tiene que reintentar más tarde."""


class Gestor:
    def __init__(self, num_workers: int, tamanio_cola: int, pool: Executor):
        self.num_workers = num_workers
        # Pool de procesos para tareas CPU-bound. Se crea una sola vez
        # (crear procesos es caro) y lo comparten todos los workers.
        self.pool = pool
        self.tareas: dict[str, Tarea] = {}
        # La cola guarda solo ids; la tarea completa vive en self.tareas.
        self.cola: asyncio.Queue[str] = asyncio.Queue(maxsize=tamanio_cola)
        self.workers: list[asyncio.Task] = []
        self.ocupados = 0

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    async def iniciar(self) -> None:
        for n in range(self.num_workers):
            self.workers.append(asyncio.create_task(self._worker(n), name=f"worker-{n}"))
        log.info("Pool iniciado con %d workers", self.num_workers)

    async def detener(self) -> None:
        for w in self.workers:
            w.cancel()
        # return_exceptions=True: que el CancelledError de cada worker
        # no corte el gather antes de que terminen los demás.
        await asyncio.gather(*self.workers, return_exceptions=True)
        log.info("Pool detenido")

    # ------------------------------------------------------------------
    # Operaciones que usan los endpoints
    # ------------------------------------------------------------------

    def encolar(self, tipo: TipoTarea, parametros: dict[str, Any], timeout: float) -> Tarea:
        tarea = Tarea(tipo=tipo, parametros=parametros, timeout=timeout)
        try:
            # put_nowait no espera: si la cola está llena, falla al instante.
            # Así el POST nunca se queda colgado.
            self.cola.put_nowait(tarea.id)
        except asyncio.QueueFull:
            raise ColaLlena() from None
        self.tareas[tarea.id] = tarea
        return tarea

    def obtener(self, tarea_id: str) -> Tarea | None:
        return self.tareas.get(tarea_id)

    def listar(self, estado: Estado | None = None) -> list[Tarea]:
        return [t for t in self.tareas.values() if estado is None or t.estado == estado]

    def cancelar(self, tarea: Tarea) -> bool:
        """Devuelve False si la tarea ya había terminado."""
        if tarea.estado in ESTADOS_FINALES:
            return False
        if tarea.estado == Estado.PENDIENTE:
            # Sigue en la cola: la marcamos y el worker la va a saltear
            # cuando la saque (asyncio.Queue no permite borrar del medio).
            tarea.estado = Estado.CANCELADA
            tarea.finalizada = time.time()
            return True
        # EJECUTANDO: se implementa en la etapa 3.
        return False

    def estadisticas(self) -> dict[str, Any]:
        por_estado = {e.value: 0 for e in Estado}
        for t in self.tareas.values():
            por_estado[t.estado.value] += 1

        duraciones = [t.duracion for t in self.tareas.values()
                      if t.estado == Estado.COMPLETADA and t.duracion is not None]
        promedio = round(sum(duraciones) / len(duraciones), 4) if duraciones else None

        return {
            "total": len(self.tareas),
            "por_estado": por_estado,
            "duracion_promedio": promedio,
            "workers_total": self.num_workers,
            "workers_ocupados": self.ocupados,
            "en_cola": self.cola.qsize(),
        }

    # ------------------------------------------------------------------
    # Worker
    # ------------------------------------------------------------------

    async def _worker(self, n: int) -> None:
        while True:
            tarea_id = await self.cola.get()
            try:
                tarea = self.tareas[tarea_id]
                if tarea.estado == Estado.CANCELADA:
                    continue  # se canceló mientras esperaba en la cola
                await self._ejecutar(tarea)
            finally:
                self.cola.task_done()

    async def _ejecutar(self, tarea: Tarea) -> None:
        tarea.estado = Estado.EJECUTANDO
        tarea.iniciada = time.time()
        self.ocupados += 1
        try:
            tarea.resultado = await EJECUTORES[tarea.tipo](tarea.parametros, self.pool)
            tarea.estado = Estado.COMPLETADA
        except Exception as e:
            # Capturamos todo para que una tarea que falla NO mate al worker.
            # Ojo: CancelledError no hereda de Exception, así que no cae acá.
            tarea.estado = Estado.FALLIDA
            tarea.error = f"{type(e).__name__}: {e}"
            log.warning("Tarea %s falló: %s", tarea.id, tarea.error)
        finally:
            tarea.finalizada = time.time()
            self.ocupados -= 1
