"""Modelos de datos: estados, tipos de tarea, body del POST y la tarea en memoria."""

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class Estado(str, Enum):
    PENDIENTE = "pendiente"
    EJECUTANDO = "ejecutando"
    COMPLETADA = "completada"
    FALLIDA = "fallida"
    CANCELADA = "cancelada"


# Estados de los que una tarea ya no sale.
ESTADOS_FINALES = {Estado.COMPLETADA, Estado.FALLIDA, Estado.CANCELADA}


class TipoTarea(str, Enum):
    DESCARGAR = "descargar"
    HASHEAR = "hashear"
    ESPERAR = "esperar"


class TareaEntrada(BaseModel):
    """Body de POST /tareas. Pydantic valida el JSON antes de que llegue al endpoint."""

    tipo: TipoTarea
    parametros: dict[str, Any] = Field(default_factory=dict)
    timeout: float | None = Field(default=None, gt=0)


@dataclass
class Tarea:
    """Una tarea tal como vive en memoria dentro del gestor."""

    tipo: TipoTarea
    parametros: dict[str, Any]
    timeout: float
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    estado: Estado = Estado.PENDIENTE
    creada: float = field(default_factory=time.time)
    iniciada: float | None = None
    finalizada: float | None = None
    resultado: Any = None
    error: str | None = None
    # La asyncio.Task que la está ejecutando (solo mientras está en EJECUTANDO).
    # No se serializa: es un objeto del event loop, no un dato.
    task: asyncio.Task | None = field(default=None, repr=False)

    @property
    def duracion(self) -> float | None:
        if self.iniciada is None or self.finalizada is None:
            return None
        return round(self.finalizada - self.iniciada, 4)

    def a_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "tipo": self.tipo.value,
            "parametros": self.parametros,
            "timeout": self.timeout,
            "estado": self.estado.value,
            "creada": self.creada,
            "iniciada": self.iniciada,
            "finalizada": self.finalizada,
            "duracion": self.duracion,
            "resultado": self.resultado,
            "error": self.error,
        }
