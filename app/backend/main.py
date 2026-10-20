from contextlib import asynccontextmanager
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.backend.api.routes import router as api_router
from app.backend.core.config import settings
from app.backend.query_engine.duckdb_client import duckdb_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicialización al arrancar
    logger.info("Iniciando %s v%s en entorno %s", settings.PROJECT_NAME, settings.VERSION, settings.ENVIRONMENT)
    duckdb_engine.refresh_views()
    logger.info("Vistas registradas en DuckDB: %s", sorted(list(duckdb_engine.registered_views)))
    yield
    # Cierre ordenado
    logger.info("Deteniendo servicios de la API analítica")


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description=(
        "API Backend de la Plataforma Extensible de Datos. Provee servicios de consulta "
        "analítica sobre DuckDB (capa Gold) y asistente conversacional con inteligencia artificial."
    ),
    lifespan=lifespan,
)

# Configuración de CORS para permitir la conexión desde Streamlit o interfaces frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Registro de rutas bajo el prefijo /api/v1
app.include_router(api_router, prefix="/api/v1")


@app.get("/", tags=["Root"])
def root():
    return {
        "project": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "docs": "/docs",
        "health": "/api/v1/health",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.backend.main:app", host=settings.HOST, port=settings.PORT, reload=True)
