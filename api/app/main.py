import logging
import time
from typing import Annotated

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from .dependencies import app_logger, db_manager, get_logger
from .routers import leagues, matches, players, predictions, profiles, teams, user_leagues

LoggerDep = Annotated[logging.Logger, Depends(get_logger)]


async def lifespan(app: FastAPI):
    logger = app_logger.get_logger()
    logger.info("Starting up Goal Archive API...")
    try:
        db_manager.init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error("Database initialization failed: %s", str(e))
        raise

    yield

    logger.info("Shutting down Goal Archive API...")
    try:
        db_manager.close_db()
        logger.info("Database closed successfully")
    except Exception as e:
        logger.error("Database shutdown error: %s", str(e))


app = FastAPI(
    title="Goal Archive API",
    description="Sports data API for players, teams, and leagues",
    version="1.0.0",
    lifespan=lifespan,
    redirect_slashes=False,
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    logger = app_logger.get_logger()
    logger.info("-> %s %s", request.method, request.url.path)
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        logger.info(
            "<- %s %s %s %.2fs",
            request.method,
            request.url.path,
            response.status_code,
            process_time,
        )
        return response
    except Exception as e:
        logger.error("Error %s %s: %s", request.method, request.url.path, str(e))
        raise


ALLOWED_ORIGINS = [
    "http://dev.goal-archive.net",
    "https://dev.goal-archive.net",
    "http://100.77.182.222:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "PUT", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)

app.include_router(players.router)
app.include_router(teams.router)
app.include_router(leagues.router)
app.include_router(matches.router)
app.include_router(profiles.router)
app.include_router(predictions.router)
app.include_router(user_leagues.router)


@app.get("/")
@limiter.limit("100/hour")
async def read_root(request: Request):
    return {"message": "Welcome to the Goal Archive API!"}
