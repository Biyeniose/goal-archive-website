from contextlib import asynccontextmanager
import time
from typing import Annotated
from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from .routers import stats
#from .routers import teams, leagues, bdor, players, stats, matches
from .dependencies import db_manager, app_logger, get_logger
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.middleware import SlowAPIMiddleware
import logging

# Type annotation for logger dependency
LoggerDep = Annotated[logging.Logger, Depends(get_logger)]

async def lifespan(app: FastAPI): 
    # Get logger instance
    logger = app_logger.get_logger()
    
    # Startup
    logger.info("🚀 Starting up Sports Data API...")
    try:
        db_manager.init_db()
        logger.info("✅ Database initialized successfully")
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {str(e)}")
        raise
    
    yield
    
    # Shutdown
    logger.info("🛑 Shutting down Sports Data API...")
    try:
        db_manager.close_db()
        logger.info("✅ Database closed successfully")
    except Exception as e:
        logger.error(f"❌ Database shutdown error: {str(e)}")

# set up FastAPI instance
app = FastAPI(
    title="Sports Data API",
    description="API for sports data",
    version="1.0.0",
    lifespan=lifespan,  # ✅ Use lifespan
    redirect_slashes=False
)

# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()

    logger = app_logger.get_logger()
    logger.info(f"📥 {request.method} {request.url.path} - Client: {request.client.host}")
    
    try:
        response = await call_next(request)
        process_time = time.time() - start_time

        logger.info(f"📤 {request.method} {request.url.path} - Status: {response.status_code} - Time: {process_time:.2f}s")
        return response
    except Exception as e:
        logger.error(f"💥 {request.method} {request.url.path} - Error: {str(e)}")
        raise

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development
    allow_credentials=True,
    allow_methods=["GET", "POST"],  # Specific methods
    allow_headers=["Authorization", "Content-Type"],
)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)

app.include_router(stats.router)
#app.include_router(matches.router)
#app.include_router(teams.router)
#app.include_router(leagues.router)
#app.include_router(bdor.router)
#app.include_router(players.router)

@app.get("/")
@limiter.limit("100/hour")
async def read_root(request: Request, logger: LoggerDep):
    """
    Base route
    """
    logger.info("Root endpoint accessed")
    return {"message": "Welcome to the Goal Archive API!"}
