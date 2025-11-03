from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routers import stats
#from .routers import teams, leagues, bdor, players, stats, matches
from .dependencies import db_manager

@asynccontextmanager
async def lifespan(app: FastAPI): 
    # Startup
    print("🚀 Starting up...", flush=True)
    db_manager.init_db()
    print("✅ After init_db call", flush=True)
    yield
    # Shutdown
    print("🛑 Shutting down...", flush=True)
    db_manager.close_db()

app = FastAPI(
    title="Sports Data API",
    description="API for sports data",
    version="1.0.0",
    lifespan=lifespan,  # ✅ Use lifespan
    redirect_slashes=False
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

"""
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://your-nextjs-domain.com",
        "https://amendments-urls-dir-know.trycloudflare.com"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
"""

#app.include_router(teams.router)
#app.include_router(leagues.router)
#app.include_router(bdor.router)
#app.include_router(players.router)
app.include_router(stats.router)
#app.include_router(matches.router)

@app.get("/")
async def read_root():
    return {"message": "Welcome to the Goal Archive API!"}
