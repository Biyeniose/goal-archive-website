#from fastapi import FastAPI, Query, Path, Depends
#from supabase import create_client, Client
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routers import teams, leagues, bdor, players, stats, matches
#from .dependencies import get_supabase_client
import os

load_dotenv() # Load environment variables from .env file
app = FastAPI() # Initialize FastAPI app

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    #allow_origins=["http://localhost:3000"],  # Allow Next.js origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Get Supabase credentials from environment variables
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

if not url or not key:
    raise ValueError("SUPABASE_URL or SUPABASE_KEY are not set")

# Initialize Supabase client

app.include_router(teams.router)
app.include_router(leagues.router)
app.include_router(bdor.router)
app.include_router(players.router)
app.include_router(stats.router)
app.include_router(matches.router)

@app.get("/")
async def read_root():
    return {"message": "Welcome to the FastAPI app with Supabase!"}
