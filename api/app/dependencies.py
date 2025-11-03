# app/dependencies.py
import os
from sqlmodel import Session, create_engine
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
from fastapi import Depends
from typing import Annotated, Optional

load_dotenv()

DATABASE_URL = os.environ.get("NEON_DB_URL")
DEV_DATABASE_URL = os.environ.get("DEV_DB_URL")

if not DEV_DATABASE_URL:
    raise ValueError("NEON_DB_URL environment variable not set")

class DatabaseManager:
    """Manages database connection pool lifecycle"""
    def __init__(self):
        self._engine: Optional[Engine] = None
    
    def init_db(self):
        """Initialize database connection pool"""
        if self._engine is not None:
            print("⚠️ Database already initialized - skipping", flush=True)
            return
        
        self._engine = create_engine(
            #DATABASE_URL,
            DEV_DATABASE_URL,
            echo=False,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        print("✅ Database connection pool initialized", flush=True)
    
    def close_db(self):
        """Close database connection pool"""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            print("✅ Database connection pool closed")
    
    @property
    def engine(self) -> Engine:
        """Get the database engine"""
        if self._engine is None:
            raise RuntimeError("Database not initialized. Call init_db() first.")
        return self._engine

# Create a single instance
db_manager = DatabaseManager()

def get_db_session():
    """Database session dependency"""
    with Session(db_manager.engine) as session:
        yield session

# Create a reusable type annotation for the session
DBSession = Annotated[Session, Depends(get_db_session)]

