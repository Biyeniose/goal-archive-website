import os
from sqlmodel import Session, create_engine
from sqlalchemy.engine import Engine
from supabase import create_client, Client
from dotenv import load_dotenv
from typing import Optional

load_dotenv()


class SupabaseDatabaseManager:
    """Manages database connection pool — Supabase in prod, local PG in dev"""

    def __init__(self):
        self._engine: Optional[Engine] = None
        self._client: Optional[Client] = None

    def init_db(self):
        if self._engine is not None:
            print("⚠️ Database already initialized - skipping", flush=True)
            return

        app_env = os.environ.get("APP_ENV", "dev")
        if app_env == "prod":
            db_url = os.environ.get("SUPABASE_DB_URL")
            if not db_url:
                raise ValueError("SUPABASE_DB_URL environment variable not set")
            print("🔌 Connecting to Supabase (prod)", flush=True)
        else:
            db_url = os.environ.get("DEV_DB_URL")
            if not db_url:
                raise ValueError("DEV_DB_URL environment variable not set")
            print("🔌 Connecting to dev database", flush=True)

        self._engine = create_engine(
            db_url,
            echo=False,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        print("✅ Database connection pool initialized", flush=True)

        supabase_api_url = os.environ.get("SUPABASE_API_URL")
        service_role_key = os.environ.get("SERVICE_ROLE_KEY")
        if supabase_api_url and service_role_key:
            self._client = create_client(supabase_api_url, service_role_key)
            print("✅ Supabase client initialized", flush=True)
        else:
            print("⚠️ Supabase client skipped (SUPABASE_API_URL or SERVICE_ROLE_KEY not set)", flush=True)

    def close_db(self):
        if self._engine:
            self._engine.dispose()
            self._engine = None
            print("✅ Database connection pool closed")

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            raise RuntimeError("Database not initialized. Call init_db() first.")
        return self._engine

    @property
    def client(self) -> Client:
        if self._client is None:
            raise RuntimeError("Supabase client not initialized. Set SUPABASE_API_URL and SERVICE_ROLE_KEY.")
        return self._client
