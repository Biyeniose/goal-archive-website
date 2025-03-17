# app/dependencies.py
import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables.")


def get_supabase_client() -> Client:
    """
    Dependency to provide a Supabase client.
    """
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return supabase
