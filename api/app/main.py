from fastapi import FastAPI
from supabase import create_client, Client
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Get Supabase credentials from environment variables
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

if not url or not key:
    raise ValueError("SUPABASE_URL or SUPABASE_KEY are not set")

# Initialize Supabase client
supabase: Client = create_client(url, key)

@app.get("/")
def read_root():
    return {"message": "Welcome to the FastAPI app with Supabase!"}

@app.get("/fetch")
def fetch_data():
    """
    Example route to fetch data from a Supabase table.
    Replace 'your_table_name' with the actual table name.
    """
    try:
        response = supabase.table("leagues").select("*").execute()
        return {"data": response.data}
    except Exception as e:
        return {"error": str(e)}

@app.get("/bdor")
def get_ballon_dor_winners():
    """
    Route to fetch all rows from the ballon_dor table where year = 2024.
    """
    try:
        # Query the ballon_dor table for rows where year = 2024
        response = supabase.table("ballon_dor").select("*").eq("year", 2024).execute()

        # Check if there are any results
        if response.data:
            return {"data": response.data}
        else:
            return {"message": "No results found for the year 2024."}
    except Exception as e:
        # Handle errors
        return {"error": str(e)}
    
@app.get("/bdor/{year}")
def get_ballon_dor_year(year: int):
    try:
        response = supabase.table("ballon_dor").select("*").eq("year", year).execute()

        # Check if there are any results
        if response.data:
            return {"data": response.data}
        else:
            return {"message": f"No results found for the year {year}."}
    except Exception as e:
        # Handle errors
        return {"error": str(e)}