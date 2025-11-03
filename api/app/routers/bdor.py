from fastapi import APIRouter, HTTPException, Depends
#from supabase import Client
#from ..dependencies import get_supabase_client

router = APIRouter(
    prefix="/v1/bdor",
    tags=["bdor"],
)

@router.get("/{year}")
async def get_rankings(year: int):
    try:
        x=1
        
    except Exception as e:
        return {"error": str(e)}