from fastapi import APIRouter, HTTPException, Depends
from supabase import Client
from ..dependencies import get_supabase_client

router = APIRouter(
    prefix="/v1/bdor",
    tags=["bdor"],
    dependencies=[Depends(get_supabase_client)],
    responses={404: {"description": "Not found"}},
)

@router.get("/{year}")
async def get_rankings(year: int, supabase: Client = Depends(get_supabase_client)):
    try:
        response = supabase.rpc("get_ballon_dor_with_logos", {"input_year": year}).execute()

        if response.data:
            return {"data": response.data}
        else:
            return {"message": f"No results found for the year {year}."}

    except Exception as e:
        return {"error": str(e)}