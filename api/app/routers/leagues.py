from fastapi import APIRouter, HTTPException, Depends
from supabase import Client
from ..dependencies import get_supabase_client

router = APIRouter(
    prefix="/v1/leagues",
    tags=["leagues"],
    dependencies=[Depends(get_supabase_client)],
    responses={404: {"description": "Not found"}},
)

@router.get("/{league_id}/rank")
async def get_teams(league_id: str, supabase: Client = Depends(get_supabase_client)):
    try:
        response = supabase.table("teams").select("curr_league_rank, curr_league_points, team_name, team_id, logo_url, league_id").eq("league_id", league_id).order("curr_league_rank", desc=False).execute()

        # Check if there are any results
        if response.data:
            #print(response.data)
            return {"data": response.data}
        else:
            return {"message": f"No results found for league {league_id}"}
    except Exception as e:
        # Handle errors
        return {"error": str(e)}
    except Exception as e:
        # Handle errors
        return {"error": str(e)}