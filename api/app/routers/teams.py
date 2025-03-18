from fastapi import APIRouter, HTTPException, Depends
from supabase import Client
from ..dependencies import get_supabase_client

router = APIRouter(
    prefix="/v1/teams",
    tags=["teams"],
    dependencies=[Depends(get_supabase_client)],
    responses={404: {"description": "Not found"}},
)

# get team details of a team
@router.get("/{team_id}")
async def get_team(team_id: str, supabase: Client = Depends(get_supabase_client)):
    try:
        response = supabase.table("teams").select("team_id, team_name, logo_url, league_id").eq("team_id", team_id).execute()

        # Check if there are any results
        if response.data:
            #print(response.data)
            return {"data": response.data}
        else:
            return {"message": "No results found for this team"}
    except Exception as e:
        # Handle errors
        return {"error": str(e)}
    except Exception as e:
        # Handle errors
        return {"error": str(e)}
    
# Get most G/A in a team with max age as parameter
@router.get("/{team_id}/most_ga")
async def get_team(team_id: str, supabase: Client = Depends(get_supabase_client)):
    """
    Fetch the top 5 players with the highest curr_ga for a given team_id.
    """
    try:
        # Step 1: Fetch top 5 players for the given team_id, sorted by curr_ga (descending)
        players_response = supabase.table("players").select(
            "player_id, player_name, age, curr_gp, curr_ga, curr_goals, curr_assists, position, nation1, nation2"
        ).eq("curr_team_id", team_id).not_.is_("curr_ga", None).order("curr_ga", desc=True).limit(5).execute()

        # Step 2: Check if there are any results
        if players_response.data:
            return {"data": players_response.data}
        else:
            return {"message": f"No players found for team_id: {team_id}"}

    except Exception as e:
        return {"error": str(e)}

# GET All Players in a Team
@router.get("/{team_id}/players")
async def get_players_by_team_id(team_id: int, supabase: Client = Depends(get_supabase_client)):
    """
    fetch all players from by team_id
    """
    try:
        response = supabase.table("players").select("player_id, player_name, curr_team_id, age, position, nation1, nation2, market_value, curr_ga, curr_goals, curr_assists, curr_gp").eq("curr_team_id", team_id).order("market_value", desc=True).execute()

        # Check if there are any results
        if response.data:
            #print(response.data)
            return {"data": response.data}
        else:
            return {"message": "No results found for this league"}
    except Exception as e:
        # Handle errors
        return {"error": str(e)}
    
