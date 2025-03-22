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
        response = supabase.table("teams").select("team_id, team_name, team_name2, logo_url, league_id").eq("team_id", team_id).execute()

        # Check if there are any results
        if response.data:
            #print(response.data)
            return {"data": response.data[0]}
        else:
            return {"message": "No results found for this team"}
    except Exception as e:
        # Handle errors
        return {"error": str(e)}
    except Exception as e:
        # Handle errors
        return {"error": str(e)}
    
# Get most G/A in a team 
@router.get("/{team_id}/most_ga")
async def get_team(team_id: str, supabase: Client = Depends(get_supabase_client)):
    """
    Fetch the top 5 players with the highest curr_ga for a given team_id.
    """
    try:
        # Call the SQL function to fetch player and team details
        response = supabase.rpc(
            "get_top_players_by_ga",
            {"input_team_id": team_id}  # Pass the player_id as BIGINT
        ).execute()

        player = response.data if response.data else None

        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        return {"data": player}

    except HTTPException as e:
        raise e
    except Exception as e:
        return {"error": str(e)}
    
# Get most assists in a team 
@router.get("/{team_id}/most_goals")
async def get_team(team_id: str, supabase: Client = Depends(get_supabase_client)):
    """
    Fetch the top 5 players with the highest curr_ga for a given team_id.
    """
    try:
        # Call the SQL function to fetch player and team details
        response = supabase.rpc(
            "get_top_players_by_goals",
            {"input_team_id": team_id}  # Pass the player_id as BIGINT
        ).execute()

        player = response.data if response.data else None

        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        return {"data": player}

    except HTTPException as e:
        raise e
    except Exception as e:
        return {"error": str(e)}

# Get most min in a team 
@router.get("/{team_id}/most_min")
async def get_team(team_id: str, supabase: Client = Depends(get_supabase_client)):
    try:
        # Call the SQL function to fetch player and team details
        response = supabase.rpc(
            "get_top_players_by_minutes",
            {"input_team_id": team_id}  # Pass the player_id as BIGINT
        ).execute()

        player = response.data if response.data else None

        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        return {"data": player}

    except HTTPException as e:
        raise e
    except Exception as e:
        return {"error": str(e)}
    
@router.get("/{team_id}/top_nations")
async def get_team(team_id: str, supabase: Client = Depends(get_supabase_client)):
    try:
        # Call the SQL function to fetch player and team details
        response = supabase.rpc(
            "get_top_nations_by_team",
            {"input_team_id": team_id}  # Pass the player_id as BIGINT
        ).execute()

        player = response.data if response.data else None

        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        return {"data": player}

    except HTTPException as e:
        raise e
    except Exception as e:
        return {"error": str(e)}

# GET All Players in a Team
@router.get("/{team_id}/players")
async def get_players_by_team_id(team_id: int, supabase: Client = Depends(get_supabase_client)):
    try:
        # Call the SQL function to fetch player and team details
        response = supabase.rpc(
            "get_players_by_team",
            {"input_team_id": team_id}  # Pass the player_id as BIGINT
        ).execute()

        players = response.data if response.data else None

        if not players:
            raise HTTPException(status_code=404, detail="Player not found")

        return {"data": players}

    except HTTPException as e:
        raise e
    except Exception as e:
        return {"error": str(e)}
    
