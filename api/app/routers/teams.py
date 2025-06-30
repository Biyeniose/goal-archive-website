from fastapi import APIRouter, HTTPException, Depends, Query
from supabase import Client
from ..dependencies import get_supabase_client
from ..classes.team import TeamService, HighestStats
from ..models.team import TeamInfoResponse, TeamData, TeamSquadDataResponse
from typing import List


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

# GET Highest G/A Per Team and Season
@router.get("/{team_id}/stats", response_model=List[HighestStats])
def get_highest_ga(team_id: int, season: int = Query(2024, description="year"), age: int = Query(50, description="Maximum age"), stat: str = Query("ga", description="Type of Stats"), supabase: Client = Depends(get_supabase_client)):
    service = TeamService(supabase)
    stats = service.most_stats_by_team(team_id=team_id, season=season, stat=stat, age=age)

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats





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


@router.get("/{team_id}/infos", response_model=TeamInfoResponse)
async def get_team(team_id: str, supabase: Client = Depends(get_supabase_client)):
    service = TeamService(supabase)

    try:
        service = TeamService(supabase)
        stats = service.get_team_info(team_id=team_id)

        if not stats:
            raise HTTPException(status_code=404, detail="Stats not found")
        
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    


@router.get("/{team_id}/squad", response_model=TeamSquadDataResponse)
async def get_team(
    team_id: str, 
    season: int = Query(2024, description="year"),  
    supabase: Client = Depends(get_supabase_client)
):
    try:
        service = TeamService(supabase)
        squad_data = service.get_team_squads_per_year(team_id=team_id, season=season)
        
        if not squad_data.data.squad:  # Check if squad list is empty
            raise HTTPException(
                status_code=404,
                detail=f"Team {team_id} has no squad data for season {season}"
            )
        
        return squad_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))