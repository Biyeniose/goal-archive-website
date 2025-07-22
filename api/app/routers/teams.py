from fastapi import APIRouter, HTTPException, Depends, Query
from supabase import Client
from ..dependencies import get_supabase_client
from ..classes.team import TeamService, TeamPlayersStatsResponse
from app.models.response import TeamInfoResponse, TeamData, TeamSquadDataResponse, LeagueMatchesResponse
from app.constants import GLOBAL_YEAR
from typing import List


router = APIRouter(
    prefix="/v1/teams",
    tags=["teams"],
    dependencies=[Depends(get_supabase_client)],
    responses={404: {"description": "Not found"}},
)


@router.get("/{team_id}/infos", response_model=TeamInfoResponse)
async def get_team(team_id: str, supabase: Client = Depends(get_supabase_client)):
    try:
        service = TeamService(supabase)
        stats = service.get_team_info(team_id=team_id)

        if not stats:
            raise HTTPException(status_code=404, detail="Stats not found")
        
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# GET team squad per year 
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


# GET all matches in a comp in a year
@router.get("/{team_id}/matches", response_model=LeagueMatchesResponse)
def get_matches(team_id: int, season: int = Query(GLOBAL_YEAR, description="year"), supabase: Client = Depends(get_supabase_client)):
    service = TeamService(supabase)
    stats = service.get_team_matches_by_year(team_id=team_id, season=season)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")
    return stats

# GET all transfers in a year and total incoming/outgoing fees




