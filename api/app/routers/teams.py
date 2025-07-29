from fastapi import APIRouter, HTTPException, Depends, Query
from supabase import Client
from datetime import date
from ..dependencies import get_supabase_client
from ..classes.team import TeamService, TeamPlayersStatsResponse
from app.models.response import TeamInfoResponse, TeamData, TeamSquadDataResponse, LeagueMatchesResponse, TeamTransfersResponse, TeamSeasonResponse, DomesticSeasonsResponse
from app.constants import GLOBAL_YEAR
from typing import List


router = APIRouter(
    prefix="/v1/teams",
    tags=["teams"],
    dependencies=[Depends(get_supabase_client)],
    responses={404: {"description": "Not found"}},
)

# team page route
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
    

# GET team squad per year (maybe include only people that in the subs)

# GET most expensive outgoing and incoming transfers

# GET all matches in a comp in a year
@router.get("/{team_id}/matches", response_model=LeagueMatchesResponse)
def get_matches(team_id: int, season: int = Query(GLOBAL_YEAR, description="year"), supabase: Client = Depends(get_supabase_client)):
    service = TeamService(supabase)
    stats = service.get_team_matches_by_year(team_id=team_id, season=season)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")
    return stats

# GET total goals_for and goals_against per season for 10 years
@router.get("/{team_id}/goals_past10", response_model=LeagueMatchesResponse)
def get_matches(team_id: int, season: int = Query(GLOBAL_YEAR, description="year"), supabase: Client = Depends(get_supabase_client)):
    service = TeamService(supabase)
    stats = service.get_team_matches_by_year(team_id=team_id, season=season)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")
    return stats

# GET highest goalscorers/assists All comps per season 10 years
@router.get("/{team_id}/topga_past10", response_model=LeagueMatchesResponse)
def get_matches(team_id: int, season: int = Query(GLOBAL_YEAR, description="year"), supabase: Client = Depends(get_supabase_client)):
    service = TeamService(supabase)
    stats = service.get_team_matches_by_year(team_id=team_id, season=season)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")
    return stats

# GET all transfers in a year and total incoming/outgoing fees
@router.get("/{team_id}/transfers", response_model=TeamTransfersResponse)
def get_transfers(team_id: int, start_date: date = Query("2024-05-01", description="Start date in YYYY-MM-DD format"), end_date: date = Query("2025-08-01", description="End date in YYYY-MM-DD format"),supabase: Client = Depends(get_supabase_client)):
    service = TeamService(supabase)
    stats = service.get_transfers_by_date(team_id=team_id, start_date=start_date, end_date=end_date)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")
    return stats

# GET ALL comp finishes by year (league ranks) and their last game in the comp
@router.get("/{team_id}/comps", response_model=TeamSeasonResponse)
def get_all_comp_finishes(team_id: int, season: int = Query(GLOBAL_YEAR, description="year"), supabase: Client = Depends(get_supabase_client)):
    service = TeamService(supabase)
    stats = service.get_comp_finishes_by_year(team_id=team_id, season=season)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")
    return stats

# GET the teams previous 5 Domestic League finishes
@router.get("/{team_id}/domestic", response_model=DomesticSeasonsResponse)
def get_domestic_finishes(team_id: int, season: int = Query(GLOBAL_YEAR, description="year"), supabase: Client = Depends(get_supabase_client)):
    service = TeamService(supabase)
    stats = service.get_domestic_finishes(team_id=team_id, season=season)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")
    return stats


