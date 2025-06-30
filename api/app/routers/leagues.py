from fastapi import APIRouter, HTTPException, Depends, Query
from supabase import Client
from ..dependencies import get_supabase_client
from ..classes.league import StatsRanking, LeagueService, AllTimeRank
from ..models.league import TopLeaguesResponse, LeagueDataResponse, TeamRank, PastSeasonRanksResponse
from typing import List

router = APIRouter(
    prefix="/v1/leagues",
    tags=["leagues"],
    dependencies=[Depends(get_supabase_client)],
    responses={404: {"description": "Not found"}},
)

# GET Highest GA Per League and Season
@router.get("/{league_id}/stats", response_model=List[StatsRanking])
def get_top_stats(league_id: int, season: int = Query(2024, description="year"), age: int = Query(50, description="Maximum age"), stat: str = Query("ga", description="Type of Stats"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.most_stats_league(league_id=league_id, season=season, stat=stat, age=age)

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

# GET Highest Goals Per Team and Season
@router.get("/{league_id}/{team_id}/stats", response_model=List[StatsRanking])
def get_top_stats(league_id: int, team_id: int, season: int = Query(2024, description="year"), age: int = Query(50, description="Maximum age"), stat: str = Query("ga", description="Type of Stats"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.teams_most_stats_league(league_id=league_id, team_id=team_id, season=season, stat=stat, age=age)

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

# GET League All time stats
@router.get("/{league_id}/stats/all", response_model=List[StatsRanking])
def get_top_stats_alltime(league_id: int, age: int = Query(50, description="Maximum age"), stat: str = Query("ga", description="Type of Stats"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.alltime_league_stats_per_season(league_id=league_id, stat=stat, age=age)

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

# GET League All League Pen goals
@router.get("/{league_id}/stats/all/pens", response_model=List[AllTimeRank])
def get_alltime_league_pens(league_id: int, supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.alltime_league_penalty_goals(league_id=league_id)

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

# GET Highest GA Per League and Season
@router.get("/{league_id}/form", response_model=List[StatsRanking])
def get_top_stats(league_id: int, season: int = Query(2024, description="year"), age: int = Query(50, description="Maximum age"), stat: str = Query("ga", description="Type of Stats"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.most_stats_league(league_id=league_id, season=season, stat=stat, age=age)

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

# GET Top 5 rankings of top teams
@router.get("/top_ranks", response_model=TopLeaguesResponse)
def get_top_league_ranks(season: int = Query(2024, description="year"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.top_leagues_rankings(season=season)

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

# GET League info
@router.get("/{league_id}/infos", response_model=LeagueDataResponse)
def get_league_data(league_id: int, season: int = Query(2024, description="year"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.get_league_info(season=season, comp_id=league_id)

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

# GET Highest stats past 3 seasons
@router.get("/{league_id}/past_seasons", response_model=PastSeasonRanksResponse)
def get_league_data(league_id: int, season: int = Query(2024, description="year"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.get_past_3years(season=season, comp_id=league_id)

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

# GET details of a certain league
@router.get("/{league_id}")
async def get_teams(league_id: str, supabase: Client = Depends(get_supabase_client)):
    try:
        response = supabase.table("leagues").select("league_name, league_id, country, type").eq("league_id", league_id).execute()

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

