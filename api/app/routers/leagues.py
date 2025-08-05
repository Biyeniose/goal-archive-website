from fastapi import APIRouter, HTTPException, Depends, Query
from supabase import Client
from datetime import date
from ..dependencies import get_supabase_client
from ..classes.league import LeagueService
from ..models.league import TeamRank
from app.models.response import LeagueDataResponse, LeagueStatsResponse, LeagueMatchesResponse, LeagueRanksResponse, LeagueFormResponse, TopCompsWinnersResponse, LeagueWinnersResponse, LeagueTeamStatResponse, LeaguePastStatsResponse
from app.constants import GLOBAL_YEAR
from typing import List

router = APIRouter(
    prefix="/v1/leagues",
    tags=["leagues"],
    dependencies=[Depends(get_supabase_client)],
    responses={404: {"description": "Not found"}},
)

# GET League info, ranks, matches
@router.get("/{league_id}/infos", response_model=LeagueDataResponse)
def get_league_data(league_id: int, season: int = Query(GLOBAL_YEAR, description="year"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.get_league_info(season=season, comp_id=league_id)

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

# GET Highest GA Per League and Season
@router.get("/{league_id}/stats", response_model=LeagueStatsResponse)
def get_top_stats(league_id: int, season: int = Query(2024, description="year"), age: int = Query(50, description="Maximum age"), stat: str = Query("ga", description="Type of Stats"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.most_stats_league(league_id=league_id, season=season, stat=stat, age=age)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

# GET All time stats
@router.get("/{league_id}/at-stats", response_model=LeagueStatsResponse)
def get_alltime_top_stats(league_id: int, age: int = Query(50, description="Maximum age"), stat: str = Query("ga", description="Type of Stats"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.most_alltime_stats_league(league_id=league_id, stat=stat, age=age)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

# GET the highest stats (by stats) for the past 10 years
"""
@router.get("/{league_id}/past-top", response_model=LeagueStatsResponse)
def get_alltime_top_stats1(league_id: int, age: int = Query(50, description="Maximum age"), stat: str = Query("ga", description="Type of Stats"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.top_ga_stats_past10(league_id=league_id, stat=stat, age=age)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats
"""

# GET the highest stats (by stats) for the past 10 years
@router.get("/{league_id}/players_topbystat", response_model=LeaguePastStatsResponse)
def get_alltime_top_stats(league_id: int, age: int = Query(50, description="Maximum age"), stat: str = Query("goals", description="Type of Stats"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.top_stats_past10_by_stat(league_id=league_id, stat=stat, age=age)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

# GET Highest Goals Per Team and Season
@router.get("/{league_id}/{team_id}/stats", response_model=LeagueStatsResponse)
def get_league_stats_by_team(league_id: int, team_id: int, age: int = Query(50, description="Maximum age"), season: int = Query(2024, description="year"), all_time: bool = Query(False, description="all time"), stat: str = Query("ga", description="Type of Stats"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.most_league_stats_by_team(team_id=team_id, league_id=league_id, season=season, stat=stat, age=age, all_time=all_time)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats


# GET all matches in a league in a season
@router.get("/{league_id}/matches", response_model=LeagueMatchesResponse)
def get_matches(league_id: int, season: int = Query(GLOBAL_YEAR, description="year"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.get_league_matches(league_id=league_id, season=season)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")
    return stats

# league ranks for a specific year
@router.get("/{league_id}/ranks", response_model=LeagueRanksResponse)
def get_ranks(league_id: int, season: int = Query(GLOBAL_YEAR, description="year"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.get_league_ranks(comp_id=league_id, season=season)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")
    return stats

# form over the 6 most recent played matches by season (isPlayed = true)
@router.get("/{league_id}/form-recent", response_model=LeagueFormResponse)
def get_recent_form(league_id: int, season: int = Query(GLOBAL_YEAR, description="year"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.get_league_form_by_year(league_id=league_id, season=season)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")
    return stats

# form over a date range
@router.get("/{league_id}/form-dates", response_model=LeagueFormResponse)
def get_form_by_dates(league_id: int, start_date: date = Query("2025-04-01", description="Start date in YYYY-MM-DD format"), end_date: date = Query("2025-07-01", description="End date in YYYY-MM-DD format"), supabase: Client = Depends(get_supabase_client)):
    # Validate date range
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="Start date must be before end date")
    # Convert dates to string format for SQL
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()

    service = LeagueService(supabase)
    stats = service.get_league_form_by_dates(league_id=league_id, start_date=start_date_str, end_date=end_date_str)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")
    return stats

# get lists of past winners of comps
@router.get("/winners", response_model=TopCompsWinnersResponse)
def get_recent_winners(supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.get_recent_winners()
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")
    return stats

# get lists of past winners of a certain comp
@router.get("/{league_id}/last_winners", response_model=LeagueWinnersResponse)
def get_recent_winners(league_id: int, supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.get_league_winners(league_id=league_id)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")
    return stats

# get HIGHEST col from league_ranks table (Highest league_ranks.GOALS_F, rank, points) past 10 years. WOULD NOT WORK for fa cups since its only Rank 1 but Ill use script to update)
@router.get("/{league_id}/teams_topbystat", response_model=LeagueTeamStatResponse)
def get_highest_league_stat(league_id: int, stat: str = Query("goals_f", description="Points, Goals F/A, Points, Wins, Losses"), start_year: int = Query(2000, description="Start year"), end_year: int = Query(GLOBAL_YEAR, description="End year"), desc: bool = Query(True, description="Desc or Asc order"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.get_highest_league_stat(league_id=league_id, stat=stat, start_year=start_year, end_year=end_year, desc=desc)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")
    return stats

# same as above but by year
@router.get("/{league_id}/highest_stat_year", response_model=LeagueTeamStatResponse)
def get_highest_league_stat_year(league_id: int, stat: str = Query("goals_f", description="Points, Goals F/A, Points, Wins, Losses"), season: int = Query(GLOBAL_YEAR, description="year"), desc: bool = Query(True, description="Desc or Asc order"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.get_highest_league_stat_by_year(league_id=league_id, stat=stat, season=season, desc=desc)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")
    return stats

# get the highest top 3 league_ranks.goals_for, wins, losses for a league by year


# maybe add versions were it returns the total goals scored in each COMP match but with start and end date

