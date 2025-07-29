from fastapi import APIRouter, Depends, Query, HTTPException
from supabase import Client
from datetime import date
from ..dependencies import get_supabase_client
from ..classes.stat import StatsRanking, StatsService, LeagueStats, TeamMatches, TeamMatchesResponse
from app.models.response import H2HResponse
from typing import List
import requests, random

router = APIRouter(
    prefix="/v1/stats",
    tags=["stats"],
    #dependencies=[Depends(get_supabase_client)],
    responses={404: {"description": "Not found"}},
)

# h2h
@router.get("/h2h", response_model=H2HResponse)
def get_h2h(team1_id: int = Query(7761, description="team1"), team2_id: int = Query(5860, description="team2"), num_matches: int = Query(5, description="number of matches"), start_date: date = Query("2005-07-01", description="Start date in YYYY-MM-DD format"), end_date: date = Query("2025-08-01", description="End date in YYYY-MM-DD format"), supabase: Client = Depends(get_supabase_client)):
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="Start date must be before end date")
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()
    service = StatsService(supabase)
    stats = service.get_teams_h2h(team1_id=team1_id, team2_id=team2_id, num_matches=num_matches, start_date=start_date_str, end_date=end_date_str)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

# matches and win pct when a certain ref


@router.get("/noloss", response_model=List[LeagueStats])
def get_bio2(supabase: Client = Depends(get_supabase_client)):
    service = StatsService(supabase)
    stats = service.get_no_losses()

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats


@router.get("/worst", response_model=List[LeagueStats])
def get_bio_2(supabase: Client = Depends(get_supabase_client)):
    service = StatsService(supabase)
    stats = service.get_worst_winners()

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats
