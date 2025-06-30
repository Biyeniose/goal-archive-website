from fastapi import APIRouter, Depends, Query, HTTPException
from supabase import Client
from ..dependencies import get_supabase_client
from ..classes.stat import StatsRanking, StatsService, LeagueStats, TeamMatches, TeamMatchesResponse
from typing import List
import requests, random

router = APIRouter(
    prefix="/v1/stats",
    tags=["stats"],
    #dependencies=[Depends(get_supabase_client)],
    responses={404: {"description": "Not found"}},
)


@router.get("/best", response_model=List[LeagueStats])
def get_bio(supabase: Client = Depends(get_supabase_client)):
    service = StatsService(supabase)
    stats = service.get_highest_points()

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

@router.get("/noloss", response_model=List[LeagueStats])
def get_bio2(supabase: Client = Depends(get_supabase_client)):
    service = StatsService(supabase)
    stats = service.get_no_losses()

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

@router.get("/{team_id}/{comp_id}/recent", response_model=List[TeamMatches])
def get_recent_matchess(team_id: int, comp_id: int,
    season_year: int = 2024, supabase: Client = Depends(get_supabase_client)):
    service = StatsService(supabase)

    stats = service.get_team_recent(
            comp_id=comp_id,
            season_year=season_year,
            team_id=team_id
    )

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

@router.get("/{team_id}/{comp_id}/recent/all", response_model=TeamMatchesResponse)
def get_recent_matches(team_id: int, comp_id: int,
    season_year: int = Query(None, description="year"), supabase: Client = Depends(get_supabase_client)):
    service = StatsService(supabase)

    stats = service.get_team_recent_form(
            comp_id=comp_id,
            season_year=season_year,
            team_id=team_id
    )

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats

# GET best team form
@router.get("/{comp_id}/best", response_model=TeamMatchesResponse)
def get_recent_matches(team_id: int, comp_id: int,
    season_year: int = Query(None, description="year"), supabase: Client = Depends(get_supabase_client)):
    service = StatsService(supabase)

    stats = service.get_team_recent_form(
            comp_id=comp_id,
            season_year=season_year,
            team_id=team_id
    )

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
