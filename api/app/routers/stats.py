from fastapi import APIRouter, Depends, Query, HTTPException
from supabase import Client
from ..dependencies import get_supabase_client
from ..classes.stat import StatsRanking, StatsService, LeagueStats
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
def get_bio(supabase: Client = Depends(get_supabase_client)):
    service = StatsService(supabase)
    stats = service.get_no_losses()

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats


@router.get("/worst", response_model=List[LeagueStats])
def get_bio(supabase: Client = Depends(get_supabase_client)):
    service = StatsService(supabase)
    stats = service.get_worst_winners()

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats
