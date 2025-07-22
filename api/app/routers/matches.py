from fastapi import APIRouter, Depends, Query, HTTPException
from supabase import Client
from ..dependencies import get_supabase_client
from ..classes.match import MatchService
from app.models.response import MatchInfoResponse
from datetime import datetime, timedelta
import pytz, requests, random

router = APIRouter(
    prefix="/v1/matches",
    tags=["matches"],
    #dependencies=[Depends(get_supabase_client)],
    responses={404: {"description": "Not found"}},
)

# GET Match Information
@router.get("/{match_id}", response_model=MatchInfoResponse)
async def get_match_data(match_id: int, supabase: Client = Depends(get_supabase_client)):
    try:
        service = MatchService(supabase)
        stats = service.get_match_data(match_id=match_id)

        if not stats:
            raise HTTPException(status_code=404, detail="Stats not found")
        
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# get the last 6 matches, these 2 teams have played against each other

# get a manager's record against a certian team