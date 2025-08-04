from fastapi import APIRouter, Depends, Query, HTTPException
from supabase import Client
from datetime import date
from ..dependencies import get_supabase_client
from ..classes.player import PlayerService
from ..models.player import PlayerPageDataResponse
from app.models.response import PlayerSeasonStatsResponse, PlayerMatchesResponse, PlayerRecentGAResponse, PlayerSearchResponse, PlayerGADistResponse
from datetime import datetime, timedelta
import pytz, requests, random
from app.constants import GLOBAL_YEAR


router = APIRouter(
    prefix="/v1/players",
    tags=["players"],
    #dependencies=[Depends(get_supabase_client)],
    responses={404: {"description": "Not found"}},
)

# Search player by name
@router.get("/search", response_model=PlayerSearchResponse)
async def get_season_stats(name: str = Query("Frank Lampard", description="Season year"),supabase: Client = Depends(get_supabase_client)):
    try:
        service = PlayerService(supabase)
        stats = service.player_search(player_name=name)

        if not stats:
            raise HTTPException(status_code=404, detail="Stats not found")
        
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# GET Players stats in all competitions per season
@router.get("/{player_id}/allstats", response_model=PlayerSeasonStatsResponse)
async def get_season_stats(player_id: int, season: int = Query(GLOBAL_YEAR, description="Season year"),supabase: Client = Depends(get_supabase_client)):
    try:
        service = PlayerService(supabase)
        stats = service.get_player_stats_all_seasons(player_id=player_id, season=season)

        if not stats:
            raise HTTPException(status_code=404, detail="Stats not found")
        
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# GET Players career overall stats
@router.get("/{player_id}/career", response_model=PlayerSeasonStatsResponse)
async def get_season_stats(player_id: int,supabase: Client = Depends(get_supabase_client)):
    try:
        service = PlayerService(supabase)
        stats = service.get_career_stats(player_id=player_id)

        if not stats:
            raise HTTPException(status_code=404, detail="Stats not found")
        
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# GET Player Information for player page
@router.get("/{player_id}/infos", response_model=PlayerPageDataResponse)
async def get_player_page_data(player_id: int, supabase: Client = Depends(get_supabase_client)):
    try:
        service = PlayerService(supabase)
        stats = service.get_player_page_data(player_id=player_id)
        if not stats:
            raise HTTPException(status_code=404, detail="Stats not found")
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# GET breakdown (teams+pens) of G/A for a season or by date range
@router.get("/{player_id}/goal-dist", response_model=PlayerGADistResponse)
async def get_player_goal_dist(player_id: int, season: int = Query(GLOBAL_YEAR, description="Season year"), supabase: Client = Depends(get_supabase_client)):
    try:
        service = PlayerService(supabase)
        stats = service.get_player_goal_distribution(player_id=player_id, season=season)
        if not stats:
            raise HTTPException(status_code=404, detail="Stats not found")
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
# GET list of all the games per season they were in xi or bench + match stats
@router.get("/{player_id}/matches", response_model=PlayerMatchesResponse)
async def get_player_match_statistics(player_id: int, season: int = Query(GLOBAL_YEAR, description="Season year"), supabase: Client = Depends(get_supabase_client)):
    try:
        service = PlayerService(supabase)
        stats = service.get_matches_by_season(player_id=player_id, season=season)
        if not stats:
            raise HTTPException(status_code=404, detail="Stats not found")
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# GET list of games by date range
@router.get("/{player_id}/matches-bydate", response_model=PlayerMatchesResponse)
def get_matches_dates(player_id: int, start_date: date = Query("2024-11-01", description="Start date in YYYY-MM-DD format"), end_date: date = Query("2025-03-01", description="End date in YYYY-MM-DD format"), supabase: Client = Depends(get_supabase_client)):
    # Validate date range
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="Start date must be before end date")
    # Convert dates to string format for SQL
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()

    service = PlayerService(supabase)
    stats = service.get_matches_by_dates(player_id=player_id, start_date=start_date_str, end_date=end_date_str)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")
    return stats

# GET details of the last game where the player had a g/a
@router.get("/{player_id}/recent-ga", response_model=PlayerRecentGAResponse)
async def get_player_recent_ga(player_id: int, supabase: Client = Depends(get_supabase_client)):
    try:
        service = PlayerService(supabase)
        stats = service.get_recent_ga(player_id=player_id)
        if not stats:
            raise HTTPException(status_code=404, detail="Stats not found")
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def get_yesterday_toronto_date():
    toronto_tz = pytz.timezone('America/Toronto')
    return (datetime.now(toronto_tz) - timedelta(days=1)).strftime("%Y-%m-%d")

def parse_supabase_timestamp(timestamp_str):
    """Handle all possible Supabase timestamp formats"""
    # Remove timezone info if present (we'll treat all as UTC)
    if 'Z' in timestamp_str:
        timestamp_str = timestamp_str.replace('Z', '')
    elif '+' in timestamp_str:
        timestamp_str = timestamp_str.split('+')[0]
    
    # Handle different decimal precision formats
    if '.' in timestamp_str:
        # Standardize to 6 decimal places
        parts = timestamp_str.split('.')
        timestamp_str = f"{parts[0]}.{parts[1][:6]}"
    
    # Parse the cleaned string
    try:
        return datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        try:
            return datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            raise ValueError(f"Could not parse timestamp: {timestamp_str}")

@router.get("/{player_id}/insta")
async def get_player_instagram_stats(
    player_id: int,
    start_date: str = Query(default_factory=get_yesterday_toronto_date, 
                          description="Start date in YYYY-MM-DD format (defaults to yesterday)"),
    supabase: Client = Depends(get_supabase_client)
):
    try:
        # Parse the input date (YYYY-MM-DD) and set to start of day in Toronto time
        toronto_tz = pytz.timezone('America/Toronto')
        start_date_naive = datetime.strptime(start_date, "%Y-%m-%d")
        start_datetime_toronto = toronto_tz.localize(start_date_naive)
        start_datetime_utc = start_datetime_toronto.astimezone(pytz.UTC)

        # Get player name
        player_response = supabase.table("players")\
            .select("player_name")\
            .eq("player_id", player_id)\
            .single()\
            .execute()
        
        player_name = player_response.data.get("player_name")
        if not player_name:
            raise HTTPException(status_code=404, detail="Player not found")

        # Get follower stats
        stats_response = supabase.table("followers")\
            .select("updated_at, num_followers")\
            .eq("player_id", player_id)\
            .gte("updated_at", start_datetime_utc.isoformat())\
            .order("updated_at")\
            .execute()

        stats_data = stats_response.data
        if not stats_data:
            raise HTTPException(status_code=404, 
                             detail="No stats found for this player since the specified date")

        # Process stats
        processed_stats = []
        for i, record in enumerate(stats_data):
            try:
                # Parse the timestamp using our custom function
                utc_time = parse_supabase_timestamp(record['updated_at'])
                utc_time = pytz.UTC.localize(utc_time)  # Make it timezone-aware
                toronto_time = utc_time.astimezone(toronto_tz)
                
                diff = record['num_followers'] - stats_data[i-1]['num_followers'] if i > 0 else 0

                processed_stats.append({
                    "date": toronto_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "followers": record['num_followers'],
                    "diff": diff
                })
            except ValueError as e:
                continue  # Skip malformed timestamps or log the error

        if not processed_stats:
            raise HTTPException(status_code=500, 
                             detail="Could not parse any valid timestamps from the database")

        return {
            "player_id": player_id,
            "player_name": player_name,
            "stats": processed_stats
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format. Use YYYY-MM-DD")
    except HTTPException:
        raise
    except Exception as e:
        return {"error": str(e)}




