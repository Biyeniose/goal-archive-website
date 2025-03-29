from fastapi import APIRouter, Depends, Query, HTTPException
from supabase import Client
from ..dependencies import get_supabase_client
from datetime import datetime, timedelta
import pytz


router = APIRouter(
    prefix="/v1/players",
    tags=["players"],
    dependencies=[Depends(get_supabase_client)],
    responses={404: {"description": "Not found"}},
)


# GET details of a Player with PlayerID
@router.get("/{player_id}")
async def get_player_details(player_id: int, supabase: Client = Depends(get_supabase_client)):
    try:
        # Call the SQL function to fetch player and team details
        response = supabase.rpc(
            "get_player_with_team_details",
            {"input_player_id": player_id}  # Pass the player_id as BIGINT
        ).execute()

        player = response.data[0] if response.data else None

        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        return {"data": player}

    except HTTPException as e:
        raise e
    except Exception as e:
        return {"error": str(e)}
 

# Top G/A all leagues with Max age parameter
@router.get("/most_ga/topleagues")
async def get_top_ga_players_top_leagues(max_age: int = Query(40, description="Maximum age of players"), supabase: Client = Depends(get_supabase_client)):
    """
    Route to fetch the top 20 players with the highest 'curr_ga' from specific leagues,
    excluding NULL curr_ga values, with an optional max_age filter.
    """
    try:
        league_id_list = [1, 2, 3, 4, 5, 7, 8, 10, 11, 12, 13, 20]

        # Step 1: Fetch all team IDs for the specified leagues
        all_team_ids = []
        for league_id in league_id_list:
            teams_response = supabase.table("teams").select("team_id").eq("league_id", league_id).execute()
            if teams_response.data:
                all_team_ids.extend([team["team_id"] for team in teams_response.data])

        if not all_team_ids:
            return {"message": f"No teams found for any of the league_ids: {league_id_list}"}

        # Step 2: Fetch top 20 players for those team IDs, sorted by 'curr_ga'
        players_response = supabase.table("players").select(
            "player_name, player_id, curr_ga, curr_goals, curr_assists, curr_gp, age, curr_team_id, nation1, nation2, foot, curr_subon, curr_suboff"
        ).in_("curr_team_id", all_team_ids).lt("age", max_age).not_.is_("curr_goals", None).order("curr_goals", desc=True).limit(10).execute()

        if not players_response.data:
            return {"message": f"No players found with non-NULL curr_ga for teams in league_ids: {league_id_list} and age less than {max_age}"}

        # Step 3: Fetch team names for the players' curr_team_id
        team_ids = list(set(player["curr_team_id"] for player in players_response.data))  # Get unique team IDs
        teams_response = supabase.table("teams").select("team_id, team_name").in_("team_id", team_ids).execute()
        team_name_map = {team["team_id"]: team["team_name"] for team in teams_response.data}  # Create a mapping of team_id to team_name

        # Step 4: Fetch logo URLs for nation1 and nation2
        nation_ids = set()
        for player in players_response.data:
            if player["nation1"]:
                nation_ids.add(player["nation1"])
            if player["nation2"]:
                nation_ids.add(player["nation2"])

        nations_response = supabase.table("teams").select("team_name, logo_url").in_("team_name", nation_ids).execute()
        nation_logo_map = {nation["team_name"]: nation["logo_url"] for nation in nations_response.data}  # Create a mapping of team_name to logo_url

        # Step 5: Add team_name, nation1_url, and nation2_url to each player's data
        enriched_data = []
        for player in players_response.data:
            player["team_name"] = team_name_map.get(player["curr_team_id"], "Unknown Team")
            player["nation1_url"] = nation_logo_map.get(player["nation1"], None) if player["nation1"] else None
            player["nation2_url"] = nation_logo_map.get(player["nation2"], None) if player["nation2"] else None
            enriched_data.append(player)

        return {"data": enriched_data}

    except Exception as e:
        return {"error": str(e)}

# Get most G/A in a League with max age as parameter
@router.get("/most_ga/{league_id}")
async def get_top_ga_by_league(league_id: int, max_age: int = Query(40, description="Maximum player age"),  limit: int = Query(3, description="Amount of players to return"), supabase: Client = Depends(get_supabase_client)):
    """
    Route to fetch the top players with the most G/A for a given league, including club_url.
    """
    try:
        # Get team_ids for the given league_id
        teams_response = supabase.table("teams").select("team_id").eq("league_id", league_id).execute()

        if not teams_response.data:
            return {"message": f"No teams found for league_id: {league_id}"}

        team_ids = [team["team_id"] for team in teams_response.data]

        # Get players under max_age for those team_ids, sorted by curr_ga, excluding NULLs
        players_response = supabase.table("players").select("player_name, player_id, age, position, curr_gp, curr_ga, curr_goals, curr_assists, curr_team_id").in_("curr_team_id", team_ids).lt("age", max_age).not_.is_("curr_ga", None).order("curr_ga", desc=True).limit(limit).execute()

        if players_response.data:
            processed_data = []
            for player in players_response.data:
                club_url = None
                team_response = supabase.table("teams").select("logo_url").eq("team_id", player["curr_team_id"]).execute()
                if team_response.data and len(team_response.data) > 0 and team_response.data[0].get("logo_url"):
                    club_url = team_response.data[0]["logo_url"]

                processed_player = {
                    **player,
                    "club_url": club_url,
                    # "curr_team_id": None, # Removed this line
                }

                processed_data.append(processed_player)

            return {"data": processed_data}
        else:
            return {"message": f"No players under {max_age} with non-NULL G/A found for teams in league_id: {league_id}"}

    except Exception as e:
        return {"error": str(e)}
    
# Most Minutes in a league by age (max age param)
@router.get("/most_min/{league_id}")
async def get_top_minutes_young_players(league_id: int, max_age: int = Query(40, description="Maximum player age"), supabase: Client = Depends(get_supabase_client)):
    """
    Route to fetch the top 10 players under 24 with the most minutes played for a given league, excluding NULL minutes values.
    """
    try:
        # Get team_ids for the given league_id
        teams_response = supabase.table("teams").select("team_id").eq("league_id", league_id).execute()

        if not teams_response.data:
            return {"message": f"No teams found for league_id: {league_id}"}

        team_ids = [team["team_id"] for team in teams_response.data]

        # Get players under 24 for those team_ids, sorted by minutes, excluding NULLs
        players_response = supabase.table("players").select("player_name, age, position, curr_gp, curr_ga, curr_goals, curr_assists, curr_minutes, transfm_url, nation1").in_("curr_team_id", team_ids).lt("age", max_age).not_.is_("curr_minutes", None).order("curr_minutes", desc=True).limit(20).execute()

        if players_response.data:
            return {"data": players_response.data}
        else:
            return {"message": f"No players under 24 with non-NULL minutes found for teams in league_id: {league_id}"}

    except Exception as e:
        return {"error": str(e)}


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
