from fastapi import APIRouter, HTTPException, Depends, Query
from supabase import Client
from ..dependencies import get_supabase_client
from ..classes.league import StatsRanking, LeagueService
from typing import List
import requests, random

router = APIRouter(
    prefix="/v1/leagues",
    tags=["leagues"],
    dependencies=[Depends(get_supabase_client)],
    responses={404: {"description": "Not found"}},
)

# GET Highest Goals Per League and Season
@router.get("/{league_id}/stats", response_model=List[StatsRanking])
def get_highest_ga(league_id: int, season: int = Query(2024, description="year"), age: int = Query(50, description="Maximum age"), stat: str = Query("ga", description="Type of Stats"), supabase: Client = Depends(get_supabase_client)):
    service = LeagueService(supabase)
    stats = service.most_stats_league(league_id=league_id, season=season, stat=stat, age=age)

    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found")

    return stats


# GET All Leagues with details
@router.get("/domestic")
async def get_teams_by_league(supabase: Client = Depends(get_supabase_client)):
    """
    Fetch all leagues and include the country's logo URL from the teams table.
    """
    try:
        # Define the list of league IDs to filter by
        league_ids = [1, 2, 3, 4, 5, 7, 8, 10, 11, 12, 13, 20, 25, 45, 75, 85, 291]

        # Step 1: Fetch leagues data
        leagues_response = supabase.table("leagues").select("league_id, league_name, country").in_("league_id", league_ids).execute()

        if not leagues_response.data:
            return {"message": f"No leagues found for league_ids: {league_ids}"}

        # Step 2: Fetch logo URLs for each country
        countries = list(set(league["country"] for league in leagues_response.data))  # Get unique country names
        teams_response = supabase.table("teams").select("team_name, logo_url").in_("team_name", countries).execute()
        country_logo_map = {team["team_name"]: team["logo_url"] for team in teams_response.data}  # Create a mapping of country to logo_url

        # Step 3: Add country_url to each league's data
        enriched_data = []
        for league in leagues_response.data:
            league["country_url"] = country_logo_map.get(league["country"], None)  # Add country_url
            enriched_data.append(league)

        return {"data": enriched_data}

    except Exception as e:
        return {"error": str(e)}

# GET All teams of a certain League with rank
@router.get("/{league_id}/rank")
async def get_teams(league_id: str, supabase: Client = Depends(get_supabase_client)):
    try:
        response = supabase.table("teams").select("team_name, team_id, curr_league_rank, curr_league_points, curr_league_gp, curr_league_gd, logo_url, league_id").eq("league_id", league_id).order("curr_league_rank", desc=False).execute()

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

# Get most G/A in a League with max age as parameter
@router.get("/{league_id}/most_ga")
async def get_top_ga_by_league(league_id: int, max_age: int = Query(40, description="Maximum player age"), supabase: Client = Depends(get_supabase_client)):
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
        players_response = supabase.table("players").select("player_name, player_id, age, position, curr_gp, curr_ga, curr_goals, curr_assists, curr_team_id").in_("curr_team_id", team_ids).lt("age", max_age).not_.is_("curr_ga", None).order("curr_ga", desc=True).limit(10).execute()

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
  

@router.get("/{league_id}/nation2")
async def get_team(league_id: str, supabase: Client = Depends(get_supabase_client), nation: str = Query("Nigeria", description="which country to search")):
    try:
        # Call the SQL function to fetch player and team details
        response = supabase.rpc(
            "get_players_by_nation2",
            {"input_league_id": league_id, "input_nation": nation},  # Pass the player_id as BIGINT
        ).execute()

        player = response.data if response.data else None

        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        return {"data": player}

    except HTTPException as e:
        raise e
    except Exception as e:
        return {"error": str(e)}