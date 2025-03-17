from fastapi import APIRouter, Depends, Query
from supabase import Client
from ..dependencies import get_supabase_client


router = APIRouter(
    prefix="/v1/players",
    tags=["players"],
    dependencies=[Depends(get_supabase_client)],
    responses={404: {"description": "Not found"}},
)

# GET all Players of a Team


# Top G/A all leagues with Max age parameter
@router.get("/mostga/topleagues")
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
            "player_name, player_id, curr_ga, curr_goals, curr_assists, curr_gp, age, curr_team_id, nation1, nation2"
        ).in_("curr_team_id", all_team_ids).lt("age", max_age).not_.is_("curr_ga", None).order("curr_ga", desc=True).limit(10).execute()

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
async def get_top_ga_by_league(league_id: int, max_age: int = Query(40, description="Maximum player age"), supabase: Client = Depends(get_supabase_client)):
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
        players_response = supabase.table("players").select("player_name, age, position, curr_gp, curr_ga, curr_goals, curr_assists, curr_minutes, transfm_url").in_("curr_team_id", team_ids).lt("age", max_age).not_.is_("curr_ga", None).order("curr_ga", desc=True).limit(10).execute()

        if players_response.data:
            return {"data": players_response.data}
        else:
            return {"message": f"No players under 24 with non-NULL minutes found for teams in league_id: {league_id}"}

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


