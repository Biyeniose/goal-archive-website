from fastapi import FastAPI, Query, Path
from supabase import create_client, Client
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
import os

load_dotenv() # Load environment variables from .env file
app = FastAPI() # Initialize FastAPI app

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    #allow_origins=["http://localhost:3000"],  # Allow Next.js origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Get Supabase credentials from environment variables
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

if not url or not key:
    raise ValueError("SUPABASE_URL or SUPABASE_KEY are not set")

# Initialize Supabase client
supabase: Client = create_client(url, key)

@app.get("/")
async def read_root():
    return {"message": "Welcome to the FastAPI app with Supabase!"}

@app.get("/v1/bdor")
async def get_ballon_dor_winners():
    """
    Route to fetch all rows from the ballon_dor table where year = 2024.
    """
    try:
        # Query the ballon_dor table for rows where year = 2024
        response = supabase.table("ballon_dor").select("*").eq("year", 2024).execute()

        # Check if there are any results
        if response.data:
            print(response.data)
            return {"data": response.data}
        else:
            return {"message": "No results found for the year 2024."}
    except Exception as e:
        # Handle errors
        return {"error": str(e)}

@app.get("/v1/bdor/{year}")
async def get_ballon_dor_year(year: int):
    try:
        response = supabase.table("ballon_dor").select("*").eq("year", year).execute()

        # Check if there are any results
        if response.data:
            return {"data": response.data}
        else:
            return {"message": f"No results found for the year {year}."}
    except Exception as e:
        # Handle errors
        return {"error": str(e)}

# GET teams by league
@app.get("/v1/teams/{league_id}")
async def get_teams_by_league(league_id: int):
    """
    fetch all teams from a certain league
    """
    try:
        # Query the ballon_dor table for rows where year = 2024
        response = supabase.table("teams").select("team_id, team_name, logo_url").eq("league_id", league_id).execute()

        # Check if there are any results
        if response.data:
            print(response.data)
            return {"data": response.data}
        else:
            return {"message": "No results found for this league"}
    except Exception as e:
        # Handle errors
        return {"error": str(e)}

# GET players by team_id
@app.get("/teams/players/{team_id}")
async def get_players_by_team_id(team_id: int):
    """
    fetch all players from by team_id
    """
    try:
        # Query the ballon_dor table for rows where year = 2024
        response = supabase.table("players").select("*").eq("curr_team_id", team_id).execute()

        # Check if there are any results
        if response.data:
            print(response.data)
            return {"data": response.data}
        else:
            return {"message": "No results found for this league"}
    except Exception as e:
        # Handle errors
        return {"error": str(e)}


# GET teams by league
@app.get("/v1/leagues")
async def get_teams_by_league():
    """
    Fetch all leagues and include the country's logo URL from the teams table.
    """
    try:
        # Define the list of league IDs to filter by
        league_ids = [1, 2, 3, 4, 5, 7, 8, 10, 11, 12]

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

# englsh players outside england
@app.get("/players/outside_epl")
async def get_top_english_players_outside_epl_and_mls():
    """
    Fetch the top 20 English players with the most 'curr_minutes' 
    who are not in the Premier League (league_id = 2) or MLS (league_id = 20).
    """
    try:
        # 1. Get team_ids from teams in EPL or MLS
        excluded_leagues = [2, 20]
        excluded_teams = supabase.table("teams").select("team_id").filter("league_id", "in", f"({','.join(map(str, excluded_leagues))})").execute()

        if excluded_teams.data:
            excluded_team_ids = [str(team["team_id"]) for team in excluded_teams.data]
        else:
            excluded_team_ids = []  # Handle case where no excluded league teams are found

        # 2. Query players, excluding teams from EPL and MLS, and filtering for English players
        query = supabase.table("players").select("player_id, player_name, age, position, curr_goals, curr_team_id, transfm_url").eq("nation1", "England")

        if excluded_team_ids:
            query = query.filter("curr_team_id", "not.in", f"({','.join(excluded_team_ids)})")

        response = query.order("curr_ga", desc=True).limit(20).execute()

        if response.data:
            return {"data": response.data}
        else:
            return {"message": "No matching players found."}
    except Exception as e:
        return {"error": str(e)}

    
# Top G/A all leagues with Max age parameter
@app.get("v1/stats/players/mostga/topleagues")
async def get_top_ga_players_top_leagues(max_age: int = Query(40, description="Maximum age of players")):
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

# Most Minutes in a league by age (max 20)
@app.get("/players/most_min/{league_id}")
async def get_top_minutes_young_players(league_id: int, max_age: int = Query(40, description="Maximum player age")):
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

# Get most G/A in a League with max age as parameter
@app.get("/players/most_ga/{league_id}")
async def get_top_ga_by_league(league_id: int, max_age: int = Query(40, description="Maximum player age")):

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

# Get most G/A in a team with max age as parameter
@app.get("/v1/teams/{team_id}/most_ga")
async def get_top_players_by_team(team_id: int):
    """
    Fetch the top 5 players with the highest curr_ga for a given team_id.
    """
    try:
        # Step 1: Fetch top 5 players for the given team_id, sorted by curr_ga (descending)
        players_response = supabase.table("players").select(
            "player_id, player_name, age, curr_gp, curr_ga, curr_goals, curr_assists, position, nation1, nation2"
        ).eq("curr_team_id", team_id).not_.is_("curr_ga", None).order("curr_ga", desc=True).limit(5).execute()

        # Step 2: Check if there are any results
        if players_response.data:
            return {"data": players_response.data}
        else:
            return {"message": f"No players found for team_id: {team_id}"}

    except Exception as e:
        return {"error": str(e)}

# gets top G/A for defenders from top leagues
@app.get("/players/topga/defenders")
async def get_top_defender_assists_leagues(max_age: int = Query(40, description="Maximum player age")):
    """
    Route to fetch the top 10 Defender players (Left-Back, Right-Back, Centre-Back)
    with the highest 'curr_assists' from specific leagues, excluding NULL curr_assists values.
    """
    try:
        league_id_list = [1, 2, 3, 4, 5, 7, 8, 10, 11, 12, 13, 20]
        #league_id_list = [1, 2, 3, 4, 5]

        all_team_ids = []
        for league_id in league_id_list:
            teams_response = supabase.table("teams").select("team_id").eq("league_id", league_id).execute()

            if teams_response.data:
                all_team_ids.extend([team["team_id"] for team in teams_response.data])

        if not all_team_ids:
            return {"message": f"No teams found for any of the league_ids: {league_id_list}"}

        positions = ["Left-Back", "Right-Back", "Centre-Back"]

        players_response = supabase.table("players").select("player_name, player_id, curr_ga, curr_goals, curr_assists, curr_gp, age, curr_team_id, nation1, position, transfm_url").in_("position", positions).in_("curr_team_id", all_team_ids).lt("age", max_age).not_.is_("curr_assists", None).order("curr_assists", desc=True).limit(12).execute()

        if players_response.data:
            return {"data": players_response.data}
        else:
            return {"message": "No Defender players found with non-NULL curr_assists in the specified leagues."}

    except Exception as e:
        return {"error": str(e)}

# gets top minutes for defenders from top leagues
@app.get("/topminutes/defenders")
async def get_top_defender_assists_leagues(max_age: int = Query(40, description="Maximum player age")):
    """
    Route to fetch the top 10 Defender players (Left-Back, Right-Back, Centre-Back)
    with the highest 'curr_assists' from specific leagues, excluding NULL curr_assists values.
    """
    try:
        #league_id_list = [1, 2, 3, 4, 5, 7, 8, 10, 11, 12, 13, 20]
        league_id_list = [1, 2, 3, 4, 5]

        all_team_ids = []
        for league_id in league_id_list:
            teams_response = supabase.table("teams").select("team_id").eq("league_id", league_id).execute()

            if teams_response.data:
                all_team_ids.extend([team["team_id"] for team in teams_response.data])

        if not all_team_ids:
            return {"message": f"No teams found for any of the league_ids: {league_id_list}"}

        positions = ["Left-Back", "Right-Back", "Centre-Back"]

        players_response = supabase.table("players").select("*").in_("position", positions).in_("curr_team_id", all_team_ids).lt("age", max_age).not_.is_("curr_minutes", None).order("curr_minutes", desc=True).limit(10).execute()

        if players_response.data:
            return {"data": players_response.data}
        else:
            return {"message": "No Defender players found with non-NULL curr_assists in the specified leagues."}

    except Exception as e:
        return {"error": str(e)}

# gets top minutes for defenders from top leagues
@app.get("/topga/attackers")
async def get_top_defender_assists_leagues(max_age: int = Query(40, description="Maximum player age")):
    """
    Route to fetch the top 10 Defender players (Left-Back, Right-Back, Centre-Back)
    with the highest 'curr_assists' from specific leagues, excluding NULL curr_assists values.
    """
    try:
        #league_id_list = [1, 2, 3, 4, 5, 7, 8, 10, 11, 12, 13, 20]
        league_id_list = [1, 2, 3, 4, 5]

        all_team_ids = []
        for league_id in league_id_list:
            teams_response = supabase.table("teams").select("team_id").eq("league_id", league_id).execute()

            if teams_response.data:
                all_team_ids.extend([team["team_id"] for team in teams_response.data])

        if not all_team_ids:
            return {"message": f"No teams found for any of the league_ids: {league_id_list}"}

        positions = ["Centre-Forward", "Right Winger", "Left Winger", "Second Striker"]

        players_response = supabase.table("players").select("*").in_("position", positions).in_("curr_team_id", all_team_ids).lt("age", max_age).not_.is_("curr_goals", None).order("curr_goals", desc=True).limit(10).execute()

        if players_response.data:
            return {"data": players_response.data}
        else:
            return {"message": "No Attackers players found with non-NULL curr_assists in the specified leagues."}

    except Exception as e:
        return {"error": str(e)}

@app.get("/season/{team_id}")
async def get_highest_ga(team_id: int):
    try:
        # Fetch all players with the specified curr_team_id
        response = supabase.table('players').select('*').eq('curr_team_id', team_id).execute()

        # Calculate g/a (goals + assists) for each player
        players = response.data
        for player in players:
            player['ga'] = player['curr_goals'] + player['curr_assists']
        
        # Sort players by g/a in descending order
        sorted_players = sorted(players, key=lambda x: x['ga'], reverse=True)
        # Get the top 5 players
        top_players = sorted_players[:5]
        
        return {"data": top_players}
    
    except Exception as e:
        # Handle errors
        return {"error": str(e)}