from fastapi import FastAPI, Query, Path, Depends
from supabase import create_client, Client
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from .routers import teams, leagues, bdor, players
from .dependencies import get_supabase_client
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

app.include_router(teams.router)
app.include_router(leagues.router)
app.include_router(bdor.router)
app.include_router(players.router)


@app.get("/")
async def read_root():
    return {"message": "Welcome to the FastAPI app with Supabase!"}


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