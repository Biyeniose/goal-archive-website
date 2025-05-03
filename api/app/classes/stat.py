from fastapi import HTTPException
from typing import List
from pydantic import BaseModel
from supabase import Client
import requests,json
from typing import Optional

# Define a Pydantic model
class StatsRanking(BaseModel):
    player_id: int
    player_name: str
    age: Optional[int] = None
    team_id: Optional[int] = None
    team_name: Optional[str] = None
    team_logo: Optional[str] = None
    nation1_id: Optional[int] = None
    nation2_id: Optional[int] = None 
    nation1: Optional[str] = None
    nation2: Optional[str] = None
    nation1_url: Optional[str] = None
    nation2_url: Optional[str] = None 
    goals: int
    assists: int
    ga: int
    gp: int

class LeagueStats(BaseModel):
    comp_id: int
    team_id: int
    season_year: int
    rank: int
    points: int
    gp: int
    wins: int
    losses: int
    draws: int
    goals_f: int
    goals_a: int
    gd: int
    rank_id: int
    team_name: str
    logo_url: str

    
    

class StatsService:
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client
        self.key = supabase_client.supabase_key
        self.url = f"{supabase_client.rest_url}/rpc/execute_sql"
        self.headers = {
            "Authorization": f"Bearer {self.key}",
            "apikey": self.key,
            "Content-Type": "application/json"
        }



    def get_highest_points(self):
        query = f"""
        SELECT json_agg(top_teams) 
        FROM (
            SELECT 
                lr.comp_id,
                lr.team_id,
                lr.season_year,
                lr.rank,
                lr.points,
                lr.gp,
                lr.wins,
                lr.losses,
                lr.draws,
                lr.goals_f,
                lr.goals_a,
                lr.gd,
                lr.rank_id,
                t.team_name,
                t.logo_url
            FROM 
                (SELECT *
                FROM league_ranks
                ORDER BY points DESC
                LIMIT 20) lr
            JOIN teams t ON lr.team_id = t.team_id
            ORDER BY lr.points DESC
        ) top_teams;
        """

        try:
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            if response.status_code == 200:
                results = response.json()
                
                # Debug print to see the raw response structure
                #print("Raw response:", results)
                
                if results and len(results) > 0:
                    # The result might be a JSON string that needs parsing
                    if isinstance(results[0].get("result"), str):
                        # Parse the JSON string if needed
                        team_data = json.loads(results[0]["result"])
                    else:
                        team_data = results[0].get("result", [])
                    
                    # Ensure we have a list of dictionaries
                    if isinstance(team_data, list):
                        return [LeagueStats(**entry) for entry in team_data]
                    elif isinstance(team_data, dict):
                        return [LeagueStats(**team_data)]
                
                return []
            else:
                raise HTTPException(status_code=response.status_code, detail=response.text)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=500, detail=f"JSON decode error: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    def get_no_losses(self):
        query = f"""
        SELECT json_agg(top_teams) 
        FROM (
            SELECT 
                lr.comp_id,
                lr.team_id,
                lr.season_year,
                lr.rank,
                lr.points,
                lr.gp,
                lr.wins,
                lr.losses,
                lr.draws,
                lr.goals_f,
                lr.goals_a,
                lr.gd,
                lr.rank_id,
                t.team_name,
                t.logo_url
            FROM 
                (SELECT *
                FROM league_ranks
                WHERE losses = 0
                ORDER BY points DESC
                LIMIT 20) lr
            JOIN teams t ON lr.team_id = t.team_id
            ORDER BY lr.points DESC
        ) top_teams;
        """

        try:
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            if response.status_code == 200:
                results = response.json()
                
                # Debug print to see the raw response structure
                #print("Raw response:", results)
                
                if results and len(results) > 0:
                    # The result might be a JSON string that needs parsing
                    if isinstance(results[0].get("result"), str):
                        # Parse the JSON string if needed
                        team_data = json.loads(results[0]["result"])
                    else:
                        team_data = results[0].get("result", [])
                    
                    # Ensure we have a list of dictionaries
                    if isinstance(team_data, list):
                        return [LeagueStats(**entry) for entry in team_data]
                    elif isinstance(team_data, dict):
                        return [LeagueStats(**team_data)]
                
                return []
            else:
                raise HTTPException(status_code=response.status_code, detail=response.text)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=500, detail=f"JSON decode error: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    def get_worst_winners(self):
        query = f"""
        SELECT json_agg(top_teams) 
        FROM (
            SELECT 
                lr.comp_id,
                lr.team_id,
                lr.season_year,
                lr.rank,
                lr.points,
                lr.gp,
                lr.wins,
                lr.losses,
                lr.draws,
                lr.goals_f,
                lr.goals_a,
                lr.gd,
                lr.rank_id,
                t.team_name,
                t.logo_url
            FROM 
                (SELECT *
                FROM league_ranks
                WHERE rank = 1 and season_year != 2024 and comp_id != 9
                ORDER BY goals_a ASC
                LIMIT 20) lr
            JOIN teams t ON lr.team_id = t.team_id
            ORDER BY lr.goals_a ASC
        ) top_teams;
        """

        try:
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            if response.status_code == 200:
                results = response.json()
                
                # Debug print to see the raw response structure
                #print("Raw response:", results)
                
                if results and len(results) > 0:
                    # The result might be a JSON string that needs parsing
                    if isinstance(results[0].get("result"), str):
                        # Parse the JSON string if needed
                        team_data = json.loads(results[0]["result"])
                    else:
                        team_data = results[0].get("result", [])
                    
                    # Ensure we have a list of dictionaries
                    if isinstance(team_data, list):
                        return [LeagueStats(**entry) for entry in team_data]
                    elif isinstance(team_data, dict):
                        return [LeagueStats(**team_data)]
                
                return []
            else:
                raise HTTPException(status_code=response.status_code, detail=response.text)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=500, detail=f"JSON decode error: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")








            















