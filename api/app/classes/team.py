from fastapi import HTTPException
from typing import List
from pydantic import BaseModel
from supabase import Client
import requests
from typing import Optional

# Define a Pydantic model
class HighestStats(BaseModel):
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

class TeamService:
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client
        self.key = supabase_client.supabase_key
        self.url = f"{supabase_client.rest_url}/rpc/execute_sql"
        self.headers = {
            "Authorization": f"Bearer {self.key}",
            "apikey": self.key,
            "Content-Type": "application/json"
        }

    def most_stats_by_team(self, team_id: int, stat: str, season: int):
        query = f"""
        SELECT json_agg(p) 
        FROM (
            SELECT 
                pl.player_name,
                pl.player_id,
                pl.age,
                pl.position,
                ps.team_id,
                t.team_name,
                t.logo_url AS team_logo,
                pl.nation1_id,
                pl.nation2_id,
                n1.team_name AS nation1,
                n2.team_name AS nation2,
                n1.logo_url AS nation1_url,
                n2.logo_url AS nation2_url,
                ps.goals,
                ps.assists,
                ps.minutes,
                ps.ga,
                ps.gp
            FROM 
                (SELECT 
                    ps.player_id,
                    ps.team_id,
                    ps.goals,
                    ps.minutes,
                    ps.assists,
                    ps.ga,
                    ps.gp
                FROM player_stats ps
                JOIN players p ON ps.player_id = p.player_id
                JOIN teams tm ON ps.team_id = tm.team_id
                
                WHERE ps.season_year = {season} and ps.team_id = {team_id} and ps.comp_id = tm.league_id
                ORDER BY ps.{stat} DESC
                LIMIT 10) ps
            JOIN players pl ON ps.player_id = pl.player_id
            LEFT JOIN teams t ON ps.team_id = t.team_id
           
            LEFT JOIN teams n1 ON pl.nation1_id = n1.team_id
            LEFT JOIN teams n2 ON pl.nation2_id = n2.team_id
            ORDER BY ps.{stat} DESC
        ) p;
        """

        try:
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            if response.status_code == 200:
                results = response.json()
            if results and isinstance(results[0].get("result"), list):
                player_list = results[0]["result"]
                data = [HighestStats(**entry) for entry in player_list]
                return data
            else:
                return []
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    

