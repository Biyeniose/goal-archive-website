from fastapi import HTTPException
from typing import List
from pydantic import BaseModel
from supabase import Client
import requests
from typing import Optional

class LeagueRanking(BaseModel):
    league_name: str

class StatsRanking(BaseModel):
    player_id: int
    player_name: str
    position: Optional[str] = None
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
    goals: Optional[int] = None
    assists: Optional[int] = None
    ga: Optional[int] = None
    gp: Optional[int] = None

class LeagueRankings(BaseModel):
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

class LeagueService:
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client
        self.key = supabase_client.supabase_key
        self.url = f"{supabase_client.rest_url}/rpc/execute_sql"
        self.headers = {
            "Authorization": f"Bearer {self.key}",
            "apikey": self.key,
            "Content-Type": "application/json"
        }


    def most_stats_league(self, league_id: int, season: int, stat: str, age: int):
        query = f"""
        SELECT json_agg(p) 
        FROM (
            SELECT 
                pl.player_name,
                pl.player_id,
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
                ps.age,
                ps.assists,
                ps.minutes,
                ps.ga,
                ps.gp
            FROM 
                (SELECT 
                    ps.player_id,
                    ps.team_id,
                    ps.goals,
                    ps.age,
                    ps.minutes,
                    ps.assists,
                    ps.ga,
                    ps.gp
                FROM player_stats ps
                JOIN players p ON ps.player_id = p.player_id AND ps.age <= {age}
                JOIN teams tm ON ps.team_id = tm.team_id
                WHERE ps.season_year = {season} AND ps.comp_id = {league_id}
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
            
            if response.status_code != 200:
                raise HTTPException(status_code=response.status_code, detail=response.text)

            results = response.json()  # Moved inside try block

            if results and isinstance(results[0].get("result"), list):
                player_list = results[0]["result"]
                data = [StatsRanking(**entry) for entry in player_list]
                return data

            return []

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
   
    def most_stats_league_all(self, league_id: int, season: int, stat: str):
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
                {"JOIN teams tm ON ps.team_id = tm.team_id AND tm.league_id IN (1,2,3,4,5,7,8,9,10)" if league_id == 9999 else f"JOIN teams tm ON ps.team_id = tm.team_id AND tm.league_id IN ({league_id})"}
                WHERE ps.season_year = {season}
                ORDER BY ps.{stat} DESC
                LIMIT 10) ps
            JOIN players pl ON ps.player_id = pl.player_id
            {"LEFT JOIN teams t ON ps.team_id = t.team_id AND t.league_id IN (1,2,3,4,5,7,8,9,10)" if league_id == 9999 else f"LEFT JOIN teams t ON ps.team_id = t.team_id AND t.league_id IN ({league_id})"}
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
                data = [StatsRanking(**entry) for entry in player_list]
                return data
            else:
                return []
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
   


    def most_goals_by_league(self, league_id: int, season: int):
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
                {"JOIN teams tm ON ps.team_id = tm.team_id AND tm.league_id IN (1,2,3,4,5,7,8,9,10)" if league_id == 9999 else f"JOIN teams tm ON ps.team_id = tm.team_id AND tm.league_id IN ({league_id})"}
                WHERE ps.season_year = {season}
                ORDER BY ps.goals DESC
                LIMIT 5) ps
            JOIN players pl ON ps.player_id = pl.player_id
            {"LEFT JOIN teams t ON ps.team_id = t.team_id AND t.league_id IN (1,2,3,4,5,7,8,9,10)" if league_id == 9999 else f"LEFT JOIN teams t ON ps.team_id = t.team_id AND t.league_id IN ({league_id})"}
            LEFT JOIN teams n1 ON pl.nation1_id = n1.team_id
            LEFT JOIN teams n2 ON pl.nation2_id = n2.team_id
            ORDER BY ps.goals DESC
        ) p;
        """

        try:
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            if response.status_code == 200:
                results = response.json()
            if results and isinstance(results[0].get("result"), list):
                player_list = results[0]["result"]
                data = [StatsRanking(**entry) for entry in player_list]
                return data
            else:
                return []
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    def most_assists_by_league(self, league_id: int, season: int):
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
                {"JOIN teams tm ON ps.team_id = tm.team_id AND tm.league_id IN (1,2,3,4,5,7,8,9,10)" if league_id == 9999 else f"JOIN teams tm ON ps.team_id = tm.team_id AND tm.league_id IN ({league_id})"}
                WHERE ps.season_year = {season}
                ORDER BY ps.assists DESC
                LIMIT 10) ps
            JOIN players pl ON ps.player_id = pl.player_id
            {"LEFT JOIN teams t ON ps.team_id = t.team_id AND t.league_id IN (1,2,3,4,5,7,8,9,10)" if league_id == 9999 else f"LEFT JOIN teams t ON ps.team_id = t.team_id AND t.league_id IN ({league_id})"}
            LEFT JOIN teams n1 ON pl.nation1_id = n1.team_id
            LEFT JOIN teams n2 ON pl.nation2_id = n2.team_id
            ORDER BY ps.assists DESC
        ) p;
        """

        try:
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            if response.status_code == 200:
                results = response.json()
            if results and isinstance(results[0].get("result"), list):
                player_list = results[0]["result"]
                data = [StatsRanking(**entry) for entry in player_list]
                return data
            else:
                return []
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    def most_min_by_league(self, league_id: int, season: int):
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
                {"JOIN teams tm ON ps.team_id = tm.team_id AND tm.league_id IN (1,2,3,4,5,7,8,9,10)" if league_id == 9999 else f"JOIN teams tm ON ps.team_id = tm.team_id AND tm.league_id IN ({league_id})"}
                WHERE ps.season_year = {season}
                ORDER BY ps.minutes DESC
                LIMIT 5) ps
            JOIN players pl ON ps.player_id = pl.player_id
            {"LEFT JOIN teams t ON ps.team_id = t.team_id AND t.league_id IN (1,2,3,4,5,7,8,9,10)" if league_id == 9999 else f"LEFT JOIN teams t ON ps.team_id = t.team_id AND t.league_id IN ({league_id})"}
            LEFT JOIN teams n1 ON pl.nation1_id = n1.team_id
            LEFT JOIN teams n2 ON pl.nation2_id = n2.team_id
            ORDER BY ps.minutes DESC
        ) p;
        """

        try:
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            if response.status_code == 200:
                results = response.json()
            if results and isinstance(results[0].get("result"), list):
                player_list = results[0]["result"]
                data = [StatsRanking(**entry) for entry in player_list]
                return data
            else:
                return []
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    

    def get_league_rankings(self):
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
                WHERE wins =0
                ORDER BY goals_f ASC
                LIMIT 20) lr
            JOIN teams t ON lr.team_id = t.team_id
            ORDER BY lr.goals_f ASC
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



